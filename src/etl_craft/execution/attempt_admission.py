"""Validate and queue a task attempt from its admitted execution definition."""

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import current_actor
from etl_craft.core.enums import AttemptStatus, Handler
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.faults import fault_point
from etl_craft.engine import transitions
from etl_craft.engine.queries import statement
from etl_craft.engine.repository import trackers
from etl_craft.engine.repository.tasks import fetch_task_execution_detail, fetch_task_parameters
from etl_craft.engine.run_definitions import for_run
from etl_craft.execution import leases
from etl_craft.execution.attempt_types import TaskRun
from etl_craft.execution.limits import task_timeout_seconds
from etl_craft.execution.placement import check_admission
from etl_craft.execution.pools import AttemptSpec
from etl_craft.execution.retries import task_retry_policy


def admit_attempt(
    engine: Engine, config: ConnectorConfig, task: TaskRun, *, rerun: bool = False
) -> AttemptSpec:
    """Check the task's settings, then bind its task run and queue an attempt to run.

    Whatever can be checked before binding is checked first, so a bad setting fails without
    touching the task's row. An attempt an earlier admission queued and nobody claimed is reused.
    The gate decisions that admitted the task are recorded with the attempt.
    """
    from etl_craft.execution.bundle_files import run_config

    with for_run(engine, task.pipeline_run_id):
        config = run_config(engine, config, task.pipeline_run_id)
        if config.config_path is None:
            raise ConfigurationError(
                "a task process needs the craft-connector.yml it was loaded from"
            )
        with engine.connect() as conn:
            params = fetch_task_parameters(conn, task.task_id)
            detail = fetch_task_execution_detail(conn, task.task_id)
            handler = detail.handler
        reason = check_admission(config, detail, params)
        timeout = task_timeout_seconds(params, config)
        policy = task_retry_policy(params, config, handler)
        fault_point("runner.after_timeout")
        with engine.begin() as conn:
            binding = transitions.find_or_create_task_run(conn, task.task_id, task.pipeline_run_id)
            active = transitions.active_attempt(conn, binding.task_run_id)
            if active is not None and active.status == AttemptStatus.QUEUED:
                attempt_id = active.attempt_id
                transitions.set_attempt_retry_policy(
                    conn, attempt_id, policy.retry_on, overwrite=False
                )
            else:
                if rerun and handler == Handler.RUN_PIPELINE:
                    conn.execute(
                        statement(conn, "trigger_generation"), {"task_run_id": binding.task_run_id}
                    )
                    conn.execute(
                        statement(conn, "trigger_wait_reset"),
                        {"pipeline_run_id": task.pipeline_run_id, "task_id": task.task_id},
                    )
                attempt_id = transitions.queue_attempt(conn, binding.task_run_id, current_actor())
                transitions.set_attempt_retry_policy(conn, attempt_id, policy.retry_on)
            if active is None or active.status != AttemptStatus.QUEUED:
                from etl_craft.engine.dispatch import dispatch_options
                from etl_craft.engine.resource_locks import notify

                dispatch_options(
                    conn, attempt_id, handler, reason, options={"force": task.force, "rerun": rerun}
                )
                notify(conn)
            trackers.record_decisions(
                conn, task.pipeline_run_id, task.decisions, attempt_id=attempt_id
            )
            active = transitions.active_attempt(conn, binding.task_run_id)
            assert active is not None
        return AttemptSpec(
            attempt_id=attempt_id,
            attempt_number=active.attempt_number,
            task_run_id=binding.task_run_id,
            pipeline_run_id=task.pipeline_run_id,
            pipeline_code=task.pipeline_code,
            task_code=task.task_code,
            handler=handler,
            timeout_seconds=timeout,
            lease_seconds=leases.LEASE_SECONDS,
            force=task.force,
            rerun=rerun,
            pool=detail.pool,
        )
