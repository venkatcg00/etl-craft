"""Pipeline admission shared by explicit initialization and pipeline trigger tasks."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import ClassVar

from sqlalchemy.engine import Connection, Row

from etl_craft.core.actor import Actor, ActorKind, current_actor
from etl_craft.core.definitions import Definition
from etl_craft.core.enums import (
    AttemptStatus,
    Handler,
    RunStatus,
    TriggerKind,
    TriggerMode,
)
from etl_craft.core.errors import MetadataError, PermissionDeniedError, RunRefusedError, UsageError
from etl_craft.core.faults import fault_point
from etl_craft.engine import controls, transitions
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pauses import fetch_open_pause
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import fetch_admitted_parameters
from etl_craft.execution import run_start
from etl_craft.execution.run_types import PipelineOutcome
from etl_craft.handlers.run_pipeline import Trigger
from etl_craft.services.cloning import run_hooks
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.log_storage import ensure_source
from etl_craft.services.operations.requests import Request, RunRequest
from etl_craft.services.trigger_validation import trigger_definition, trigger_graph_problems


@dataclass(frozen=True)
class TriggerView(PipelineOutcome):
    """A child admission and the triggering task's resulting state."""

    SCHEMA: ClassVar[str] = "etl-craft/trigger/1"


@dataclass(frozen=True, kw_only=True)
class TriggerRequest(Request):
    """The exact owned attempt asking to admit a child."""

    task_run_id: int
    attempt_id: int | None
    owner: str | None

    def __post_init__(self) -> None:
        """Refuse a request that cannot identify an owned attempt."""
        if self.task_run_id < 1 or not self.attempt_id or not self.owner:
            raise UsageError(
                f"trigger task_run_id={self.task_run_id}, attempt_id={self.attempt_id}, "
                f"owner={self.owner!r}; expected an owned attempt; dispatch the trigger as a task"
            )


def initialize(ctx: OperationContext, request: RunRequest | TriggerRequest) -> PipelineOutcome:
    """Admit for the controller, preserving explicit run identities and initiating actors."""
    if isinstance(request, RunRequest):
        return run_start.init_pipeline_run(
            ctx.engine,
            ctx.config,
            request.pipeline_code,
            run_date=request.run_date,
            selector=request.selector,
            hooks=run_hooks(ctx.config, ctx.engine),
        )
    assert request.attempt_id is not None and request.owner is not None
    with ctx.engine.begin() as conn:
        row = _trigger_row(conn, request.task_run_id)
        if ctx.actor != Actor(row.started_by, ActorKind(row.started_by_kind)):
            raise PermissionDeniedError(
                f"trigger task_run_id={request.task_run_id}: actor={ctx.actor.name!r}; "
                f"expected parent initiator {row.started_by!r}; dispatch from the parent run"
            )
        controls.assert_dispatch(conn, row.pipeline_run_id, row.task_id)
        transitions.assert_attempt(conn, request.attempt_id, request.task_run_id, request.owner)
        trigger = trigger_definition(
            conn, row.pipeline_id, fetch_admitted_parameters(conn, row.task_id, row.pipeline_run_id)
        )
        problems = trigger_graph_problems(conn, (row.pipeline_code, trigger.pipeline_code))
        if problems:
            raise MetadataError("; ".join(problem for _, problem in problems))
        child_id = resolve_pipeline_id(conn, trigger.pipeline_code)
        key = f"trigger:{request.task_run_id}:{row.trigger_generation}"
        existing = conn.execute(
            statement(conn, "trigger_run"), {"task_run_id": request.task_run_id, "run_key": key}
        ).one_or_none()
        if existing is None:
            pause = fetch_open_pause(conn, child_id)
            if pause is not None:
                raise RunRefusedError(
                    f"child pipeline {trigger.pipeline_code}: {pause.describe()}; "
                    "expected an unpaused pipeline; resume it or mark the trigger task"
                )
            run_id = transitions.create_run(
                conn,
                child_id,
                current_actor(),
                run_date=row.run_date,
                trigger_kind=TriggerKind.PIPELINE_TASK,
                run_key=key,
                status=RunStatus.QUEUED,
            )
            conn.execute(
                statement(conn, "trigger_link"),
                {"run_id": run_id, "task_run_id": request.task_run_id},
            )
        else:
            if existing.pipeline_id != child_id:
                raise MetadataError(
                    f"trigger task_run_id={request.task_run_id}: "
                    f"child pipeline_id={existing.pipeline_id}; "
                    f"target now resolves to {child_id}; restore the target "
                    "or explicitly rerun the trigger"
                )
            run_id = existing.pipeline_run_id
        fault_point("trigger.after_child")
        message = f"child pipeline_run_id={run_id}; mode={trigger.mode}"
        _end_trigger(conn, request, trigger, run_id)
    return TriggerView(
        RunStatus.IN_PROGRESS if trigger.mode == TriggerMode.WAIT else RunStatus.SUCCESS,
        message,
        run_id,
    )


def _trigger_row(conn: Connection, task_run_id: int) -> Row[tuple[object, ...]]:
    """Require the parent run's admitted trigger handler before authorizing child admission."""
    row = conn.execute(
        statement(conn, "trigger_context"), {"task_run_id": task_run_id}
    ).one_or_none()
    found = (
        None
        if row is None
        else Definition.read(row.metadata_snapshot).row("CFG_TASKS", "task_id", row.task_id)[
            "handler"
        ]
    )
    if row is None or found != Handler.RUN_PIPELINE:
        raise UsageError(
            f"trigger task_run_id={task_run_id}: found {found!r}; "
            "expected an existing RUN_PIPELINE ETL task run; dispatch that task's owned attempt"
        )
    return row


def _end_trigger(conn: Connection, request: TriggerRequest, trigger: Trigger, run_id: int) -> None:
    """Commit the child reference, handoff budget and attempt ending together."""
    assert request.attempt_id is not None and request.owner is not None
    ensure_source(conn, request.attempt_id)
    now = datetime.now(UTC)
    row = conn.execute(
        statement(conn, "trigger_context"), {"task_run_id": request.task_run_id}
    ).one()
    message = f"child pipeline_run_id={run_id}; mode={trigger.mode}"
    if trigger.mode == TriggerMode.WAIT:
        deadline = (
            None
            if trigger.timeout_minutes is None
            else now + timedelta(minutes=trigger.timeout_minutes)
        )
        conn.execute(
            statement(conn, "trigger_wait_create"),
            {
                "pipeline_run_id": row.pipeline_run_id,
                "task_id": row.task_id,
                "now": now,
                "deadline": deadline,
                "observations": json.dumps({"child_run_id": run_id}),
            },
        )
        transitions.finish_attempt(
            conn,
            request.attempt_id,
            AttemptStatus.HANDED_OFF,
            current_actor(),
            owner=request.owner,
            task_log=message,
        )
    else:
        transitions.finish_task_run(
            conn,
            request.task_run_id,
            status=RunStatus.SUCCESS,
            task_log=message,
            attempt_id=request.attempt_id,
            owner=request.owner,
        )
