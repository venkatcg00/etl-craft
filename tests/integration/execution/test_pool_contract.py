"""Shared pool guarantees use the same oracle for local and API-only task processes."""

import signal

import pytest
from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import AttemptStatus, RunStatus
from etl_craft.core.errors import StaleTransitionError
from etl_craft.engine import transitions
from etl_craft.execution.leases import process_start
from etl_craft.execution.pools import ExecutionHandle, HandleState
from fixtures.pool_contract import until
from fixtures.races import two_at_once

pytestmark = [
    pytest.mark.cluster,
    pytest.mark.parametrize(
        "empty_engine_db",
        [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
        indirect=True,
    ),
]


def test_duplicate_submission_starts_one_process(provider):
    spec = provider.admit()
    first, second = two_at_once(
        lambda: provider.pool.submit(spec),
        lambda: provider.pool.submit(spec),
        at=(provider.pool, "submit"),
    )
    assert first == second
    if provider.queued:
        provider.start_agent()
    until(lambda: bool(provider.evidence))
    assert provider.row(spec.attempt_id).status == AttemptStatus.RUNNING
    provider.release()
    result = until(lambda: ended(provider, first))
    assert result.outcome.status == RunStatus.SUCCESS
    assert len(provider.evidence) == 1
    assert provider.row(spec.attempt_id).status == AttemptStatus.SUCCESS


def ended(provider, handle):
    result = provider.pool.status(handle)
    assert result.error is None, result.error
    return result if result.state == HandleState.ENDED else None


def test_foreign_handles_cannot_observe_or_cancel_an_admitted_attempt(provider):
    spec = provider.admit()
    foreign = ExecutionHandle("unrelated-provider", spec.attempt_id)
    assert provider.pool.status(foreign).state == HandleState.UNKNOWN
    provider.pool.cancel(foreign, 1)
    assert provider.row(spec.attempt_id).status == AttemptStatus.QUEUED
    assert provider.evidence == []


def test_cancellation_stops_the_exact_started_process(provider):
    spec = provider.admit()
    handle = provider.submit(spec)
    before = provider.row(spec.attempt_id)
    provider.pool.cancel(handle, 1)
    result = until(lambda: ended(provider, handle))
    assert result.outcome.status in {RunStatus.FAILED, RunStatus.CANCELLED}
    row = provider.row(spec.attempt_id)
    assert row.status in {AttemptStatus.FAILED, AttemptStatus.CANCELLED}
    assert row.pid == before.pid
    assert len(provider.evidence) == 1
    if provider.queued:
        assert row.acknowledged is not None


@pytest.mark.chaos
def test_a_dead_worker_is_lost_before_another_worker_retries(provider):
    spec = provider.admit()
    if provider.queued:
        provider.submit(spec)
        worker = provider.agent
    else:
        worker = provider.start_local_worker(spec)
        until(lambda: bool(provider.evidence))
    old = provider.row(spec.attempt_id)
    worker.descendants()
    worker.signal(signal.SIGKILL)
    assert worker.wait() == -signal.SIGKILL
    if provider.queued:
        until(lambda: process_start(old.pid) != old.birth)
        provider.agent = None
    report = provider.expire(spec)
    assert spec.attempt_id in report.lost
    assert provider.row(spec.attempt_id).status == AttemptStatus.LOST
    retry = provider.retry(spec)
    assert retry.task_run_id == spec.task_run_id
    assert retry.attempt_id != spec.attempt_id
    handle = provider.submit(retry)
    provider.release()
    assert until(lambda: ended(provider, handle)).outcome.status == RunStatus.SUCCESS
    assert len(provider.evidence) == 2
    assert provider.row(retry.attempt_id).pid != old.pid


@pytest.mark.chaos
def test_a_late_success_from_a_lost_worker_is_fenced(provider):
    spec = provider.admit()
    if provider.queued:
        provider.submit(spec)
        worker = provider.agent
    else:
        worker = provider.start_local_worker(spec)
        until(lambda: bool(provider.evidence))
    old = provider.row(spec.attempt_id)
    worker.descendants()
    worker.signal(signal.SIGKILL)
    worker.wait()
    if provider.queued:
        until(lambda: process_start(old.pid) != old.birth)
    provider.expire(spec)
    with provider.project.engine.begin() as conn, pytest.raises(StaleTransitionError):
        transitions.finish_attempt(
            conn, spec.attempt_id, AttemptStatus.SUCCESS, SYSTEM_ACTOR, owner=old.owner_id
        )
    assert provider.row(spec.attempt_id).status == AttemptStatus.LOST
    assert len(provider.evidence) == 1


@pytest.mark.chaos
def test_controller_restart_preserves_the_run_and_does_not_duplicate_live_work(provider):
    if provider.queued:
        spec = provider.admit()
        provider.submit(spec)
    else:
        code, output = provider.project.run(
            "run", "--pipeline-code", "P", "--init-only", "--run-key", "contract"
        )
        assert code == 0, output
    controller = provider.project.start("controller", "--no-ui")
    until(lambda: bool(provider.evidence))
    if not provider.queued:
        with provider.project.engine.connect() as conn:
            attempt_id = conn.execute(
                text("SELECT ATTEMPT_ID AS attempt_id FROM AUD_TASK_ATTEMPTS")
            ).scalar_one()
        spec = provider.row(attempt_id)
    provider.project.wait_for(
        "SELECT COUNT(*) AS leased FROM AUD_PIPELINES_RUN_LOG WHERE OWNER_ID IS NOT NULL",
        expected=1,
    )
    old = provider.row(spec.attempt_id)
    controller.descendants()
    controller.signal(signal.SIGKILL)
    assert controller.wait() == -signal.SIGKILL
    if provider.queued:
        assert process_start(old.pid) == old.birth
        from datetime import UTC, datetime, timedelta

        with provider.project.engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE AUD_PIPELINES_RUN_LOG SET LEASE_EXPIRES_AT=:past "
                    "WHERE PIPELINE_RUN_ID=:id"
                ),
                {"past": datetime.now(UTC) - timedelta(seconds=1), "id": spec.pipeline_run_id},
            )
    else:
        provider.expire(spec)
    replacement = provider.project.start("controller", "--no-ui")
    if not provider.queued:
        until(lambda: len(provider.evidence) == 2)
    provider.release()
    provider.project.wait_for(
        "SELECT STATUS AS status FROM AUD_PIPELINES_RUN_LOG",
        expected=RunStatus.SUCCESS,
    )
    replacement.signal(signal.SIGTERM)
    assert replacement.wait() == 0, replacement.output
    with provider.project.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT STATUS AS status,TASK_RUN_ID AS task_run_id FROM AUD_TASK_ATTEMPTS "
                "ORDER BY ATTEMPT_NUMBER"
            )
        ).all()
    assert [row.status for row in rows] == (
        [AttemptStatus.SUCCESS] if provider.queued else [AttemptStatus.LOST, AttemptStatus.SUCCESS]
    )
    assert all(row.task_run_id == spec.task_run_id for row in rows)
    assert len(provider.evidence) == (1 if provider.queued else 2)


@pytest.mark.chaos
def test_claim_acknowledgement_loss_is_reconciled_before_retry(provider):
    import httpx2 as httpx

    from etl_craft import __version__
    from fixtures.cluster import start_ui

    spec = provider.admit()
    if provider.queued:
        _, headers = provider.register()
        broken, url = start_ui(provider.project, fault="agent.after_claim:kill")
        with pytest.raises(httpx.TransportError):
            httpx.post(
                url + "/api/v1/agent/claim",
                headers=headers,
                json={"version": __version__, "free_kinds": ["python"]},
                timeout=5,
            )
        assert broken.wait() == 137, broken.output
        assert httpx.get(provider.url + "/health").status_code == 200
    else:
        worker = provider.start_local_worker(spec, fault="runner.after_bind:kill")
        assert worker.wait() == 137, worker.output
    old = provider.row(spec.attempt_id)
    assert old.status == AttemptStatus.CLAIMED
    assert old.pid is None
    assert provider.evidence == []
    assert provider.expire(spec).lost == [spec.attempt_id]
    retry = provider.retry(spec)
    handle = provider.submit(retry)
    provider.release()
    assert until(lambda: ended(provider, handle)).outcome.status == RunStatus.SUCCESS
    assert retry.task_run_id == spec.task_run_id
    assert len(provider.evidence) == 1


def test_an_incompatible_agent_version_cannot_register(queue_provider):
    import httpx2 as httpx

    from etl_craft import __version__
    from etl_craft.services.operations.serialization import encode
    from fixtures.agents import resources

    response = httpx.post(
        queue_provider.url + "/api/v1/agent/register",
        headers={"Authorization": f"Bearer {queue_provider.token.token}"},
        json={"version": "wrong-version", "pool": "default", "resources": encode(resources())},
    )
    assert response.status_code == 409
    assert "wrong-version" in response.text
    assert __version__ in response.text
    with queue_provider.project.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) AS agents FROM AUD_AGENTS")).scalar_one() == 0
    assert queue_provider.evidence == []


def test_capacity_keeps_excess_work_queued(provider):
    from etl_craft import __version__
    from etl_craft.core.actor import acting_as
    from etl_craft.core.enums import SlotKind
    from etl_craft.execution.attempt_types import TaskRun
    from etl_craft.execution.runner import admit_attempt
    from etl_craft.services.operations.agent_claims import claim_once
    from etl_craft.services.operations.agent_models import AgentClaim
    from fixtures.metadata import add_task

    with provider.project.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PIPELINES SET MAX_PARALLEL_TASKS=1 WHERE PIPELINE_ID=:id"),
            {"id": provider.project.pipeline_id},
        )
        second_task = add_task(
            conn,
            provider.project.pipeline_id,
            "second",
            "PYTHON",
            SCRIPT_FILE="python_scripts/load.py",
        )
    provider.prepare()
    first = provider.admit()
    first_handle = provider.submit(first)
    second = admit_attempt(
        provider.project.engine,
        provider.project.config,
        TaskRun("P", "second", second_task, first.pipeline_run_id, False),
    )
    second_handle = provider.pool.submit(second)
    if provider.queued:
        ctx, _ = provider.register(name="spare-agent")
        with acting_as(ctx.actor):
            assert (
                claim_once(
                    ctx,
                    AgentClaim(version=__version__, free_kinds=(SlotKind.PYTHON,)),
                    ctx.credential_token_id,
                )
                is None
            )
    else:
        until(lambda: bool(provider.row(second.attempt_id).reason))
    queued = provider.row(second.attempt_id)
    assert queued.status == AttemptStatus.QUEUED
    assert "MAX_PARALLEL_TASKS=1" in queued.reason or "free python slot" in queued.reason
    assert len(provider.evidence) == 1
    provider.release()
    assert until(lambda: ended(provider, first_handle)).outcome.status == RunStatus.SUCCESS
    assert until(lambda: ended(provider, second_handle)).outcome.status == RunStatus.SUCCESS
    assert len(provider.evidence) == 2


def test_agent_drain_finishes_owned_work_and_takes_no_new_attempt(queue_provider):
    from etl_craft.core.enums import ActionName, AgentCommand, AgentStatus, ApiRole
    from etl_craft.execution.attempt_types import TaskRun
    from etl_craft.execution.runner import admit_attempt
    from etl_craft.services.operations import OperationContext
    from etl_craft.services.operations.actions import invoke
    from etl_craft.services.operations.agent_models import AgentControl
    from fixtures.metadata import add_task

    provider = queue_provider
    with provider.project.engine.begin() as conn:
        second_task = add_task(
            conn,
            provider.project.pipeline_id,
            "second",
            "PYTHON",
            SCRIPT_FILE="python_scripts/load.py",
        )
    provider.prepare()
    first = provider.admit()
    first_handle = provider.submit(first)
    admin = OperationContext(
        provider.project.engine, provider.project.config, SYSTEM_ACTOR, role=ApiRole.ADMIN
    )
    owner = provider.token.metadata.token_id
    invoke(
        admin, ActionName.AGENT_CONTROL, AgentControl(agent_id=owner, command=AgentCommand.DRAIN)
    )
    provider.project.wait_for(
        "SELECT STATUS AS status FROM AUD_AGENTS WHERE AGENT_ID=" + str(owner),
        expected=AgentStatus.DRAINING,
    )
    second = admit_attempt(
        provider.project.engine,
        provider.project.config,
        TaskRun("P", "second", second_task, first.pipeline_run_id, False),
    )
    second_handle = provider.pool.submit(second)
    provider.release()
    assert until(lambda: ended(provider, first_handle)).outcome.status == RunStatus.SUCCESS
    assert provider.row(second.attempt_id).status == AttemptStatus.QUEUED
    assert len(provider.evidence) == 1
    invoke(
        admin, ActionName.AGENT_CONTROL, AgentControl(agent_id=owner, command=AgentCommand.RESUME)
    )
    assert until(lambda: ended(provider, second_handle)).outcome.status == RunStatus.SUCCESS
    assert len(provider.evidence) == 2
