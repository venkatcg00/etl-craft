"""Recovery, durable restart, capabilities and explicit bulk selection contracts."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from etl_craft.cli import main
from etl_craft.core.actor import current_actor
from etl_craft.core.enums import ActionName, ApiRole, ControlAction, ControlStatus, RunStatus
from etl_craft.core.errors import RunRefusedError, UsageError
from etl_craft.engine import controls, run_scopes, runlog, transitions
from etl_craft.engine.repository.offsets import StoredOffset, fetch_task_offset, save_task_offset
from etl_craft.engine.repository.pauses import fetch_open_pause
from etl_craft.engine.runlog import RunSelector
from etl_craft.execution import pipeline
from etl_craft.execution.interventions import pause_pipeline
from etl_craft.services.operations import from_json, to_json
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.controls import ControlRequest
from fixtures.actions import act
from fixtures.metadata import (
    add_dependency,
    add_pipeline,
    add_task,
    finish_run,
    start_run,
    task_run,
)
from fixtures.races import two_at_once
from fixtures.scoped_controls import ctx as ctx


def scene(ctx, status=RunStatus.FAILED, handler="PYTHON"):
    with ctx.engine.begin() as conn:
        p = add_pipeline(conn, "P")
        a = add_task(conn, p, "a", handler, BEHAVIOUR="succeed")
        b = add_task(conn, p, "b", handler, BEHAVIOUR="succeed")
        run = start_run(conn, p)
        task_run(conn, a, run, RunStatus.SUCCESS, rows_written=7)
        task_run(conn, b, run, RunStatus.FAILED)
        if status != RunStatus.IN_PROGRESS:
            finish_run(conn, run, status)
    return p, run, a, b


def request(run, action, task=None, key=None):
    return ControlRequest(
        pipeline_code="P",
        action=action,
        reason="operator repair",
        run_id=run,
        task_code=task,
        request_key=key,
    )


def test_recovery_resumes_same_run_and_keeps_success_checkpoint_and_repair_revision(ctx):
    _, run, a, b = scene(ctx)
    with ctx.engine.begin() as conn:
        save_task_offset(conn, b, StoredOffset("NUMBER", "42"))
    prepared = invoke(ctx, ActionName.CONTROL, request(run, ControlAction.RECOVER, key="recover"))
    assert prepared.execution_run_id == run
    assert prepared.status == ControlStatus.COMPLETED
    done = pipeline.run_pipeline(
        ctx.engine, ctx.config, "P", selector=RunSelector(run_id=run), child=ctx.child
    )
    assert done.status == RunStatus.SUCCESS
    with ctx.engine.connect() as conn:
        assert fetch_task_offset(conn, b).offset_value == "42"
        assert runlog.fetch_task_run_status(conn, a, run) == RunStatus.SUCCESS
        assert (
            conn.execute(
                text(
                    "SELECT OUTPUT_REVISION AS revision FROM AUD_PIPELINES_RUN_LOG "
                    "WHERE PIPELINE_RUN_ID=:r"
                ),
                {"r": run},
            ).scalar_one()
            == 2
        )
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS n FROM AUD_TASK_ATTEMPTS t JOIN AUD_TASK_RUN_LOG s "
                    "ON s.TASK_RUN_ID=t.TASK_RUN_ID WHERE s.TASK_ID=:a"
                ),
                {"a": a},
            ).scalar_one()
            == 0
        )
    assert (
        invoke(
            ctx, ActionName.CONTROL, request(run, ControlAction.RECOVER, key="recover")
        ).control_id
        == prepared.control_id
    )
    assert (
        act(ctx, ActionName.CONTROL_REQUEST, control_id=prepared.control_id).run_status
        == RunStatus.SUCCESS
    )


def test_restart_keeps_source_and_shared_checkpoint_and_has_durable_new_identity(ctx):
    _, run, _, b = scene(ctx)
    with ctx.engine.begin() as conn:
        save_task_offset(conn, b, StoredOffset("NUMBER", "101"))
    first = invoke(ctx, ActionName.CONTROL, request(run, ControlAction.RESTART, key="restart"))
    assert first.pipeline_run_id == run and first.execution_run_id != run
    assert (
        invoke(
            ctx, ActionName.CONTROL, request(run, ControlAction.RESTART, key="restart")
        ).execution_run_id
        == first.execution_run_id
    )
    with ctx.engine.connect() as conn:
        selected = runlog.select_run(
            conn, first.pipeline_id, RunSelector(run_id=first.execution_run_id)
        )
        assert selected.status == RunStatus.QUEUED
        assert selected.run_key == f"control-restart:{first.control_id}"
        assert (
            selected.run_date
            == runlog.select_run(conn, first.pipeline_id, RunSelector(run_id=run)).run_date
        )
        assert runlog.fetch_task_run_status(conn, b, run) == RunStatus.FAILED
        assert fetch_task_offset(conn, b).offset_value == "101"
    assert from_json(type(first), to_json(first)) == first


def test_concurrent_restart_clicks_create_only_one_new_context(ctx):
    _, run, _, _ = scene(ctx)
    results = two_at_once(
        lambda: invoke(ctx, ActionName.CONTROL, request(run, ControlAction.RESTART, key="same")),
        lambda: invoke(ctx, ActionName.CONTROL, request(run, ControlAction.RESTART, key="same")),
        at=(ctx.engine, "begin"),
    )
    assert results[0].control_id == results[1].control_id
    assert results[0].execution_run_id == results[1].execution_run_id
    with ctx.engine.connect() as conn:
        assert len(runlog.run_candidates(conn, results[0].pipeline_id)) == 2


def test_new_restart_key_requests_a_distinct_new_context(ctx):
    _, run, _, _ = scene(ctx)
    first = invoke(ctx, ActionName.CONTROL, request(run, ControlAction.RESTART, key="one"))
    second = invoke(ctx, ActionName.CONTROL, request(run, ControlAction.RESTART, key="two"))
    assert first.execution_run_id != second.execution_run_id


@pytest.mark.parametrize("action", [ControlAction.RECOVER, ControlAction.RESTART])
def test_recovery_and_restart_refuse_live_owners_without_touching_history(ctx, action):
    _, run, _, _ = scene(ctx, RunStatus.IN_PROGRESS)
    with ctx.engine.begin() as conn:
        transitions.start_run(
            conn,
            run,
            current_actor(),
            owner="live",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
    with pytest.raises(RunRefusedError, match=r"ended ownership.*owner='live'"):
        invoke(ctx, ActionName.CONTROL, request(run, action))
    with ctx.engine.connect() as conn:
        assert len(runlog.run_candidates(conn, 1)) == 1
        assert controls.blocked(conn, run) == {}


def test_pending_stop_is_not_released_by_recovery_or_pause_permission(ctx):
    _, run, _, b = scene(ctx, RunStatus.IN_PROGRESS)
    with ctx.engine.begin() as conn:
        binding = transitions.find_or_create_task_run(conn, b, run)
        attempt = transitions.queue_attempt(conn, binding.task_run_id, current_actor())
        transitions.claim_attempt(
            conn,
            attempt,
            current_actor(),
            owner="agent",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
    stop = invoke(ctx, ActionName.CONTROL, request(run, ControlAction.STOP))
    assert stop.status == ControlStatus.REQUESTED
    invoke(ctx, ActionName.CONTROL, request(run, ControlAction.RUN_PAUSED))
    with pytest.raises(RunRefusedError, match="ended ownership"):
        invoke(ctx, ActionName.CONTROL, request(run, ControlAction.RECOVER))
    with pytest.raises(RunRefusedError, match="pending controls"):
        invoke(ctx, ActionName.CONTROL, request(run, ControlAction.RELEASE))
    with ctx.engine.connect() as conn:
        assert controls.blocked(conn, run) == {None: ControlAction.STOP}


def test_task_restart_is_isolated_and_does_not_clear_source_stop(ctx):
    _, run, _, b = scene(ctx, RunStatus.IN_PROGRESS, "SQL")
    stopped = invoke(ctx, ActionName.CONTROL, request(run, ControlAction.STOP))
    assert stopped.run_status == RunStatus.CANCELLED
    restarted = invoke(
        ctx, ActionName.CONTROL, request(run, ControlAction.RESTART, "b", "task-restart")
    )
    with ctx.engine.connect() as conn:
        assert run_scopes.fetch_selection(conn, restarted.execution_run_id).task_ids == (b,)
        assert controls.blocked(conn, run) == {None: ControlAction.STOP}
        assert runlog.fetch_task_run_status(conn, b, run) == RunStatus.FAILED


def test_task_restart_refuses_unisolated_checkpoints_with_an_actionable_reason(ctx):
    _, run, _, _ = scene(ctx)
    with pytest.raises(RunRefusedError, match=r"independent execution.*checkpoint"):
        invoke(ctx, ActionName.CONTROL, request(run, ControlAction.RESTART, "b"))


def test_task_restart_does_not_bypass_dependencies(ctx):
    p, run, a, b = scene(ctx, handler="SQL")
    with ctx.engine.begin() as conn:
        add_dependency(conn, p, b, a)
    with pytest.raises(RunRefusedError, match="dependencies outside selection"):
        invoke(ctx, ActionName.CONTROL, request(run, ControlAction.RESTART, "b"))


def test_task_recovery_preserves_other_scoped_holds(ctx):
    _, run, _, _ = scene(ctx, RunStatus.IN_PROGRESS)
    invoke(ctx, ActionName.CONTROL, request(run, ControlAction.HOLD, "a"))
    invoke(ctx, ActionName.CONTROL, request(run, ControlAction.HOLD, "b"))
    invoke(ctx, ActionName.CONTROL, request(run, ControlAction.RECOVER, "b"))
    with ctx.engine.connect() as conn:
        assert set(controls.blocked(conn, run)) == {1}


def test_one_run_pause_permission_keeps_definition_pause_and_other_runs_blocked(ctx):
    p, source, _, _ = scene(ctx)
    restarted = invoke(
        ctx, ActionName.CONTROL, request(source, ControlAction.RESTART, key="paused-restart")
    )
    run = restarted.execution_run_id
    pause_pipeline(ctx.engine, ctx.config, "P", "schedule maintenance")
    invoke(ctx, ActionName.CONTROL, request(run, ControlAction.RUN_PAUSED, key="permission"))
    done = pipeline.run_pipeline(
        ctx.engine, ctx.config, "P", selector=RunSelector(run_id=run), child=ctx.child
    )
    assert done.status == RunStatus.SUCCESS
    assert (
        pipeline.run_pipeline(ctx.engine, ctx.config, "P", child=ctx.child).status
        == RunStatus.SKIPPED
    )
    with ctx.engine.connect() as conn:
        assert fetch_open_pause(conn, p) is not None
        assert not controls.permits_paused(conn, source)


@pytest.mark.parametrize("role", [ApiRole.VIEWER, ApiRole.OPERATOR])
def test_capability_reasons_match_control_policy_and_preserve_state(ctx, role):
    _, run, _, _ = scene(ctx)
    reader = replace(ctx, role=role)
    caps = act(reader, ActionName.CAPABILITIES, pipeline_code="P", run_id=run)
    by_action = {a.action: a for a in caps.actions}
    assert by_action[ControlAction.RESTART].available == (role == ApiRole.OPERATOR)
    assert by_action[ControlAction.RESTART].retains_checkpoints
    assert not by_action[ControlAction.RESTART].preserves_successful_work
    expected = "new run" if role == ApiRole.OPERATOR else "controls need the operator role"
    assert expected in by_action[ControlAction.RESTART].reason
    assert not by_action[ControlAction.HOLD].available
    with ctx.engine.connect() as conn:
        assert controls.blocked(conn, run) == {}
    assert from_json(type(caps), to_json(caps)) == caps


def test_bulk_is_selected_idempotent_and_returns_independent_refusals(ctx):
    _, run, a, b = scene(ctx, RunStatus.IN_PROGRESS)
    selected = (
        request(run, ControlAction.HOLD, "a", "a"),
        request(run, ControlAction.RESTART, "b", "b"),
    )
    batch = act(ctx, ActionName.CONTROL_BULK, requests=tuple(selected))
    assert batch.outcomes[0].control.task_id == a
    assert batch.outcomes[1].control is None and "terminal source" in batch.outcomes[1].error
    again = act(ctx, ActionName.CONTROL_BULK, requests=tuple(selected))
    assert again.outcomes[0].control.control_id == batch.outcomes[0].control.control_id
    with ctx.engine.connect() as conn:
        assert controls.blocked(conn, run) == {a: ControlAction.HOLD}
        assert runlog.fetch_task_run_status(conn, b, run) == RunStatus.FAILED
    assert from_json(type(batch), to_json(batch)) == batch


def test_bulk_rejects_implicit_identity_before_applying_any_object(ctx):
    _, run, _, _ = scene(ctx, RunStatus.IN_PROGRESS)
    with pytest.raises(UsageError, match="`run_id` or `run_key`"):
        act(
            ctx,
            ActionName.CONTROL_BULK,
            requests=(
                request(run, ControlAction.HOLD, "a", "a"),
                ControlRequest(
                    pipeline_code="P",
                    action=ControlAction.HOLD,
                    reason="reason",
                    request_key="implicit",
                ),
            ),
        )
    with ctx.engine.connect() as conn:
        assert controls.blocked(conn, run) == {}


def test_real_cli_prepares_restart_and_reads_its_capabilities(ctx, capsys, monkeypatch):
    import json

    from etl_craft.cli.commands import common

    _, run, _, _ = scene(ctx)
    from contextlib import contextmanager

    @contextmanager
    def context(args, role=None):
        yield ctx

    monkeypatch.setattr("etl_craft.cli.access_context", context)
    monkeypatch.setattr(common, "command_context", context)
    assert (
        main(
            [
                "run",
                "--pipeline-code",
                "P",
                "--run-id",
                str(run),
                "--restart",
                "--reason",
                "repair",
                "--request-key",
                "cli",
                "--format",
                "json",
            ]
        )
        == 0
    )
    document = json.loads(capsys.readouterr().out)
    assert document["execution_run_id"] != run
    assert (
        main(
            [
                "capabilities",
                "--pipeline-code",
                "P",
                "--run-id",
                str(run),
                "--format",
                "json",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["schema"] == "etl-craft/capabilities/1"
