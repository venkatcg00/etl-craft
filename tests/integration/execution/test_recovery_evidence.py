"""Recovery never substitutes lease expiry for physical completion evidence."""

from datetime import UTC, datetime, timedelta

import pytest

from etl_craft.controller.supervisor import WorkingSet
from etl_craft.core.actor import current_actor
from etl_craft.core.enums import ControlAction, RunStatus
from etl_craft.core.errors import RunRefusedError
from etl_craft.engine import controls, locks, transitions
from etl_craft.execution.interventions import pause_pipeline
from etl_craft.services.operations.controls import request_control
from fixtures.metadata import finish_run
from fixtures.scoped_controls import ctx as ctx
from integration.execution.test_recovery_controls import request, scene


@pytest.mark.parametrize("action", [ControlAction.RECOVER, ControlAction.RESTART])
def test_recovery_needs_query_evidence_even_after_a_lost_worker_has_exited(ctx, action):
    _, run, _, task = scene(ctx, RunStatus.IN_PROGRESS)
    with ctx.engine.begin() as conn:
        binding = transitions.find_or_create_task_run(conn, task, run)
        attempt = transitions.queue_attempt(conn, binding.task_run_id, current_actor())
        transitions.claim_attempt(
            conn,
            attempt,
            current_actor(),
            owner="lost",
            lease_expires_at=datetime.now(UTC) - timedelta(seconds=1),
        )
        transitions.lose_attempt(conn, attempt, current_actor(), owner="lost")
        controls.observe_exit(conn, attempt, queries_ended=False)
        if action == ControlAction.RESTART:
            finish_run(conn, run, RunStatus.FAILED)
    with pytest.raises(RunRefusedError, match="process/query evidence"):
        request_control(ctx, request(run, action, key="evidence"))
    with ctx.engine.begin() as conn:
        controls.observe_exit(conn, attempt, queries_ended=True)
    prepared = request_control(ctx, request(run, action, key="evidence"))
    assert prepared.execution_run_id is not None


def test_controller_only_admits_the_permitted_nonterminal_run_under_definition_pause(ctx):
    _, source, _, _ = scene(ctx)
    restarted = request_control(ctx, request(source, ControlAction.RESTART, key="new"))
    pause_pipeline(ctx.engine, ctx.config, "P", "automatic starts paused")
    working = WorkingSet()
    assert working.refresh(ctx) == []
    request_control(ctx, request(restarted.execution_run_id, ControlAction.RUN_PAUSED))
    assert {r.pipeline_run_id for r in working.refresh(ctx)} == {restarted.execution_run_id}


def test_new_run_key_without_permission_still_respects_definition_pause(ctx):
    from etl_craft.engine.runlog import RunSelector
    from etl_craft.execution.pipeline import run_pipeline

    scene(ctx)
    pause_pipeline(ctx.engine, ctx.config, "P", "automatic starts paused")
    outcome = run_pipeline(
        ctx.engine, ctx.config, "P", selector=RunSelector(run_key="new-key"), child=ctx.child
    )
    assert outcome.status == RunStatus.SKIPPED and outcome.pipeline_run_id is None


@pytest.mark.parametrize("task_code", [None, "single"])
def test_new_paused_context_is_idempotent_and_keeps_the_definition_pause(ctx, task_code):
    from etl_craft.engine import run_scopes, runlog
    from etl_craft.engine.repository.pauses import fetch_open_pause
    from etl_craft.services.operations.controls import ControlRequest
    from fixtures.metadata import add_pipeline, add_task
    from fixtures.races import two_at_once

    with ctx.engine.begin() as conn:
        p = add_pipeline(conn, "P")
        t = add_task(conn, p, "single", "SQL")
    pause_pipeline(ctx.engine, ctx.config, "P", "scheduled work paused")
    selected = ControlRequest(
        "P", ControlAction.RUN_PAUSED, "one-off", task_code=task_code, request_key="one-off"
    )
    left, right = two_at_once(
        lambda: request_control(ctx, selected),
        lambda: request_control(ctx, selected),
        at=(locks.EngineLock, "hold"),
    )
    assert left.control_id == right.control_id and left.execution_run_id == right.execution_run_id
    with ctx.engine.connect() as conn:
        assert fetch_open_pause(conn, p) is not None
        assert controls.permits_paused(conn, left.execution_run_id)
        scope = run_scopes.fetch_selection(conn, left.execution_run_id)
        assert (scope is None) == (task_code is None)
        if scope is not None:
            assert scope.task_ids == (t,)
        assert len(runlog.run_candidates(conn, p)) == 1
