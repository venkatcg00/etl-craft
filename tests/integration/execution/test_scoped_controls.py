"""Scoped fences and durable agent/query acknowledgements on both Engine DBs."""

import contextvars
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from etl_craft.config import ConnectionProfile
from etl_craft.core.actor import current_actor
from etl_craft.core.enums import (
    ApiRole,
    AttemptStatus,
    ControlAction,
    ControlStatus,
    RunStatus,
)
from etl_craft.core.errors import PermissionDeniedError, RunRefusedError, RunStateError, UsageError
from etl_craft.engine import controls, runlog, transitions
from etl_craft.engine.runlog import RunSelector
from etl_craft.execution import control_flow
from etl_craft.execution.attempt_types import ChildOptions
from etl_craft.execution.pipeline import run_pipeline
from etl_craft.execution.runner import admit_attempt, execute_attempt
from etl_craft.services.operations import from_json, to_json
from etl_craft.services.operations.controls import ControlRequest, get_control, request_control
from fixtures.metadata import add_dependency, add_pipeline, add_task, start_run
from fixtures.races import paused_after, two_at_once
from fixtures.scoped_controls import ctx as ctx

CHILD = ChildOptions(module="fixtures.task_child", kill_grace_seconds=1, cancel_poll_seconds=0.05)


def seed(ctx):
    with ctx.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        first = add_task(conn, pipeline, "first", "PYTHON", BEHAVIOUR="succeed")
        second = add_task(conn, pipeline, "second", "PYTHON", BEHAVIOUR="succeed")
        run = start_run(conn, pipeline)
    return pipeline, run, first, second


def ask(ctx, run, action, task=None, key=None):
    return request_control(
        ctx, ControlRequest("P", action, "maintenance", RunSelector(run_id=run), task, key)
    )


def queue(ctx, run, task):
    with ctx.engine.begin() as conn:
        binding = transitions.find_or_create_task_run(conn, task, run)
        return transitions.queue_attempt(conn, binding.task_run_id, current_actor())


def test_task_hold_release_and_force_admission_are_scoped(ctx):
    _, run, first, second = seed(ctx)
    held = ask(ctx, run, ControlAction.HOLD, "first", "hold:first")
    assert held.status == ControlStatus.COMPLETED and held.attempt_ids == ()
    assert held.requested_by == "operator" and held.task_id == first
    with pytest.raises(RunRefusedError, match="fenced by HOLD"):
        admit_attempt(ctx.engine, ctx.config, first, "first", "P", run, True)
    assert queue(ctx, run, second)
    released = ask(ctx, run, ControlAction.RELEASE, "first")
    assert released.status == ControlStatus.COMPLETED
    assert queue(ctx, run, first)
    assert get_control(ctx, held.control_id).action == ControlAction.HOLD
    assert from_json(type(held), to_json(held)) == held


@pytest.mark.parametrize("action", [ControlAction.STOP, ControlAction.ABORT])
def test_queued_work_is_acknowledged_without_starting_a_process(ctx, action):
    _, run, first, _ = seed(ctx)
    attempt = queue(ctx, run, first)
    done = ask(ctx, run, action)
    assert done.status == ControlStatus.COMPLETED
    assert done.run_status == RunStatus.CANCELLED and done.attempt_ids == (attempt,)
    assert done.attempts[0].status == AttemptStatus.CANCELLED
    assert done.attempts[0].process_ended_at and done.attempts[0].queries_ended_at
    assert done.acknowledged_at and done.completed_at
    assert ask(ctx, run, action).control_id == done.control_id


def test_repeated_keys_and_concurrent_controls_keep_original_scope(ctx):
    _, run, _, _ = seed(ctx)
    results = two_at_once(
        lambda: ask(ctx, run, ControlAction.HOLD, key="same"),
        lambda: ask(ctx, run, ControlAction.HOLD, key="same"),
        at=(ctx.engine, "begin"),
    )
    assert results[0].control_id == results[1].control_id
    with pytest.raises(RunStateError, match="another scope/action"):
        ask(ctx, run, ControlAction.ABORT, "first", "same")
    assert get_control(ctx, results[0].control_id).requested_by == "operator"


def test_claim_racing_hold_is_either_owned_or_fenced(ctx):
    _, run, first, _ = seed(ctx)
    attempt = queue(ctx, run, first)

    def claim():
        try:
            with ctx.engine.begin() as conn:
                transitions.claim_attempt(
                    conn,
                    attempt,
                    current_actor(),
                    owner="agent",
                    lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
                )
            return True
        except RunRefusedError:
            return False

    owned, held = two_at_once(
        claim,
        lambda: ask(ctx, run, ControlAction.HOLD, "first"),
        at=(ctx.engine, "begin"),
    )
    assert held.status == ControlStatus.COMPLETED
    with ctx.engine.connect() as conn:
        actual = controls.affected(conn, (attempt,))[0]
    assert actual.status == (AttemptStatus.CLAIMED if owned else AttemptStatus.QUEUED)
    with pytest.raises(RunRefusedError, match="fenced by HOLD"):
        claim_again(ctx, attempt)


def claim_again(ctx, attempt):
    with ctx.engine.begin() as conn:
        transitions.claim_attempt(
            conn,
            attempt,
            current_actor(),
            owner="other",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )


def test_unknown_query_acknowledgement_stays_pending_after_lease_loss(ctx):
    _, run, first, _ = seed(ctx)
    attempt = queue(ctx, run, first)
    with ctx.engine.begin() as conn:
        transitions.claim_attempt(
            conn,
            attempt,
            current_actor(),
            owner="missing",
            lease_expires_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    request = ask(ctx, run, ControlAction.STOP)
    assert request.status == ControlStatus.REQUESTED
    with ctx.engine.begin() as conn:
        transitions.lose_attempt(
            conn, attempt, current_actor(), owner="missing", error_message="lost"
        )
        controls.observe_exit(conn, attempt, queries_ended=False)
    control_flow.advance(ctx.engine, run, observed=True)
    pending = get_control(ctx, request.control_id)
    assert pending.status == ControlStatus.ACKNOWLEDGED
    assert pending.run_status == RunStatus.IN_PROGRESS and pending.completed_at is None
    assert "expired lease is not completion" in pending.message
    with pytest.raises(RunRefusedError, match="expired lease is not completion"):
        ask(ctx, run, ControlAction.RELEASE)


def test_server_query_abort_refuses_before_mutating_the_scope(ctx):
    _, run, first, _ = seed(ctx)
    attempt = queue(ctx, run, first)
    with ctx.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_TASKS SET HANDLER='SQL' WHERE TASK_ID=:id"), {"id": first})
    claim_again(ctx, attempt)
    server = replace(
        ctx,
        config=replace(
            ctx.config,
            warehouse=ConnectionProfile(
                "WAREHOUSE", "remote", "jdbc:postgresql://localhost/warehouse", "", "none"
            ),
        ),
    )
    with pytest.raises(
        RunRefusedError, match=r"cancellation acknowledgement adapter.*ABORT is refused"
    ):
        ask(server, run, ControlAction.ABORT)
    with ctx.engine.connect() as conn:
        assert controls.blocked(conn, run) == {}
        assert controls.affected(conn, (attempt,))[0].status == AttemptStatus.CLAIMED
    assert ask(server, run, ControlAction.STOP).status == ControlStatus.REQUESTED


def test_role_legacy_and_reason_refusals_are_explicit(ctx):
    _, run, _, _ = seed(ctx)
    with pytest.raises(PermissionDeniedError):
        ask(replace(ctx, role=ApiRole.VIEWER), run, ControlAction.HOLD)
    with pytest.raises(UsageError, match="nonblank reason"):
        request_control(ctx, ControlRequest("P", ControlAction.STOP, " "))


def test_completed_summary_is_not_process_exit_evidence(ctx):
    _, run, first, _ = seed(ctx)
    spec = admit_attempt(ctx.engine, ctx.config, first, "first", "P", run, False)
    from etl_craft.execution import runner

    # The child has committed SUCCESS and exited, but its parent has not recorded that observation.
    with paused_after(
        lambda: execute_attempt(ctx.engine, ctx.config, spec, CHILD),
        owner=runner,
        name="run_child",
        when=lambda *a, **kw: True,
    ):
        request = ask(ctx, run, ControlAction.STOP)
        assert request.run_status == RunStatus.IN_PROGRESS
        assert request.status in {ControlStatus.REQUESTED, ControlStatus.ACKNOWLEDGED}
        assert request.attempt_ids == (spec.attempt_id,)
        assert request.attempts[0].status == AttemptStatus.SUCCESS
        assert request.attempts[0].process_ended_at is None
    done = get_control(ctx, request.control_id)
    assert done.status == ControlStatus.COMPLETED and done.run_status == RunStatus.CANCELLED
    assert done.attempts[0].status == AttemptStatus.SUCCESS
    assert done.attempts[0].process_ended_at and done.attempts[0].queries_ended_at


def test_task_hold_keeps_downstream_pending_but_runs_other_tasks(ctx):
    pipeline, run, first, second = seed(ctx)
    with ctx.engine.begin() as conn:
        third = add_task(conn, pipeline, "third", "PYTHON", BEHAVIOUR="succeed")
        add_dependency(conn, pipeline, third, first)
    held = ask(ctx, run, ControlAction.HOLD, "first")
    done = run_pipeline(ctx.engine, ctx.config, "P", child=CHILD, selector=RunSelector(run_id=run))
    assert done.status == RunStatus.IN_PROGRESS
    with ctx.engine.connect() as conn:
        states = runlog.fetch_run_state(conn, run, [first, second, third])
    assert first not in states and third not in states
    assert states[second].status == RunStatus.SUCCESS
    ask(ctx, run, ControlAction.RELEASE, "first")
    done = run_pipeline(ctx.engine, ctx.config, "P", child=CHILD, selector=RunSelector(run_id=run))
    assert done.status == RunStatus.SUCCESS
    assert get_control(ctx, held.control_id).action == ControlAction.HOLD


def wait_for(check, timeout=20):
    """Poll an observable condition with a bounded wait, never a timing assumption."""
    import time

    deadline = time.monotonic() + timeout
    while not check():
        assert time.monotonic() < deadline, "expected agent observation did not arrive"
        threading.Event().wait(0.05)


@pytest.mark.parametrize("action", [ControlAction.STOP, ControlAction.ABORT])
def test_task_control_observes_just_its_worker_and_preserves_other_work(ctx, tmp_path, action):
    from concurrent.futures import ThreadPoolExecutor

    _, run, first, second = seed(ctx)
    ready, release = tmp_path / "ready", tmp_path / "release"
    other_ready, other_release = tmp_path / "other-ready", tmp_path / "other-release"
    with ctx.engine.begin() as conn:
        for task, marker, gate in ((first, ready, release), (second, other_ready, other_release)):
            conn.execute(
                text(
                    "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE='barrier' "
                    "WHERE TASK_ID=:task AND PARAMETER_NAME='BEHAVIOUR'"
                ),
                {"task": task},
            )
            for name, value in (("READY_FILE", marker), ("RELEASE_FILE", gate)):
                conn.execute(
                    text(
                        "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID,PARAMETER_NAME,PARAMETER_VALUE) "
                        "VALUES (:task,:name,:value)"
                    ),
                    {"task": task, "name": name, "value": str(value)},
                )
    first_spec = admit_attempt(ctx.engine, ctx.config, first, "first", "P", run, False)
    other_spec = admit_attempt(ctx.engine, ctx.config, second, "second", "P", run, False)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first_future = pool.submit(
            contextvars.copy_context().run,
            execute_attempt,
            ctx.engine,
            ctx.config,
            first_spec,
            CHILD,
        )
        other_future = pool.submit(
            contextvars.copy_context().run,
            execute_attempt,
            ctx.engine,
            ctx.config,
            other_spec,
            CHILD,
        )
        try:
            wait_for(lambda: ready.exists() and other_ready.exists())
            request = ask(ctx, run, action, "first")
            assert request.status in {ControlStatus.REQUESTED, ControlStatus.ACKNOWLEDGED}
            assert request.attempt_ids == (first_spec.attempt_id,)
            assert not other_future.done()
            if action == ControlAction.STOP:
                assert not first_future.done()
                release.touch()
            first_result = first_future.result(timeout=20)
            assert first_result.status == (
                RunStatus.SUCCESS if action == ControlAction.STOP else RunStatus.CANCELLED
            )
            done = get_control(ctx, request.control_id)
            assert done.status == ControlStatus.COMPLETED
            assert done.run_status == RunStatus.IN_PROGRESS
            assert done.attempts[0].process_ended_at and done.attempts[0].queries_ended_at
            assert not other_future.done()
        finally:
            release.touch()
            other_release.touch()
            first_future.result(timeout=20)
            assert other_future.result(timeout=20).status == RunStatus.SUCCESS
    with ctx.engine.connect() as conn:
        state = runlog.fetch_run_state(conn, run, [first, second])
    assert state[second].status == RunStatus.SUCCESS


def test_run_hold_is_independent_of_definition_pause_and_other_pipeline(ctx):
    _, run, first, _ = seed(ctx)
    with ctx.engine.begin() as conn:
        other_pipeline = add_pipeline(conn, "OTHER")
        other_task = add_task(conn, other_pipeline, "other", "PYTHON")
        other_run = start_run(conn, other_pipeline)
    ask(ctx, run, ControlAction.HOLD)
    with pytest.raises(RunRefusedError, match="fenced by HOLD"):
        queue(ctx, run, first)
    assert queue(ctx, other_run, other_task)
    ask(ctx, run, ControlAction.RELEASE, "first")
    with pytest.raises(RunRefusedError, match="fenced by HOLD"):
        queue(ctx, run, first)
    ask(ctx, run, ControlAction.RELEASE)
    assert queue(ctx, run, first)


def test_event_probe_control_is_fenced_without_claiming_an_unobserved_query_ended(ctx, tmp_path):
    from etl_craft.engine import locks
    from etl_craft.execution.events import check_event

    pipeline, run, _, _ = seed(ctx)
    path = tmp_path / "ready.csv"
    path.write_text("ready")
    with ctx.engine.begin() as conn:
        task = add_task(
            conn,
            pipeline,
            "wait",
            "EVENT_WAIT",
            EVENT_FILE=str(path),
            EVENT_SETTLE_SECONDS="0",
            EVENT_TIMEOUT_MINUTES="NONE",
        )
    with (
        locks.event(task).hold(ctx.engine),
        pytest.raises(RunRefusedError, match="event probe is active; no control was accepted"),
    ):
        ask(ctx, run, ControlAction.STOP, "wait")
    held = ask(ctx, run, ControlAction.HOLD, "wait")
    waiting = check_event(ctx.engine, ctx.config, run, task, datetime.now(UTC))
    assert waiting.status is None and waiting.next_check_at
    assert held.attempt_ids == () and held.status == ControlStatus.COMPLETED
    ask(ctx, run, ControlAction.RELEASE, "wait")
    assert (
        check_event(ctx.engine, ctx.config, run, task, datetime.now(UTC)).status
        == RunStatus.SUCCESS
    )


def test_query_unknown_after_process_exit_is_still_affected_by_a_new_stop(ctx):
    _, run, first, _ = seed(ctx)
    attempt = queue(ctx, run, first)
    claim_again(ctx, attempt)
    with ctx.engine.begin() as conn:
        transitions.ensure_attempt_started(
            conn,
            attempt,
            current_actor(),
            owner="other",
            host="missing-host",
            pid=12345,
            process_start="old-birth",
        )
        transitions.finish_attempt(
            conn, attempt, AttemptStatus.FAILED, current_actor(), owner="other"
        )
        controls.observe_exit(conn, attempt, queries_ended=False)
    pending = ask(ctx, run, ControlAction.STOP)
    assert pending.attempt_ids == (attempt,)
    assert pending.completed_at is None and pending.run_status == RunStatus.IN_PROGRESS
    assert pending.attempts[0].process_ended_at and pending.attempts[0].queries_ended_at is None


def test_control_identity_and_acknowledgements_are_immutable(ctx):
    from sqlalchemy.exc import DBAPIError

    _, run, first, _ = seed(ctx)
    attempt = queue(ctx, run, first)
    done = ask(ctx, run, ControlAction.STOP)
    with (
        pytest.raises(DBAPIError, match=r"control request identity.*immutable"),
        ctx.engine.begin() as conn,
    ):
        conn.execute(
            text("UPDATE AUD_CONTROL_REQUESTS SET REASON='changed' WHERE CONTROL_ID=:id"),
            {"id": done.control_id},
        )
    with (
        pytest.raises(DBAPIError, match="acknowledgement is immutable"),
        ctx.engine.begin() as conn,
    ):
        conn.execute(
            text("DELETE FROM AUD_ATTEMPT_ACKNOWLEDGEMENTS WHERE ATTEMPT_ID=:id"), {"id": attempt}
        )
    original = get_control(ctx, done.control_id)
    assert original.reason == "maintenance"
    assert original.attempts[0].process_ended_at and original.attempts[0].queries_ended_at


def test_pending_abort_cannot_be_downgraded_or_released(ctx):
    _, run, first, _ = seed(ctx)
    attempt = queue(ctx, run, first)
    claim_again(ctx, attempt)
    aborted = ask(ctx, run, ControlAction.ABORT, "first")
    assert aborted.completed_at is None
    for action in (ControlAction.STOP, ControlAction.RELEASE):
        with pytest.raises(RunRefusedError, match="inspect its process/query acknowledgements"):
            ask(ctx, run, action, "first")
    with ctx.engine.connect() as conn:
        assert controls.blocked(conn, run)[first] == ControlAction.ABORT


def test_request_key_racing_across_runs_has_one_owner(ctx):
    _, run, _, _ = seed(ctx)
    with ctx.engine.begin() as conn:
        other = add_pipeline(conn, "OTHER")
        other_run = start_run(conn, other)

    def submit(code, run_id):
        try:
            return request_control(
                ctx,
                ControlRequest(
                    code,
                    ControlAction.HOLD,
                    "test",
                    RunSelector(run_id=run_id),
                    request_key="global-key",
                ),
            )
        except RunStateError as error:
            return error

    results = two_at_once(
        lambda: submit("P", run), lambda: submit("OTHER", other_run), at=(ctx.engine, "begin")
    )
    assert sum(isinstance(result, RunStateError) for result in results) == 1
    assert any("another scope/action" in str(result) for result in results)


@pytest.mark.parametrize("behavior", ["succeed", "exit"])
def test_server_query_acknowledgement_requires_handler_completion_not_just_exit(ctx, behavior):
    _, run, first, _ = seed(ctx)
    with ctx.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_TASKS SET HANDLER='SQL' WHERE TASK_ID=:id"), {"id": first})
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE=:behavior "
                "WHERE TASK_ID=:task AND PARAMETER_NAME='BEHAVIOUR'"
            ),
            {"behavior": behavior, "task": first},
        )
    server = replace(
        ctx,
        config=replace(
            ctx.config,
            warehouse=ConnectionProfile(
                "WAREHOUSE", "remote", "jdbc:postgresql://localhost/warehouse", "", "none"
            ),
        ),
    )
    # The fake handler exercises the agent protocol without issuing a warehouse query.
    spec = admit_attempt(ctx.engine, server.config, first, "first", "P", run, False)
    result = execute_attempt(ctx.engine, server.config, spec, CHILD)
    assert result.status == (RunStatus.SUCCESS if behavior == "succeed" else RunStatus.FAILED)
    with ctx.engine.connect() as conn:
        observed = controls.affected(conn, (spec.attempt_id,))[0]
    assert observed.process_ended_at is not None
    assert (observed.queries_ended_at is not None) == (behavior == "succeed")
    stopped = ask(server, run, ControlAction.STOP)
    assert stopped.status == (
        ControlStatus.COMPLETED if behavior == "succeed" else ControlStatus.REQUESTED
    )
    assert stopped.run_status == (
        RunStatus.CANCELLED if behavior == "succeed" else RunStatus.IN_PROGRESS
    )


def test_control_of_manual_run_does_not_fence_same_task_in_whole_pipeline(ctx):
    from etl_craft.engine.run_scopes import TaskSelection, create_manual_run

    pipeline, whole, _, _ = seed(ctx)
    with ctx.engine.begin() as conn:
        email = add_task(conn, pipeline, "notify", "EMAIL_ALERT")
        manual = create_manual_run(
            conn,
            pipeline,
            current_actor(),
            TaskSelection((email,)),
            "manual:email",
            datetime.now(UTC).date(),
        )
    manual_attempt = queue(ctx, manual, email)
    whole_attempt = queue(ctx, whole, email)
    stopped = ask(ctx, manual, ControlAction.STOP)
    assert stopped.status == ControlStatus.COMPLETED and stopped.attempt_ids == (manual_attempt,)
    with ctx.engine.connect() as conn:
        assert runlog.fetch_pipeline_run_status(conn, whole) == RunStatus.IN_PROGRESS
        assert controls.blocked(conn, whole) == {}
        assert controls.affected(conn, (whole_attempt,))[0].status == AttemptStatus.QUEUED


def test_active_run_is_pinned_before_event_locks_and_never_reselected(ctx, monkeypatch):
    from contextlib import contextmanager

    from etl_craft.services.operations import controls as operations

    pipeline, original_run, _, _ = seed(ctx)
    original = operations.control_scope
    replacements = []

    @contextmanager
    def replace_active(caller, request):
        with original(caller, request) as pinned:
            with ctx.engine.begin() as conn:
                transitions.mark_run(conn, original_run, RunStatus.SUCCESS, current_actor())
                replacements.append(start_run(conn, pipeline))
            yield pinned

    monkeypatch.setattr(operations, "control_scope", replace_active)
    with pytest.raises(RunStateError, match="controls need a non-terminal run"):
        request_control(ctx, ControlRequest("P", ControlAction.HOLD, "maintenance"))
    with ctx.engine.connect() as conn:
        assert controls.blocked(conn, replacements[0]) == {}
        assert runlog.fetch_pipeline_run_status(conn, replacements[0]) == RunStatus.IN_PROGRESS


def test_reconcile_revisits_lost_workers_but_never_guesses_an_unreadable_identity(ctx, monkeypatch):
    import socket

    from etl_craft.services.operations import runs

    _, run, first, _ = seed(ctx)
    attempt = queue(ctx, run, first)
    claim_again(ctx, attempt)
    with ctx.engine.begin() as conn:
        transitions.ensure_attempt_started(
            conn,
            attempt,
            current_actor(),
            owner="other",
            host=socket.gethostname(),
            pid=12345,
            process_start="old-birth",
        )
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:expired WHERE ATTEMPT_ID=:id"),
            {"expired": datetime.now(UTC) - timedelta(seconds=1), "id": attempt},
        )
        transitions.lose_attempt(
            conn, attempt, current_actor(), owner="other", error_message="lost earlier"
        )
    pending = ask(ctx, run, ControlAction.STOP)
    monkeypatch.setattr(control_flow, "process_start", lambda pid: None)

    def unreadable(pid):
        raise control_flow.psutil.AccessDenied(pid)

    monkeypatch.setattr(control_flow.psutil, "Process", unreadable)
    assert runs.reconcile_runs(ctx, "P").lost_attempt_ids == ()
    assert get_control(ctx, pending.control_id).completed_at is None

    def gone(pid):
        raise control_flow.psutil.NoSuchProcess(pid)

    monkeypatch.setattr(control_flow.psutil, "Process", gone)
    assert runs.reconcile_runs(ctx, "P").lost_attempt_ids == ()
    done = get_control(ctx, pending.control_id)
    assert done.status == ControlStatus.COMPLETED and done.run_status == RunStatus.CANCELLED
    assert done.attempts[0].status == AttemptStatus.LOST


def test_server_python_stop_exposes_missing_query_capability_before_acceptance(ctx):
    _, run, first, _ = seed(ctx)
    attempt = queue(ctx, run, first)
    claim_again(ctx, attempt)
    server = replace(
        ctx,
        config=replace(
            ctx.config,
            warehouse=ConnectionProfile(
                "WAREHOUSE", "remote", "jdbc:postgresql://localhost/warehouse", "", "none"
            ),
        ),
    )
    with pytest.raises(
        RunRefusedError, match=r"Python attempts.*scoped STOP needs a query adapter"
    ):
        ask(server, run, ControlAction.STOP)
    with ctx.engine.connect() as conn:
        assert controls.blocked(conn, run) == {}


@pytest.mark.parametrize("outcome", [AttemptStatus.SUCCESS, AttemptStatus.FAILED])
def test_reconcile_observes_exited_terminal_worker_without_rewriting_its_outcome(
    ctx, monkeypatch, outcome
):
    import socket

    from etl_craft.core.counts import Counts
    from etl_craft.core.enums import ControlAcknowledgement
    from etl_craft.services.operations import runs

    _, run, first, _ = seed(ctx)
    attempt = queue(ctx, run, first)
    claim_again(ctx, attempt)
    with ctx.engine.begin() as conn:
        transitions.ensure_attempt_started(
            conn,
            attempt,
            current_actor(),
            owner="other",
            host=socket.gethostname(),
            pid=12345,
            process_start="old-birth",
        )
        transitions.finish_attempt(
            conn, attempt, outcome, current_actor(), owner="other", counts=Counts(target_count=7)
        )
        if outcome == AttemptStatus.SUCCESS:
            controls.acknowledge(conn, attempt, ControlAcknowledgement.QUERIES)
    pending = ask(ctx, run, ControlAction.STOP)
    assert pending.completed_at is None and pending.attempt_ids == (attempt,)
    monkeypatch.setattr(control_flow, "process_start", lambda pid: "different-birth")
    assert runs.reconcile_runs(ctx, "P").lost_attempt_ids == ()
    done = get_control(ctx, pending.control_id)
    assert done.status == ControlStatus.COMPLETED and done.run_status == RunStatus.CANCELLED
    assert done.attempts[0].status == outcome
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT TARGET_COUNT AS target_count FROM AUD_TASK_ATTEMPTS "
                    "WHERE ATTEMPT_ID=:id"
                ),
                {"id": attempt},
            ).scalar_one()
            == 7
        )
