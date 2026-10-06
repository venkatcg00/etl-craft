"""Guarded run/attempt transitions and atomic summaries on both Engine DB dialects."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.errors import StaleTransitionError
from etl_craft.engine import runlog
from etl_craft.engine import transitions as tr
from fixtures.metadata import add_pipeline, add_task

ACTOR = Actor("scheduler:test", ActorKind.SCHEDULE)
OWNER = "host:123:uuid"
STATUSES = ("QUEUED", "CLAIMED", "RUNNING", "SUCCESS", "FAILED", "TIMED_OUT", "CANCELLED", "LOST")
OPERATIONS = {
    "claim_attempt": {"QUEUED"},
    "start_attempt": {"CLAIMED"},
    "finish_attempt": {"CLAIMED", "RUNNING"},
    "time_out_attempt": {"RUNNING"},
    "cancel_attempt": {"QUEUED", "CLAIMED", "RUNNING"},
    "lose_attempt": {"CLAIMED", "RUNNING"},
    "renew_lease": {"CLAIMED", "RUNNING"},
}


def scene(conn):
    pipeline = add_pipeline(conn, "P")
    task = add_task(conn, pipeline, "T")
    run = tr.create_run(conn, pipeline, ACTOR)
    summary = tr.create_task_run(conn, task, run, ACTOR)
    return pipeline, task, run, summary


def seed_attempt(conn, task, status, *, expired=False):
    lease = datetime.now(UTC) + timedelta(seconds=-10 if expired else 60)
    return int(
        conn.execute(
            text(
                "INSERT INTO AUD_TASK_ATTEMPTS (TASK_RUN_ID, ATTEMPT_NUMBER, STATUS, OWNER_ID, "
                "LEASE_EXPIRES_AT, QUEUED_AT) VALUES (:task, 1, :status, :owner, :lease, :now) "
                "RETURNING ATTEMPT_ID AS attempt_id"
            ),
            {
                "task": task,
                "status": status,
                "owner": None if status == "QUEUED" else OWNER,
                "lease": lease,
                "now": datetime.now(UTC),
            },
        ).scalar_one()
    )


def apply(conn, operation, attempt, *, owner=OWNER):
    if operation == "claim_attempt":
        tr.claim_attempt(
            conn,
            attempt,
            ACTOR,
            owner=owner,
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
    elif operation == "start_attempt":
        tr.start_attempt(conn, attempt, ACTOR, owner=owner, host="host", pid=123)
    elif operation == "finish_attempt":
        tr.finish_attempt(
            conn,
            attempt,
            "SUCCESS",
            ACTOR,
            owner=owner,
            source_count=10,
            target_count=9,
            insert_count=7,
            update_count=2,
            delete_count=1,
            rows_written=9,
            task_log="values",
            exit_code=0,
        )
    elif operation == "time_out_attempt":
        tr.time_out_attempt(conn, attempt, ACTOR, owner=owner, error_message="time limit")
    elif operation == "cancel_attempt":
        tr.cancel_attempt(conn, attempt, ACTOR, error_message="operator cancel")
    elif operation == "lose_attempt":
        tr.lose_attempt(conn, attempt, ACTOR, owner=owner)
    else:
        tr.renew_lease(
            conn,
            attempt,
            ACTOR,
            owner=owner,
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=120),
        )


@pytest.mark.parametrize("status", STATUSES)
@pytest.mark.parametrize("operation", OPERATIONS)
def test_attempt_state_matrix(engine_db, operation, status):
    with engine_db.engine.begin() as conn:
        _, _, _, task = scene(conn)
        attempt = seed_attempt(conn, task, status, expired=operation == "lose_attempt")
        if status not in OPERATIONS[operation]:
            with pytest.raises(StaleTransitionError, match=rf"row_id={attempt}.*expected.*found"):
                apply(conn, operation, attempt)
            assert conn.execute(text("SELECT STATUS FROM AUD_TASK_ATTEMPTS")).scalar_one() == status
        else:
            apply(conn, operation, attempt)
            row = conn.execute(text("SELECT STATUS FROM AUD_TASK_ATTEMPTS")).scalar_one()
            expected = {
                "claim_attempt": "CLAIMED",
                "start_attempt": "RUNNING",
                "finish_attempt": "SUCCESS",
                "time_out_attempt": "TIMED_OUT",
                "cancel_attempt": "CANCELLED",
                "lose_attempt": "LOST",
            }.get(operation, status)
            assert row == expected
            if operation != "renew_lease":
                summary = runlog.fetch_task_run_result(conn, task)
                compatible = {
                    "CLAIMED": "IN-PROGRESS",
                    "RUNNING": "IN-PROGRESS",
                    "TIMED_OUT": "FAILED",
                    "LOST": "FAILED",
                }.get(expected, expected)
                assert summary.status == compatible
                assert summary.attempt_count == 1
            if operation == "lose_attempt":
                assert OWNER in runlog.fetch_task_run_result(conn, task).error_message


@pytest.mark.parametrize(
    "operation",
    ["start_attempt", "finish_attempt", "time_out_attempt", "lose_attempt", "renew_lease"],
)
def test_wrong_owner_cannot_change_attempt(engine_db, operation):
    with engine_db.engine.begin() as conn:
        _, _, _, task = scene(conn)
        status = "CLAIMED" if operation == "start_attempt" else "RUNNING"
        attempt = seed_attempt(conn, task, status, expired=operation == "lose_attempt")
        with pytest.raises(StaleTransitionError, match=r"owner='wrong'.*found"):
            apply(conn, operation, attempt, owner="wrong")
        assert conn.execute(text("SELECT STATUS FROM AUD_TASK_ATTEMPTS")).scalar_one() == status


def test_queue_retries_keep_immutable_outcomes_and_all_counts(engine_db):
    with engine_db.engine.begin() as conn:
        _, _, run, task = scene(conn)
        first = tr.queue_attempt(conn, task, ACTOR, log_path="attempt-1.log")
        with pytest.raises(StaleTransitionError):
            tr.queue_attempt(conn, task, ACTOR)
        apply(conn, "claim_attempt", first)
        apply(conn, "start_attempt", first)
        apply(conn, "finish_attempt", first)
        summary = conn.execute(
            text(
                "SELECT SOURCE_COUNT, TARGET_COUNT, INSERT_COUNT, UPDATE_COUNT, "
                "DELETE_COUNT, ROWS_WRITTEN, TASK_LOG FROM AUD_TASK_RUN_LOG"
            )
        ).one()
        assert tuple(summary) == (10, 9, 7, 2, 1, 9, "values")
        second = tr.queue_attempt(conn, task, ACTOR)
        assert second != first
        assert runlog.fetch_task_run_result(conn, task).attempt_count == 2
        with pytest.raises(StaleTransitionError):
            apply(conn, "finish_attempt", first)
        assert runlog.fetch_task_run_result(conn, task).status == "IN-PROGRESS"
        assert conn.execute(
            text(
                "SELECT REQUESTED_BY, REQUESTED_BY_KIND FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"
            ),
            {"id": second},
        ).one() == (ACTOR.name, "SCHEDULE")
        # A queued attempt has no process and does not block run finalization.
        tr.cancel_attempt(conn, second, ACTOR, error_message="done")
        tr.finish_run(conn, run, "SUCCESS", ACTOR)


def test_live_lease_cannot_be_lost_and_expired_lease_cannot_be_renewed(engine_db):
    with engine_db.engine.begin() as conn:
        _, _, _, task = scene(conn)
        attempt = seed_attempt(conn, task, "RUNNING")
        with pytest.raises(StaleTransitionError):
            tr.lose_attempt(conn, attempt, ACTOR, owner=OWNER)
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:lease"),
            {"lease": datetime.now(UTC) - timedelta(seconds=1)},
        )
        with pytest.raises(StaleTransitionError):
            apply(conn, "renew_lease", attempt)


@pytest.mark.parametrize("status", ["IN-PROGRESS", "SUCCESS", "FAILED", "SKIPPED", "CANCELLED"])
@pytest.mark.parametrize("operation", ["finish", "reopen", "mark", "start"])
def test_run_state_matrix(engine_db, status, operation):
    with engine_db.engine.begin() as conn:
        pipeline, _, run, _ = scene(conn)
        conn.execute(text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS=:status"), {"status": status})
        allowed = (
            operation == "mark"
            or (operation == "reopen" and status != "IN-PROGRESS")
            or (operation in {"finish", "start"} and status == "IN-PROGRESS")
        )

        def change():
            if operation == "finish":
                tr.finish_run(conn, run, "SUCCESS", ACTOR)
            elif operation == "reopen":
                tr.reopen_run(conn, run, ACTOR)
            elif operation == "mark":
                tr.mark_run(conn, run, "SKIPPED", ACTOR)
            else:
                tr.start_run(
                    conn,
                    run,
                    ACTOR,
                    owner=OWNER,
                    lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
                )

        if allowed:
            change()
        else:
            with pytest.raises(StaleTransitionError):
                change()
        assert pipeline > 0


def test_run_owner_live_attempt_and_other_run_guards(engine_db):
    with engine_db.engine.begin() as conn:
        pipeline, task_id, run, task = scene(conn)
        tr.start_run(
            conn,
            run,
            ACTOR,
            owner=OWNER,
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
        with pytest.raises(StaleTransitionError):
            tr.finish_run(conn, run, "SUCCESS", ACTOR, owner="wrong")
        with pytest.raises(StaleTransitionError):
            tr.mark_run(conn, run, "SUCCESS", ACTOR, owner="wrong")
        with pytest.raises(StaleTransitionError):
            tr.start_run(conn, run, ACTOR, owner="wrong", lease_expires_at=datetime.now(UTC))
        attempt = tr.queue_attempt(conn, task, ACTOR)
        apply(conn, "claim_attempt", attempt)
        for fn in [tr.finish_run, tr.mark_run]:
            with pytest.raises(StaleTransitionError):
                fn(conn, run, "SUCCESS", ACTOR, owner=OWNER)
        tr.cancel_attempt(conn, attempt, ACTOR, error_message="cancel")
        tr.finish_run(conn, run, "CANCELLED", ACTOR, owner=OWNER)
        with pytest.raises(StaleTransitionError):
            tr.reopen_run(conn, run, ACTOR, owner="wrong")
        other = tr.create_run(conn, pipeline, ACTOR)
        with pytest.raises(StaleTransitionError):
            tr.reopen_run(conn, run, ACTOR, owner=OWNER)
        with pytest.raises(StaleTransitionError):
            tr.create_run(conn, pipeline, ACTOR)
        with pytest.raises(StaleTransitionError):
            tr.create_task_run(conn, task_id, run, ACTOR)
        assert runlog.fetch_pipeline_run_status(conn, other) == "IN-PROGRESS"


def test_attempt_and_summary_failure_roll_back_together(engine_db):
    with engine_db.engine.begin() as conn:
        _, _, _, task = scene(conn)
        attempt = seed_attempt(conn, task, "RUNNING")
        if engine_db.dialect.name == "sqlite":
            conn.exec_driver_sql(
                "CREATE TRIGGER reject_summary BEFORE UPDATE ON AUD_TASK_RUN_LOG "
                "BEGIN SELECT RAISE(ABORT, 'summary rejected'); END"
            )
        else:
            conn.exec_driver_sql(
                "CREATE FUNCTION reject_summary() RETURNS trigger LANGUAGE plpgsql "
                "AS $$ BEGIN RAISE EXCEPTION 'summary rejected'; END $$"
            )
            conn.exec_driver_sql(
                "CREATE TRIGGER reject_summary BEFORE UPDATE ON AUD_TASK_RUN_LOG "
                "FOR EACH ROW EXECUTE FUNCTION reject_summary()"
            )
        with pytest.raises(DBAPIError, match="summary rejected"):
            apply(conn, "finish_attempt", attempt)
        assert conn.execute(text("SELECT STATUS FROM AUD_TASK_ATTEMPTS")).scalar_one() == "RUNNING"
        assert runlog.fetch_task_run_result(conn, task).status == "IN-PROGRESS"


def test_concurrent_queue_has_one_winner(engine_db):
    with engine_db.engine.begin() as conn:
        _, _, _, task = scene(conn)
    barrier = Barrier(2)

    def queue():
        barrier.wait(timeout=10)
        try:
            with engine_db.engine.begin() as conn:
                return tr.queue_attempt(conn, task, ACTOR)
        except StaleTransitionError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: queue(), range(2)))
    assert sum(value is not None for value in results) == 1
    with engine_db.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 1


def test_historical_requesters_stay_unknown_after_completion(engine_db):
    with engine_db.engine.begin() as conn:
        _, _, _, task = scene(conn)
        attempt = seed_attempt(conn, task, "RUNNING")
        conn.execute(text("UPDATE AUD_TASK_ATTEMPTS SET REQUESTED_BY=NULL, REQUESTED_BY_KIND=NULL"))
        apply(conn, "finish_attempt", attempt)
        assert conn.execute(
            text("SELECT REQUESTED_BY, REQUESTED_BY_KIND FROM AUD_TASK_ATTEMPTS")
        ).one() == (None, None)


def test_parent_and_child_acknowledge_one_process_and_cannot_execute_twice(engine_db):
    with engine_db.engine.begin() as conn:
        _, _, _, task = scene(conn)
        attempt = seed_attempt(conn, task, "CLAIMED")
        tr.ensure_attempt_started(conn, attempt, ACTOR, owner=OWNER, host="host", pid=123)
        tr.ensure_attempt_started(conn, attempt, ACTOR, owner=OWNER, host="host", pid=123)
        with pytest.raises(StaleTransitionError):
            tr.ensure_attempt_started(conn, attempt, ACTOR, owner=OWNER, host="host", pid=456)
        apply(conn, "finish_attempt", attempt)
        tr.ensure_attempt_started(
            conn, attempt, ACTOR, owner=OWNER, host="host", pid=123, completed=True
        )
        with pytest.raises(StaleTransitionError):
            tr.ensure_attempt_started(conn, attempt, ACTOR, owner=OWNER, host="host", pid=123)


def test_old_parent_cannot_append_logs_to_a_new_attempt(engine_db):
    with engine_db.engine.begin() as conn:
        _, _, _, task = scene(conn)
        first = seed_attempt(conn, task, "RUNNING")
        apply(conn, "finish_attempt", first)
        tr.set_task_log(conn, task, first, OWNER, "captured output")
        second = tr.queue_attempt(conn, task, ACTOR)
        with pytest.raises(StaleTransitionError):
            tr.set_task_log(conn, task, first, OWNER, "old parent's output")
        with pytest.raises(StaleTransitionError):
            tr.assert_attempt(conn, first, task, OWNER)
        assert second > first
        assert conn.execute(text("SELECT TASK_LOG FROM AUD_TASK_RUN_LOG")).scalar_one() is None


def test_run_reopen_and_history_are_atomic(engine_db):
    with engine_db.engine.begin() as conn:
        _, _, run, _ = scene(conn)
        tr.finish_run(conn, run, "FAILED", ACTOR)
        tr.reopen_run(conn, run, ACTOR, reason="fixed source")
        row = conn.execute(
            text(
                "SELECT ACTION, FROM_STATUS, TO_STATUS, REASON, REQUESTED_BY, "
                "REQUESTED_BY_KIND FROM AUD_RUN_INTERVENTIONS"
            )
        ).one()
        assert tuple(row) == (
            "REOPEN",
            "FAILED",
            "IN-PROGRESS",
            "fixed source",
            ACTOR.name,
            "SCHEDULE",
        )


@pytest.mark.chaos
def test_cancelled_attempt_cannot_overwrite_an_operator_mark(engine_db):
    with engine_db.engine.begin() as conn:
        _, _, _, task = scene(conn)
        attempt = seed_attempt(conn, task, "RUNNING")
        tr.mark_task_run(
            conn, task, status="SUCCESS", error_message="loaded by hand", target_count=12
        )
        with pytest.raises(StaleTransitionError):
            tr.finish_attempt(conn, attempt, "SUCCESS", ACTOR, owner=OWNER, target_count=1)
        with pytest.raises(StaleTransitionError):
            tr.set_task_log(conn, task, attempt, OWNER, "old process output")
        assert conn.execute(
            text("SELECT STATUS, TARGET_COUNT, ERROR_MESSAGE FROM AUD_TASK_RUN_LOG")
        ).one() == ("SUCCESS", 12, "loaded by hand")


def test_task_creation_requires_its_own_active_pipeline(engine_db):
    with engine_db.engine.begin() as conn:
        _, task, run, _ = scene(conn)
        other = add_pipeline(conn, "Other")
        other_run = tr.create_run(conn, other, ACTOR)
        with pytest.raises(StaleTransitionError):
            tr.create_task_run(conn, task, other_run, ACTOR)
        next_task = add_task(conn, other, "Next")
        tr.finish_run(conn, other_run, "SUCCESS", ACTOR)
        with pytest.raises(StaleTransitionError):
            tr.create_task_run(conn, next_task, other_run, ACTOR)
        assert runlog.fetch_pipeline_run_status(conn, run) == "IN-PROGRESS"


def test_missing_rows_and_invalid_outcomes_are_stale(engine_db):
    with engine_db.engine.begin() as conn:
        for action in [
            lambda: tr.start_run(conn, 999, ACTOR, owner=OWNER, lease_expires_at=datetime.now(UTC)),
            lambda: tr.reopen_run(conn, 999, ACTOR),
            lambda: tr.finish_run(conn, 999, "IN-PROGRESS", ACTOR),
            lambda: tr.mark_run(conn, 999, "CANCELLED", ACTOR),
            lambda: tr.finish_attempt(conn, 999, "SKIPPED", ACTOR, owner=OWNER),
            lambda: tr.queue_attempt(conn, 999, ACTOR),
        ]:
            with pytest.raises(StaleTransitionError, match="missing"):
                action()


@pytest.mark.parametrize(
    "status,count,expected",
    [("SUCCESS", 4, 5), ("FAILED", 2, 3), ("SKIPPED", 1, 1), ("IN-PROGRESS", 0, 1)],
)
def test_attempt_number_preserves_legacy_summary_history(engine_db, status, count, expected):
    with engine_db.engine.begin() as conn:
        _, _, _, task = scene(conn)
        conn.execute(
            text("UPDATE AUD_TASK_RUN_LOG SET STATUS=:status, ATTEMPT_COUNT=:count"),
            {"status": status, "count": count},
        )
        attempt = tr.queue_attempt(conn, task, ACTOR)
        assert tr.active_attempt(conn, task).attempt_number == expected
        assert runlog.fetch_task_run_result(conn, task).attempt_count == expected
        assert tr.active_attempt(conn, task).attempt_id == attempt


@pytest.mark.chaos
def test_operator_mark_refuses_an_attempt_admitted_after_its_read(engine_db, monkeypatch):
    with engine_db.engine.begin() as conn:
        _, _, _, task = scene(conn)
        attempt = tr.queue_attempt(conn, task, ACTOR)
        apply(conn, "claim_attempt", attempt)
        monkeypatch.setattr(tr, "active_attempt", lambda *_: None)
        with pytest.raises(StaleTransitionError, match=r"no active attempt.*active attempt"):
            tr.mark_task_run(conn, task, status="SUCCESS", error_message="manual", target_count=99)
        assert runlog.fetch_task_run_result(conn, task).status == "IN-PROGRESS"
        assert conn.execute(text("SELECT STATUS FROM AUD_TASK_ATTEMPTS")).scalar_one() == "CLAIMED"


@pytest.mark.chaos
def test_operator_cancel_refuses_a_run_that_finished_after_its_read(engine_db):
    with engine_db.engine.begin() as conn:
        _, _, run, _ = scene(conn)
        tr.finish_run(conn, run, "SUCCESS", ACTOR)
        with pytest.raises(StaleTransitionError, match=r"expected status IN-PROGRESS.*SUCCESS"):
            tr.cancel_pipeline_run(conn, run)
        assert runlog.fetch_pipeline_run_status(conn, run) == "SUCCESS"


@pytest.mark.chaos
def test_stale_attempt_cannot_advance_its_offset(engine_db):
    from etl_craft.engine.repository.offsets import (
        StoredOffset,
        fetch_task_offset,
        save_task_offset,
    )

    with engine_db.engine.begin() as conn:
        _, task, _, summary = scene(conn)
        attempt = seed_attempt(conn, summary, "RUNNING")
        save_task_offset(conn, task, StoredOffset("NUMBER", "3"))
        with pytest.raises(StaleTransitionError):
            tr.finish_attempt(
                conn,
                attempt,
                "SUCCESS",
                ACTOR,
                owner="wrong-owner",
                offset=StoredOffset("NUMBER", "9"),
            )
        assert fetch_task_offset(conn, task) == StoredOffset("NUMBER", "3")
        assert conn.execute(text("SELECT STATUS FROM AUD_TASK_ATTEMPTS")).scalar_one() == "RUNNING"
