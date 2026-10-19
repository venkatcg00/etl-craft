"""Guarded lifecycle writes and atomic task-attempt summaries."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from typing import Any, Literal
from uuid import uuid4

from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError

from etl_craft.core.actor import SYSTEM_ACTOR, Actor, current_actor
from etl_craft.core.counts import NO_COUNTS, Counts
from etl_craft.core.enums import (
    FINISHED_RUN_STATUSES,
    AttemptStatus,
    RunStatus,
    SlaStatus,
    TriggerKind,
    YesNo,
)
from etl_craft.core.errors import RunStateError, StaleTransitionError
from etl_craft.core.faults import fault_point
from etl_craft.core.time import as_utc
from etl_craft.engine import controls, run_scopes, runlog
from etl_craft.engine.attempts import (
    ActiveAttempt as ActiveAttempt,
)
from etl_craft.engine.attempts import (
    active_attempt as active_attempt,
)
from etl_craft.engine.attempts import (
    record_attempt_ending,
)
from etl_craft.engine.attempts import (
    record_attempt_failure as record_attempt_failure,
)
from etl_craft.engine.attempts import (
    set_attempt_retry_policy as set_attempt_retry_policy,
)
from etl_craft.engine.queries import statement
from etl_craft.engine.repository import trackers
from etl_craft.engine.repository.offsets import StoredOffset, save_task_offset


def create_active_run(
    conn: Connection,
    pipeline_id: int,
    *,
    run_date: date | None = None,
    backfill: bool = False,
    trigger_kind: Literal["MANUAL", "BACKFILL", "STAND_IN"] | None = None,
) -> int | None:
    """Start an exclusive run of ``pipeline_id``; concurrent starts return None."""
    kind = trigger_kind or ("BACKFILL" if backfill else "MANUAL")
    if backfill and kind != TriggerKind.BACKFILL:
        raise RunStateError(
            f"pipeline_id={pipeline_id}: trigger_kind={kind!r}, backfill={backfill!r}; "
            "expected MANUAL, BACKFILL or STAND_IN, with BACKFILL for a backfill run. "
            "Use BACKFILL for backfills and MANUAL or STAND_IN for other runs."
        )
    logical_date = run_date or runlog.today()
    prefix = "stand-in" if kind == TriggerKind.STAND_IN else kind.lower()
    suffix = f"{logical_date}:{uuid4()}" if kind == TriggerKind.BACKFILL else str(uuid4())
    run_key = f"{prefix}:{suffix}"
    try:
        return create_run(
            conn,
            pipeline_id,
            current_actor(),
            run_date=logical_date,
            trigger_kind=kind,
            run_key=run_key,
        )
    except StaleTransitionError:
        return None


def end_run_if(conn: Connection, pipeline_run_id: int, from_status: str, status: str) -> bool:
    """End ``pipeline_run_id`` with ``status`` if still ``from_status``; return whether it did."""
    if from_status != RunStatus.IN_PROGRESS:
        return False
    try:
        finish_run(conn, pipeline_run_id, status, SYSTEM_ACTOR)
        return True
    except StaleTransitionError:
        return False


def resolve_run(
    conn: Connection,
    pipeline_id: int,
    *,
    force: bool = False,
    rerun: bool = False,
    reason: str = "task requested under an ended run",
    selector: runlog.RunSelector = runlog.ACTIVE_RUN,
) -> tuple[int, str | None]:
    """Bind to the exact selected run; force or an explicit rerun may reopen it."""
    selected = runlog.select_run(conn, pipeline_id, selector)
    if selected.status == RunStatus.IN_PROGRESS:
        return selected.pipeline_run_id, None
    if not rerun and (not force or selected.status == RunStatus.CANCELLED):
        raise RunStateError(
            f"pipeline_id={pipeline_id}: pipeline_run_id={selected.pipeline_run_id} "
            f"is {selected.status}; start a new run with --init-only, or use --force "
            "with an explicit run identity for an ended, non-cancelled run"
        )
    reopen_run(conn, selected.pipeline_run_id, current_actor(), reason=reason)
    return selected.pipeline_run_id, selected.status


def find_or_create_task_run(
    conn: Connection, task_id: int, pipeline_run_id: int
) -> runlog.TaskRunBinding:
    """Return ``task_id``'s row under ``pipeline_run_id``, creating it; a losing insert reads it."""
    run_scopes.check_task(conn, pipeline_run_id, task_id)
    params = {"task_id": task_id, "pipeline_run_id": pipeline_run_id}
    existing = conn.execute(statement(conn, "task_run"), params).one_or_none()
    if existing is not None:
        return runlog.TaskRunBinding(existing.task_run_id, existing.status)
    try:
        with conn.begin_nested():
            task_run_id = conn.execute(
                statement(conn, "transition_insert_task_run"), params
            ).scalar_one()
        return runlog.TaskRunBinding(int(task_run_id), RunStatus.IN_PROGRESS, created=True)
    except IntegrityError:
        winner = conn.execute(statement(conn, "task_run"), params).one_or_none()
        if winner is None:
            raise RunStateError(
                f"task_id={task_id}, pipeline_run_id={pipeline_run_id}: binding hit a unique "
                "violation, but no row exists afterwards"
            ) from None
        return runlog.TaskRunBinding(winner.task_run_id, winner.status)


def finish_task_run(
    conn: Connection,
    task_run_id: int,
    *,
    status: str,
    counts: Counts = NO_COUNTS,
    error_message: str | None = None,
    task_log: str | None = None,
    attempt_id: int | None = None,
    owner: str | None = None,
    offset: StoredOffset | None = None,
    retryable: bool = True,
) -> None:
    """Record the current attempt's outcome on its row; each attempt writes only its own counts."""
    if attempt_id is not None:
        assert_attempt(conn, attempt_id, task_run_id, owner)
        if owner is None:
            raise _stale(conn, "attempt", attempt_id, "owner supplied", owner)
        finish_attempt(
            conn,
            attempt_id,
            status,
            current_actor(),
            owner=owner,
            counts=counts,
            error_message=error_message,
            task_log=task_log,
            retryable=retryable,
            offset=offset,
        )
        return
    with conn.begin_nested():
        changed = conn.execute(
            statement(conn, "transition_finish_task_run"),
            {
                "task_run_id": task_run_id,
                "status": status,
                "now": datetime.now(UTC),
                **counts.parameters(),
                "error_message": error_message,
                "task_log": task_log,
            },
        )
        if changed.rowcount and status == RunStatus.SUCCESS and offset is not None:
            task_id = (
                conn.execute(statement(conn, "task_run_context"), {"task_run_id": task_run_id})
                .one()
                .task_id
            )
            save_task_offset(conn, task_id, offset)
            fault_point("script.after_offset")


def finalize_pipeline_run(
    conn: Connection,
    pipeline_run_id: int,
    status: str,
    *,
    sla_in_hours: float | None = None,
    owner: str | None = None,
    consume: bool = False,
) -> runlog.RunEnding:
    """End an active run, guarding task liveness and committing SLA and consumption."""
    now = datetime.now(UTC)
    sla = None
    if sla_in_hours is not None:
        start = conn.execute(
            statement(conn, "pipeline_run_start"), {"pipeline_run_id": pipeline_run_id}
        ).scalar_one()
        hours = runlog.elapsed_hours(start, now)
        sla = runlog.SlaResult(
            SlaStatus.BREACHED if hours > sla_in_hours else SlaStatus.MET,
            float(sla_in_hours),
            hours,
        )
    try:
        with conn.begin_nested():
            finish_run(
                conn,
                pipeline_run_id,
                status,
                SYSTEM_ACTOR,
                sla_status=None if sla is None else str(sla.status),
                owner=owner,
            )
            ended = True
            if consume and status == RunStatus.SUCCESS:
                fault_point("pipeline.before_consumption")
                trackers.consume_pipeline_decisions(conn, pipeline_run_id)
                fault_point("pipeline.after_consumption")
    except StaleTransitionError:
        ended = False
    if sla is not None:
        recorded = runlog.fetch_run_sla(conn, pipeline_run_id).sla_status
        if recorded is not None and recorded != sla.status:
            sla = replace(sla, status=SlaStatus(recorded))
    return runlog.RunEnding(ended, sla)


def mark_sla_breached(conn: Connection, pipeline_run_id: int) -> bool:
    """Mark a still running ``pipeline_run_id`` ``BREACHED``; return whether this call did."""
    result = conn.execute(
        statement(conn, "transition_mark_sla_breached"), {"pipeline_run_id": pipeline_run_id}
    )
    return bool(result.rowcount)


def mark_task_run(
    conn: Connection,
    task_run_id: int,
    *,
    status: str,
    error_message: str,
    target_count: int | None,
) -> None:
    """Set a task row's status as an operator marked it."""
    active = active_attempt(conn, task_run_id)
    if active is not None:
        cancel_attempt(conn, active.attempt_id, current_actor(), error_message=error_message)
    result = conn.execute(
        statement(conn, "transition_mark_task_run"),
        {
            "task_run_id": task_run_id,
            "status": status,
            "error_message": error_message,
            "target_count": target_count,
            "sets_count": 1 if status == RunStatus.SUCCESS else 0,
            "now": datetime.now(UTC),
        },
    )

    if result.rowcount != 1:
        raise _stale(conn, "task", task_run_id, "existing summary; no active attempt", None)


def cancel_pipeline_run(conn: Connection, pipeline_run_id: int) -> None:
    """Cancel an active run on request; refuse a changed status or owner."""
    row = conn.execute(statement(conn, "run_lease"), {"row_id": pipeline_run_id}).one()
    finish_run(conn, pipeline_run_id, RunStatus.CANCELLED, current_actor(), owner=row.owner_id)


def delete_skipped_task_run(conn: Connection, task_run_id: int) -> None:
    """Remove a ``SKIPPED`` row of a task that never ran, so the resumed run decides again."""
    conn.execute(statement(conn, "delete_task_run"), {"task_run_id": task_run_id})


def _stale(
    conn: Connection, kind: str, row_id: int, expected: str, owner: str | None
) -> StaleTransitionError:
    row = (
        conn.execute(statement(conn, f"transition_row_{kind}"), {"row_id": row_id})
        .mappings()
        .one_or_none()
    )
    found = "missing" if row is None else repr(dict(row))
    if kind == "task":
        active = (
            conn.execute(statement(conn, "active_attempt"), {"task_run_id": row_id})
            .mappings()
            .one_or_none()
        )
        if active is not None:
            found += f"; active attempt {dict(active)!r}"
    return StaleTransitionError(
        f"{kind} row_id={row_id}: expected status {expected}, owner={owner!r}; found {found}. "
        "Read the run history and refresh the status and owner before retrying."
    )


def _actor_params(actor: Actor) -> dict[str, Any]:
    return {"actor": actor.name, "actor_kind": actor.kind.value, "now": datetime.now(UTC)}


def _guard(
    conn: Connection,
    query: str,
    kind: str,
    row_id: int,
    expected: str,
    owner: str | None,
    params: dict[str, Any],
) -> None:
    result = conn.execute(
        statement(conn, f"transition_{query}"), {"row_id": row_id, "owner": owner, **params}
    )
    if result.rowcount != 1:
        raise _stale(conn, kind, row_id, expected, owner)


def create_run(
    conn: Connection,
    pipeline_id: int,
    actor: Actor,
    *,
    run_date: date | None = None,
    trigger_kind: str = "MANUAL",
    run_key: str | None = None,
    status: str = RunStatus.IN_PROGRESS,
) -> int:
    """Create an active run or a queued/skipped schedule with its exact key."""
    if status not in {RunStatus.IN_PROGRESS, RunStatus.QUEUED, RunStatus.SKIPPED} or (
        status == RunStatus.SKIPPED and trigger_kind != TriggerKind.SCHEDULE
    ):
        raise RunStateError(
            f"pipeline_id={pipeline_id}: initial status={status!r}; "
            "only schedules may start SKIPPED"
        )
    if trigger_kind not in set(TriggerKind):
        raise RunStateError(
            f"pipeline_id={pipeline_id}: invalid trigger_kind={trigger_kind!r}; "
            "use SCHEDULE, MANUAL, BACKFILL, ORCHESTRATOR or STAND_IN"
        )
    logical_date = run_date or runlog.today()
    key = run_key or f"{trigger_kind.lower().replace('_', '-')}:{uuid4()}"
    try:
        with conn.begin_nested():
            result = conn.execute(
                statement(conn, "transition_create_run"),
                {
                    **_actor_params(actor),
                    "pipeline_id": pipeline_id,
                    "run_date": logical_date,
                    "status": status,
                    "end_date": datetime.now(UTC) if status == RunStatus.SKIPPED else None,
                    "ended_by": actor.name if status == RunStatus.SKIPPED else None,
                    "ended_by_kind": actor.kind.value if status == RunStatus.SKIPPED else None,
                    "backfill": YesNo.of(trigger_kind == TriggerKind.BACKFILL),
                    "trigger_kind": trigger_kind,
                    "run_key": key,
                },
            )
            row = result.scalar_one()
            return int(row)
    except IntegrityError as error:
        found = (
            conn.execute(
                statement(conn, "run_conflict"), {"pipeline_id": pipeline_id, "run_key": key}
            )
            .mappings()
            .one_or_none()
        )
        state = "invalid parent" if found is None else repr(dict(found))
        raise StaleTransitionError(
            f"pipeline_id={pipeline_id}, run_key={key!r}: expected no active run and a new key, "
            f"owner=None; found {state}. Check run history before retrying."
        ) from error


def admit_run(conn: Connection, pipeline_run_id: int, actor: Actor) -> None:
    """Admit one queued run only while its pipeline has no active run."""
    try:
        with conn.begin_nested():
            _guard(
                conn,
                "admit_run",
                "run",
                pipeline_run_id,
                "QUEUED; no active sibling",
                None,
                _actor_params(actor),
            )
    except IntegrityError as error:
        raise StaleTransitionError(
            f"pipeline_run_id={pipeline_run_id}: another run became active; leave this run queued"
        ) from error


def lock_active_run(conn: Connection, pipeline_run_id: int) -> None:
    """Serialize an event check's commit with cancellation, requiring an active run."""
    _guard(conn, "lock_active_run", "run", pipeline_run_id, "IN-PROGRESS", None, {})


def start_run(
    conn: Connection, pipeline_run_id: int, actor: Actor, *, owner: str, lease_expires_at: datetime
) -> None:
    """Attach the controller to an active run that has no owner."""
    _guard(
        conn,
        "start_run",
        "run",
        pipeline_run_id,
        "IN-PROGRESS (unowned)",
        None,
        {**_actor_params(actor), "owner": owner, "lease": lease_expires_at},
    )


def finish_run(
    conn: Connection,
    pipeline_run_id: int,
    status: str,
    actor: Actor,
    *,
    owner: str | None = None,
    sla_status: str | None = None,
) -> None:
    """End an active run only when no claimed or running attempt remains."""
    if status not in FINISHED_RUN_STATUSES:
        raise _stale(conn, "run", pipeline_run_id, "terminal outcome", owner)
    query = "cancel_run" if status == RunStatus.CANCELLED else "finish_run"
    _guard(
        conn,
        query,
        "run",
        pipeline_run_id,
        "IN-PROGRESS; no live attempt",
        owner,
        {**_actor_params(actor), "status": status, "sla_status": sla_status},
    )


def reopen_run(
    conn: Connection,
    pipeline_run_id: int,
    actor: Actor,
    *,
    owner: str | None = None,
    reason: str = "run reopened",
) -> None:
    """Reopen a terminal run and record REOPEN in the same transaction."""
    row = conn.execute(
        statement(conn, "transition_row_run"), {"row_id": pipeline_run_id}
    ).one_or_none()
    if row is None or row.status not in FINISHED_RUN_STATUSES:
        raise _stale(conn, "run", pipeline_run_id, "terminal; no other active run", owner)
    try:
        with conn.begin_nested():
            _guard(
                conn,
                "reopen_run",
                "run",
                pipeline_run_id,
                "terminal; no other active run",
                owner,
                {**_actor_params(actor), "from_status": row.status},
            )
            conn.execute(
                statement(conn, "insert_intervention"),
                {
                    "pipeline_id": row.pipeline_id,
                    "pipeline_run_id": pipeline_run_id,
                    "task_id": None,
                    "action": "REOPEN",
                    "from_status": row.status,
                    "to_status": RunStatus.IN_PROGRESS,
                    "target_count": None,
                    "previous_message": None,
                    "reason": reason,
                    "requested_by": actor.name,
                    "requested_by_kind": actor.kind.value,
                    "now": datetime.now(UTC),
                },
            )
    except IntegrityError as error:
        raise _stale(
            conn, "run", pipeline_run_id, "terminal; no other active run", owner
        ) from error


def mark_run(
    conn: Connection, pipeline_run_id: int, status: str, actor: Actor, *, owner: str | None = None
) -> None:
    """Record an operator's outcome only when no claimed or running attempt remains."""
    if status not in {RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.SKIPPED}:
        raise _stale(conn, "run", pipeline_run_id, "SUCCESS, FAILED or SKIPPED outcome", owner)
    _guard(
        conn,
        "mark_run",
        "run",
        pipeline_run_id,
        "valid run; no live attempt",
        owner,
        {**_actor_params(actor), "status": status},
    )


def queue_attempt(
    conn: Connection,
    task_run_id: int,
    actor: Actor,
    *,
    log_path: str | None = None,
    not_before: datetime | None = None,
    previous_attempt_id: int | None = None,
) -> int:
    """Queue one attempt, serializing admission through its task summary."""
    controls.guard_task(conn, task_run_id)
    try:
        with conn.begin_nested():
            params = {
                **_actor_params(actor),
                "row_id": task_run_id,
                "log_path": log_path,
                "not_before": not_before,
                "previous_attempt_id": previous_attempt_id,
            }
            _guard(
                conn,
                "reserve_attempt",
                "task",
                task_run_id,
                "no active attempt; active run",
                None,
                params,
            )
            attempt_id = int(
                conn.execute(statement(conn, "transition_queue_attempt"), params).scalar_one()
            )
            _sync_summary(conn, attempt_id)
            return attempt_id
    except IntegrityError as error:
        raise _stale(conn, "task", task_run_id, "no active attempt", None) from error


def _sync_summary(conn: Connection, attempt_id: int) -> None:
    _guard(conn, "sync_attempt_summary", "attempt", attempt_id, "latest attempt", None, {})


def claim_attempt(
    conn: Connection,
    attempt_id: int,
    actor: Actor,
    *,
    owner: str,
    lease_expires_at: datetime,
    host: str | None = None,
) -> None:
    """Claim a queued attempt for one owner."""
    controls.guard_attempt(conn, attempt_id)
    with conn.begin_nested():
        _guard(
            conn,
            "claim_attempt",
            "attempt",
            attempt_id,
            "QUEUED (unowned)",
            None,
            {**_actor_params(actor), "owner": owner, "lease": lease_expires_at, "host": host},
        )
        _sync_summary(conn, attempt_id)


def start_attempt(
    conn: Connection,
    attempt_id: int,
    actor: Actor,
    *,
    owner: str,
    host: str | None = None,
    pid: int | None = None,
    process_start: str | None = None,
) -> None:
    """Start a claimed attempt, fenced by its owner."""
    with conn.begin_nested():
        _guard(
            conn,
            "start_attempt",
            "attempt",
            attempt_id,
            "CLAIMED",
            owner,
            {**_actor_params(actor), "host": host, "pid": pid, "process_start": process_start},
        )
        _sync_summary(conn, attempt_id)


def _end_attempt(
    conn: Connection,
    attempt_id: int,
    actor: Actor,
    query: str,
    expected: str,
    owner: str | None,
    status: str,
    *,
    counts: Counts = NO_COUNTS,
    error_message: str | None = None,
    task_log: str | None = None,
    exit_code: int | None = None,
    retryable: bool = True,
) -> None:
    with conn.begin_nested():
        params = {
            **_actor_params(actor),
            "row_id": attempt_id,
            "owner": owner,
            "status": status,
            **counts.parameters(),
            "error_message": error_message,
            "task_log": task_log,
            "exit_code": exit_code,
            "retryable": retryable,
        }
        record_attempt_ending(conn, attempt_id, status, retryable)
        row = conn.execute(statement(conn, f"transition_{query}"), params).one_or_none()
        if row is None:
            raise _stale(conn, "attempt", attempt_id, expected, owner)
        if query == "finish_attempt" and status == AttemptStatus.SUCCESS:
            fault_point("attempt.before_summary")
        _sync_summary(conn, attempt_id)


def finish_attempt(
    conn: Connection,
    attempt_id: int,
    status: str,
    actor: Actor,
    *,
    owner: str,
    counts: Counts = NO_COUNTS,
    error_message: str | None = None,
    task_log: str | None = None,
    exit_code: int | None = None,
    offset: StoredOffset | None = None,
    retryable: bool = True,
) -> None:
    """Record the owned outcome, summary, offset and admitted consumption atomically."""
    if status not in {AttemptStatus.SUCCESS, AttemptStatus.FAILED}:
        raise _stale(conn, "attempt", attempt_id, "SUCCESS or FAILED outcome", owner)
    with conn.begin_nested():
        _end_attempt(
            conn,
            attempt_id,
            actor,
            "finish_attempt",
            "CLAIMED or RUNNING",
            owner,
            status,
            counts=counts,
            error_message=error_message,
            task_log=task_log,
            exit_code=exit_code,
            retryable=retryable,
        )
        if status == AttemptStatus.SUCCESS:
            fault_point("attempt.after_status")
            row = conn.execute(
                statement(conn, "task_run_for_attempt"), {"attempt_id": attempt_id}
            ).one()
            if offset is not None:
                save_task_offset(conn, row.task_id, offset)
                fault_point("script.after_offset")
            fault_point("runner.before_consumption")
            trackers.consume_task_decisions(
                conn, row.task_id, row.pipeline_run_id, attempt_id=attempt_id
            )
            fault_point("attempt.after_consumption")


def time_out_attempt(
    conn: Connection,
    attempt_id: int,
    actor: Actor,
    *,
    owner: str,
    error_message: str,
    task_log: str | None = None,
) -> None:
    """End an owned running attempt that exceeded its time limit."""
    _end_attempt(
        conn,
        attempt_id,
        actor,
        "time_out_attempt",
        "RUNNING",
        owner,
        AttemptStatus.TIMED_OUT,
        error_message=error_message,
        task_log=task_log,
    )


def cancel_attempt(
    conn: Connection,
    attempt_id: int,
    actor: Actor,
    *,
    error_message: str,
    task_log: str | None = None,
) -> None:
    """Cancel a nonterminal attempt on behalf of an operator or controller."""
    _end_attempt(
        conn,
        attempt_id,
        actor,
        "cancel_attempt",
        "QUEUED, CLAIMED or RUNNING",
        None,
        AttemptStatus.CANCELLED,
        error_message=error_message,
        task_log=task_log,
    )


def lose_attempt(
    conn: Connection,
    attempt_id: int,
    actor: Actor,
    *,
    owner: str | None,
    error_message: str | None = None,
) -> None:
    """Record an uncertain outcome for an owner whose lease expired."""
    _end_attempt(
        conn,
        attempt_id,
        actor,
        "lose_attempt",
        "CLAIMED or RUNNING; expired lease",
        owner,
        AttemptStatus.LOST,
        error_message=error_message or f"lost owner {owner}: side effects are uncertain",
    )


def renew_lease(
    conn: Connection, attempt_id: int, actor: Actor, *, owner: str, lease_expires_at: datetime
) -> None:
    """Renew an owned live lease; an expired lease cannot be revived."""
    _guard(
        conn,
        "renew_lease",
        "attempt",
        attempt_id,
        "CLAIMED or RUNNING; live lease",
        owner,
        {**_actor_params(actor), "lease": lease_expires_at},
    )


def restart_rule_run(conn: Connection, business_rule_run_id: int, now: datetime) -> None:
    """Restart a business-rule summary that has not succeeded."""
    conn.execute(
        statement(conn, "transition_restart_business_rule_run"),
        {"business_rule_run_id": business_rule_run_id, "now": now},
    )


def finish_rule_run(
    conn: Connection, business_rule_run_id: int, status: str, now: datetime
) -> None:
    """Record a business-rule summary's outcome."""
    conn.execute(
        statement(conn, "transition_finish_business_rule_run"),
        {"business_rule_run_id": business_rule_run_id, "status": status, "now": now},
    )


def assert_attempt(conn: Connection, attempt_id: int, task_run_id: int, owner: str | None) -> None:
    """Reject a result referring to another task, owner or superseded attempt."""
    row = conn.execute(
        statement(conn, "transition_row_attempt"), {"row_id": attempt_id}
    ).one_or_none()
    newer = conn.execute(
        statement(conn, "latest_attempt"), {"task_run_id": task_run_id}
    ).scalar_one_or_none()
    if (
        row is None
        or row.task_run_id != task_run_id
        or row.owner_id != owner
        or newer != attempt_id
    ):
        raise _stale(
            conn, "attempt", attempt_id, f"latest attempt of task_run_id={task_run_id}", owner
        )


def set_task_log(
    conn: Connection, task_run_id: int, attempt_id: int, owner: str, task_log: str | None
) -> None:
    """Append captured process output to the summary of this exact attempt."""
    _guard(
        conn,
        "set_task_log",
        "attempt",
        attempt_id,
        "latest attempt",
        owner,
        {
            "task_run_id": task_run_id,
            "attempt_id": attempt_id,
            "task_log": task_log,
            "now": datetime.now(UTC),
        },
    )


def ensure_attempt_started(
    conn: Connection,
    attempt_id: int,
    actor: Actor,
    *,
    owner: str,
    host: str,
    pid: int,
    completed: bool = False,
    process_start: str | None = None,
) -> None:
    """Accept the same start acknowledged by parent and child, with no second write."""
    try:
        start_attempt(
            conn, attempt_id, actor, owner=owner, host=host, pid=pid, process_start=process_start
        )
    except StaleTransitionError:
        row = conn.execute(
            statement(conn, "transition_row_attempt"), {"row_id": attempt_id}
        ).one_or_none()
        running = {AttemptStatus.RUNNING}
        allowed = running | {AttemptStatus.SUCCESS, AttemptStatus.FAILED} if completed else running
        if row is not None and row.status == AttemptStatus.RUNNING:
            expiry = row.lease_expires_at
            instant = None if expiry is None else as_utc(expiry)
            if instant is None or runlog.elapsed_hours(instant, datetime.now(UTC)) >= 0:
                raise
        if row is not None and process_start is not None and row.process_start != process_start:
            raise
        if row is None or row.owner_id != owner or row.pid != pid or row.status not in allowed:
            raise


def create_rule_run(conn: Connection, business_rule_id: int, task_run_id: int) -> int:
    """Create a business-rule summary under its task run."""
    return int(
        conn.execute(
            statement(conn, "transition_insert_business_rule_run"),
            {"business_rule_id": business_rule_id, "task_run_id": task_run_id},
        ).scalar_one()
    )


def renew_run_lease(
    conn: Connection, run_id: int, actor: Actor, *, owner: str, lease_expires_at: datetime
) -> None:
    """Renew a supervisor's run lease only while its existing lease is live."""
    _guard(
        conn,
        "renew_run_lease",
        "run",
        run_id,
        "IN-PROGRESS; live lease",
        owner,
        {**_actor_params(actor), "lease": lease_expires_at},
    )


def release_run_lease(conn: Connection, run_id: int, *, owner: str, expired: bool = False) -> bool:
    """Release this exact supervisor when no active attempt remains, optionally after expiry."""
    query = "transition_expire_run_lease" if expired else "transition_release_run_lease"
    result = conn.execute(
        statement(conn, query), {"row_id": run_id, "owner": owner, "now": datetime.now(UTC)}
    )
    return result.rowcount == 1


def record_watermark_range(
    conn: Connection,
    task_run_id: int,
    watermark_from: str,
    watermark_to: str,
    *,
    attempt_id: int | None = None,
    owner: str | None = None,
) -> None:
    """Record a staged range only on the live owned attempt, preserving failed ranges."""
    params = {
        "row_id": attempt_id,
        "task_run_id": task_run_id,
        "owner": owner,
        "now": datetime.now(UTC),
        "watermark_from": watermark_from,
        "watermark_to": watermark_to,
    }
    with conn.begin_nested():
        query = (
            "transition_record_watermark"
            if attempt_id is not None
            else "transition_record_task_watermark"
        )
        changed = conn.execute(statement(conn, query), params)
        if changed.rowcount != 1:
            raise _stale(conn, "task", task_run_id, "live owned attempt for watermark range", owner)
        if attempt_id is not None:
            _sync_summary(conn, attempt_id)


def update_control(conn: Connection, parameters: dict[str, object]) -> None:
    """Advance a durable control status without rewriting its original request."""
    conn.execute(statement(conn, "transition_control"), parameters)
