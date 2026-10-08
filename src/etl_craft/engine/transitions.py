"""Guarded lifecycle writes and atomic task-attempt summaries."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from typing import Any, Literal
from uuid import uuid4

from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError

from etl_craft.core.actor import SYSTEM_ACTOR, Actor, current_actor
from etl_craft.core.counts import NO_COUNTS, Counts
from etl_craft.core.enums import FINISHED_RUN_STATUSES, RunStatus, SlaStatus
from etl_craft.core.errors import RunStateError, StaleTransitionError
from etl_craft.core.faults import fault_point
from etl_craft.core.time import as_utc
from etl_craft.engine import runlog
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
    """Start a new ``IN-PROGRESS`` run of ``pipeline_id`` and return its id.

    Return ``None`` when the pipeline already has a run in progress: the unique index on
    ``IN-PROGRESS`` runs refuses a second one, so of several processes starting a run at once,
    exactly one gets an id. A new run runs as of ``run_date`` (today, in UTC, unless given),
    and ``backfill`` marks it part of a backfill.
    """
    kind = trigger_kind or ("BACKFILL" if backfill else "MANUAL")
    if backfill and kind != "BACKFILL":
        raise RunStateError(
            f"pipeline_id={pipeline_id}: trigger_kind={kind!r}, backfill={backfill!r}; "
            "expected MANUAL, BACKFILL or STAND_IN, with BACKFILL for a backfill run. "
            "Use BACKFILL for backfills and MANUAL or STAND_IN for other runs."
        )
    logical_date = run_date or runlog.today()
    prefix = "stand-in" if kind == "STAND_IN" else kind.lower()
    run_key = f"{prefix}:{logical_date}:{uuid4()}" if kind == "BACKFILL" else f"{prefix}:{uuid4()}"
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
    """End ``pipeline_run_id`` with ``status`` only if it is still ``from_status``.

    Return whether it did. The caller decides what a refusal means; nothing is changed then.
    """
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
    orchestrated: bool = False,
    reason: str = "task requested under an ended run",
    selector: runlog.RunSelector = runlog.ACTIVE_RUN,
) -> tuple[int, str | None]:
    """Bind to the exact selected run; force or an orchestrator may reopen it."""
    selected = runlog.select_run(conn, pipeline_id, selector)
    if selected.status == RunStatus.IN_PROGRESS:
        return selected.pipeline_run_id, None
    if not orchestrated and (not force or selected.status == RunStatus.CANCELLED):
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
    """Return the row of ``task_id`` under ``pipeline_run_id``, creating it ``IN-PROGRESS``.

    A concurrent insert that loses reads back the winner.
    """
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
        if changed.rowcount and status == "SUCCESS" and offset is not None:
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
    """End ``pipeline_run_id`` with ``status`` and END_DATE now, if it is still ``IN-PROGRESS``.

    With ``sla_in_hours`` (the pipeline's ``SLA_IN_HOURS``, whenever it has one) the run is also
    marked ``MET`` or ``BREACHED``, measured from START_DATE, whether or not SLA emails are on,
    unless it already has an SLA status: a run reopened after it met its SLA stays ``MET``, and
    the returned result carries the recorded status. STATUS is left alone either way: a late run
    did its work. A run that is no longer ``IN-PROGRESS`` is not changed, and ``ended`` is false.
    """
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
            if consume and status == "SUCCESS":
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
    """Mark a still running ``pipeline_run_id`` ``BREACHED``; return whether this call did.

    ``False`` when the run already finished or was already marked.
    """
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


def cancel_task_run(conn: Connection, task_run_id: int, error_message: str) -> bool:
    """End an ``IN-PROGRESS`` task row ``CANCELLED``; return whether it was still running."""
    active = active_attempt(conn, task_run_id)
    if active is not None:
        cancel_attempt(conn, active.attempt_id, current_actor(), error_message=error_message)
        return True
    result = conn.execute(
        statement(conn, "transition_cancel_task_run"),
        {"task_run_id": task_run_id, "error_message": error_message, "now": datetime.now(UTC)},
    )
    return bool(result.rowcount)


def cancel_pipeline_run(conn: Connection, pipeline_run_id: int) -> None:
    """Cancel an active run on request; refuse a changed status or owner."""
    row = conn.execute(statement(conn, "run_lease"), {"row_id": pipeline_run_id}).one()
    finish_run(conn, pipeline_run_id, "CANCELLED", current_actor(), owner=row.owner_id)


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
    status: str = "IN-PROGRESS",
) -> int:
    """Create an active run or a queued/skipped schedule with its exact key."""
    if status not in {"IN-PROGRESS", "QUEUED", "SKIPPED"} or (
        status == "SKIPPED" and trigger_kind != "SCHEDULE"
    ):
        raise RunStateError(
            f"pipeline_id={pipeline_id}: initial status={status!r}; "
            "only schedules may start SKIPPED"
        )
    if trigger_kind not in {"SCHEDULE", "MANUAL", "BACKFILL", "ORCHESTRATOR", "STAND_IN"}:
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
                    "end_date": datetime.now(UTC) if status == "SKIPPED" else None,
                    "ended_by": actor.name if status == "SKIPPED" else None,
                    "ended_by_kind": actor.kind.value if status == "SKIPPED" else None,
                    "backfill": "Y" if trigger_kind == "BACKFILL" else "N",
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


def start_run(
    conn: Connection, pipeline_run_id: int, actor: Actor, *, owner: str, lease_expires_at: datetime
) -> None:
    """Attach the overseer to an active run that has no owner."""
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
    if status not in {"SUCCESS", "FAILED", "SKIPPED", "CANCELLED"}:
        raise _stale(conn, "run", pipeline_run_id, "terminal outcome", owner)
    query = "cancel_run" if status == "CANCELLED" else "finish_run"
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
                    "to_status": "IN-PROGRESS",
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
    if status not in {"SUCCESS", "FAILED", "SKIPPED"}:
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
    conn: Connection, task_run_id: int, actor: Actor, *, log_path: str | None = None
) -> int:
    """Queue one attempt, serializing admission through its task summary."""
    try:
        with conn.begin_nested():
            params = {**_actor_params(actor), "row_id": task_run_id, "log_path": log_path}
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
        }
        row = conn.execute(statement(conn, f"transition_{query}"), params).one_or_none()
        if row is None:
            raise _stale(conn, "attempt", attempt_id, expected, owner)
        if query == "finish_attempt" and status == "SUCCESS":
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
) -> None:
    """Record the owned outcome, summary, offset and admitted consumption atomically."""
    if status not in {"SUCCESS", "FAILED"}:
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
        )
        if status == "SUCCESS":
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
        "TIMED_OUT",
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
    """Cancel a nonterminal attempt on behalf of an operator or overseer."""
    _end_attempt(
        conn,
        attempt_id,
        actor,
        "cancel_attempt",
        "QUEUED, CLAIMED or RUNNING",
        None,
        "CANCELLED",
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
        "LOST",
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


@dataclass(frozen=True)
class ActiveAttempt:
    """Identity and owner of a nonterminal attempt."""

    attempt_id: int
    attempt_number: int
    owner: str | None
    status: str


def active_attempt(conn: Connection, task_run_id: int) -> ActiveAttempt | None:
    """Return the task's nonterminal attempt, if one exists."""
    row = conn.execute(
        statement(conn, "active_attempt"), {"task_run_id": task_run_id}
    ).one_or_none()
    return (
        None
        if row is None
        else ActiveAttempt(int(row.attempt_id), int(row.attempt_number), row.owner_id, row.status)
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
        allowed = {"RUNNING", "SUCCESS", "FAILED"} if completed else {"RUNNING"}
        if row is not None and row.status == "RUNNING":
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
