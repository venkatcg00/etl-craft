"""Fenced, append-only item observations for one exact task run."""

import json
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError

from etl_craft.core.actor import current_actor
from etl_craft.core.enums import AttemptStatus, ItemStatus
from etl_craft.core.errors import HandlerError, StaleTransitionError
from etl_craft.core.items import ItemProgress, ItemRecord, valid_item_key
from etl_craft.core.text import mask_arguments
from etl_craft.engine.queries import statement

DONE = frozenset({ItemStatus.SUCCESS, ItemStatus.SKIPPED, ItemStatus.CANCELLED})


def latest(conn: Connection, task_run_id: int, key: str | None = None) -> tuple[ItemRecord, ...]:
    """Read each key's latest evidence without resolving a run by recency."""
    rows = conn.execute(statement(conn, "item_latest"), {"task_run_id": task_run_id, "key": key})
    return tuple(
        ItemRecord(
            r.key,
            r.number,
            observed_status(r.status, r.attempt_status),
            r.row_count,
            r.error,
            None if r.started is None else str(r.started),
            None if r.ended is None else str(r.ended),
            r.report_id,
            r.attempt_id,
        )
        for r in rows
    )


def append(conn: Connection, task_run_id: int, attempt_id: int, event: ItemRecord) -> ItemRecord:
    """Append an attributed observation; callers serialize admission and fence its attempt."""
    now = datetime.now(UTC)
    stored = replace(
        event,
        report_id=event.report_id or str(uuid4()),
        attempt_id=attempt_id,
        error=mask_error(event.error),
    )
    if stored.status == ItemStatus.RUNNING:
        stored = replace(stored, started=now.isoformat(), ended=None)
    elif stored.status in DONE | {ItemStatus.FAILED}:
        stored = replace(stored, ended=now.isoformat())
    try:
        conn.execute(
            statement(conn, "item_insert"),
            {
                **vars(stored),
                "task_run_id": task_run_id,
                "actor": current_actor().name,
                "now": now,
                "started": None
                if stored.started is None
                else datetime.fromisoformat(stored.started),
                "ended": None if stored.ended is None else datetime.fromisoformat(stored.ended),
                "error": stored.error,
            },
        )
    except IntegrityError as error:
        raise StaleTransitionError(
            f"item {event.key}: report_id={stored.report_id} conflicts with recorded evidence; "
            "expected a unique report for this attempt; reload its item state before retrying"
        ) from error
    return stored


def record(conn: Connection, task_run_id: int, attempt_id: int, event: ItemRecord) -> ItemRecord:
    """Record once and prevent a cancelled key from starting."""
    if not valid_item_key(event.key):
        raise HandlerError(
            f"task run {task_run_id}: item key={event.key!r}; expected UTF-8 text "
            "of 1-512 characters without NUL; "
            "give each item a nonempty stable key"
        )
    validate_numbers(event)
    validate_report_id(event.report_id)
    duplicate = conn.execute(statement(conn, "item_report"), {"report_id": event.report_id}).first()
    previous = latest(conn, task_run_id, event.key)
    old = previous[0] if previous else None
    if duplicate is not None:
        check_duplicate(duplicate, task_run_id, attempt_id, event)
        assert old is not None
        return old
    if event.status == ItemStatus.RUNNING:
        if old is None:
            raise StaleTransitionError(
                f"item {event.key}: no registered key; expected a bounded prepared work list; "
                "prepare items before starting the function"
            )
        if prevents_start(old):
            return old
        require_ended(event, old)
        expected = old.number + 1
        if event.number != expected:
            raise StaleTransitionError(
                f"item {event.key}: attempt={event.number}; expected {expected}; "
                f"reload item evidence"
            )
    elif event.status in {ItemStatus.SUCCESS, ItemStatus.FAILED}:
        if old is None or old.attempt_id != attempt_id or old.number != event.number:
            raise StaleTransitionError(
                f"item {event.key}: no matching live item attempt; reload item evidence"
            )
        if old.status == ItemStatus.CANCEL_REQUESTED:
            event = replace(event, status=ItemStatus.CANCELLED)
        elif old.status != ItemStatus.RUNNING:
            raise StaleTransitionError(
                f"item {event.key}: status={old.status}; expected RUNNING; reload item evidence"
            )
    else:
        raise HandlerError(
            f"item {event.key}: status={event.status}; expected RUNNING, SUCCESS or "
            f"FAILED; prepare keys before starts or use operator controls"
        )
    return append(conn, task_run_id, attempt_id, event)


def validate_numbers(event: ItemRecord) -> None:
    """Refuse malformed counts before any lifecycle write."""
    if (
        type(event.row_count) is not int
        or type(event.number) is not int
        or min(event.row_count, event.number) < 0
    ):
        raise HandlerError(
            f"item {event.key}: rows={event.row_count}, attempt={event.number}; expected "
            "nonnegative whole numbers; correct the item result"
        )


def observed_status(status: str, attempt: str) -> ItemStatus:
    """Derive ended thread state, retaining unknown cancellation on LOST."""
    if attempt == AttemptStatus.CANCELLED and status in {ItemStatus.RUNNING, ItemStatus.QUEUED}:
        return ItemStatus.CANCELLED
    if attempt not in {AttemptStatus.RUNNING, AttemptStatus.CLAIMED}:
        if status == ItemStatus.RUNNING:
            return ItemStatus.FAILED
        if status == ItemStatus.CANCEL_REQUESTED and attempt != AttemptStatus.LOST:
            return ItemStatus.CANCELLED
    return ItemStatus(status)


def check_duplicate(old: object, run: int, attempt: int, event: ItemRecord) -> None:
    """Refuse a different observation under an existing report identity."""
    from sqlalchemy.engine import Row

    assert isinstance(old, Row)
    if (old.task_run_id, old.attempt_id, old.key, old.number, old.row_count, old.error) != (
        run,
        attempt,
        event.key,
        event.number,
        event.row_count,
        mask_error(event.error),
    ) or not (
        old.status == event.status
        or (
            old.status == ItemStatus.CANCELLED
            and event.status in {ItemStatus.SUCCESS, ItemStatus.FAILED}
        )
    ):
        raise StaleTransitionError(
            f"item {event.key}: report differs from its recorded observation; "
            "expected the original report payload; resend it or use a new report id"
        )


def require_ended(event: ItemRecord, old: ItemRecord | None) -> None:
    """Never start a second function while the same key's first function is running."""
    if old is not None and old.status == ItemStatus.RUNNING:
        raise StaleTransitionError(
            f"item {event.key}: status=RUNNING; expected an ended item before retry; "
            "wait for its result or reconcile its expired task attempt"
        )


def mask_error(error: str | None) -> str | None:
    """Apply the same masking before database storage and immediate API responses."""
    return None if error is None else str(mask_arguments({"message": error})["message"])[:4096]


def validate_report_id(value: str) -> None:
    """Require stable bounded identities for retried agent reports."""
    if not isinstance(value, str) or len(value) > 36:
        raise HandlerError(
            f"item report_id={value!r}; expected a UUID of at most 36 characters; "
            "resend the original report identity"
        )
    try:
        UUID(value)
    except (ValueError, TypeError, AttributeError) as error:
        raise HandlerError(
            f"item report_id={value!r}; expected a UUID; resend the original report identity"
        ) from error


def prevents_start(old: ItemRecord) -> bool:
    """Resume parent-cancelled work, while retaining explicitly cancelled item endings."""
    parent_cancelled = old.status == ItemStatus.CANCELLED and old.ended is None
    return old.status in DONE | {ItemStatus.CANCEL_REQUESTED} and not parent_cancelled


def summaries(conn: Connection, task_runs: Sequence[int]) -> dict[int, ItemProgress]:
    """Read bounded progress in one batch, without loading item payloads per catalog row."""
    if not task_runs:
        return {}
    rows = conn.execute(statement(conn, "item_summaries"), {"task_run_ids": json.dumps(task_runs)})
    return {r.task_run_id: ItemProgress(r.total, r.done, r.failed, r.skipped) for r in rows}


def attempt_summaries(conn: Connection, task_run: int) -> dict[int, ItemProgress]:
    """Keep each attempt's historical item counts alongside the current task-run counts."""
    rows = conn.execute(statement(conn, "item_attempt_summaries"), {"task_run_id": task_run})
    return {r.attempt_id: ItemProgress(r.total, r.done, r.failed, r.skipped) for r in rows}


def prepare(conn: Connection, run: int, attempt: int, keys: list[str]) -> None:
    """Insert new queued keys and retained successes once per attempt, in one statement."""
    entries = [{"key": key, "report_id": str(uuid4())} for key in keys]
    conn.execute(
        statement(conn, "item_prepare"),
        {
            "task_run_id": run,
            "attempt_id": attempt,
            "entries": json.dumps(entries),
            "actor": current_actor().name,
            "now": datetime.now(UTC),
        },
    )
