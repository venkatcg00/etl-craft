"""Serialized Engine-clock starts and retry-stable timestamp cursor admission."""

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import bindparam
from sqlalchemy.engine import Connection

from etl_craft.core.errors import MetadataError, RunRefusedError
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.offsets import StoredOffset, fetch_task_offset, save_task_offset
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.transition_guards import stale as _stale


@dataclass(frozen=True)
class TimestampWindow:
    """The half-open range frozen for one exact task run."""

    low: datetime
    high: datetime


@dataclass(frozen=True)
class WindowDefinition:
    """The pinned reader definition shared by local and remote admission."""

    initial: StoredOffset
    source_codes: tuple[str, ...] = ()
    backfill: bool = False


def checked_clock(conn: Connection) -> datetime:
    """Read the clock after acquiring the transaction's start/admission/reset fence."""
    conn.execute(statement(conn, "window_lock"))
    now = as_utc(conn.execute(statement(conn, "engine_clock")).scalar_one())
    for row in conn.execute(statement(conn, "window_clock_floor")):
        for value in (row.committed, row.pending):
            if value is not None and now < as_utc(value):
                raise RunRefusedError(
                    f"Engine DB clock {now.isoformat()} is behind recorded offset clock fence "
                    f"{as_utc(value).isoformat()}; restore the Engine DB clock before starting work"
                )
    return now


def recorded_start(conn: Connection, task_run_id: int, attempt_id: int | None) -> datetime:
    """Read the recorded start for the exact attempt, or an explicitly unowned task run."""
    value = conn.execute(
        statement(conn, "window_start"),
        {"task_run_id": task_run_id, "attempt_id": attempt_id},
    ).scalar_one()
    if value is None:
        raise RunRefusedError(
            f"task_run_id={task_run_id}, attempt_id={attempt_id}: start is NULL; "
            "expected a recorded Engine DB start; start the attempt before running SQL"
        )
    return as_utc(value)


def admit_window(
    conn: Connection,
    task_id: int,
    task_run_id: int,
    started_at: datetime,
    definition: WindowDefinition,
) -> TimestampWindow:
    """Freeze both bounds before SQL; a retry cannot steal or recalculate another window."""
    now = checked_clock(conn)
    initial, source_codes = definition.initial, definition.source_codes
    previous = conn.execute(statement(conn, "window_range"), {"task_run_id": task_run_id}).one()
    if previous.watermark_to is not None and definition.backfill:
        return TimestampWindow(as_utc(previous.watermark_from), as_utc(previous.watermark_to))
    before = None if definition.backfill else fetch_task_offset(conn, task_id)
    if before is not None and before.pending_task_run_id is not None:
        if before.pending_task_run_id != task_run_id:
            raise RunRefusedError(
                f"task_id={task_id}: pending timestamp window belongs to task_run_id="
                f"{before.pending_task_run_id}, requested {task_run_id}; resume its owner or "
                "cancel and reconcile it before admitting another run"
            )
        assert before.offset_value is not None and before.pending_upper_bound is not None
        return TimestampWindow(
            as_utc(previous.watermark_from or before.offset_value), before.pending_upper_bound
        )
    low = as_utc((before or initial).offset_value)
    high = min(started_at, now)
    if source_codes:
        ids = source_pipeline_ids(conn, source_codes)
        query = statement(conn, "window_sources").bindparams(
            bindparam("pipeline_ids", expanding=True)
        )
        for row in conn.execute(query, {"pipeline_ids": ids}):
            high = min(high, as_utc(row.started_at))
    if high < low:
        raise RunRefusedError(
            f"task_id={task_id}: Engine DB window high {high.isoformat()} is below committed "
            f"low {low.isoformat()}; reconcile source owners and restore the Engine DB clock"
        )
    if previous.watermark_to is not None:
        frozen_low, frozen_high = as_utc(previous.watermark_from), as_utc(previous.watermark_to)
        if low not in {frozen_low, frozen_high}:
            raise RunRefusedError(
                f"task_id={task_id}, task_run_id={task_run_id}: committed cursor {low.isoformat()} "
                f"conflicts with frozen window [{frozen_low.isoformat()}, "
                f"{frozen_high.isoformat()}); "
                "resume the cursor's current owner or explicitly reset before reopening this run"
            )
        low, high = frozen_low, frozen_high
    if definition.backfill:
        return TimestampWindow(low, high)
    if before is None:
        save_task_offset(conn, task_id, initial)
    conn.execute(
        statement(conn, "window_reserve"),
        {
            "task_id": task_id,
            "task_run_id": task_run_id,
            "high": high,
        },
    )
    return TimestampWindow(low, high)


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
    from etl_craft.engine.transitions import sync_attempt_summary

    check_pending_range(conn, task_run_id, watermark_from, watermark_to)
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
            sync_attempt_summary(conn, attempt_id)


def source_pipeline_ids(conn: Connection, codes: tuple[str, ...]) -> tuple[int, ...]:
    """Resolve the same checked project codes at validation and cursor admission."""
    try:
        return tuple(resolve_pipeline_id(conn, code) for code in codes)
    except MetadataError as error:
        raise MetadataError(
            f"WATERMARK_SOURCE_PIPELINES={codes!r}: {error}; expected existing unique project "
            "pipeline codes; correct the parameter and refresh the reader pipeline"
        ) from error


def check_pending_range(conn: Connection, task_run_id: int, low: str, high: str) -> None:
    """Keep every range report bound to its exact admitted cursor pair."""
    row = conn.execute(
        statement(conn, "window_pending_range"), {"task_run_id": task_run_id}
    ).one_or_none()
    if row is None:
        return
    try:
        unchanged = as_utc(low) == as_utc(row.watermark_from) and as_utc(high) == as_utc(
            row.pending_upper_bound
        )
    except ValueError as error:
        raise RunRefusedError(
            f"task_run_id={task_run_id}: invalid timestamp window [{low!r}, {high!r}); "
            "expected the admitted ISO timestamps; report the frozen window unchanged"
        ) from error
    if not unchanged:
        raise RunRefusedError(
            f"task_run_id={task_run_id}: reported window [{low}, {high}) differs from admitted "
            f"[{row.watermark_from}, {row.pending_upper_bound}); report the frozen window unchanged"
        )
