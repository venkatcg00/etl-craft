"""Shared persisted schedule boundaries for controller ticks and explicit resume."""

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy.engine import Connection, RowMapping

from etl_craft.core.actor import Actor
from etl_craft.core.calendars import Schedule
from etl_craft.core.cron import timezone
from etl_craft.core.enums import InterventionAction, NonBusinessDay, RunStatus, TriggerKind
from etl_craft.core.errors import MetadataError
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.calendars import load_calendar
from etl_craft.engine.repository.intervention_rows import record_intervention
from etl_craft.engine.run_creation import RunCreation, create
from etl_craft.engine.runlog import as_date


def schedule(conn: Connection, row: RowMapping, zone: ZoneInfo) -> Schedule:
    """Return a ``controller_schedules`` row's schedule, with its calendar and policy."""
    calendar = (
        None
        if row["calendar_id"] is None
        else load_calendar(conn, int(row["calendar_id"]), row["pipeline_code"])
    )
    policy = NonBusinessDay(row["non_business_day"] or NonBusinessDay.SKIP)
    return Schedule(row["run_schedule"], zone, calendar, policy)


def cursor(row: RowMapping, default_zone: str, now: datetime) -> tuple[ZoneInfo, datetime]:
    """Validate the persisted schedule boundary before finding its next tick."""
    try:
        zone = timezone(
            row["schedule_timezone"] if row["schedule_timezone"] is not None else default_zone
        )
        if row["schedule_start_date"] is not None:
            start = as_date(row["schedule_start_date"])
            cursor = datetime.combine(start, time(), zone).astimezone(UTC) - timedelta(
                microseconds=1
            )
        else:
            cursor = as_utc(row["create_date"]) if row["create_date"] is not None else now
    except ValueError as error:
        raise MetadataError(
            f"{row['pipeline_code']}: SCHEDULE_START_DATE={row['schedule_start_date']!r}; "
            "expected YYYY-MM-DD; correct the schedule"
        ) from error
    if row.get("resumed_at") is not None:
        cursor = max(cursor, as_utc(row["resumed_at"]))
    key = row["last_run_key"]
    if key is not None:
        problem = (
            f"{row['pipeline_code']}: scheduled RUN_KEY={key!r}; "
            "expected schedule:<UTC ISO instant>; repair the historical key"
        )
        try:
            previous = datetime.fromisoformat(key.removeprefix("schedule:"))
        except ValueError as error:
            raise MetadataError(problem) from error
        if previous.utcoffset() != timedelta(0) or not key.startswith("schedule:"):
            raise MetadataError(problem)
        cursor = max(cursor, previous)
    return zone, cursor


def backfill_dates(
    conn: Connection, pipeline_id: int, pipeline_code: str, first: date, last: date
) -> list[tuple[date, str | None]]:
    """Return a backfill's run dates, oldest first, each with the reason it is skipped.

    Without a calendar every date runs. With ``SKIP`` a closed date is skipped, naming the
    calendar and date; with ``NEXT_BUSINESS_DAY`` it runs as the next business day, once, even
    when that day is after ``last``.
    """
    days = [first + timedelta(days=offset) for offset in range((last - first).days + 1)]
    row = conn.execute(statement(conn, "pipeline_calendar"), {"pipeline_id": pipeline_id}).one()
    if row.calendar_id is None:
        return [(day, None) for day in days]
    calendar = load_calendar(conn, int(row.calendar_id), pipeline_code)
    if NonBusinessDay(row.non_business_day) == NonBusinessDay.NEXT_BUSINESS_DAY:
        moved = {
            day if calendar.closed(day) is None else calendar.next_business_day(day) for day in days
        }
        return [(day, None) for day in sorted(moved)]
    return [(day, calendar.closed(day)) for day in days]


def record_skipped_backfill(
    conn: Connection, pipeline_id: int, run_date: date, reason: str, actor: Actor
) -> int:
    """Record a backfill date the pipeline's calendar closes as a ``SKIPPED`` run."""
    run_id = create(
        conn,
        pipeline_id,
        actor,
        RunCreation(run_date=run_date, trigger_kind=TriggerKind.BACKFILL, status=RunStatus.SKIPPED),
    )
    record_intervention(
        conn,
        pipeline_id=pipeline_id,
        pipeline_run_id=run_id,
        action=InterventionAction.NEW_RUN,
        reason=reason,
        requested_by=actor.name,
        to_status=RunStatus.SKIPPED,
    )
    return run_id
