"""Shared persisted schedule boundaries for controller ticks and explicit resume."""

from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy.engine import RowMapping

from etl_craft.core.cron import timezone
from etl_craft.core.errors import MetadataError
from etl_craft.core.time import as_utc
from etl_craft.engine.runlog import as_date


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
