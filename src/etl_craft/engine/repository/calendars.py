"""Read a project's business calendars from the Engine DB."""

from __future__ import annotations

from sqlalchemy.engine import Connection

from etl_craft.core.calendars import BusinessCalendar, parse_weekend
from etl_craft.core.enums import YesNo
from etl_craft.core.errors import MetadataError
from etl_craft.engine.queries import statement
from etl_craft.engine.runlog import as_date


def load_calendar(conn: Connection, calendar_id: int, pipeline_code: str) -> BusinessCalendar:
    """Return calendar ``calendar_id`` with its active holidays; refuse a retired calendar."""
    row = conn.execute(
        statement(conn, "calendar_definition"), {"calendar_id": calendar_id}
    ).one_or_none()
    if row is None or row.active_flag != YesNo.YES:
        named = f"calendar_id={calendar_id}" if row is None else f"calendar {row.calendar_code}"
        raise MetadataError(
            f"{pipeline_code}: CALENDAR names {named}, which is missing or inactive; set the "
            "calendar's ACTIVE_FLAG to Y, or clear the pipeline's CALENDAR_CODE"
        )
    holidays = {
        as_date(holiday.holiday_date): holiday.holiday_name
        for holiday in conn.execute(
            statement(conn, "calendar_holidays"), {"calendar_id": calendar_id}
        )
    }
    return BusinessCalendar(
        row.calendar_code, parse_weekend(row.calendar_code, row.weekend_days), holidays
    )
