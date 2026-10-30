"""Checks of business calendars and the schedules that use them, for validate and doctor."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta

from sqlalchemy.engine import Connection, Row

from etl_craft.core.calendars import NonBusinessDay, Schedule, parse_weekend
from etl_craft.core.cron import business_rule, parse, timezone
from etl_craft.core.errors import EtlCraftError
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.calendars import load_calendar
from etl_craft.engine.runlog import as_date

VALIDATE_DAYS = 30
"""``validate`` fails a calendar whose holiday list ends sooner than this."""
DOCTOR_DAYS = 60
"""``doctor`` reports a calendar whose holiday list ends sooner than this."""


def holiday_lists_ending(conn: Connection, now: datetime, days: int) -> list[tuple[str, str]]:
    """Return ``(calendar, problem)`` for active calendars whose holidays end within ``days``.

    A calendar with no holidays, only weekend days, has no list to end. A bad
    ``WEEKEND_DAYS`` value is a problem too.
    """
    problems = []
    for row in conn.execute(statement(conn, "project_calendars")):
        where = f"calendar {row.calendar_code}"
        try:
            parse_weekend(row.calendar_code, row.weekend_days)
        except EtlCraftError as error:
            problems.append((where, str(error)))
        if row.last_holiday is None:
            continue
        last = as_date(row.last_holiday)
        if last < (now + timedelta(days=days)).date():
            problems.append(
                (
                    where,
                    f"the holiday list ends on {last.isoformat()}, within {days} days; add the "
                    "next year's holidays to calendar_holidays.csv",
                )
            )
    return problems


def schedule_problems(
    conn: Connection, schedules: Iterable[Row[tuple[object, ...]]], default_zone: str, now: datetime
) -> list[tuple[str, str]]:
    """Return ``(pipeline, problem)`` for business-day schedules that cannot run.

    A business-day rule needs a calendar; a calendar must be active; and a schedule must run at
    least once in the next year rather than skip every tick.
    """
    problems = []
    for row in schedules:
        if row.run_schedule is None:
            continue
        try:
            parse(str(row.run_schedule))
            zone = timezone(str(row.schedule_timezone or default_zone))
        except EtlCraftError:
            continue  # validate reports a bad schedule setting with the others
        try:
            rule = business_rule(str(row.run_schedule))
            if row.calendar_id is None:
                if rule is not None:
                    problems.append(
                        (
                            str(row.pipeline_code),
                            f"RUN_SCHEDULE={row.run_schedule!r} uses business days, but the "
                            "pipeline names no CALENDAR_CODE; name its calendar",
                        )
                    )
                continue
            calendar = load_calendar(conn, int(str(row.calendar_id)), str(row.pipeline_code))
            policy = NonBusinessDay(row.non_business_day or NonBusinessDay.SKIP)
            plan = Schedule(str(row.run_schedule), zone, calendar, policy)
            if plan.first_run(now, 366) is None:
                problems.append(
                    (
                        str(row.pipeline_code),
                        f"RUN_SCHEDULE={row.run_schedule!r} with calendar {calendar.code} and "
                        f"NON_BUSINESS_DAY={policy} runs on no date in the next year; correct "
                        "the schedule or the calendar",
                    )
                )
        except EtlCraftError as error:
            problems.append((str(row.pipeline_code), str(error)))
    return problems
