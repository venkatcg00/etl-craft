"""Business calendars: which days a pipeline's schedule treats as business days.

A calendar closes its weekend days and its listed holidays; every other day is a business day.
A schedule names business days with ``core.cron`` rules (``3BD``, ``LBD``), or keeps an
ordinary cron rule and a ``NON_BUSINESS_DAY`` policy for the ticks that fall on closed days:

- ``SKIP``: the tick is recorded as ``SKIPPED``, with the calendar and date as its reason;
- ``NEXT_BUSINESS_DAY``: the tick moves to the same wall time on the next business day. A
  moved tick that meets one already scheduled there is the same run.

The schedule always returns the earliest tick after a given instant, so ticks stay in order
for catch-up and for the persisted cursor.
"""

from __future__ import annotations

import calendar as month_calendar
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from etl_craft.core.cron import business_rule, day_ticks, matches_day, next_after
from etl_craft.core.enums import NonBusinessDay
from etl_craft.core.errors import MetadataError

WEEKDAYS = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")
SEARCH_DAYS = 366 * 8 + 1
"""How far a search looks: a full Gregorian leap-day cycle of days, as ``core.cron`` does."""


@dataclass(frozen=True)
class BusinessCalendar:
    """A calendar's weekend days (``date.weekday()`` numbers) and named holidays."""

    code: str
    weekend: frozenset[int]
    holidays: Mapping[date, str | None] = field(default_factory=dict)

    def closed(self, day: date) -> str | None:
        """Why ``day`` is not a business day, naming the calendar and date, or ``None``."""
        if day in self.holidays:
            name = self.holidays[day]
            return f"calendar {self.code}: {day.isoformat()} is a holiday" + (
                f" ({name})" if name else ""
            )
        if day.weekday() in self.weekend:
            return (
                f"calendar {self.code}: {day.isoformat()} is a weekend day "
                f"({WEEKDAYS[day.weekday()]})"
            )
        return None

    def next_business_day(self, day: date) -> date:
        """Return the first business day after ``day``."""
        for _ in range(SEARCH_DAYS):
            day += timedelta(days=1)
            if self.closed(day) is None:
                return day
        raise MetadataError(f"calendar {self.code} has no business day after {day.isoformat()}")

    def business_days(self, year: int, month: int) -> list[date]:
        """Return the month's business days, in order."""
        last = month_calendar.monthrange(year, month)[1]
        days = (date(year, month, number) for number in range(1, last + 1))
        return [day for day in days if self.closed(day) is None]


def parse_weekend(code: str, written: str | None) -> frozenset[int]:
    """Read ``WEEKEND_DAYS`` such as ``SAT|SUN``; empty means no weekend."""
    names = [name.strip().upper() for name in (written or "").split("|") if name.strip()]
    unknown = [name for name in names if name not in WEEKDAYS]
    if unknown or len(set(names)) != len(names) or len(names) == len(WEEKDAYS):
        raise MetadataError(
            f"calendar {code}: WEEKEND_DAYS={written!r}; expected distinct day names from "
            f"{', '.join(WEEKDAYS)} separated by '|', leaving at least one business weekday"
        )
    return frozenset(WEEKDAYS.index(name) for name in names)


@dataclass(frozen=True)
class Tick:
    """One scheduled tick: when it is due, the run date it runs as, and why it is skipped."""

    instant: datetime
    run_date: date
    skipped: str | None = None


@dataclass(frozen=True)
class Schedule:
    """A pipeline's cron rule, timezone and, optionally, its calendar and closed-day policy."""

    expr: str
    zone: ZoneInfo
    calendar: BusinessCalendar | None = None
    policy: NonBusinessDay = NonBusinessDay.SKIP

    def next_tick(self, instant: datetime) -> Tick:
        """Return the earliest tick strictly after the aware ``instant``."""
        calendar = self.calendar
        if calendar is None:
            tick = next_after(self.expr, instant, self.zone)
            return Tick(tick, tick.astimezone(self.zone).date())
        rule = business_rule(self.expr)
        day = instant.astimezone(self.zone).date()
        for _ in range(SEARCH_DAYS):
            due = self._due(calendar, rule, day)
            if due is not None:
                for candidate in day_ticks(self.expr, day, self.zone):
                    if candidate > instant:
                        return Tick(candidate, day, due or None)
            if day == date.max:
                break
            day += timedelta(days=1)
        raise MetadataError(
            f"RUN_SCHEDULE={self.expr!r} with calendar {calendar.code}: no future tick; "
            "correct the schedule or the calendar"
        )

    def _due(
        self, calendar: BusinessCalendar, rule: tuple[frozenset[int], bool] | None, day: date
    ) -> str | None:
        """Return ``""`` when ``day`` has ticks to run, a reason when they are skipped.

        ``None`` means the schedule has no ticks on ``day``.
        """
        closed = calendar.closed(day)
        if rule is not None:
            if closed is not None or not matches_day(self.expr, day):
                return None
            ordinals, last = rule
            business = calendar.business_days(day.year, day.month)
            number = business.index(day) + 1
            return "" if number in ordinals or (last and day == business[-1]) else None
        if self.policy == NonBusinessDay.SKIP:
            return (closed or "") if matches_day(self.expr, day) else None
        if closed is not None:
            return None
        previous = day - timedelta(days=1)
        moved_here = False
        for _ in range(SEARCH_DAYS):
            if calendar.closed(previous) is None:
                break
            moved_here = moved_here or matches_day(self.expr, previous)
            previous -= timedelta(days=1)
        return "" if moved_here or matches_day(self.expr, day) else None

    def first_run(self, instant: datetime, days: int) -> Tick | None:
        """Return the first tick after ``instant`` that runs, if one falls within ``days``."""
        horizon = instant + timedelta(days=days)
        tick = self.next_tick(instant)
        while tick.instant <= horizon:
            if tick.skipped is None:
                return tick
            tick = self.next_tick(
                datetime.combine(tick.run_date + timedelta(days=1), datetime.min.time(), self.zone)
                - timedelta(microseconds=1)
            )
        return None
