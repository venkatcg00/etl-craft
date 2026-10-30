"""Five-field cron ticks, with one firing through timezone gaps and overlaps.

The day-of-month field may instead hold business-day rules, ``3BD`` (the third business day
of the month) or ``LBD`` (the last), comma-separated, with day-of-week ``*``. Those need the
pipeline's business calendar (``core.calendars``); ``next_after`` refuses them.
"""

from __future__ import annotations

import calendar
import re
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from etl_craft.core.errors import MetadataError

MACROS = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@weekly": "0 0 * * SUN",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}
BUSINESS_DAY_TOKEN = re.compile(r"(?:[1-9]|1[0-9]|2[0-3])BD|LBD")


def timezone(name: str) -> ZoneInfo:
    """Resolve an IANA name, refusing an invalid schedule or project timezone."""
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as error:
        raise MetadataError(
            f"timezone={name!r}: expected an IANA name such as UTC; correct it"
        ) from error


def _fields(expr: str) -> list[str]:
    return MACROS.get(expr.strip().lower(), expr).upper().split()


@lru_cache(maxsize=256)
def business_rule(expr: str) -> tuple[frozenset[int], bool] | None:
    """Return a business-day schedule's ordinals and whether ``LBD`` is set, or ``None``."""
    fields = _fields(expr)
    if len(fields) != 5:
        return None
    tokens = fields[2].split(",")
    if not any(token.endswith("BD") for token in tokens):
        return None
    if not all(BUSINESS_DAY_TOKEN.fullmatch(token) for token in tokens):
        raise MetadataError(
            f"RUN_SCHEDULE={expr!r}: business-day rules are 1BD to 23BD or LBD, comma-separated; "
            "correct the day-of-month field"
        )
    if fields[4] != "*":
        raise MetadataError(
            f"RUN_SCHEDULE={expr!r}: a business-day rule needs day-of-week '*'; the calendar "
            "decides which days are business days"
        )
    ordinals = frozenset(int(token[:-2]) for token in tokens if token != "LBD")
    return ordinals, "LBD" in tokens


@lru_cache(maxsize=256)
def parse(expr: str) -> tuple[tuple[tuple[int, ...], ...], bool]:
    """Validate fields and return their sorted values and the day-field OR rule.

    A business-day rule parses as every day of the month; ``business_rule`` narrows it.
    """
    fields = _fields(expr)
    try:
        if len(fields) != 5:
            raise ValueError("expected five fields")
        if business_rule(expr) is not None:
            fields[2] = "*"
        values = []
        for field, (low, high), names in zip(
            fields,
            ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7)),
            (
                {},
                {},
                {},
                {
                    n: i
                    for i, n in enumerate(
                        [
                            "JAN",
                            "FEB",
                            "MAR",
                            "APR",
                            "MAY",
                            "JUN",
                            "JUL",
                            "AUG",
                            "SEP",
                            "OCT",
                            "NOV",
                            "DEC",
                        ],
                        1,
                    )
                },
                {n: i for i, n in enumerate(("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"))},
            ),
            strict=True,
        ):
            selected: set[int] = set()

            def number(raw: str, names: dict[str, int] = names) -> int:
                return names[raw] if raw in names else int(raw)

            for part in field.split(","):
                base, sep, step_raw = part.partition("/")
                step = int(step_raw) if sep else 1
                if step <= 0:
                    raise ValueError("step must be positive")
                if base == "*":
                    start, end = low, high
                elif "-" in base:
                    first, last = base.split("-")
                    start, end = number(first), number(last)
                else:
                    start = number(base)
                    end = high if sep else start
                if not low <= start <= end <= high:
                    raise ValueError(f"{field!r} is outside {low}..{high}")
                selected.update(range(start, end + 1, step))
            if high == 7:
                selected = {n % 7 for n in selected}
            values.append(tuple(sorted(selected)))
        day_or = "*" not in fields[2] and "*" not in fields[4]
        if not day_or and not any(
            day <= calendar.monthrange(2000, month)[1] for month in values[3] for day in values[2]
        ):
            raise ValueError("no calendar date matches")
        return tuple(values), day_or
    except (ValueError, KeyError) as error:
        raise MetadataError(
            f"RUN_SCHEDULE={expr!r}: {error}; use five-field cron or a supported macro"
        ) from error


def next_after(expr: str, instant: datetime, tz: str | ZoneInfo) -> datetime:
    """Return the first tick strictly after an aware instant, normalized to UTC.

    Restricted day-of-month and day-of-week fields match either day. A missing wall minute
    fires at the next valid minute; an ambiguous wall minute uses its first occurrence.
    """
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise MetadataError("cron instant must include a timezone; supply an aware UTC datetime")
    if business_rule(expr) is not None:
        raise MetadataError(
            f"RUN_SCHEDULE={expr!r} uses business days; name the pipeline's CALENDAR"
        )
    zone = timezone(tz) if isinstance(tz, str) else tz
    day = instant.astimezone(zone).date()
    for _ in range(366 * 8 + 1):
        if matches_day(expr, day):
            for candidate in day_ticks(expr, day, zone):
                if candidate > instant.astimezone(UTC):
                    return candidate
        if day == date.max:
            break
        day += timedelta(days=1)
    raise MetadataError(
        f"RUN_SCHEDULE={expr!r}: no future tick in the Gregorian leap-day cycle; correct it"
    )


def matches_day(expr: str, day: date) -> bool:
    """Whether the schedule's month and day fields select ``day``."""
    (_, _, days, months, weekdays), day_or = parse(expr)
    dom, dow = day.day in days, (day.weekday() + 1) % 7 in weekdays
    return day.month in months and ((dom or dow) if day_or else (dom and dow))


def day_ticks(expr: str, day: date, zone: ZoneInfo) -> list[datetime]:
    """Return the schedule's UTC instants on the wall-clock ``day``, in order.

    A wall minute missing in a timezone gap fires at the next valid minute.
    """
    (minutes, hours, _, _, _), _ = parse(expr)
    ticks = []
    for hour in hours:
        for minute in minutes:
            wall = datetime(day.year, day.month, day.day, hour, minute)
            candidate = wall.replace(tzinfo=zone, fold=0).astimezone(UTC)
            while candidate.astimezone(zone).replace(tzinfo=None) != wall:
                wall += timedelta(minutes=1)
                candidate = wall.replace(tzinfo=zone, fold=0).astimezone(UTC)
            ticks.append(candidate)
    return ticks
