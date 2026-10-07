"""Five-field cron ticks, with one firing through timezone gaps and overlaps."""

from __future__ import annotations

import calendar
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from etl_craft.core.errors import MetadataError

MACROS = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@weekly": "0 0 * * SUN",
    "@monthly": "0 0 1 * *",
}


def timezone(name: str) -> ZoneInfo:
    """Resolve an IANA name, refusing an invalid schedule or project timezone."""
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as error:
        raise MetadataError(
            f"timezone={name!r}: expected an IANA name such as UTC; correct it"
        ) from error


@lru_cache(maxsize=256)
def parse(expr: str) -> tuple[tuple[tuple[int, ...], ...], bool]:
    """Validate fields and return their sorted values and the day-field OR rule."""
    fields = MACROS.get(expr.strip().lower(), expr).upper().split()
    try:
        if len(fields) != 5:
            raise ValueError("expected five fields")
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
    zone = timezone(tz) if isinstance(tz, str) else tz
    (minutes, hours, days, months, weekdays), day_or = parse(expr)
    day = instant.astimezone(zone).date()
    for _ in range(366 * 8 + 1):
        dom, dow = day.day in days, (day.weekday() + 1) % 7 in weekdays
        if day.month in months and ((dom or dow) if day_or else (dom and dow)):
            for hour in hours:
                for minute in minutes:
                    wall = datetime(day.year, day.month, day.day, hour, minute)
                    candidate = wall.replace(tzinfo=zone, fold=0).astimezone(UTC)
                    while candidate.astimezone(zone).replace(tzinfo=None) != wall:
                        wall += timedelta(minutes=1)
                        candidate = wall.replace(tzinfo=zone, fold=0).astimezone(UTC)
                    if candidate > instant.astimezone(UTC):
                        return candidate
        if day.year == 9999 and day.month == 12 and day.day == 31:
            break
        day += timedelta(days=1)
    raise MetadataError(
        f"RUN_SCHEDULE={expr!r}: no future tick in the Gregorian leap-day cycle; correct it"
    )
