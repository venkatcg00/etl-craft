"""Cron field syntax and one UTC tick through civil-time changes."""

from datetime import datetime

import pytest

from etl_craft.core.cron import next_after, parse
from etl_craft.core.errors import MetadataError

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("expr", "instant", "zone", "expected"),
    [
        (
            "*/15 9-17 * JAN,MAR MON-FRI",
            "2026-01-02T09:01:00+00:00",
            "UTC",
            "2026-01-02T09:15:00+00:00",
        ),
        ("@hourly", "2026-01-02T09:00:00+00:00", "UTC", "2026-01-02T10:00:00+00:00"),
        ("@daily", "2026-01-02T09:00:00+00:00", "Asia/Kolkata", "2026-01-02T18:30:00+00:00"),
        ("@weekly", "2026-01-03T09:00:00+00:00", "UTC", "2026-01-04T00:00:00+00:00"),
        ("@yearly", "2026-01-02T09:00:00+00:00", "UTC", "2027-01-01T00:00:00+00:00"),
        ("@annually", "2026-01-02T09:00:00+00:00", "UTC", "2027-01-01T00:00:00+00:00"),
        ("@monthly", "2026-01-02T09:00:00+00:00", "UTC", "2026-02-01T00:00:00+00:00"),
        ("0 0 29 FEB *", "2097-01-01T00:00:00+00:00", "UTC", "2104-02-29T00:00:00+00:00"),
        ("0 0 1 * FRI", "2026-01-02T00:00:00+00:00", "UTC", "2026-01-09T00:00:00+00:00"),
        ("0 0 * * 7", "2026-01-03T00:00:00+00:00", "UTC", "2026-01-04T00:00:00+00:00"),
        (
            "30 2 * * *",
            "2026-03-08T06:00:00+00:00",
            "America/New_York",
            "2026-03-08T07:00:00+00:00",
        ),
        (
            "30 1 * * *",
            "2026-11-01T04:00:00+00:00",
            "America/New_York",
            "2026-11-01T05:30:00+00:00",
        ),
        (
            "30 1 * * *",
            "2026-11-01T05:30:00+00:00",
            "America/New_York",
            "2026-11-02T06:30:00+00:00",
        ),
        ("30 1 * * *", "2026-03-29T00:00:00+00:00", "Europe/London", "2026-03-29T01:00:00+00:00"),
        ("30 1 * * *", "2026-10-25T00:30:00+00:00", "Europe/London", "2026-10-26T01:30:00+00:00"),
    ],
)
def test_next_tick(expr, instant, zone, expected):
    assert next_after(expr, datetime.fromisoformat(instant), zone) == datetime.fromisoformat(
        expected
    )


@pytest.mark.parametrize(
    "expr",
    [
        "0 0 30 FEB *",
        "60 * * * *",
        "0 24 * * *",
        "* * * *",
        "@unsupported",
        "*/0 * * * *",
        "0 0 * FOO *",
        "0 0 * * SAT-MON",
        "0,,1 * * * *",
    ],
)
def test_invalid_cron_names_exact_expression(expr):
    with pytest.raises(MetadataError, match="RUN_SCHEDULE="):
        parse(expr)


def test_instant_and_timezone_validation():
    with pytest.raises(MetadataError, match="aware"):
        next_after("@daily", datetime(2026, 1, 1), "UTC")
    with pytest.raises(MetadataError, match="IANA"):
        next_after("@daily", datetime.fromisoformat("2026-01-01T00:00:00+00:00"), "missing/zone")
