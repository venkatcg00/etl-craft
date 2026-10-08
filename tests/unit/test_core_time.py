"""Database timestamp forms identify the same UTC instant."""

from datetime import UTC, datetime

import pytest

from etl_craft.core.time import as_utc

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "value",
    [
        datetime(2026, 10, 8, 12),
        datetime(2026, 10, 8, 12, tzinfo=UTC),
        "2026-10-08T12:00:00",
        "2026-10-08T17:30:00+05:30",
    ],
)
def test_timestamps_normalize_to_the_same_instant(value):
    assert as_utc(value) == datetime(2026, 10, 8, 12, tzinfo=UTC)
