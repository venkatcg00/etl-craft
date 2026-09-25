"""The ingestion-script contract's offsets."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from etl_craft.core.errors import HandlerError
from etl_craft.scripting import Offset

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "offset",
    [
        Offset.number(42),
        Offset.number(Decimal("12.50")),
        Offset.text("cursor-9"),
        Offset.timestamp(datetime(2026, 9, 25, 10, 30, tzinfo=UTC)),
    ],
)
def test_an_offset_is_stored_and_read_back_unchanged(offset):
    assert Offset.from_stored(offset.type, offset.stored()) == offset


@pytest.mark.parametrize(
    ("build", "message"),
    [
        (lambda: Offset.number(True), "Offset.number needs an int or Decimal"),
        (lambda: Offset.number("7"), "Offset.number needs an int or Decimal"),
        (lambda: Offset.text(7), "Offset.text needs a str"),
        (lambda: Offset.timestamp("2026-01-01"), "Offset.timestamp needs a datetime"),
        (lambda: Offset.from_stored("NUMBER", "abc"), "the stored offset 'abc' is not a valid"),
        (lambda: Offset.from_stored("DATE", "x"), "is not a valid DATE"),
    ],
)
def test_a_wrong_offset_says_what_it_needs(build, message):
    with pytest.raises(HandlerError, match=message):
        build()
