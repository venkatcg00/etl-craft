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
    assert Offset.from_stored(offset.datatype, offset.stored()) == offset


def test_an_offset_is_a_value_cast_to_its_declared_datatype():
    assert Offset(7, "number") == Offset.number(7)
    # A declared datatype is a cast, as CAST(value AS type) in SQL.
    assert Offset("7", "NUMBER").value == 7
    assert Offset(" 12.50 ", "NUMBER").value == Decimal("12.50")
    assert Offset(7.5, "NUMBER").value == Decimal("7.5")
    assert Offset(42, "TEXT").value == "42"
    assert Offset("2026-09-25T10:30:00+00:00", "TIMESTAMP").value == datetime(
        2026, 9, 25, 10, 30, tzinfo=UTC
    )
    assert Offset(datetime(2026, 9, 25, tzinfo=UTC), "TEXT").value == "2026-09-25T00:00:00+00:00"


@pytest.mark.parametrize(
    ("build", "message"),
    [
        (lambda: Offset("abc", "NUMBER"), "cannot cast str 'abc' to a NUMBER offset"),
        (lambda: Offset("nan", "NUMBER"), "cannot cast str 'nan' to a NUMBER offset"),
        (lambda: Offset(True, "NUMBER"), "cannot cast bool True to a NUMBER offset"),
        (lambda: Offset(None, "TEXT"), "cannot cast NoneType None to a TEXT offset"),
        (lambda: Offset("yesterday", "TIMESTAMP"), "cannot cast str 'yesterday' to a TIMESTAMP"),
        (lambda: Offset(5, "TIMESTAMP"), "cannot cast int 5 to a TIMESTAMP offset"),
        (lambda: Offset(1, "DATE"), "offset datatype 'DATE' is not one of NUMBER, TEXT, TIMESTAMP"),
        (lambda: Offset.from_stored("NUMBER", "abc"), "cannot cast str 'abc' to a NUMBER"),
    ],
)
def test_a_value_that_cannot_be_cast_fails(build, message):
    with pytest.raises(HandlerError, match=message):
        build()
