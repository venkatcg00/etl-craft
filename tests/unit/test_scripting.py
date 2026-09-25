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


def test_an_offset_is_a_value_and_its_datatype():
    offset = Offset(7, "number")
    assert (offset.value, offset.datatype) == (7, "NUMBER")
    assert offset == Offset.number(7)


@pytest.mark.parametrize(
    ("build", "message"),
    [
        # Nothing is converted: the value must already be of its datatype.
        (lambda: Offset("7", "NUMBER"), "a NUMBER offset needs a int or Decimal value, got str"),
        (lambda: Offset(7.5, "NUMBER"), "got float 7.5; the engine does not convert it"),
        (lambda: Offset.number(True), "a NUMBER offset needs a int or Decimal value, got bool"),
        (lambda: Offset.text(7), "a TEXT offset needs a str value, got int"),
        (lambda: Offset("2026-01-01", "TIMESTAMP"), "a TIMESTAMP offset needs a datetime value"),
        (lambda: Offset(1, "DATE"), "offset datatype 'DATE' is not one of NUMBER, TEXT, TIMESTAMP"),
        (lambda: Offset.from_stored("NUMBER", "abc"), "the stored offset 'abc' is not a valid"),
    ],
)
def test_a_wrong_offset_says_what_it_needs(build, message):
    with pytest.raises(HandlerError, match=message):
        build()
