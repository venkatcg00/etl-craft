"""Portable snapshots retain exact values and refuse corrupt persisted shapes."""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from etl_craft.core.archive import ArchiveBatch, ArchiveColumn, ArchivePart, bound, encoded, kind_of
from etl_craft.core.enums import ArchiveKind, ArchiveStep
from etl_craft.core.errors import ArchiveError

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        123,
        "λ'",
        Decimal("1234567890.12345678901234567890"),
        0.12345678901234567,
        datetime(2000, 1, 1, tzinfo=UTC),
        date(2000, 1, 1),
        {"nested": [None, "λ"]},
        b"\x00\xff",
    ],
)
def test_scalar_encoding_is_exact_and_warehouse_roundtrips(value):
    column = ArchiveColumn("VALUE", kind_of(type(value)))
    frozen = encoded(column, value)
    assert encoded(column, bound(column, frozen), warehouse=True) == frozen
    if isinstance(value, Decimal):
        assert frozen == "1234567890.12345678901234567890"
    if isinstance(value, bytes):
        assert frozen == "AP8="


def test_frozen_batch_restores_types_keys_and_content_version():
    part = ArchivePart("AUD_SAMPLE", (ArchiveColumn("ID", ArchiveKind.INTEGER),), ("ID",), ((1,),))
    batch = ArchiveBatch(
        "batch",
        "origin",
        "0057",
        "destination",
        "history",
        ArchiveStep.DELETE,
        "retention_aud_sample",
        None,
        (part,),
    )
    assert ArchiveBatch.load(batch.dump()) == batch
    assert part.key((1,)) == (1,)
    assert part.digest((1,), "0057") != part.digest((1,), "0058")


@pytest.mark.parametrize("value", ["not json", "[]", "{}", '{"step":"UNKNOWN"}'])
def test_corrupt_pending_body_is_refused(value):
    with pytest.raises(ArchiveError, match="BODY is invalid"):
        ArchiveBatch.load(value)


@pytest.mark.parametrize("name", ["x; DELETE FROM AUD_ACTIONS", "needs space", "../outside"])
def test_source_column_identifier_is_refused(name):
    with pytest.raises(ArchiveError, match="expected a SQL identifier"):
        ArchiveColumn(name, ArchiveKind.TEXT)


def test_source_shape_requires_a_real_source_key():
    with pytest.raises(ArchiveError, match="invalid source shape"):
        ArchivePart("CFG_SAMPLE", (ArchiveColumn("ID", ArchiveKind.INTEGER),), (), ((1,),))
