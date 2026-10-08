"""Task-count aggregation and serialization."""

from dataclasses import FrozenInstanceError, replace

import pytest

from etl_craft.core.counts import Counts
from etl_craft.handlers.registry import HandlerResult

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("counts", "writes"),
    [
        (Counts(), None),
        (Counts(insert_count=0), 0),
        (Counts(insert_count=7, update_count=2, delete_count=1), 10),
        (Counts(insert_count=7, rows_written=9), 9),
    ],
)
def test_counts_keep_unknown_zero_and_explicit_write_totals(counts, writes):
    assert counts.rows_written == writes
    assert counts.parameters()["rows_written"] == writes
    with pytest.raises(FrozenInstanceError):
        counts.rows_written = 99


def test_handler_counts_exclude_variables_and_offsets():
    result = HandlerResult(insert_count=2, variables={"done": object()})
    assert result.rows_written == 2
    assert replace(result, insert_count=4).rows_written == 4
    assert set(result.parameters()) == {
        "source_count",
        "target_count",
        "insert_count",
        "update_count",
        "delete_count",
        "rows_written",
    }
