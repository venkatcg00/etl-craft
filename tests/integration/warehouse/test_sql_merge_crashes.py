"""Real merge process loss converges on every warehouse's commit strategy."""

import pytest

from fixtures.sql_merges import MERGE_FAULTS, check_killed_merge


@pytest.mark.parametrize("kind,point", MERGE_FAULTS)
def test_killed_merge_converges_without_lost_or_doubled_rows(sql_world, kind, point):
    check_killed_merge(sql_world, kind, point)
