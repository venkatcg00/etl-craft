"""The complete PostgreSQL SCD1 action updates a large changed stage within its budget."""

import time

import pytest

pytestmark = pytest.mark.warehouse_postgres


@pytest.mark.parametrize("sql_world", ["postgres"], indirect=True)
def test_scd1_updates_100000_changed_rows_in_under_30_seconds(sql_world):
    w = sql_world
    source = (
        "SELECT id, CAST('old' AS VARCHAR(20)) AS name FROM generate_series(1, 100000) AS g(id)"
    )
    w.setup("people", source, "SCD1_MERGE")
    params = {
        "SQL_ACTION": "SCD1_MERGE",
        "TARGET_OBJECT": "people",
        "MERGE_KEY": "id",
        "MERGE_COMPARE_COLUMNS": "name",
        "SOURCE_SQL": source,
    }
    inserted = w.run("merge", **params)
    assert inserted.insert_count == 100000
    started = time.monotonic()
    updated = w.run("merge", **{**params, "SOURCE_SQL": source.replace("'old'", "'new'")})
    elapsed = time.monotonic() - started
    assert elapsed < 30, f"100,000-row SCD1 took {elapsed:.2f}s"
    assert updated.update_count == 100000 and updated.insert_count == 0
    assert w.rows(
        f"SELECT COUNT(*) FROM {w.name('people')} WHERE name = 'new' AND delete_flag = 'N'"
    ) == [(100000,)]
