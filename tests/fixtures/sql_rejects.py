"""REJECT_LIMIT assertions shared by the local and cloud warehouse suites."""

from datetime import date

import pytest

from etl_craft.core.errors import HandlerError

SHAPE = "SELECT CAST(1 AS BIGINT) AS id, CAST(NULL AS DATE) AS order_date, 'Pune' AS city"
"""A target's shape: a key, a date and a text column."""

MESSY = (
    "SELECT CAST(1 AS BIGINT) AS id, '2026-01-31' AS order_date, 'Pune' AS city "
    "UNION ALL SELECT 2, '2026-02-30', 'Delhi' "
    "UNION ALL SELECT NULL, '2026-01-01', 'Agra' "
    "UNION ALL SELECT 3, '2026-01-02', 'Goa' "
    "UNION ALL SELECT 3, '2026-01-03', 'Goa' "
    "UNION ALL SELECT 4, '2026-01-04', 'Leh'"
)
"""Two good rows, a bad date, a NULL key and a key returned twice."""

MERGE = {"MERGE_KEY": "id", "MERGE_COMPARE_COLUMNS": "order_date|city"}


def set_aside(w, target):
    """Return ``<target>__rejects`` as (id, order_date, column, reason, task run), lower case."""
    rows = w.rows(
        "SELECT id, order_date, REJECT_COLUMN, REJECT_REASON, TASK_RUN_ID "
        f"FROM {w.name(target + '__rejects')}"
    )
    return sorted(
        ((row[0], row[1], row[2].lower(), row[3].lower(), row[4]) for row in rows), key=str
    )


def loaded(w, target):
    """Return the target's rows as (id, order_date, city)."""
    return sorted(w.rows(f"SELECT id, order_date, city FROM {w.name(target)}"), key=str)


REJECTS_COLUMNS = [
    "id",
    "order_date",
    "city",
    "pipeline_run_id",
    "pipeline_id",
    "task_run_id",
    "create_date",
    "created_by",
    "reject_reason",
    "reject_column",
]


def check_rejects(w, target, merge="SCD1_MERGE"):
    """The rows that do not fit always wait aside; the good ones load only within the limit."""
    params = {**MERGE, "SQL_ACTION": merge, "TARGET_OBJECT": target}
    w.setup(target, SHAPE, merge)
    with pytest.raises(
        HandlerError,
        match=r"4 of 6 row\(s\) do not fit, more than REJECT_LIMIT=3 allows \(3\), so the target",
    ):
        w.run("load", **params, SELECT_QUERY=MESSY, REJECT_LIMIT="3")
    assert loaded(w, target) == []
    run = w.task("load").task_run_id
    messy = [
        ("2", "2026-02-30", "order_date", "does not cast to date", run),
        ("3", "2026-01-02", "id", "merge_key returned more than once", run),
        ("3", "2026-01-03", "id", "merge_key returned more than once", run),
        (None, "2026-01-01", "id", "null in merge_key", run),
    ]
    assert set_aside(w, target) == messy
    assert w.columns(target + "__rejects") == REJECTS_COLUMNS

    result = w.run("load", **params, SELECT_QUERY=MESSY, REJECT_LIMIT="67%")
    assert (result.source_count, result.insert_count, result.reject_count) == (6, 2, 4)
    assert loaded(w, target) == [(1, date(2026, 1, 31), "Pune"), (4, date(2026, 1, 4), "Leh")]
    assert set_aside(w, target) == messy
    stamped = w.rows(
        "SELECT DISTINCT pipeline_run_id, pipeline_id, create_date IS NOT NULL "
        f"FROM {w.name(target + '__rejects')}"
    )
    assert stamped == [(w.pipeline_run_id, w.pipeline_id, True)]

    retry = "SELECT CAST(2 AS BIGINT) AS id, '2026-02-30' AS order_date, 'Delhi' AS city"
    assert w.run("load", **params, SELECT_QUERY=retry, REJECT_LIMIT="1").reject_count == 1
    assert set_aside(w, target) == [("2", "2026-02-30", "order_date", "does not cast to date", run)]
