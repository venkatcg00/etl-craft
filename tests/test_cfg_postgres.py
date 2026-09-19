"""Integration tests for etl_craft.cfg against a real Postgres.

Purely SELECT-based against sql/schema.sql, so there's no separate
non-Postgres logic worth stand-in testing — see tests/conftest.py for how
the Postgres connection is resolved (and skipped if none is reachable).
"""

import pytest
from sqlalchemy import text

from etl_craft.cfg import (
    CfgError,
    fetch_pipeline_graph,
    fetch_task_handler,
    resolve_pipeline_id,
    resolve_task_id,
)


def test_resolve_pipeline_id(pg_conn, cfg_pipeline):
    assert resolve_pipeline_id(pg_conn, "TEST_PL") == cfg_pipeline


def test_resolve_pipeline_id_unknown_code_raises(pg_conn):
    with pytest.raises(CfgError):
        resolve_pipeline_id(pg_conn, "NO_SUCH_PIPELINE")


def test_resolve_pipeline_id_ignores_inactive(pg_conn, cfg_pipeline):
    pg_conn.execute(
        text("UPDATE CFG_PIPELINES SET ACTIVE_FLAG = 'N' WHERE PIPELINE_ID = :id"),
        {"id": cfg_pipeline},
    )
    with pytest.raises(CfgError):
        resolve_pipeline_id(pg_conn, "TEST_PL")


def test_resolve_task_id(pg_conn, cfg_pipeline, cfg_task):
    assert resolve_task_id(pg_conn, cfg_pipeline, "test_task") == cfg_task


def test_resolve_task_id_unknown_code_raises(pg_conn, cfg_pipeline):
    with pytest.raises(CfgError):
        resolve_task_id(pg_conn, cfg_pipeline, "no_such_task")


def test_fetch_task_handler(pg_conn, cfg_task):
    assert fetch_task_handler(pg_conn, cfg_task) == "SQL"


def test_fetch_pipeline_graph_no_dependencies(pg_conn, cfg_pipeline, cfg_task):
    data = fetch_pipeline_graph(pg_conn, cfg_pipeline)
    assert [t.task_id for t in data.tasks] == [cfg_task]
    assert data.same_pipeline_edges == []
    assert data.cross_pipeline_task_ids == frozenset()


def test_fetch_pipeline_graph_same_pipeline_edge(pg_conn, cfg_pipeline, cfg_task):
    task_b = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('task_b', 'ETL', :pipeline_id, 'SQL') RETURNING TASK_ID"
        ),
        {"pipeline_id": cfg_pipeline},
    ).scalar_one()
    pg_conn.execute(
        text(
            "INSERT INTO CFG_TASK_DEPENDENCY "
            "(PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) "
            "VALUES (:pipeline_id, :task_b, :pipeline_id, :cfg_task, 'SUCCESS')"
        ),
        {"pipeline_id": cfg_pipeline, "task_b": task_b, "cfg_task": cfg_task},
    )

    data = fetch_pipeline_graph(pg_conn, cfg_pipeline)
    assert {t.task_id for t in data.tasks} == {cfg_task, task_b}
    assert len(data.same_pipeline_edges) == 1
    edge = data.same_pipeline_edges[0]
    assert edge.task_id == task_b
    assert edge.depends_on_task_id == cfg_task
    assert edge.dependency_type == "SUCCESS"
    assert data.cross_pipeline_task_ids == frozenset()


def test_fetch_pipeline_graph_cross_pipeline_edge_excluded(pg_conn, cfg_pipeline, cfg_task):
    other_pipeline = pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
            "VALUES ('TEST_OTHER_PL', 'Other Pipeline', 'FULL') RETURNING PIPELINE_ID"
        )
    ).scalar_one()
    other_task = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('other_task', 'ETL', :pipeline_id, 'SQL') RETURNING TASK_ID"
        ),
        {"pipeline_id": other_pipeline},
    ).scalar_one()
    pg_conn.execute(
        text(
            "INSERT INTO CFG_TASK_DEPENDENCY "
            "(PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) "
            "VALUES (:pipeline_id, :task_id, :other_pipeline, :other_task, 'SUCCESS')"
        ),
        {
            "pipeline_id": cfg_pipeline,
            "task_id": cfg_task,
            "other_pipeline": other_pipeline,
            "other_task": other_task,
        },
    )

    data = fetch_pipeline_graph(pg_conn, cfg_pipeline)
    assert data.same_pipeline_edges == []
    assert data.cross_pipeline_task_ids == frozenset({cfg_task})
