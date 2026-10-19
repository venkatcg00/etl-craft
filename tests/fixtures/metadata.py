"""Rows for tests: pipelines, tasks, dependencies, and finished runs of them."""

from sqlalchemy import inspect, text

from etl_craft.core.counts import Counts
from etl_craft.engine import runlog, transitions


def insert(conn, sql, id_column, **params):
    return conn.execute(text(f"{sql} RETURNING {id_column}"), params).scalar_one()


def add_pipeline(conn, code, *, refresh_type="FULL", sla_in_hours=None):
    return insert(
        conn,
        "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE, SLA_IN_HOURS) "
        "VALUES (:code, :code, :refresh, :sla)",
        "PIPELINE_ID",
        code=code,
        refresh=refresh_type,
        sla=sla_in_hours,
    )


def add_task(conn, pipeline_id, code, handler="SQL", **params):
    task_id = insert(
        conn,
        "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
        "VALUES (:code, 'ETL', :pipeline, :handler)",
        "TASK_ID",
        code=code,
        pipeline=pipeline_id,
        handler=handler,
    )
    params = dict(params)
    target = params.get("TARGET_OBJECT")
    task_name = "cfg_tasks" if conn.dialect.name == "postgresql" else "CFG_TASKS"
    if target is not None and any(
        column["name"].upper() == "TARGET_TABLE_ID"
        for column in inspect(conn).get_columns(task_name)
    ):
        set_task_target(conn, task_id, params.pop("TARGET_OBJECT"))
    for name, value in params.items():
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) "
                "VALUES (:task, :name, :value)"
            ),
            {"task": task_id, "name": name, "value": str(value)},
        )
    return task_id


def add_dependency(
    conn, pipeline_id, task_id, depends_on, kind="SUCCESS", *, upstream_pipeline=None
):
    return insert(
        conn,
        "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, "
        "DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) VALUES (:p, :t, :up, :u, :k)",
        "TASK_DEPENDENCY_ID",
        p=pipeline_id,
        t=task_id,
        up=upstream_pipeline or pipeline_id,
        u=depends_on,
        k=kind,
    )


def add_pipeline_dependency(conn, pipeline_id, depends_on, kind="SUCCESS"):
    return insert(
        conn,
        "INSERT INTO CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, "
        "DEPENDENCY_TYPE) VALUES (:p, :d, :k)",
        "PIPELINE_DEPENDENCY_ID",
        p=pipeline_id,
        d=depends_on,
        k=kind,
    )


def start_run(conn, pipeline_id, *, run_date=None, backfill=False):
    """Reuse the active run for a test scene, creating it when none exists."""
    existing = runlog.fetch_active_pipeline_run_id(conn, pipeline_id)
    if existing is not None:
        return existing
    created = transitions.create_active_run(conn, pipeline_id, run_date=run_date, backfill=backfill)
    if created is None:
        # Another session created the run between the read and the insert.
        created = runlog.fetch_active_pipeline_run_id(conn, pipeline_id)
    assert created is not None
    return created


def finish_run(conn, pipeline_run_id, status="SUCCESS"):
    transitions.finalize_pipeline_run(conn, pipeline_run_id, status)


def task_run(
    conn, task_id, pipeline_run_id, status="SUCCESS", target_count=None, rows_written=None
):
    """Bind ``task_id`` under the run and end it with ``status``; return its row id."""
    binding = transitions.find_or_create_task_run(conn, task_id, pipeline_run_id)
    if status != "IN-PROGRESS":
        transitions.finish_task_run(
            conn,
            binding.task_run_id,
            status=status,
            counts=Counts(target_count=target_count, rows_written=rows_written),
        )
    return binding.task_run_id


def upstream_run(conn, pipeline_id, task_statuses, status="SUCCESS"):
    """A finished run of ``pipeline_id`` with its tasks ended as given; return the run and rows."""
    run_id = start_run(conn, pipeline_id)
    rows = {
        task_id: task_run(conn, task_id, run_id, *spec) for task_id, spec in task_statuses.items()
    }
    if status != "IN-PROGRESS":
        finish_run(conn, run_id, status)
    return run_id, rows


def add_table(conn, table):
    """Declare a column under one scoped table contract."""
    from etl_craft.engine.repository.normalization import object_identity

    catalog, schema, name = object_identity(table, None, None)
    params = {"catalog": catalog, "schema": schema, "table": name}
    table_id = conn.execute(
        text(
            "SELECT TABLE_ID AS table_id FROM CFG_TABLES WHERE SCHEMA_NAME=:schema "
            "AND TABLE_NAME=:table AND (CATALOG_NAME=:catalog OR "
            "(CATALOG_NAME IS NULL AND :catalog IS NULL))"
        ),
        params,
    ).scalar_one_or_none()
    if table_id is None:
        table_id = conn.execute(
            text(
                "INSERT INTO CFG_TABLES (CATALOG_NAME, SCHEMA_NAME, TABLE_NAME) "
                "VALUES (:catalog, :schema, :table) RETURNING TABLE_ID AS table_id"
            ),
            params,
        ).scalar_one()
    return table_id


def add_source_column(conn, table, column, data_type, position):
    """Declare a column under one scoped table contract."""
    table_id = add_table(conn, table)
    return conn.execute(
        text(
            "INSERT INTO CFG_COLUMNS (TABLE_ID, COLUMN_NAME, DATA_TYPE, COLUMN_POSITION) "
            "VALUES (:table_id, :column, :type, :position) RETURNING COLUMN_ID AS column_id"
        ),
        {"table_id": table_id, "column": column, "type": data_type, "position": position},
    ).scalar_one()


def set_task_target(conn, task_id, target):
    """Set the scoped target of a current task fixture."""
    conn.execute(
        text(
            "UPDATE CFG_TASKS SET TARGET_TABLE_ID=:target, TARGET_ACTIVE_FLAG='Y' WHERE "
            "TASK_ID=:task"
        ),
        {"task": task_id, "target": add_table(conn, target) if target else None},
    )


def finalize_pipeline(engine, config, pipeline_code, *, hooks=None):
    """Finalize the explicit active run for outcome and publication fixtures."""
    from etl_craft.engine.repository.pipelines import resolve_pipeline_id
    from etl_craft.engine.runlog import select_run
    from etl_craft.execution.run_finish import finalize
    from etl_craft.execution.run_start import load_pipeline
    from etl_craft.execution.run_types import PipelineRun, default_hooks

    with engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        run_id = select_run(conn, pipeline_id).pipeline_run_id
        detail, graph, codes = load_pipeline(conn, pipeline_id, run_id=run_id)
    return finalize(
        engine,
        PipelineRun(pipeline_code, pipeline_id, run_id, graph, codes, detail.sla_in_hours),
        hooks or default_hooks(config, engine),
    )
