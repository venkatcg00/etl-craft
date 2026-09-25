"""Rows for tests: pipelines, tasks, dependencies, and finished runs of them."""

from sqlalchemy import text

from etl_craft.engine import runlog


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


def start_run(conn, pipeline_id):
    return runlog.find_or_create_active_run(conn, pipeline_id)


def finish_run(conn, pipeline_run_id, status="SUCCESS"):
    runlog.finalize_pipeline_run(conn, pipeline_run_id, status)


def task_run(conn, task_id, pipeline_run_id, status="SUCCESS", target_count=None):
    """Bind ``task_id`` under the run and end it with ``status``; return its row id."""
    binding = runlog.find_or_create_task_run(conn, task_id, pipeline_run_id)
    if status != "IN-PROGRESS":
        runlog.finish_task_run(conn, binding.task_run_id, status=status, target_count=target_count)
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
