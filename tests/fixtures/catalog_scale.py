"""Set-based synthetic catalog metadata/history for query-count and load acceptance."""

from sqlalchemy import text

SEQUENCE = """WITH RECURSIVE seq(n) AS (
    SELECT 0 WHERE :count > 0
    UNION ALL SELECT n + 1 FROM seq WHERE n + 1 < :count
)
"""
IN_PREFIX = "substr(p.PIPELINE_CODE, 1, length(:prefix) + 1) = :prefix || '_'"


def seed_catalog(conn, *, prefix, pipelines, tasks, runs):
    values = {"prefix": prefix, "tasks": tasks}
    conn.execute(
        text(
            SEQUENCE
            + """INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE)
        SELECT :prefix || '_' || CAST(n AS VARCHAR), :prefix || '_' || CAST(n AS VARCHAR), 'FULL'
        FROM seq"""
        ),
        values | {"count": pipelines},
    )
    conn.execute(
        text(
            SEQUENCE
            + """INSERT INTO CFG_TABLES (SCHEMA_NAME, TABLE_NAME)
        SELECT 'scale', lower(:prefix) || '_t_' || CAST(n AS VARCHAR) FROM seq"""
        ),
        values | {"count": pipelines * tasks},
    )
    conn.execute(
        text(
            SEQUENCE
            + """INSERT INTO CFG_TASKS
            (PIPELINE_ID, TASK_CODE, TASK_TYPE, HANDLER, TARGET_TABLE_ID)
        SELECT p.PIPELINE_ID, 'task_' || CAST(n % :tasks AS VARCHAR), 'ETL', 'SQL', o.TABLE_ID
        FROM seq
        JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = :prefix || '_' || CAST(n / :tasks AS VARCHAR)
        JOIN CFG_TABLES o ON o.SCHEMA_NAME = 'scale'
            AND o.TABLE_NAME = lower(:prefix) || '_t_' || CAST(n AS VARCHAR)"""
        ),
        values | {"count": pipelines * tasks},
    )
    conn.execute(
        text(
            """INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE)
        SELECT t.TASK_ID, v.name, v.value
        FROM CFG_TASKS t JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID
        CROSS JOIN (SELECT 'SQL_ACTION' AS name, 'CREATE_TABLE' AS value
                    UNION ALL SELECT 'SOURCE_SQL', 'SELECT 1 AS id') v
        WHERE """
            + IN_PREFIX
        ),
        values,
    )
    conn.execute(
        text(
            SEQUENCE
            + """INSERT INTO AUD_PIPELINES_RUN_LOG
            (PIPELINE_ID, STATUS, START_DATE, END_DATE, RUN_KEY, TRIGGER_KIND)
        SELECT p.PIPELINE_ID, 'SUCCESS', '2026-10-01 00:00:00.000000+00:00',
               '2026-10-01 00:00:01.000000+00:00',
               'scale:' || p.PIPELINE_CODE || ':' || CAST(n AS VARCHAR), 'SCHEDULE'
        FROM seq CROSS JOIN CFG_PIPELINES p WHERE """
            + IN_PREFIX
        ),
        values | {"count": runs},
    )
    conn.execute(
        text(
            """INSERT INTO AUD_TASK_RUN_LOG
            (TASK_ID, PIPELINE_RUN_ID, STATUS, START_DATE, END_DATE, ATTEMPT_COUNT,
             SOURCE_COUNT, TARGET_COUNT, INSERT_COUNT)
        SELECT t.TASK_ID, r.PIPELINE_RUN_ID, 'SUCCESS',
               '2026-10-01 00:00:00.000000+00:00', '2026-10-01 00:00:01.000000+00:00', 1, 1, 1, 1
        FROM CFG_TASKS t JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID
        JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_ID = p.PIPELINE_ID
        WHERE """
            + IN_PREFIX
            + " ORDER BY r.PIPELINE_RUN_ID, t.TASK_ID"
        ),
        values,
    )
