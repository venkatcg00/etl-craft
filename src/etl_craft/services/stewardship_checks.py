"""Stewardable graph and managed-write checks shared by validate and refresh."""

from collections.abc import Sequence

from sqlalchemy.engine import Connection

from etl_craft.core.enums import DependencyType, Handler, SqlAction, YesNo
from etl_craft.dialects.engine import for_engine
from etl_craft.dialects.query_analysis import output_columns
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.validation import ActiveTask, DependencyEdge
from etl_craft.handlers.registry import TaskContext
from etl_craft.handlers.sql import task_dialect
from etl_craft.handlers.sql.spec import MERGES, WRITERS, SqlTask


def _stewardship_schema(conn: Connection) -> bool:
    """Whether the Engine DB has stewardship; before its migration no pipeline is stewardable."""
    return for_engine(conn).has_table(conn, "AUD_UPLOADS")


def graph_problems(
    conn: Connection, pipeline_id: int, tasks: Sequence[ActiveTask], edges: Sequence[DependencyEdge]
) -> list[tuple[str, str]]:
    """Require HAS_DATA after validation, and one validation task where uploads arrive."""
    if not _stewardship_schema(conn):
        return []
    validators = {
        task.task_code
        for task in tasks
        if task.pipeline_id == pipeline_id and task.handler == Handler.FILE_VALIDATION
    }
    problems = [
        (
            f"{edge.pipeline_code}.{edge.task_code}",
            f"depends on FILE_VALIDATION task {edge.depends_on_task_code} with "
            f"{edge.dependency_type}; expected HAS_DATA, so a run with no accepted files skips "
            "it; change the dependency to HAS_DATA",
        )
        for edge in edges
        if edge.depends_on_pipeline_id == pipeline_id
        and edge.depends_on_handler == Handler.FILE_VALIDATION
        and edge.dependency_type == DependencyType.SUCCESS
    ]
    row = conn.execute(statement(conn, "stewardship_pipeline"), {"pipeline_id": pipeline_id}).one()
    if row.stewardable_flag == YesNo.YES and len(validators) != 1:
        problems.append(
            (
                row.pipeline_code,
                f"FILE_VALIDATION tasks={len(validators)} on a pipeline that takes uploads; "
                "expected exactly one to accept uploaded files; add one validation task",
            )
        )
    return problems


def sql_problems(conn: Connection, context: TaskContext, spec: SqlTask) -> list[str]:
    """Refuse unsupported protection and writes that lose their file identity."""
    if not _stewardship_schema(conn):
        return []
    pipeline = conn.execute(
        statement(conn, "stewardship_pipeline"), {"pipeline_id": context.pipeline_id}
    ).one()
    if pipeline.stewardable_flag != YesNo.YES:
        return []
    dialect = task_dialect(context)
    where = f"{context.pipeline_code}.{context.task_code}, target {spec.target_object}"
    if spec.action in MERGES and not dialect.atomic_protection:
        return [
            f"{where}: stewarded {spec.action} protection is unsupported on "
            f"{dialect.spec.display_name}; expected atomic S5.O.3 human protection; "
            "use native PostgreSQL or DuckDB, or a non-protected target"
        ]
    problems = []
    if spec.action in WRITERS | {SqlAction.DELETE_ROWS}:
        names = output_columns(spec.select_sql or "", dialect=dialect.sqlglot_dialect)
        if names is None or "upload_id" not in {name.lower() for name in names}:
            problems.append(
                f"{where}: SELECT output has no explicit UPLOAD_ID; expected the "
                "accepted file's id on every written row; SELECT UPLOAD_ID explicitly"
            )
    if spec.action in MERGES and (spec.dedupe_order or "").strip().upper() not in {
        "UPLOAD_ID",
        "UPLOAD_ID ASC",
    }:
        problems.append(
            f"{where}: MERGE_DEDUPE_ORDER={spec.dedupe_order!r}; expected UPLOAD_ID "
            "ASC, newest upload last; set MERGE_DEDUPE_ORDER=UPLOAD_ID ASC"
        )
    return problems
