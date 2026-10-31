"""Stewardable graph and managed-write checks shared by validate and refresh."""

from collections.abc import Sequence

from sqlalchemy.engine import Connection

from etl_craft.core.enums import DependencyType, Handler, SqlAction, YesNo
from etl_craft.dialects.query_analysis import output_columns
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.validation import ActiveTask, DependencyEdge
from etl_craft.handlers.registry import TaskContext
from etl_craft.handlers.sql import task_dialect
from etl_craft.handlers.sql.spec import MERGES, WRITERS, SqlTask


def graph_problems(
    conn: Connection, pipeline_id: int, tasks: Sequence[ActiveTask], edges: Sequence[DependencyEdge]
) -> list[tuple[str, str]]:
    """Require one mandatory successful validation root for every stewardable pipeline."""
    row = conn.execute(statement(conn, "stewardship_pipeline"), {"pipeline_id": pipeline_id}).one()
    mine = [task for task in tasks if task.pipeline_id == pipeline_id]
    validators = [task for task in mine if task.handler == Handler.FILE_VALIDATION]
    if row.stewardable_flag != YesNo.YES:
        return [
            (
                task.label,
                "FILE_VALIDATION requires STEWARDABLE_FLAG=Y; move the task to a "
                "stewardable pipeline or remove the handler",
            )
            for task in validators
        ]
    problems = []
    code = row.pipeline_code
    if row.run_schedule is not None:
        problems.append(
            (
                code,
                f"RUN_SCHEDULE={row.run_schedule!r}; expected no schedule on a "
                "stewardable pipeline; remove the schedule and upload a file",
            )
        )
    if len(validators) != 1:
        return [
            *problems,
            (
                code,
                f"FILE_VALIDATION tasks={len(validators)}; expected exactly "
                "one validation root; configure it before uploading a file",
            ),
        ]
    validator = validators[0].task_code
    local = [
        edge
        for edge in edges
        if edge.pipeline_code == code and edge.depends_on_pipeline_code == code
    ]
    roots = {task.task_code for task in mine} - {edge.task_code for edge in local}
    if roots != {validator}:
        problems.append(
            (
                code,
                f"roots={sorted(roots)}; expected only FILE_VALIDATION task "
                f"{validator}; make every other task depend on it with SUCCESS",
            )
        )
    problems.extend(_success_paths(validator, mine, local))
    return problems


def _success_paths(
    validator: str, mine: Sequence[ActiveTask], local: Sequence[DependencyEdge]
) -> list[tuple[str, str]]:
    """Find tasks lacking a successful dependency path from the validation root."""
    problems: list[tuple[str, str]] = []
    reached = {validator}
    while True:
        following = reached | {
            edge.task_code
            for edge in local
            if edge.dependency_type == DependencyType.SUCCESS
            and edge.depends_on_task_code in reached
        }
        if following == reached:
            break
        reached = following
    for task in mine:
        if task.task_code not in reached:
            problems.append(
                (
                    task.label,
                    f"no SUCCESS path from FILE_VALIDATION task {validator}; "
                    "expected validation before processing; add the SUCCESS dependencies",
                )
            )
    return problems


def sql_problems(conn: Connection, context: TaskContext, spec: SqlTask) -> list[str]:
    """Refuse unsupported protection and writes that lose their file identity."""
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
