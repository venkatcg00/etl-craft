"""Execution boundaries that cannot turn a stewardship refusal into accepted data."""

from sqlalchemy.engine import Connection

from etl_craft.core.definitions import Definition, current
from etl_craft.core.enums import Handler, RunStatus, YesNo
from etl_craft.core.errors import RunRefusedError
from etl_craft.engine.queries import statement


def refuse_bypass(
    conn: Connection, pipeline_id: int, task_id: int | None, outcome: str | None, operation: str
) -> None:
    """Preserve validation for marks, pauses, item outcomes and standalone execution."""
    row = conn.execute(
        statement(conn, "stewardship_task"),
        {
            "pipeline_id": pipeline_id,
            "task_id": task_id,
        },
    ).one_or_none()
    if row is None:
        return
    code, task, handler = row.pipeline_code, row.task_code, row.handler
    stewardable = row.stewardable_flag == YesNo.YES
    if definition := current.get():
        pipeline = definition.row("CFG_PIPELINES", "pipeline_id", pipeline_id)
        if pipeline.get("stewardable_flag") == YesNo.YES:
            stewardable = True
            if task_id is not None:
                pinned = definition.row("CFG_TASKS", "task_id", task_id)
                code, task, handler = (
                    pipeline["pipeline_code"],
                    pinned["task_code"],
                    pinned["handler"],
                )
    if not stewardable:
        return
    if operation != "standalone execution" and (
        handler != Handler.FILE_VALIDATION or outcome not in {RunStatus.SUCCESS, RunStatus.SKIPPED}
    ):
        return
    raise RunRefusedError(
        f"pipeline {code}, task {task or Handler.FILE_VALIDATION}: "
        f"{operation} with outcome={outcome} would bypass FILE_VALIDATION; expected "
        "the validation script to accept the input; upload a corrected file"
    )


def require_upload_run(conn: Connection, pipeline_id: int, run_id: int) -> None:
    """Refuse resuming or reopening a legacy run that never admitted an upload."""
    row = conn.execute(statement(conn, "stewardship_pipeline"), {"pipeline_id": pipeline_id}).one()
    if row.stewardable_flag != YesNo.YES:
        return
    uploads = conn.execute(
        statement(conn, "run_uploads"),
        {
            "run_id": run_id,
            "validation": True,
        },
    ).first()
    if uploads is None:
        raise RunRefusedError(
            f"pipeline {row.pipeline_code}, task {row.validation_task or Handler.FILE_VALIDATION} "
            f"(FILE_VALIDATION), run {run_id}: no upload; "
            "expected the run's stored files; upload a corrected file"
        )


def require_validation(
    conn: Connection, definition: Definition, run_id: int, task: str, pipeline_id: int
) -> None:
    """Fence every downstream execution behind the admitted validation task's success."""
    validators = [
        row
        for row in definition.rows["CFG_TASKS"]
        if row["handler"] == Handler.FILE_VALIDATION
        and row["pipeline_id"] == pipeline_id
        and row["active_flag"] == YesNo.YES
    ]
    validator = validators[0] if len(validators) == 1 else None
    accepted = (
        validator is not None
        and conn.execute(
            statement(conn, "upload_validation_success"),
            {"run_id": run_id, "task_id": validator["task_id"]},
        ).first()
        is not None
    )
    if not accepted:
        pipeline = definition.row("CFG_PIPELINES", "pipeline_id", pipeline_id)["pipeline_code"]
        raise RunRefusedError(
            f"pipeline {pipeline}, task {task}, run {run_id}: FILE_VALIDATION has not "
            "accepted any file; expected its successful validation before processing; "
            "upload a corrected file"
        )
