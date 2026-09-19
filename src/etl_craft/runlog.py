"""Resolve pipeline_run_id and task_run_id per CLAUDE.md's "Run-id resolution"."""

# pipeline_run_id is never passed between tasks — every task resolves it
# itself by querying AUD_PIPELINES_RUN_LOG. Two distinct entry points here
# mirror the two distinct roles CLAUDE.md describes:
#
#   * `find_or_create_active_run` is for whatever process mints the run for
#     a pipeline invocation: the local orchestrator (`run --pipeline_code X`
#     with no --task_code) before it spawns any task subprocess, or the
#     synthetic run-id-creation step generated as step 1 of an Airflow DAG.
#     It reuses an IN-PROGRESS row if one exists, else mints a new one,
#     racing safely against concurrent callers via the DB's own partial
#     unique index (ux_pipeline_run_one_active) rather than any
#     application-level check-then-insert.
#
#   * `resolve_run_for_task` is what a plain `run --task_code Y` invocation
#     calls — it never mints a fresh run itself. It binds to whatever run is
#     currently IN-PROGRESS; if none is (the "dev/ad-hoc convenience path"
#     from CLAUDE.md point 5 — not an everyday scenario), it falls back to
#     the most recently logged run for that pipeline, regardless of status,
#     and touches its END_DATE rather than fabricate a new run or silently
#     reopen a terminal one's STATUS (see resolve_run_for_task's docstring).
#
# `find_or_create_task_run` is the per-task binding used by every task
# regardless of which of the above resolved the pipeline_run_id.

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError

from etl_craft.resolver import TaskRunState

TERMINAL_STATUSES = frozenset({"SUCCESS", "FAILED", "SKIPPED"})


class RunLogError(Exception):
    """Raised when run-id or task-run-log resolution hits an unrecoverable state."""


@dataclass(frozen=True)
class TaskRunBinding:
    """A task's current AUD_TASK_RUN_LOG row: its id and its logged status."""

    task_run_id: int
    status: str


def find_or_create_active_run(conn: Connection, pipeline_id: int) -> int:
    """Reuse this pipeline's IN-PROGRESS run, or atomically mint a new one."""
    existing = conn.execute(
        text(
            "SELECT PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG "
            "WHERE PIPELINE_ID = :pipeline_id AND STATUS = 'IN-PROGRESS'"
        ),
        {"pipeline_id": pipeline_id},
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    try:
        with conn.begin_nested():
            return conn.execute(
                text(
                    "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) "
                    "VALUES (:pipeline_id, 'IN-PROGRESS') RETURNING PIPELINE_RUN_ID"
                ),
                {"pipeline_id": pipeline_id},
            ).scalar_one()
    except IntegrityError:
        # Lost the race against ux_pipeline_run_one_active — the winner's
        # row is now visible to us.
        winner = conn.execute(
            text(
                "SELECT PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG "
                "WHERE PIPELINE_ID = :pipeline_id AND STATUS = 'IN-PROGRESS'"
            ),
            {"pipeline_id": pipeline_id},
        ).scalar_one_or_none()
        if winner is None:
            raise RunLogError(
                f"pipeline_id={pipeline_id}: insert failed on a unique violation, "
                "but no IN-PROGRESS row exists afterward"
            ) from None
        return winner


def resolve_run_for_task(conn: Connection, pipeline_id: int) -> int:
    """Resolve the pipeline_run_id a `run --task_code` invocation should bind to."""
    active = conn.execute(
        text(
            "SELECT PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG "
            "WHERE PIPELINE_ID = :pipeline_id AND STATUS = 'IN-PROGRESS'"
        ),
        {"pipeline_id": pipeline_id},
    ).scalar_one_or_none()
    if active is not None:
        return active

    # Dev/ad-hoc convenience path (CLAUDE.md point 5): nothing is active for
    # this pipeline, so bind to the latest logged run instead of minting a
    # new one. [CHOICE] "updates its dates" is interpreted as touching only
    # END_DATE — STATUS and START_DATE are left alone, since silently
    # flipping a terminal run's STATUS back to IN-PROGRESS would let it
    # collide with ux_pipeline_run_one_active the next time this pipeline
    # genuinely kicks off, and overwriting START_DATE would misrepresent
    # when that run actually began.
    latest = conn.execute(
        text(
            "SELECT PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG "
            "WHERE PIPELINE_ID = :pipeline_id ORDER BY START_DATE DESC LIMIT 1"
        ),
        {"pipeline_id": pipeline_id},
    ).scalar_one_or_none()
    if latest is None:
        raise RunLogError(
            f"pipeline_id={pipeline_id} has no logged run at all — a single-task "
            "invocation has nothing to bind to. Run the pipeline (or at least its "
            "first task) at least once first."
        )
    conn.execute(
        text("UPDATE AUD_PIPELINES_RUN_LOG SET END_DATE = :now WHERE PIPELINE_RUN_ID = :run_id"),
        {"run_id": latest, "now": datetime.now(UTC)},
    )
    return latest


def find_or_create_task_run(conn: Connection, task_id: int, pipeline_run_id: int) -> TaskRunBinding:
    """Reuse this task's binding under `pipeline_run_id`, or create a new one."""
    existing = conn.execute(
        text(
            "SELECT TASK_RUN_ID AS task_run_id, STATUS AS status FROM AUD_TASK_RUN_LOG "
            "WHERE TASK_ID = :task_id AND PIPELINE_RUN_ID = :pipeline_run_id"
        ),
        {"task_id": task_id, "pipeline_run_id": pipeline_run_id},
    ).one_or_none()
    if existing is not None:
        return TaskRunBinding(task_run_id=existing.task_run_id, status=existing.status)

    try:
        with conn.begin_nested():
            task_run_id = conn.execute(
                text(
                    "INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS) "
                    "VALUES (:task_id, :pipeline_run_id, 'IN-PROGRESS') RETURNING TASK_RUN_ID"
                ),
                {"task_id": task_id, "pipeline_run_id": pipeline_run_id},
            ).scalar_one()
        return TaskRunBinding(task_run_id=task_run_id, status="IN-PROGRESS")
    except IntegrityError:
        # Lost the race against ux_task_run_one_per_pipeline_run.
        winner = conn.execute(
            text(
                "SELECT TASK_RUN_ID AS task_run_id, STATUS AS status FROM AUD_TASK_RUN_LOG "
                "WHERE TASK_ID = :task_id AND PIPELINE_RUN_ID = :pipeline_run_id"
            ),
            {"task_id": task_id, "pipeline_run_id": pipeline_run_id},
        ).one_or_none()
        if winner is None:
            raise RunLogError(
                f"task_id={task_id}, pipeline_run_id={pipeline_run_id}: insert failed on "
                "a unique violation, but no row exists afterward"
            ) from None
        return TaskRunBinding(task_run_id=winner.task_run_id, status=winner.status)


def update_task_run(
    conn: Connection,
    task_run_id: int,
    *,
    status: str,
    source_count: int | None = None,
    target_count: int | None = None,
    insert_count: int | None = None,
    update_count: int | None = None,
    delete_count: int | None = None,
    error_message: str | None = None,
    task_log: str | None = None,
) -> None:
    """Update `task_run_id` in place — never insert a second row per retry."""
    conn.execute(
        text(
            "UPDATE AUD_TASK_RUN_LOG SET "
            "STATUS = :status, END_DATE = :now, "
            "SOURCE_COUNT = COALESCE(:source_count, SOURCE_COUNT), "
            "TARGET_COUNT = COALESCE(:target_count, TARGET_COUNT), "
            "INSERT_COUNT = COALESCE(:insert_count, INSERT_COUNT), "
            "UPDATE_COUNT = COALESCE(:update_count, UPDATE_COUNT), "
            "DELETE_COUNT = COALESCE(:delete_count, DELETE_COUNT), "
            "ERROR_MESSAGE = COALESCE(:error_message, ERROR_MESSAGE), "
            "TASK_LOG = COALESCE(:task_log, TASK_LOG) "
            "WHERE TASK_RUN_ID = :task_run_id"
        ),
        {
            "task_run_id": task_run_id,
            "status": status,
            "now": datetime.now(UTC),
            "source_count": source_count,
            "target_count": target_count,
            "insert_count": insert_count,
            "update_count": update_count,
            "delete_count": delete_count,
            "error_message": error_message,
            "task_log": task_log,
        },
    )


def fetch_task_run_status(conn: Connection, task_id: int, pipeline_run_id: int) -> str | None:
    """Return this task's current STATUS under `pipeline_run_id`, or None if unbound."""
    return conn.execute(
        text(
            "SELECT STATUS FROM AUD_TASK_RUN_LOG "
            "WHERE TASK_ID = :task_id AND PIPELINE_RUN_ID = :pipeline_run_id"
        ),
        {"task_id": task_id, "pipeline_run_id": pipeline_run_id},
    ).scalar_one_or_none()


@dataclass(frozen=True)
class TaskRunResult:
    """A task run's current STATUS and ERROR_MESSAGE, read back by TASK_RUN_ID after dispatch."""

    status: str
    error_message: str | None


def fetch_task_run_result(conn: Connection, task_run_id: int) -> TaskRunResult:
    """Return TASK_RUN_ID's current STATUS/ERROR_MESSAGE, to check for a crash after a fork."""
    row = conn.execute(
        text(
            "SELECT STATUS AS status, ERROR_MESSAGE AS error_message "
            "FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID = :task_run_id"
        ),
        {"task_run_id": task_run_id},
    ).one()
    return TaskRunResult(status=row.status, error_message=row.error_message)


def fetch_run_state(
    conn: Connection, pipeline_run_id: int, task_ids: list[int]
) -> dict[int, TaskRunState]:
    """Fetch AUD_TASK_RUN_LOG state for `task_ids`, shaped for DependencyGraph.ready()."""
    if not task_ids:
        return {}
    stmt = text(
        "SELECT TASK_ID AS task_id, STATUS AS status, TARGET_COUNT AS target_count "
        "FROM AUD_TASK_RUN_LOG WHERE PIPELINE_RUN_ID = :pipeline_run_id AND TASK_ID IN :task_ids"
    ).bindparams(bindparam("task_ids", expanding=True))
    rows = conn.execute(stmt, {"pipeline_run_id": pipeline_run_id, "task_ids": task_ids}).all()
    return {
        row.task_id: TaskRunState(status=row.status, target_count=row.target_count) for row in rows
    }


def finalize_pipeline_run(conn: Connection, pipeline_run_id: int, status: str) -> None:
    """Mark `pipeline_run_id` terminal (SUCCESS/FAILED), stamping END_DATE."""
    conn.execute(
        text(
            "UPDATE AUD_PIPELINES_RUN_LOG SET STATUS = :status, END_DATE = :now "
            "WHERE PIPELINE_RUN_ID = :pipeline_run_id"
        ),
        {"pipeline_run_id": pipeline_run_id, "status": status, "now": datetime.now(UTC)},
    )
