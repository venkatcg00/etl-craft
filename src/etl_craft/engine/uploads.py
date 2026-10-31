"""Run-bound upload metadata and per-file validation dispositions."""

from datetime import UTC, datetime

from sqlalchemy.engine import Connection

from etl_craft.core.enums import ActorKind, Handler
from etl_craft.core.errors import HandlerError, PermissionDeniedError, StaleTransitionError
from etl_craft.core.text import mask_arguments
from etl_craft.core.uploads import UploadInfo, UploadStatus
from etl_craft.engine.queries import statement


def run_uploads(conn: Connection, run_id: int, handler: str) -> tuple[UploadInfo, ...]:
    """Keep upload order stable and hide refused files from downstream tasks."""
    uploads = []
    for row in conn.execute(
        statement(conn, "run_uploads"),
        {
            "run_id": run_id,
            "validation": handler == Handler.FILE_VALIDATION,
        },
    ):
        if row.actor_kind is None:
            raise HandlerError(
                f"upload {row.upload_id}, account {row.account!r}, run {run_id}: account "
                "identity is missing; expected the registered uploader; restore the account "
                "before retrying this upload run"
            )
        uploads.append(UploadInfo(**{**row._mapping, "actor_kind": ActorKind(row.actor_kind)}))
    return tuple(uploads)


def validation_result(
    conn: Connection, run_id: int, handler: str, upload_id: int, reason: str | None
) -> None:
    """Record only the validator's decision for a file admitted to its exact run."""
    if handler != Handler.FILE_VALIDATION:
        raise PermissionDeniedError(
            f"upload {upload_id}, run {run_id}: handler={handler}; expected FILE_VALIDATION; "
            "let the validation task decide whether the corrected file is accepted"
        )
    if reason is not None:
        reason = str(mask_arguments({"reason": reason})["reason"])
    result = conn.execute(
        statement(conn, "transition_upload_validation_result"),
        {
            "run_id": run_id,
            "upload_id": upload_id,
            "reason": reason,
            "status": UploadStatus.REFUSED if reason is not None else UploadStatus.QUEUED,
            "ended_at": datetime.now(UTC) if reason is not None else None,
        },
    )
    if result.rowcount != 1:
        raise StaleTransitionError(
            f"upload {upload_id}, run {run_id}: no admitted file changed; expected exactly "
            "one upload of this run; use the run's recorded upload identities"
        )


def admit_queued(conn: Connection, pipeline_id: int) -> int | None:
    """Use exclusive admission to atomically take the pipeline's complete ready queue."""
    from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
    from etl_craft.engine import runlog, standing_pauses
    from etl_craft.engine.run_creation import RunCreation, create

    standing_pauses.lock(conn, pipeline_id)
    active = runlog.fetch_exclusive_run_id(conn, pipeline_id)
    if active is not None:
        return None
    ids = tuple(
        conn.execute(statement(conn, "upload_pending"), {"pipeline_id": pipeline_id}).scalars()
    )
    if not ids:
        return None
    with acting_as(SYSTEM_ACTOR):
        return create(conn, pipeline_id, SYSTEM_ACTOR, RunCreation(upload_ids=ids))


def take_uploads(conn: Connection, pipeline_id: int, run_id: int, ids: tuple[int, ...]) -> None:
    """Bind every selected file once in the transaction that creates its exclusive run."""
    from sqlalchemy import bindparam

    result = conn.execute(
        statement(conn, "upload_take").bindparams(bindparam("upload_ids", expanding=True)),
        {"pipeline_id": pipeline_id, "run_id": run_id, "upload_ids": ids, "now": datetime.now(UTC)},
    )
    if result.rowcount != len(ids):
        raise StaleTransitionError(
            f"pipeline {pipeline_id}, run {run_id}: took {result.rowcount} uploads; "
            f"expected {ids} still queued; retry upload admission after reconciliation"
        )


def end_uploads(conn: Connection, run_id: int, status: str) -> None:
    """End accepted files in the same transaction as their run's outcome."""
    from etl_craft.core.enums import RunStatus

    conn.execute(
        statement(conn, "transition_upload_run_ending"),
        {
            "run_id": run_id,
            "status": UploadStatus.LOADED if status == RunStatus.SUCCESS else UploadStatus.FAILED,
            "reason": None if status == RunStatus.SUCCESS else f"run ended {status}",
            "now": datetime.now(UTC),
        },
    )
