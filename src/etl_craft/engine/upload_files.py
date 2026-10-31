"""Attempt-scoped upload reads shared by local tasks and the agent service."""

from collections.abc import Iterator
from dataclasses import dataclass

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.definitions import Definition
from etl_craft.core.enums import AttemptStatus, Handler, YesNo
from etl_craft.core.errors import ConfigurationError, PermissionDeniedError
from etl_craft.core.file_bytes import open_regular
from etl_craft.core.uploads import UploadStatus
from etl_craft.engine.queries import statement
from etl_craft.engine.task_store import TaskIdentity


@dataclass(frozen=True)
class UploadRead:
    """Pin the reader's admitted handler while checking its live ownership per chunk."""

    identity: TaskIdentity
    upload_id: int
    handler: str
    validator_id: int | None


def stream_upload(
    engine: Engine, config: ConnectorConfig, identity: TaskIdentity, upload_id: int
) -> Iterator[bytes]:
    """Check admission eagerly, then recheck the lease before each storage chunk."""
    with engine.connect() as conn:
        row = conn.execute(
            statement(conn, "upload_attempt_definition"),
            {
                "attempt_id": identity.attempt_id,
            },
        ).one_or_none()
    if row is None:
        raise PermissionDeniedError(
            f"upload {upload_id}, attempt {identity.attempt_id}, run {identity.pipeline_run_id}: "
            "attempt is missing; expected its live owning attempt; reconcile before retrying"
        )
    definition = Definition.read(row.metadata_snapshot, identity.pipeline_run_id)
    handler = str(definition.row("CFG_TASKS", "task_id", row.task_id)["handler"])
    validators = [
        int(str(task["task_id"]))
        for task in definition.rows["CFG_TASKS"]
        if task["handler"] == Handler.FILE_VALIDATION
        and task["pipeline_id"] == identity.pipeline_id
        and task["active_flag"] == YesNo.YES
    ]
    access = UploadRead(
        identity, upload_id, handler, validators[0] if len(validators) == 1 else None
    )
    _require(engine, access)
    return _chunks(engine, config, access)


def _require(engine: Engine, access: UploadRead) -> None:
    """Never let a task read another run's files or bypass a validation refusal."""
    identity, upload_id, handler = access.identity, access.upload_id, access.handler
    with engine.connect() as conn:
        row = conn.execute(
            statement(conn, "upload_attempt_file"),
            {"upload_id": upload_id, "attempt_id": identity.attempt_id},
        ).one_or_none()
    reason = (
        "upload or attempt is missing"
        if row is None
        else "lease is expired or attempt is no longer running"
        if not row.lease_live
        or row.attempt_status not in {AttemptStatus.CLAIMED, AttemptStatus.RUNNING}
        else "attempt ownership differs"
        if (
            row.owner_id != identity.owner
            or row.task_id != identity.task_id
            or row.task_run_id != identity.task_run_id
            or row.pipeline_id != identity.pipeline_id
        )
        else "upload belongs to another run"
        if row.upload_run_id != row.run_id or row.run_id != identity.pipeline_run_id
        else "validation refused the upload"
        if handler != Handler.FILE_VALIDATION and row.upload_status == UploadStatus.REFUSED
        else None
    )
    if reason is None and handler != Handler.FILE_VALIDATION:
        with engine.connect() as conn:
            accepted = (
                access.validator_id is not None
                and conn.execute(
                    statement(conn, "upload_validation_success"),
                    {"task_id": access.validator_id, "run_id": identity.pipeline_run_id},
                ).first()
                is not None
            )
        if not accepted:
            reason = "FILE_VALIDATION has not accepted the run's files"
    if reason is not None:
        raise PermissionDeniedError(
            f"upload {upload_id}, attempt {identity.attempt_id}, run "
            f"{identity.pipeline_run_id}: {reason}; expected a live owning attempt and "
            "an accepted upload of its own run; reconcile or upload a corrected file"
        )


def _chunks(engine: Engine, config: ConnectorConfig, access: UploadRead) -> Iterator[bytes]:
    identity, upload_id = access.identity, access.upload_id
    try:
        with open_regular(config.host_dir / "uploads", f"{upload_id}/content") as stream:
            while chunk := stream.read(65536):
                _require(engine, access)
                yield chunk
    except ConfigurationError as error:
        raise ConfigurationError(
            f"upload {upload_id}, attempt {identity.attempt_id}, run "
            f"{identity.pipeline_run_id}: stored file is unavailable or unsafe; expected "
            "its recorded regular file; restore the original upload from a trusted copy"
        ) from error
