"""One streaming upload receipt and admission implementation for the CLI and HTTP API."""

import hashlib
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO, ClassVar

from etl_craft.core.actor import acting_as
from etl_craft.core.enums import ActionName, ActorKind, ApiRole, YesNo
from etl_craft.core.enums_access import Permission
from etl_craft.core.errors import ConfigurationError, PermissionDeniedError, UsageError
from etl_craft.core.file_bytes import parent_directory
from etl_craft.core.text import mask_arguments
from etl_craft.core.uploads import UploadStatus
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.accounts import find_account
from etl_craft.engine.repository.pipelines import resolve_pipeline_id, selected_project
from etl_craft.engine.runlog import fetch_exclusive_run_id
from etl_craft.engine.uploads import admit_queued
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import pipeline_access
from etl_craft.services.operations.recording import recorded
from etl_craft.services.operations.requests import NoInput, PipelineInput, arguments


@dataclass(frozen=True, kw_only=True)
class UploadRequest(PipelineInput):
    """Name the chosen pipeline; file bytes are always streamed separately."""


@dataclass(frozen=True)
class UploadSubmission:
    """The receipt's assigned run or the exclusive run it waits behind."""

    SCHEMA: ClassVar[str] = "etl-craft/upload/1"
    upload_id: int
    pipeline_run_id: int | None
    waiting_behind_run_id: int | None
    status: UploadStatus


@dataclass(frozen=True)
class UploadHistory:
    """Visible file receipts and their validation and run outcomes."""

    SCHEMA: ClassVar[str] = "etl-craft/uploads/1"
    uploads: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class UploadPipelines:
    """Active pipelines that accept files from this caller."""

    SCHEMA: ClassVar[str] = "etl-craft/upload-pipelines/1"
    pipelines: tuple[dict[str, object], ...]


@dataclass
class UploadReceipt:
    """A single request's bounded sink; original client names never become paths."""

    ctx: OperationContext
    pipeline_id: int
    upload_id: int
    created_file: bool = False
    file_count: int = 0
    file_name: str = ""
    size_bytes: int = 0
    _stream: BinaryIO | None = field(default=None, repr=False)
    result: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Start one streaming digest for the receipt."""
        self._digest = hashlib.sha256()

    def begin_file(self, file_name: str, content_type: str) -> None:
        """Reject extra files, path-shaped names and unsupported declared media types."""
        self.file_count += 1
        if self.file_count != 1:
            raise UsageError(
                f"upload {self.upload_id}: files={self.file_count}; expected exactly one; "
                "upload one corrected file"
            )
        lowered = file_name.lower()
        if (
            not file_name
            or not file_name.isprintable()
            or len(file_name) > 255
            or any(c in file_name for c in ("/", "\\", "\0"))
            or file_name in {".", ".."}
            or any(c in lowered for c in ("%2f", "%5c", "%00"))
        ):
            raise UsageError(
                f"upload {self.upload_id}: file_name={file_name!r}; expected a plain file "
                "name without path traversal; upload a corrected file"
            )
        suffix = Path(file_name).suffix.lower()
        allowed = {
            ".csv": {"text/csv", "text/plain", "application/octet-stream"},
            ".txt": {"text/plain", "text/csv", "application/octet-stream"},
            ".xlsx": {
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "application/octet-stream",
            },
        }
        if suffix not in allowed or content_type.split(";", 1)[0].lower() not in allowed[suffix]:
            raise UsageError(
                f"upload {self.upload_id}: extension={suffix!r}, content_type={content_type!r}; "
                "expected pipe-delimited CSV/TXT or macro-free XLSX with a matching type; "
                "upload a corrected file"
            )
        self.file_name = file_name
        root = self.ctx.config.host_dir / "uploads"
        with parent_directory(root, f"{self.upload_id}/content", create=True) as parent:
            descriptor = os.open(
                "content",
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=parent,
            )
        self._stream = os.fdopen(descriptor, "wb")
        self.created_file = True

    def write(self, data: bytes) -> None:
        """Enforce the project ceiling before writing each received chunk."""
        if self._stream is None:
            raise UsageError(f"upload {self.upload_id}: no file; expected one file; choose a file")
        found = self.size_bytes + len(data)
        maximum = self.ctx.config.uploads.max_file_mb * 1024 * 1024
        if found > maximum:
            raise UsageError(
                f"upload {self.upload_id} {self.file_name!r}: size exceeds {maximum} bytes "
                f"(received {found}); expected "
                f"Uploads.Max_file_mb={self.ctx.config.uploads.max_file_mb}; "
                "upload a smaller corrected file"
            )
        self._stream.write(data)
        self._digest.update(data)
        self.size_bytes = found

    def finish(self) -> dict[str, object]:
        """Publish the complete digest, refuse duplicates, and take the durable queue."""
        if self.file_count != 1 or self._stream is None:
            raise UsageError(
                f"upload {self.upload_id}: files={self.file_count}; expected exactly one; "
                "choose one corrected file"
            )
        self._stream.flush()
        os.fsync(self._stream.fileno())
        self._stream.close()
        self._stream = None
        root = self.ctx.config.host_dir / "uploads"
        with parent_directory(root, f"{self.upload_id}/content") as parent:
            os.fsync(parent)
        with parent_directory(root, "directory") as parent:
            os.fsync(parent)
        digest = self._digest.hexdigest()
        with self.ctx.engine.begin() as conn:
            from etl_craft.engine.standing_pauses import lock

            lock(conn, self.pipeline_id)
            duplicate = conn.execute(
                statement(conn, "upload_duplicate"),
                {
                    "pipeline_id": self.pipeline_id,
                    "sha256": digest,
                    "upload_id": self.upload_id,
                },
            ).scalar_one_or_none()
            if duplicate is not None:
                raise UsageError(
                    f"upload {self.upload_id}: SHA-256={digest} duplicates upload {duplicate}; "
                    "expected new file contents; upload a corrected file"
                )
            conn.execute(
                statement(conn, "upload_insert"),
                {
                    "upload_id": self.upload_id,
                    "pipeline_id": self.pipeline_id,
                    "account": self.ctx.actor.name,
                    "status": UploadStatus.QUEUED,
                    "reason": None,
                    "now": datetime.now(UTC),
                    "ended_at": None,
                    "file_name": self.file_name,
                    "size_bytes": self.size_bytes,
                    "sha256": digest,
                },
            )
            run_id = admit_queued(conn, self.pipeline_id)
            waiting = fetch_exclusive_run_id(conn, self.pipeline_id) if run_id is None else None
        self.result = {
            "upload_id": self.upload_id,
            "pipeline_run_id": run_id,
            "waiting_behind_run_id": waiting,
            "status": UploadStatus.QUEUED,
        }
        return self.result


def _require_pipeline(ctx: OperationContext, request: UploadRequest) -> int:
    """Recheck current grants before receipt and before its durable queue commit."""
    with ctx.engine.connect() as conn:
        account = find_account(conn, ctx.actor.name)
        if account is None or account.active_flag != YesNo.YES or ctx.role in {None, ApiRole.AGENT}:
            raise PermissionDeniedError(
                f"pipeline {request.pipeline_code}, account {ctx.actor.name}: identity is "
                "missing, disabled or not signed in; expected an active account with CAN_UPLOAD; "
                "sign in and ask a pipeline manager for access"
            )
        ctx = replace(ctx, role=min(ctx.role, ApiRole(account.role), key=tuple(ApiRole).index))
        access = pipeline_access(conn, ctx)
        access.require(ctx, request.pipeline_code, ApiRole.VIEWER)
        level = access.levels.get(request.pipeline_code)
        if (
            ctx.actor.kind == ActorKind.SYSTEM
            or level is None
            or tuple(Permission).index(level) < tuple(Permission).index(Permission.CAN_UPLOAD)
        ):
            raise PermissionDeniedError(
                f"account {ctx.actor.name}, pipeline {request.pipeline_code}: permission={level}; "
                "expected CAN_UPLOAD or higher on the pipeline; ask a pipeline manager for access"
            )
        pipeline_id = resolve_pipeline_id(conn, request.pipeline_code)
        pipeline = conn.execute(
            statement(conn, "stewardship_pipeline"), {"pipeline_id": pipeline_id}
        ).one()
        if pipeline.stewardable_flag != YesNo.YES or pipeline.active_flag != YesNo.YES:
            raise UsageError(
                f"pipeline {request.pipeline_code}: STEWARDABLE_FLAG={pipeline.stewardable_flag}, "
                f"ACTIVE_FLAG={pipeline.active_flag}; "
                "expected both Y; select an active stewardable pipeline"
            )
    return pipeline_id


@contextmanager
def receive(ctx: OperationContext, request: UploadRequest) -> Iterator[UploadReceipt]:
    """Reserve an audited identity, then publish only complete, durable file receipts."""
    with (
        acting_as(ctx.actor),
        selected_project(ctx.project_id),
        recorded(ctx, ActionName.UPLOAD, arguments(request), on_completion=True) as audit,
    ):
        audit.record()
        assert audit.action_id is not None
        receipt = None
        pipeline_id = None
        try:
            pipeline_id = _require_pipeline(ctx, request)
            receipt = UploadReceipt(ctx, pipeline_id, audit.action_id)
            yield receipt
            _require_pipeline(ctx, request)
            receipt.finish()
        except Exception as error:
            failure = (
                ConfigurationError(
                    f"upload {audit.action_id}, pipeline {request.pipeline_code}: storage "
                    f"failed with errno={error.errno}; expected writable durable upload storage; "
                    "restore disk space and permissions, then upload a corrected file"
                )
                if isinstance(error, OSError)
                else error
            )
            # Receipt boundary: preserve refusals without ever publishing an incomplete queue entry.
            with ctx.engine.begin() as conn:
                conn.execute(
                    statement(conn, "upload_insert"),
                    {
                        "upload_id": audit.action_id,
                        "pipeline_id": pipeline_id,
                        "account": ctx.actor.name,
                        "file_name": receipt.file_name
                        if receipt and receipt.file_name
                        else "(no file)",
                        "size_bytes": receipt.size_bytes if receipt else 0,
                        "sha256": None,
                        "status": UploadStatus.REFUSED,
                        "reason": str(mask_arguments({"reason": str(failure)})["reason"]),
                        "now": audit.requested_at,
                        "ended_at": datetime.now(UTC),
                    },
                )
            if failure is error:
                raise
            raise failure from error
        finally:
            if receipt is not None and receipt._stream is not None:
                receipt._stream.close()
            if receipt is not None and receipt.created_file and not receipt.result:
                with parent_directory(
                    ctx.config.host_dir / "uploads", f"{receipt.upload_id}/content"
                ) as parent:
                    os.unlink("content", dir_fd=parent)


def history(ctx: OperationContext, request: UploadRequest) -> UploadHistory:
    """Return the latest hundred receipts with per-file reasons and their run outcomes."""
    with ctx.engine.connect() as conn:
        pipeline_access(conn, ctx).require(ctx, request.pipeline_code, ApiRole.VIEWER)
        pipeline_id = resolve_pipeline_id(conn, request.pipeline_code)
        rows = conn.execute(statement(conn, "upload_history"), {"pipeline_id": pipeline_id})
        return UploadHistory(tuple(dict(row._mapping) for row in rows))


def eligible(ctx: OperationContext, request: NoInput) -> UploadPipelines:
    """List active stewardable pipelines on which the caller may submit a file."""
    with ctx.engine.connect() as conn:
        access = pipeline_access(conn, ctx)
        rows = conn.execute(statement(conn, "upload_eligible"), {"project_id": ctx.project_id})
        return UploadPipelines(
            tuple(
                dict(row._mapping)
                for row in rows
                if access.levels.get(row.pipeline_code)
                in {
                    Permission.CAN_UPLOAD,
                    Permission.CAN_RUN,
                    Permission.CAN_MANAGE,
                }
            )
        )
