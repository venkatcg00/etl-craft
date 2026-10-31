"""Inputs shared by the SQL action implementations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from etl_craft.core.errors import SqlGuardError
from etl_craft.core.uploads import UploadWriter
from etl_craft.dialects.warehouse.uploads import invalid_upload_rows, writer_parameters
from etl_craft.engine.task_store import TaskState, TaskStore
from etl_craft.handlers.registry import TaskContext
from etl_craft.handlers.sql.session import Session
from etl_craft.handlers.sql.spec import MERGES, SqlTask
from etl_craft.handlers.sql.tables import build_stage
from etl_craft.handlers.sql.watermarks import record_range


@dataclass(frozen=True)
class ActionContext:
    """What every action needs besides the warehouse session."""

    task: SqlTask
    context: TaskContext
    store: TaskStore
    user: str
    now: datetime
    staged: str | None = None
    respect_protection: bool = True
    state: TaskState | None = None
    uploads: tuple[UploadWriter, ...] = ()

    @property
    def select_sql(self) -> str:
        """The task's SELECT; every action but ``DROP_TABLE`` has one."""
        assert self.task.select_sql is not None
        return self.task.select_sql

    def build_stage(self, session: Session, *, empty: bool = False) -> str:
        """Stage the SELECT once and record its watermark before any target mutation."""
        if self.staged is not None:
            return self.staged
        immutable = (
            self.task.action not in MERGES
            and self.task.reject_limit is None
            and session.protection is None
        )
        stage = build_stage(session, self.select_sql, empty=empty, immutable=immutable)
        self.check_uploads(session, stage)
        if self.task.watermark is not None:
            record_range(session, stage, self.task.watermark, self.context, self.store)
        return stage

    def check_uploads(self, session: Session, stage: str) -> None:
        """Refuse missing or foreign file identities before any target mutation."""
        if not self.uploads:
            return
        ids = tuple(upload.upload_id for upload in self.uploads)
        if "upload_id" not in {name.lower() for name, _ in session.columns(stage)}:
            raise SqlGuardError(
                f"{session.target}: staged UPLOAD_ID is missing; expected run upload ids "
                f"{ids}; SELECT the accepted file's UPLOAD_ID on every written row"
            )
        invalid = invalid_upload_rows(stage, self.uploads)
        parameters = writer_parameters(self.uploads)
        count = session.run(
            f"SELECT COUNT(*) FROM ({invalid}) invalid_uploads",
            parameters,
            step="count rows without an admitted upload",
        ).scalar_one()
        if count:
            sample = session.run(
                f"{invalid} LIMIT 1", parameters, step="sample an unowned upload row"
            ).scalar_one()
            raise SqlGuardError(
                f"{session.target}: {count} staged rows have invalid UPLOAD_ID, "
                f"sample={sample!r}; expected run upload ids {ids}; correct the load "
                "script to retain each accepted file's UPLOAD_ID and upload a corrected file"
            )

    @property
    def stamp(self) -> dict[str, object]:
        """The values the audit columns are stamped with."""
        return {
            "pipeline_run_id": None
            if self.staged and not self.uploads
            else self.context.pipeline_run_id,
            "pipeline_id": None if self.staged and not self.uploads else self.context.pipeline_id,
            "task_run_id": None if self.staged and not self.uploads else self.context.task_run_id,
            "now": self.now,
            "updated_by": self.user,
            **writer_parameters(self.uploads),
        }
