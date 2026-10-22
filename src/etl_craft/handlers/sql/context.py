"""Inputs shared by the SQL action implementations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from etl_craft.engine.task_store import TaskState, TaskStore
from etl_craft.handlers.registry import TaskContext
from etl_craft.handlers.sql.session import Session
from etl_craft.handlers.sql.spec import SqlTask
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

    @property
    def select_sql(self) -> str:
        """The task's SELECT; every action but ``DROP_TABLE`` has one."""
        assert self.task.select_sql is not None
        return self.task.select_sql

    def build_stage(self, session: Session, *, empty: bool = False) -> str:
        """Stage the SELECT once and record its watermark before any target mutation."""
        if self.staged is not None:
            return self.staged
        stage = build_stage(session, self.select_sql, empty=empty)
        if self.task.watermark is not None:
            record_range(session, stage, self.task.watermark, self.context, self.store)
        return stage

    @property
    def stamp(self) -> dict[str, object]:
        """The values the audit columns are stamped with."""
        return {
            "pipeline_run_id": None if self.staged else self.context.pipeline_run_id,
            "pipeline_id": None if self.staged else self.context.pipeline_id,
            "task_run_id": None if self.staged else self.context.task_run_id,
            "now": self.now,
            "updated_by": self.user,
        }
