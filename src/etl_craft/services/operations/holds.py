"""Holds still in force on a project's queued and running runs."""

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from etl_craft.core.time import as_utc
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.queries import statement
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import pipeline_access
from etl_craft.services.operations.requests import NoInput


@dataclass(frozen=True)
class Hold:
    """One hold: on a whole run, or on one task when ``task_code`` is set."""

    control_id: int
    pipeline_run_id: int
    pipeline_code: str
    task_code: str | None
    requested_by: str
    reason: str | None
    requested_at: datetime


@dataclass(frozen=True)
class Holds:
    """The visible holds, newest first, at most 200."""

    SCHEMA: ClassVar[str] = "etl-craft/holds/1"
    holds: tuple[Hold, ...]


def read_holds(ctx: OperationContext, request: NoInput) -> Holds:
    """List holds a later release, stop, abort or recover has not replaced."""
    with read_snapshot(ctx.engine) as conn:
        access = pipeline_access(conn, ctx)
        rows = conn.execute(statement(conn, "holds"), {"project_id": ctx.project_id}).all()
    return Holds(
        tuple(
            Hold(
                row.control_id,
                row.pipeline_run_id,
                row.pipeline_code,
                row.task_code,
                row.requested_by,
                row.reason,
                as_utc(row.requested_at),
            )
            for row in rows
            if access.permits(row.pipeline_code)
        )
    )
