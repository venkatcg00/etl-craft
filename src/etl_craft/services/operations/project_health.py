"""Grant-filtered health aggregates over one explicit rolling UTC window."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import ClassVar

from etl_craft.core.enums import YesNo
from etl_craft.core.enums_access import EnvironmentType, Governance
from etl_craft.core.errors import UsageError
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.queries import statement
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import (
    project_not_found,
    validate_visibility,
    visibility_parameters,
)
from etl_craft.services.operations.requests import Request


@dataclass(frozen=True, kw_only=True)
class HealthInput(Request):
    """An optional exact project and a bounded project-code cursor."""

    project: str | None = None
    after: str | None = None
    limit: int = 25

    def __post_init__(self) -> None:
        """Refuse invalid preferences or unbounded requests before database work."""
        if not 1 <= self.limit <= 100:
            raise UsageError(
                f"limit={self.limit}; expected 1 to 100; choose a bounded project page"
            )


@dataclass(frozen=True)
class ProjectCounts:
    """Current visible work and recorded recent outcomes for one project."""

    project_id: int
    project: str
    project_name: str
    environment_type: EnvironmentType
    environment_name: str | None
    governance: Governance
    running: int
    failed: int
    sla_misses: int
    queued_tasks: int


@dataclass(frozen=True)
class ProjectHealth:
    """A bounded project page over one rolling UTC interval."""

    SCHEMA: ClassVar[str] = "etl-craft/project-health/1"
    window_start: datetime
    window_end: datetime
    projects: tuple[ProjectCounts, ...]
    after: str | None


def read_health(ctx: OperationContext, request: HealthInput) -> ProjectHealth:
    """Count visible work in SQL; end-date windows are start-inclusive and end-exclusive."""
    end = datetime.now(UTC)
    start = end - timedelta(hours=24)
    params = visibility_parameters(ctx) | {
        "project_id": None,
        "after_project": request.after,
        "project_limit": request.limit + 1,
        "window_start": start,
        "window_end": end,
    }
    with read_snapshot(ctx.engine) as conn:
        if request.project is not None:
            row = conn.execute(
                statement(conn, "project_by_code"), {"code": request.project}
            ).one_or_none()
            if row is None or row.active_flag != YesNo.YES:
                raise project_not_found(request.project)
            params["project_id"] = row.project_id
        validate_visibility(conn, params)
        rows = conn.execute(statement(conn, "project_health"), params).all()
        if request.project is not None and not rows:
            raise project_not_found(request.project)
    more = len(rows) > request.limit
    listed = rows[: request.limit]
    return ProjectHealth(
        start,
        end,
        tuple(ProjectCounts(**row._mapping) for row in listed),
        listed[-1].project if more else None,
    )
