"""Deployment administrator project and pool identities."""

from dataclasses import dataclass
from typing import ClassVar

from sqlalchemy.engine import Connection

from etl_craft.core.cron import timezone
from etl_craft.core.enums import YesNo
from etl_craft.core.errors import MetadataError, UsageError
from etl_craft.core.text import is_metadata_code
from etl_craft.engine import locks
from etl_craft.engine.queries import statement
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import Request


@dataclass(frozen=True, kw_only=True)
class ProjectIdentity(Request):
    """Explicit project identity and the administrator's reason."""

    reason: str
    project: str | None = None


@dataclass(frozen=True, kw_only=True)
class ProjectFields(ProjectIdentity):
    """Only supplied fields change an existing project."""

    name: str | None = None
    timezone: str | None = None
    default_pool: str | None = None
    max_running_tasks: int | None = None


@dataclass(frozen=True, kw_only=True)
class PoolIdentity(Request):
    """Pool identity and the administrator's reason."""

    reason: str
    name: str | None = None


@dataclass(frozen=True, kw_only=True)
class PoolFields(PoolIdentity):
    """A pool's editable description."""

    description: str | None = None


@dataclass(frozen=True)
class Project:
    """Current project administration fields."""

    project: str
    name: str
    timezone: str
    default_pool: str
    max_running_tasks: int
    active_flag: str


@dataclass(frozen=True)
class Pool:
    """Current pool administration fields."""

    name: str
    description: str | None
    active_flag: str


@dataclass(frozen=True)
class Projects:
    """Project administration result."""

    SCHEMA: ClassVar[str] = "etl-craft/projects/1"
    projects: tuple[Project, ...]


@dataclass(frozen=True)
class Pools:
    """Pool administration result."""

    SCHEMA: ClassVar[str] = "etl-craft/pools/1"
    pools: tuple[Pool, ...]


def reason(value: str) -> None:
    """Require a usable audit explanation on every administrator request."""
    if not value.strip():
        raise UsageError(f"--reason={value!r}: expected a non-empty reason; explain the request")


def code(flag: str, value: str | None) -> None:
    """Use the metadata code contract for project and pool identities."""
    if value is None or not is_metadata_code(value):
        raise UsageError(
            f"{flag}={value!r}: expected a letter followed by up to 127 letters, "
            f"digits or underscores; supply a valid code explicitly"
        )


def pool_name(value: str | None) -> None:
    """Keep existing pool names valid under their VARCHAR(128) identity column."""
    if value is None or not value.strip() or len(value) > 128:
        raise UsageError(
            f"--name={value!r}: expected a nonblank pool name of at most 128 characters; "
            "supply the existing name or a distinct new name"
        )


def list_projects(ctx: OperationContext, request: ProjectIdentity) -> Projects:
    """List deployment project identities, optionally selecting one code."""
    reason(request.reason)
    with ctx.engine.connect() as conn:
        rows = conn.execute(statement(conn, "admin_projects"))
        return Projects(
            tuple(
                Project(
                    r.project_code,
                    r.project_name,
                    r.timezone,
                    r.default_pool,
                    r.max_running_tasks,
                    r.active_flag,
                )
                for r in rows
                if request.project is None or r.project_code == request.project
            )
        )


def list_pools(ctx: OperationContext, request: PoolIdentity) -> Pools:
    """List retained pools, including disabled identities."""
    reason(request.reason)
    with ctx.engine.connect() as conn:
        return Pools(
            tuple(
                Pool(r.name, r.description, r.active_flag)
                for r in conn.execute(statement(conn, "admin_pools"))
                if request.name is None or request.name == r.name
            )
        )


def project_values(conn: Connection, request: ProjectFields) -> None:
    """Check all supplied fields before publishing metadata."""
    if request.name is not None and not request.name.strip():
        raise UsageError(
            f"--name={request.name!r}: expected a nonblank project name; fix the project name"
        )
    if request.timezone is not None:
        try:
            timezone(request.timezone)
        except MetadataError as error:
            raise UsageError(
                f"--timezone={request.timezone!r}: expected an IANA timezone name; use e.g. UTC"
            ) from error
    if request.max_running_tasks is not None and request.max_running_tasks <= 0:
        raise UsageError(
            f"--max-running-tasks={request.max_running_tasks!r}: expected a "
            f"positive integer; supply at least 1"
        )
    if request.default_pool is not None:
        pool = conn.execute(
            statement(conn, "placement_pool"), {"pool": request.default_pool}
        ).one_or_none()
        if pool is None or pool.active_flag != YesNo.YES:
            raise UsageError(
                f"--default-pool={request.default_pool!r}: expected an active "
                f"pool; use etl-craft pool add or pool list"
            )


def change_project(ctx: OperationContext, request: ProjectFields, *, adding: bool) -> Projects:
    """Serialize project changes with config publication and apply only given fields."""
    reason(request.reason)
    code("--project", request.project)
    fields = (request.name, request.timezone, request.default_pool, request.max_running_tasks)
    if not adding and all(value is None for value in fields):
        raise UsageError(
            "project set: no field given; supply --name, --timezone, "
            "--default-pool or --max-running-tasks"
        )
    with locks.MIGRATE.hold(ctx.engine, wait_seconds=30), ctx.engine.begin() as conn:
        old = conn.execute(
            statement(conn, "project_by_code"), {"code": request.project}
        ).one_or_none()
        if (old is not None) == adding:
            raise UsageError(
                f"--project={request.project!r}: expected "
                f"{'a new code' if adding else 'an existing project'}; use project "
                f"list and {'project set' if adding else 'project add'}"
            )
        effective = ProjectFields(
            reason=request.reason,
            project=request.project,
            name=request.name if request.name is not None else request.project if adding else None,
            timezone=request.timezone
            if request.timezone is not None
            else "UTC"
            if adding
            else None,
            default_pool=request.default_pool
            if request.default_pool is not None
            else "default"
            if adding
            else None,
            max_running_tasks=request.max_running_tasks
            if request.max_running_tasks is not None
            else 8
            if adding
            else None,
        )
        project_values(conn, effective)
        values = {
            "code": request.project,
            "name": effective.name,
            "timezone": effective.timezone,
            "default_pool": effective.default_pool,
            "max_running_tasks": effective.max_running_tasks,
        }
        conn.execute(
            statement(conn, "admin_project_add" if adding else "admin_project_set"), values
        )
    return list_projects(ctx, request)


def add_project(ctx: OperationContext, request: ProjectFields) -> Projects:
    """Create a project; its default pool is explicitly default."""
    return change_project(ctx, request, adding=True)


def set_project(ctx: OperationContext, request: ProjectFields) -> Projects:
    """Change only supplied fields."""
    return change_project(ctx, request, adding=False)


def disable_project(ctx: OperationContext, request: ProjectIdentity) -> Projects:
    """Disable an explicit project without touching its metadata or history."""
    reason(request.reason)
    code("--project", request.project)
    with locks.MIGRATE.hold(ctx.engine, wait_seconds=30), ctx.engine.begin() as conn:
        if (
            conn.execute(
                statement(conn, "project_by_code"), {"code": request.project}
            ).one_or_none()
            is None
        ):
            raise UsageError(
                f"--project={request.project!r}: expected an existing project; use project list"
            )
        conn.execute(statement(conn, "admin_project_disable"), {"code": request.project})
    return list_projects(ctx, request)


def change_pool(ctx: OperationContext, request: PoolFields, *, adding: bool) -> Pools:
    """Serialize pool changes with project and task metadata publication."""
    reason(request.reason)
    pool_name(request.name)
    if not adding and request.description is None:
        raise UsageError("pool set: no field given; supply --description")
    with locks.MIGRATE.hold(ctx.engine, wait_seconds=30), ctx.engine.begin() as conn:
        old = conn.execute(statement(conn, "placement_pool"), {"pool": request.name}).one_or_none()
        if (old is not None) == adding:
            raise UsageError(
                f"--name={request.name!r}: expected "
                f"{'a new pool' if adding else 'an existing pool'}; use pool list "
                f"and {'pool set' if adding else 'pool add'}"
            )
        conn.execute(
            statement(conn, "admin_pool_add" if adding else "admin_pool_set"),
            {"name": request.name, "description": request.description},
        )
    return list_pools(ctx, request)


def add_pool(ctx: OperationContext, request: PoolFields) -> Pools:
    """Create a pool identity."""
    return change_pool(ctx, request, adding=True)


def set_pool(ctx: OperationContext, request: PoolFields) -> Pools:
    """Change a pool's supplied description."""
    return change_pool(ctx, request, adding=False)


def disable_pool(ctx: OperationContext, request: PoolIdentity) -> Pools:
    """Refuse to retire a pool referenced by an active project or task."""
    reason(request.reason)
    pool_name(request.name)
    with locks.MIGRATE.hold(ctx.engine, wait_seconds=30), ctx.engine.begin() as conn:
        old = conn.execute(statement(conn, "placement_pool"), {"pool": request.name}).one_or_none()
        if old is None:
            raise UsageError(f"--name={request.name!r}: expected an existing pool; use pool list")
        refs = tuple(conn.execute(statement(conn, "admin_pool_references"), {"pool": request.name}))
        if refs:
            found = ", ".join(f"{r.kind} {r.identity}" for r in refs)
            raise UsageError(
                f"--name={request.name!r}: referenced by {found}; expected no "
                f"active references; change their DEFAULT_POOL or POOL before pool "
                f"disable"
            )
        conn.execute(statement(conn, "admin_pool_disable"), {"name": request.name})
    return list_pools(ctx, request)
