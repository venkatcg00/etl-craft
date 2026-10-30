"""Attributed code changes preserve executable identity and refresh in one transaction."""

import json
from dataclasses import dataclass
from typing import ClassVar

from sqlalchemy.engine import Connection

from etl_craft.core.actor import migration
from etl_craft.core.enums_access import Permission
from etl_craft.core.errors import ResourceNotFoundError, RunRefusedError, SqlGuardError, UsageError
from etl_craft.core.governance import check_reason
from etl_craft.core.text import is_metadata_code
from etl_craft.dialects.engine import for_engine
from etl_craft.engine import locks, standing_pauses
from etl_craft.engine.queries import statement
from etl_craft.handlers.resources import LocalLocker
from etl_craft.handlers.sql.watermarks import source_pipelines
from etl_craft.services.config_renames import mentioned_files
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import require_project
from etl_craft.services.operations.pipeline_refresh import RefreshInput, refresh_on
from etl_craft.services.operations.requests import Request, optional_text, require_text
from etl_craft.services.refresh_models import PipelineRefresh
from etl_craft.warehouse.connection import single_writer_lock


@dataclass(frozen=True, kw_only=True)
class RenameInput(Request):
    """An existing pipeline or one of its tasks and the replacement code."""

    pipeline_code: str
    to: str
    task_code: str | None = None
    reason: str = ""

    def __post_init__(self) -> None:
        """Check required selectors before opening resources."""
        require_text("pipeline_code", self.pipeline_code)
        optional_text("task_code", self.task_code)
        if not is_metadata_code(self.to):
            raise UsageError(
                f"to={self.to!r}: expected an ASCII letter followed by letters, digits or "
                "underscores, at most 128 characters; choose a valid unused code"
            )


@dataclass(frozen=True)
class RenameResult:
    """The identity retained, checked publications and repository files needing attention."""

    SCHEMA: ClassVar[str] = "etl-craft/rename/1"
    pipeline_id: int
    task_id: int | None
    old_code: str
    new_code: str
    config_files: tuple[str, ...]
    refreshes: tuple[PipelineRefresh, ...]
    message: str = "References in team-owned CFG_ tables and team SQL are the team's to update."


def rename(ctx: OperationContext, request: RenameInput) -> RenameResult:
    """Rename only quiescent metadata; references and passing refreshes commit with it."""
    check_reason(request.reason)
    for_engine(ctx.engine).artifact_location(ctx.config)
    old = request.task_code or request.pipeline_code
    files = mentioned_files(ctx.config, old)
    token = migration.set("rename")
    try:
        with locks.MIGRATE.hold(ctx.engine):
            with ctx.engine.begin() as preflight:
                _quiescent_pipeline(preflight, ctx, request)
            with (
                locks.BUNDLES.hold(ctx.engine),
                single_writer_lock(ctx.config, LocalLocker(ctx.engine, ctx.config)),
                ctx.engine.begin() as conn,
            ):
                pipeline_id = _quiescent_pipeline(conn, ctx, request)
                task_id, affected = _change(conn, ctx, request, pipeline_id)
                refreshed = refresh_on(conn, ctx, RefreshInput(), pipeline_ids=affected)
                if refreshed.failed:
                    raise RunRefusedError(
                        f"pipeline {request.pipeline_code!r}: rename refresh failed; expected "
                        "passing affected definitions; correct these findings and retry: "
                        + "; ".join(
                            f"{f.file}:{f.line}: {f.value}: {f.message}; {f.remedy}"
                            for r in refreshed.refreshes
                            for f in r.findings
                            if f.failed
                        )
                    )
    finally:
        migration.reset(token)
    return RenameResult(
        pipeline_id,
        task_id,
        old,
        request.to,
        files,
        refreshed.refreshes,
    )


def _quiescent_pipeline(conn: Connection, ctx: OperationContext, request: RenameInput) -> int:
    """Refuse busy identities before waiting for resources and again under admission fencing."""
    require_project(conn, ctx, Permission.CAN_MANAGE)
    params = {"project_id": ctx.project_id, "code": request.pipeline_code}
    pipeline = conn.execute(statement(conn, "rename_pipeline"), params).one_or_none()
    if pipeline is None:
        raise ResourceNotFoundError(
            f"project {ctx.project!r}, pipeline {request.pipeline_code!r}: missing; "
            "expected a pipeline in this project; select it with `etl-craft pipelines`"
        )
    pipeline_id = int(pipeline.pipeline_id)
    referenced = (
        _parameters(conn, ctx, request, update=False) if request.task_code is None else set()
    )
    for identity in sorted({pipeline_id, *referenced}):
        _require_idle(conn, identity, request.pipeline_code)
    return pipeline_id


def _require_idle(conn: Connection, pipeline_id: int, code: str) -> None:
    """Fence admissions whose pinned code references would become unresolvable."""
    standing_pauses.lock(conn, pipeline_id)
    blockers = conn.execute(statement(conn, "rename_blockers"), {"pipeline_id": pipeline_id}).all()
    if blockers:
        raise RunRefusedError(
            f"rename pipeline {code!r}, referenced pipeline_id={pipeline_id}: found "
            + ", ".join(f"{b.kind} {b.identity}={b.value}" for b in blockers)
            + "; expected no non-terminal runs or pending windows; finish or cancel "
            "the run and explicitly abandon any pending window before renaming"
        )


def _change(
    conn: Connection, ctx: OperationContext, request: RenameInput, pipeline_id: int
) -> tuple[int | None, frozenset[int]]:
    params = {"pipeline_id": pipeline_id, "project_id": ctx.project_id, "code": request.to}
    kind = "task" if request.task_code is not None else "pipeline"
    if conn.execute(statement(conn, f"rename_{kind}"), params).first() is not None:
        raise UsageError(
            f"project {ctx.project!r}, pipeline {request.pipeline_code!r}, "
            f"{kind} to={request.to!r}: already exists; "
            "expected an unused code; choose a different code, including from retired rows"
        )
    dependents = conn.execute(statement(conn, "rename_dependents"), params).all()
    external = [r for r in dependents if r.project_id != ctx.project_id]
    if external:
        raise RunRefusedError(
            f"pipeline {request.pipeline_code!r}: cross-project references "
            f"{[(r.project_id, r.pipeline_code) for r in external]!r} cannot be refreshed "
            "from this project folder; expected safely refreshable references; coordinate "
            "removing those dependencies before renaming and restore them afterward"
        )
    affected = {pipeline_id, *(int(r.pipeline_id) for r in dependents)}
    task_id = None
    if request.task_code is not None:
        task_id = conn.execute(
            statement(conn, "rename_task"), params | {"code": request.task_code}
        ).scalar_one_or_none()
        if task_id is None:
            raise ResourceNotFoundError(
                f"task {request.pipeline_code}.{request.task_code}: missing; expected a task "
                "in this pipeline; select an existing task with `etl-craft steps`"
            )
    else:
        affected.update(_parameters(conn, ctx, request))
        conn.execute(
            statement(conn, "rename_grants"),
            params
            | {
                "old": request.pipeline_code,
                "new": request.to,
            },
        )
    conn.execute(
        statement(conn, f"rename_{kind}_update"),
        params
        | {
            "old": request.task_code or request.pipeline_code,
            "new": request.to,
            "task_id": task_id,
        },
    )
    return (None if task_id is None else int(task_id)), frozenset(affected)


def _parameters(
    conn: Connection, ctx: OperationContext, request: RenameInput, *, update: bool = True
) -> set[int]:
    affected = set()
    for row in conn.execute(statement(conn, "rename_parameters"), {"project_id": ctx.project_id}):
        value = row.value
        if row.name == "TRIGGER_PIPELINE_CODE":
            if value != request.pipeline_code:
                continue
            value = request.to
        else:
            try:
                codes = source_pipelines({row.name: value})
            except SqlGuardError as error:
                raise RunRefusedError(
                    f"{row.pipeline_code}.{row.task_code} {row.name}={value!r}: reference "
                    f"cannot be updated safely: {error}; correct the parameter before renaming"
                ) from error
            if request.pipeline_code not in codes:
                continue
            value = json.dumps([request.to if c == request.pipeline_code else c for c in codes])
        affected.add(int(row.pipeline_id))
        if not update:
            continue
        conn.execute(
            statement(conn, "rename_parameter_update"),
            {
                "parameter_id": row.parameter_id,
                "value": value,
            },
        )
    return affected
