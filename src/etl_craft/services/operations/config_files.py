"""``etl-craft config``: plan, apply and export the project's config files as the caller."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path

from sqlalchemy.engine import Connection

from etl_craft.core.enums import Permission
from etl_craft.core.errors import PermissionDeniedError, ResourceNotFoundError, UsageError
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement
from etl_craft.services import config_files
from etl_craft.services.config_models import TagAccessChange
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import (
    pipeline_access,
    project_permission,
    require_project,
)
from etl_craft.services.operations.requests import Request
from etl_craft.services.operations.tag_access import tag_access_changes


@dataclass(frozen=True, kw_only=True)
class ConfigDirectory(Request):
    """The config files' folder; ``config/`` in the project directory unless given."""

    config_dir: Path | None = None


@dataclass(frozen=True, kw_only=True)
class ConfigExportInput(ConfigDirectory):
    """Where to write the ``CFG_`` rows, whether to overwrite, and as of when."""

    force: bool = False
    as_of: str | None = None


def plan_config(ctx: OperationContext, request: ConfigDirectory) -> config_files.ConfigSync:
    """Load the config files, validate the result and roll it back: what ``apply`` would do."""
    return _sync(ctx, request, apply=False)


def apply_config(ctx: OperationContext, request: ConfigDirectory) -> config_files.ConfigSync:
    """Merge submitted ``CFG_`` changes, unless ``validate`` fails the result."""
    result = _sync(ctx, request, apply=True)
    if result.failed:
        return result
    from etl_craft.core.enums import ActionName
    from etl_craft.services.operations.actions import invoke
    from etl_craft.services.operations.pipeline_refresh import RefreshInput
    from etl_craft.services.refresh_models import RefreshResults

    checked = invoke(ctx, ActionName.PIPELINE_REFRESH, RefreshInput())
    assert isinstance(checked, RefreshResults)
    return replace(result, refreshes=checked.refreshes)


def _sync(
    ctx: OperationContext, request: ConfigDirectory, *, apply: bool
) -> config_files.ConfigSync:
    """Capture tag effects in the same locked snapshot used to validate submitted changes."""
    target = config_files.config_directory(ctx.config, request.config_dir)
    files = config_files.read_config_files(target)
    changes: list[TagAccessChange] = []

    def authorize(conn: Connection, completed: config_files.ConfigFiles) -> None:
        check_dependencies(conn, completed, ctx=ctx)
        changes.extend(tag_access_changes(conn, ctx, completed))

    done = config_files.sync_config(ctx.engine, ctx.config, files, apply=apply, authorize=authorize)
    return replace(done, tag_access_changes=tuple(changes))


def export_config(ctx: OperationContext, request: ConfigExportInput) -> config_files.ConfigExport:
    """Write the ``CFG_`` rows as config files: as they are now, or as of a moment.

    ``as_of`` is a ``pipeline_run_id``, meaning the moment that run started, or an ISO 8601
    time; a time without a zone is UTC.
    """
    with ctx.engine.connect() as conn:
        policy = pipeline_access(conn, ctx)
        if any(not policy.permits(code) for code in policy.roles):
            raise PermissionDeniedError(
                f"account {ctx.actor.name!r}: project {ctx.project!r} config export requires "
                "CAN_VIEW on every pipeline to export the full state; ask its project manager "
                "for the missing permissions or ask the manager to export it"
            )
    target = config_files.config_directory(ctx.config, request.config_dir)
    moment = None if request.as_of is None else resolve_as_of(ctx, request.as_of)
    return config_files.export_config(
        ctx.engine,
        target,
        overwrite=request.force,
        as_of=moment,
        config=ctx.config,
        project_code=ctx.project,
    )


def resolve_as_of(ctx: OperationContext, as_of: str) -> datetime:
    """Resolve the shared run-id or ISO timestamp selector to UTC."""
    written = as_of.strip()
    if written.isdigit():
        with ctx.engine.connect() as conn:
            code = conn.execute(
                statement(conn, "api_run_code"), {"run_id": int(written)}
            ).scalar_one_or_none()
            allowed = code is not None and pipeline_access(conn, ctx).permits(code)
            started = (
                conn.execute(
                    statement(conn, "pipeline_run_start"), {"pipeline_run_id": int(written)}
                ).scalar_one_or_none()
                if allowed
                else None
            )
        if started is None:
            raise ResourceNotFoundError(
                f"`as_of` {written}: no pipeline run has pipeline_run_id {written}; give an "
                "existing run id or a time such as 2026-10-11T08:00:00Z"
            )
        return as_utc(started)
    try:
        return as_utc(datetime.fromisoformat(written))
    except ValueError:
        raise UsageError(
            f"`as_of` {written!r} is neither a pipeline_run_id nor an ISO 8601 time such as "
            "2026-10-11T08:00:00Z"
        ) from None


def check_dependencies(
    conn: Connection, files: config_files.ConfigFiles, *, ctx: OperationContext
) -> None:
    """Recheck deployment rights and upstream project access before publishing file changes."""
    require_project(conn, ctx, Permission.CAN_MANAGE)
    for filename in ("pipeline_dependencies.csv", "task_dependencies.csv"):
        for row in files.rows.get(filename, {}).values():
            code = row.values["DEPENDS_ON_PROJECT_CODE"]
            if not row.active or not code or code == ctx.project:
                continue
            upstream = conn.execute(
                statement(conn, "project_by_code"), {"code": code}
            ).one_or_none()
            visible = (
                upstream is not None
                and project_permission(
                    conn, replace(ctx, project=str(code), project_id=int(upstream.project_id))
                )
                is not None
            )
            if not visible:
                raise ResourceNotFoundError(
                    f"{filename} line {row.line}: project {ctx.project!r} "
                    "declares upstream project "
                    f"{code!r}, which is missing or not permitted; "
                    "expected CAN_VIEW on the upstream "
                    "project; ask its project manager or a deployment administrator for access"
                )
