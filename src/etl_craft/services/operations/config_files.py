"""``etl-craft config``: plan, apply and export the project's config files as the caller."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from etl_craft.core.enums import ApiRole
from etl_craft.core.errors import ResourceNotFoundError, UsageError
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement
from etl_craft.services import config_files
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import Request


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
    target = config_files.config_directory(ctx.config, request.config_dir)
    files = config_files.read_config_files(target)
    return config_files.sync_config(ctx.engine, ctx.config, files, apply=False)


def apply_config(ctx: OperationContext, request: ConfigDirectory) -> config_files.ConfigSync:
    """Make the active ``CFG_`` rows match the config files, unless ``validate`` fails them."""
    target = config_files.config_directory(ctx.config, request.config_dir)
    files = config_files.read_config_files(target)
    return config_files.sync_config(
        ctx.engine, ctx.config, files, apply=True, allow_grants=ctx.role == ApiRole.ADMIN
    )


def export_config(ctx: OperationContext, request: ConfigExportInput) -> config_files.ConfigExport:
    """Write the ``CFG_`` rows as config files: as they are now, or as of a moment.

    ``as_of`` is a ``pipeline_run_id``, meaning the moment that run started, or an ISO 8601
    time; a time without a zone is UTC.
    """
    target = config_files.config_directory(ctx.config, request.config_dir)
    moment = None if request.as_of is None else resolve_as_of(ctx, request.as_of)
    return config_files.export_config(
        ctx.engine, target, overwrite=request.force, as_of=moment, config=ctx.config
    )


def resolve_as_of(ctx: OperationContext, as_of: str) -> datetime:
    """Resolve the shared run-id or ISO timestamp selector to UTC."""
    written = as_of.strip()
    if written.isdigit():
        with ctx.engine.connect() as conn:
            started = conn.execute(
                statement(conn, "pipeline_run_start"), {"pipeline_run_id": int(written)}
            ).scalar_one_or_none()
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
