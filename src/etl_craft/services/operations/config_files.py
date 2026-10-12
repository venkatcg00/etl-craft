"""``etl-craft config``: plan, apply and export the project's config files as the caller."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from etl_craft.core.actor import acting_as
from etl_craft.core.errors import ResourceNotFoundError, UsageError
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement
from etl_craft.services import config_files
from etl_craft.services.operations.context import OperationContext, operation


def plan_config(ctx: OperationContext, directory: Path | None = None) -> config_files.ConfigSync:
    """Load the config files, validate the result and roll it back: what ``apply`` would do."""
    files = config_files.read_config_files(config_files.config_directory(ctx.config, directory))
    with acting_as(ctx.actor):
        return config_files.sync_config(ctx.engine, ctx.config, files, apply=False)


def apply_config(ctx: OperationContext, directory: Path | None = None) -> config_files.ConfigSync:
    """Make the active ``CFG_`` rows match the config files, unless ``validate`` fails them."""
    target = config_files.config_directory(ctx.config, directory)
    with operation(ctx, "config apply", {"config_dir": str(target)}):
        files = config_files.read_config_files(target)
        return config_files.sync_config(ctx.engine, ctx.config, files, apply=True)


def export_config(
    ctx: OperationContext,
    directory: Path | None = None,
    *,
    overwrite: bool = False,
    as_of: str | None = None,
) -> config_files.ConfigExport:
    """Write the ``CFG_`` rows as config files: as they are now, or as of a moment.

    ``as_of`` is a ``pipeline_run_id``, meaning the moment that run started, or an ISO 8601
    time; a time without a zone is UTC.
    """
    target = config_files.config_directory(ctx.config, directory)
    moment = None if as_of is None else _moment(ctx, as_of)
    with acting_as(ctx.actor):
        return config_files.export_config(
            ctx.engine, target, overwrite=overwrite, as_of=moment, config=ctx.config
        )


def _moment(ctx: OperationContext, as_of: str) -> datetime:
    written = as_of.strip()
    if written.isdigit():
        with ctx.engine.connect() as conn:
            started = conn.execute(
                statement(conn, "pipeline_run_start"), {"pipeline_run_id": int(written)}
            ).scalar_one_or_none()
        if started is None:
            raise ResourceNotFoundError(
                f"--as-of {written}: no pipeline run has pipeline_run_id {written}; give an "
                "existing run id or a time such as 2026-10-11T08:00:00Z"
            )
        return as_utc(started)
    try:
        return as_utc(datetime.fromisoformat(written))
    except ValueError:
        raise UsageError(
            f"--as-of {written!r} is neither a pipeline_run_id nor an ISO 8601 time such as "
            "2026-10-11T08:00:00Z"
        ) from None
