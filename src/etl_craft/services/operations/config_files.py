"""``etl-craft config``: plan, apply and export the project's config files as the caller."""

from __future__ import annotations

from pathlib import Path

from etl_craft.core.actor import acting_as
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
    ctx: OperationContext, directory: Path | None = None, *, overwrite: bool = False
) -> config_files.ConfigExport:
    """Write the active ``CFG_`` rows as config files."""
    target = config_files.config_directory(ctx.config, directory)
    with acting_as(ctx.actor):
        return config_files.export_config(ctx.engine, target, overwrite=overwrite)
