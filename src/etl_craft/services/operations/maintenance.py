"""Engine DB provisioning and warehouse maintenance, whatever interface requests them."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from etl_craft.config import load_config
from etl_craft.core.enums import SqlAction
from etl_craft.core.errors import ConfigurationError
from etl_craft.dialects.engine import build_engine
from etl_craft.engine.migrations import apply_pending_migrations
from etl_craft.engine.privileges import grants_sql
from etl_craft.engine.schema import InitResult, init_db
from etl_craft.services.cloning import ClonedTable, clone
from etl_craft.services.lineage import dialect_and_catalog
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import NoInput, Request
from etl_craft.services.rehash import RehashResult as RehashResult
from etl_craft.services.rehash import rehash
from etl_craft.services.setup import SetupResult as SetupResult
from etl_craft.services.setup import setup
from etl_craft.services.upgrade_targets import UpgradeResult, upgrade_targets


@dataclass(frozen=True, kw_only=True)
class InitInput(Request):
    """Whether to recreate an Engine DB that already has tables."""

    force: bool = False


@dataclass(frozen=True, kw_only=True)
class MigrateInput(Request):
    """Project migrations and connector files for shared-project warehouse dialects."""

    migrations_dir: Path | None = None
    project_configs: tuple[Path, ...] = ()


@dataclass(frozen=True, kw_only=True)
class RehashInput(Request):
    """One existing merge target, as ``schema.table`` or ``database.schema.table``."""

    target: str


@dataclass(frozen=True, kw_only=True)
class UpgradeInput(Request):
    """Which targets to upgrade, and whether only to show the SQL."""

    action: SqlAction | None = None
    target: str | None = None
    dry_run: bool = False


@dataclass(frozen=True)
class Clones:
    """Each Engine DB table mirrored into the warehouse."""

    SCHEMA: ClassVar[str] = "etl-craft/clones/1"
    tables: tuple[ClonedTable, ...]


@dataclass(frozen=True)
class TargetUpgrades:
    """Each target's audit-column upgrade, done or shown."""

    SCHEMA: ClassVar[str] = "etl-craft/target-upgrades/1"
    targets: tuple[UpgradeResult, ...]


@dataclass(frozen=True)
class Grants:
    """The SQL giving people read-only Engine DB logins, or SQLite's permission note."""

    SCHEMA: ClassVar[str] = "etl-craft/grants/1"
    sql: str


def initialize(ctx: OperationContext, request: InitInput) -> InitResult:
    """Create the Engine DB from the packaged schema."""
    return init_db(ctx.engine, force=request.force)


def _migration_dialects(ctx: OperationContext, paths: tuple[Path, ...]) -> dict[str, str]:
    """Resolve supplied project dialects, refusing another database or duplicate project."""
    dialect, _ = dialect_and_catalog(ctx.config)
    resolved = {ctx.config.project: dialect} if dialect is not None else {}
    for path in paths:
        try:
            config = load_config(path)
        except (OSError, UnicodeError) as error:
            raise ConfigurationError(
                f"project config {path}: {error}; expected a readable UTF-8 connector config; "
                "restore its permissions/encoding and retry migrate --project-config"
            ) from error
        candidate = build_engine(config)
        try:
            actual = candidate.url.set(username=None, password=None)
            expected = ctx.engine.url.set(username=None, password=None)
            same = actual == expected and config.engine.schema == ctx.config.engine.schema
        finally:
            candidate.dispose()
        if not same:
            raise ConfigurationError(
                f"project config {path}: Engine address={str(actual)!r}, "
                f"Schema={config.engine.schema!r}; expected the selected Engine DB "
                f"address={str(expected)!r}, Schema={ctx.config.engine.schema!r}; "
                "point every --project-config at the same Engine DB and schema as --config"
            )
        dialect, _ = dialect_and_catalog(config)
        if dialect is None or config.project in resolved:
            raise ConfigurationError(
                f"project config {path}: project={config.project!r}, "
                f"Warehouse dialect={dialect!r}; "
                "expected one config with a Warehouse per project; remove duplicate "
                "--project-config arguments or configure its Warehouse"
            )
        resolved[config.project] = dialect
    return resolved


def migrate(ctx: OperationContext, request: MigrateInput) -> list[str]:
    """Apply pending packaged and project migrations."""
    dialect, catalog = dialect_and_catalog(ctx.config)
    return apply_pending_migrations(
        ctx.engine,
        request.migrations_dir,
        project_default=ctx.config.migrations_dir,
        catalog=catalog,
        warehouse_dialect=dialect,
        project_dialects=_migration_dialects(ctx, request.project_configs),
    )


def clone_tables(ctx: OperationContext, request: NoInput) -> Clones:
    """Mirror the configured Engine DB tables into the warehouse."""
    if not ctx.config.cloning.enabled:
        raise ConfigurationError(
            "Cloning is off in craft-connector.yml: set Cloning.Enabled: true and a Scope of "
            "cfg, aud or all"
        )
    return Clones(tuple(clone(ctx.engine, ctx.config)))


def plan_rehash(ctx: OperationContext, request: RehashInput) -> RehashResult:
    """Show the SQL that would publish hash version 2 for an existing merge target."""
    return rehash(ctx.engine, ctx.config, request.target, dry_run=True)


def rehash_target(ctx: OperationContext, request: RehashInput) -> RehashResult:
    """Publish hash version 2 for an existing merge target."""
    return rehash(ctx.engine, ctx.config, request.target, dry_run=False)


def upgrade_audit_columns(ctx: OperationContext, request: UpgradeInput) -> TargetUpgrades:
    """Add the current audit columns to existing targets, or show the SQL that would."""
    return TargetUpgrades(
        tuple(
            upgrade_targets(
                ctx.engine,
                ctx.config,
                action=request.action,
                target=request.target,
                dry_run=request.dry_run,
            )
        )
    )


def setup_engine(ctx: OperationContext, request: NoInput) -> SetupResult:
    """Check the deployment, then create or upgrade its Engine DB."""
    return setup(ctx.config)


def grants(ctx: OperationContext, request: NoInput) -> Grants:
    """Return the SQL giving people read-only Engine DB logins, or SQLite's permission note."""
    if not ctx.config.engine.jdbc_url.startswith("jdbc:postgresql:"):
        return Grants("SQLite uses file permissions; restrict the database file to its owner.")
    return Grants(grants_sql(ctx.config.engine.schema or "public"))
