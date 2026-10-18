"""Engine DB provisioning and warehouse maintenance, whatever interface requests them."""

from pathlib import Path

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import ConfigurationError
from etl_craft.engine.migrations import apply_pending_migrations
from etl_craft.engine.privileges import grants_sql
from etl_craft.engine.schema import InitResult, init_db
from etl_craft.services.cloning import ClonedTable, clone
from etl_craft.services.lineage import dialect_and_catalog
from etl_craft.services.operations.context import OperationContext, operation
from etl_craft.services.rehash import RehashResult, rehash
from etl_craft.services.setup import SetupResult, setup
from etl_craft.services.upgrade_targets import UpgradeResult, upgrade_targets


def initialize(
    ctx: OperationContext, *, force: bool = False, provisioning: bool = False
) -> InitResult:
    """Create the Engine DB from the packaged schema; see ``operation`` for ``provisioning``."""
    with operation(ctx, "init-db", {"force": force}, provisioning=provisioning):
        return init_db(ctx.engine, force=force)


def migrate(
    ctx: OperationContext, migrations_dir: Path | None = None, *, provisioning: bool = False
) -> list[str]:
    """Apply pending packaged and project migrations; see ``operation`` for ``provisioning``."""
    arguments = {"migrations_dir": migrations_dir}
    with operation(ctx, "migrate", arguments, provisioning=provisioning):
        dialect, catalog = dialect_and_catalog(ctx.config)
        return apply_pending_migrations(
            ctx.engine,
            migrations_dir,
            project_default=ctx.config.migrations_dir,
            catalog=catalog,
            warehouse_dialect=dialect,
        )


def clone_tables(ctx: OperationContext) -> list[ClonedTable]:
    """Mirror the configured Engine DB tables into the warehouse."""
    with operation(ctx, "clone", {}):
        if not ctx.config.cloning.enabled:
            raise ConfigurationError(
                "Cloning is off in craft-connector.yml: set Cloning.Enabled: true and a Scope of "
                "cfg, aud or all"
            )
        return clone(ctx.engine, ctx.config)


def rehash_target(ctx: OperationContext, target: str, *, dry_run: bool = False) -> RehashResult:
    """Publish hash version 2 for an existing merge target, or show the SQL that would."""
    with operation(ctx, "rehash", {"target": target, "dry_run": dry_run}):
        return rehash(ctx.engine, ctx.config, target, dry_run=dry_run)


def upgrade_audit_columns(
    ctx: OperationContext, *, action: str | None, target: str | None, dry_run: bool
) -> list[UpgradeResult]:
    """Add the current audit columns to existing targets, or show the SQL that would."""
    arguments = {"action": action, "target": target, "dry_run": dry_run}
    with operation(ctx, "upgrade-targets", arguments):
        return upgrade_targets(
            ctx.engine, ctx.config, action=action, target=target, dry_run=dry_run
        )


def setup_engine(config: ConnectorConfig) -> SetupResult:
    """Check the deployment, then create or upgrade its Engine DB; local provisioning only."""
    return setup(config)


def grants(config: ConnectorConfig) -> str:
    """Return the SQL giving people read-only Engine DB logins, or SQLite's permission note."""
    if not config.engine.jdbc_url.startswith("jdbc:postgresql:"):
        return "SQLite uses file permissions; restrict the database file to its owner."
    return grants_sql(config.engine.schema or "public")
