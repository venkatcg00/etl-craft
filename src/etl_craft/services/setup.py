"""``setup``: check a configuration, then create or bring up to date the Engine DB.

Setup first runs ``doctor``'s checks, apart from the Engine DB's tables and migrations, which it
is about to create. If any check fails it stops with nothing changed: every connection the
engine uses must work before it builds anything. Otherwise it creates the Engine DB tables in a
database that has none, then applies the migrations still pending, packaged and the project's
own. On an Engine DB that is up to date it changes nothing, so it can be run again safely.

Setup creates nothing outside the Engine DB's own tables: the Engine DB schema, the warehouse
database and schema, and the email relay must already exist.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from etl_craft.config import ConnectorConfig
from etl_craft.engine.connection import engine_db
from etl_craft.engine.migrations import apply_pending_migrations
from etl_craft.engine.schema import existing_engine_tables, init_db
from etl_craft.services.doctor import Check, Status, run_checks

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SetupResult:
    """What setup checked, and what it did once the checks passed."""

    checks: list[Check]
    created_tables: bool = False
    applied_migrations: list[str] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        """Whether a check failed, so nothing was changed."""
        return any(check.status is Status.FAIL for check in self.checks)


def setup(config: ConnectorConfig) -> SetupResult:
    """Check the configuration; when nothing fails, create and migrate the Engine DB."""
    checks = run_checks(config, engine_state=False)
    result = SetupResult(checks)
    if result.failed:
        logger.info("setup changed nothing: a check failed")
        return result
    engine = engine_db(config)
    try:
        created = not existing_engine_tables(engine)
        if created:
            init_db(engine)
            logger.info("created the Engine DB tables")
        applied = apply_pending_migrations(engine, project_default=config.migrations_dir)
    finally:
        engine.dispose()
    return SetupResult(checks, created_tables=created, applied_migrations=applied)
