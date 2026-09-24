"""``init-db``: applying the packaged schema to an empty Engine DB."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy.engine import Engine

from etl_craft.core.errors import EngineDbError
from etl_craft.dialects.engine import for_engine
from etl_craft.engine import locks
from etl_craft.engine.migrations import mark_packaged_migrations_applied
from etl_craft.engine.queries import run_script

logger = logging.getLogger(__name__)

SENTINEL_TABLES = ("cfg_pipelines", "aud_pipelines_run_log")
"""Tables whose presence means the database already holds an Engine DB."""


@dataclass(frozen=True)
class InitResult:
    """What ``init-db`` did: statements applied, and packaged migrations recorded as included."""

    statements: int
    recorded_migrations: tuple[str, ...]


def existing_engine_tables(engine: Engine) -> list[str]:
    """Return the Engine DB tables that already exist in this database."""
    return for_engine(engine).existing_tables(engine, SENTINEL_TABLES)


def init_db(engine: Engine, *, force: bool = False) -> InitResult:
    """Apply the packaged schema to an empty database in one transaction.

    The schema is plain CREATE statements, so a database that already has Engine DB tables is
    refused rather than failing halfway; ``migrate`` carries an existing one forward. ``force``
    applies it anyway, for a database known to be empty apart from a leftover table. Packaged
    migrations are then recorded as applied, because the schema already includes them.
    Raises ``EngineDbError``.
    """
    if not force:
        existing = existing_engine_tables(engine)
        if existing:
            raise EngineDbError(
                f"this database already has Engine DB table(s) {existing} — init-db applies the "
                "full schema to an empty database. Use `etl-craft migrate` to bring an existing "
                "one up to date, or pass --force if this one should be initialized anyway."
            )
    dialect = for_engine(engine)
    schema_path = dialect.schema_path()
    statements = dialect.split_statements(schema_path.read_text(encoding="utf-8"))
    with locks.MIGRATE.hold(engine):
        try:
            with engine.begin() as conn:
                dialect.begin_ddl_transaction(conn)
                run_script(conn, statements)
        except Exception as error:
            raise EngineDbError(
                f"failed applying the {dialect.name} schema ({schema_path.name}): {error}"
            ) from error
    logger.info("applied %d statements from the %s schema", len(statements), dialect.name)
    recorded = mark_packaged_migrations_applied(engine)
    return InitResult(len(statements), tuple(recorded))
