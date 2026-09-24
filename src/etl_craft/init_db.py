"""`etl-craft init-db` — apply the packaged schema to an empty Engine DB.

[ADDITION, 2026-09-20, E2-13] There was previously no way to create the
Engine DB from an installed package at all. `schema.sql` is the single
authoritative full definition for a fresh install, and it lived only in the
git checkout — a team running `uv add etl-craft` could not get to a working
database by any documented route. Each Engine DB dialect now ships its own,
in dialects/engine_dialects/<dialect>/ (2026-09-24).

Deliberately separate from `migrate`, and deliberately refusing to run
against a database that already has engine tables. `schema.sql` is plain
`CREATE TABLE`, not idempotent by design ("meant to run once against an
empty DB"), so re-running it against a populated database would fail
partway with a confusing duplicate-object error. The refusal names what it
found instead, and points at `migrate` — which is the right tool for
carrying an *existing* database forward.
"""

from __future__ import annotations

from sqlalchemy.engine import Engine

from etl_craft.dialects.engine_dialects import for_engine

# Enough of the schema to tell "empty" from "already set up". Checking one
# table rather than all of them keeps the message honest: a half-applied
# schema is still "not empty", and this tool is not the thing to repair it.
_SENTINEL_TABLES = ("cfg_pipelines", "aud_pipelines_run_log")


class InitDbError(Exception):
    """Raised when the Engine DB cannot be initialized."""


def existing_engine_tables(engine: Engine) -> list[str]:
    """Return whichever engine tables already exist in this database."""
    return for_engine(engine).existing_tables(engine, _SENTINEL_TABLES)


def init_db(engine: Engine, *, force: bool = False) -> int:
    """Apply the packaged schema. Returns the number of statements executed.

    `force` skips the already-initialized refusal — for a database a team
    knows is empty despite a leftover table, and never something this runs on
    its own initiative.
    """
    if not force:
        existing = existing_engine_tables(engine)
        if existing:
            raise InitDbError(
                f"this database already has engine table(s) {existing} — `init-db` applies "
                "the full schema to an *empty* database and is not idempotent. Use "
                "`etl-craft migrate` to carry an existing database forward, or pass "
                "--force if you are certain this one should be re-initialized."
            )

    dialect = for_engine(engine)
    schema_path = dialect.schema_path()
    if not schema_path.is_file():
        raise InitDbError(
            f"packaged schema not found at {str(schema_path)!r} — the installed package is "
            "missing its dialect SQL files"
        )
    statements = dialect.split_statements(schema_path.read_text(encoding="utf-8"))
    try:
        with engine.begin() as conn:
            dialect.begin_ddl_transaction(conn)
            for statement in statements:
                # exec_driver_sql, not execute(text(...)) — see the same
                # change in migrate.apply_pending_migrations (E2-79). schema.sql
                # is a trusted file of whole DDL statements with nothing to
                # bind, and text() would re-scan it for :name parameters with
                # none of the dialect splitter's quote awareness.
                conn.exec_driver_sql(statement)
    except Exception as exc:
        raise InitDbError(f"failed applying {dialect.name} {schema_path.name}: {exc}") from exc
    return len(statements)
