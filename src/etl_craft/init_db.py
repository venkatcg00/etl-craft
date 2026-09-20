"""`etl-craft init-db` — apply the packaged schema to an empty Engine DB.

[ADDITION, 2026-09-20, E2-13] There was previously no way to create the
Engine DB from an installed package at all. `sql/schema.sql` is the single
authoritative full definition for a fresh install, and it lived only in the
git checkout — a team running `uv add etl-craft` could not get to a working
database by any documented route.

Deliberately separate from `migrate`, and deliberately refusing to run
against a database that already has engine tables. `schema.sql` is plain
`CREATE TABLE`, not idempotent by design ("meant to run once against an
empty DB"), so re-running it against a populated database would fail
partway with a confusing duplicate-object error. The refusal names what it
found instead, and points at `migrate` — which is the right tool for
carrying an *existing* database forward.
"""

from __future__ import annotations

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from etl_craft.migrate import _split_statements
from etl_craft.packaged_sql import packaged_schema_path, read_packaged_schema

# Enough of the schema to tell "empty" from "already set up". Checking one
# table rather than all of them keeps the message honest: a half-applied
# schema is still "not empty", and this tool is not the thing to repair it.
_SENTINEL_TABLES = ("cfg_pipelines", "aud_pipelines_run_log")


class InitDbError(Exception):
    """Raised when the Engine DB cannot be initialized."""


def existing_engine_tables(engine: Engine) -> list[str]:
    """Return whichever engine tables already exist in this database."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = CURRENT_SCHEMA() AND LOWER(TABLE_NAME) IN :names"
            ).bindparams(bindparam("names", expanding=True)),
            {"names": list(_SENTINEL_TABLES)},
        ).all()
    return sorted(row[0] for row in rows)


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

    statements = _split_statements(read_packaged_schema())
    try:
        with engine.begin() as conn:
            for statement in statements:
                conn.execute(text(statement))
    except Exception as exc:
        raise InitDbError(f"failed applying {packaged_schema_path().name}: {exc}") from exc
    return len(statements)
