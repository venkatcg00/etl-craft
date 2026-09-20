"""`etl-craft migrate` — apply pending sql/migrations/*.sql files to an existing Engine DB.

[ADDITION] Closes CLAUDE.md open question #7 ("No migration tooling
(Alembic or otherwise) has been discussed... on the assumption schema
review happens before that matters"), per explicit permission ("you may
implement the migration mechanism as well"). Deliberately a small,
Alembic-*lite* runner, not a full migration framework — see
sql/migrations/README.md for the file convention this reads, and
schema.sql's own SCHEMA_MIGRATIONS table comment for why a fresh install
(via schema.sql) and an empty SCHEMA_MIGRATIONS table are consistent by
construction (no migration file has ever existed for anything already
baked into schema.sql — see that file's own note on this).

Each pending file (by filename order, whatever SCHEMA_MIGRATIONS doesn't
yet list) runs inside its own transaction together with the bookkeeping
row for it — a failure rolls both back and stops before any later file
runs, so there's never a "some migrations silently skipped" state to
diagnose.

[CHOICE] A migration file is split into individual `;`-terminated
statements and each is executed separately, rather than handed to the
driver as one multi-statement string — psycopg3's extended query protocol
(what SQLAlchemy's `text()` normally goes through) doesn't reliably support
more than one statement per call the way `psql`'s own simple-query mode
does. The split is a plain string split on `;`, not a real SQL parse (same
limitation this codebase already accepts elsewhere for the same reason,
per Non-goals) — a statement containing a literal semicolon inside a
string literal would be mis-split. Acceptable for a deliberately small
tool; flagged rather than silently assumed safe.
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine

DEFAULT_MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "sql" / "migrations"


class MigrationError(Exception):
    """Raised when a migration file fails to apply."""


def _split_statements(sql_text: str) -> list[str]:
    return [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]


def _pending_migrations(engine: Engine, migrations_dir: Path) -> list[Path]:
    files = sorted(p for p in migrations_dir.glob("*.sql") if p.is_file())
    with engine.connect() as conn:
        applied = set(conn.execute(text("SELECT VERSION FROM SCHEMA_MIGRATIONS")).scalars().all())
    return [f for f in files if f.name not in applied]


def apply_pending_migrations(
    engine: Engine, migrations_dir: Path = DEFAULT_MIGRATIONS_DIR
) -> list[str]:
    """Apply every not-yet-applied sql/migrations/*.sql file, in filename order.

    Returns the filenames actually applied (empty if already fully
    up to date). Raises MigrationError, naming the offending file, on the
    first failure — nothing after it is attempted.
    """
    applied: list[str] = []
    for path in _pending_migrations(engine, migrations_dir):
        try:
            with engine.begin() as conn:
                for statement in _split_statements(path.read_text()):
                    conn.execute(text(statement))
                conn.execute(
                    text("INSERT INTO SCHEMA_MIGRATIONS (VERSION) VALUES (:version)"),
                    {"version": path.name},
                )
        except Exception as exc:
            raise MigrationError(f"{path.name} failed to apply: {exc}") from exc
        applied.append(path.name)
    return applied
