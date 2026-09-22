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

[DEVIATION, 2026-09-20, E2-05] Three plumbing fixes, all found by the
iteration-2 review rather than by anything failing:

  * `DEFAULT_MIGRATIONS_DIR` used to be `Path(__file__).parents[2] / "sql" /
    "migrations"`, which resolves to the repo checkout — and to whatever sits
    above `site-packages` once installed, where the wheel shipped no `sql/`
    at all (E2-13). `Path.glob` on a nonexistent directory yields nothing
    without error, so `migrate` printed "already up to date" and silently
    skipped a team's migrations. Resolution order is now: an explicit
    `--migrations-dir`, then `$ETL_CRAFT_MIGRATIONS_DIR`, then
    `./sql/migrations` relative to the *current working directory* if it
    exists, then the packaged copy that now ships inside the wheel.
  * `SELECT VERSION FROM SCHEMA_MIGRATIONS` raised a raw `ProgrammingError`
    from outside the try against any database predating that table, so the
    CLI produced a traceback and nothing could bootstrap it. The table is
    created if absent.
  * Two concurrent `migrate` runs were unserialized. A Postgres advisory lock
    now spans the whole run, so the second waits rather than double-applying.

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

import os
import re
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl_craft.packaged_sql import packaged_migrations_dir

MIGRATIONS_DIR_ENV_VAR = "ETL_CRAFT_MIGRATIONS_DIR"
# Arbitrary but fixed: the key both `migrate` runs agree on. Postgres advisory
# locks are namespaced only by the integer itself, so it is written here once
# rather than computed, and chosen far from anything a team's own SQL is
# likely to pick.
_ADVISORY_LOCK_KEY = 8_241_007


class MigrationError(Exception):
    """Raised when a migration file fails to apply."""


def resolve_migrations_dir(explicit: Path | str | None = None) -> Path:
    """Resolve which migrations directory to read, most-specific first.

    [DEVIATION, 2026-09-20, E2-05] See this module's own docstring: the old
    package-relative default silently resolved to a nonexistent path once
    installed, and `migrate` then reported success having applied nothing.
    """
    if explicit is not None:
        return Path(explicit)
    from_env = os.environ.get(MIGRATIONS_DIR_ENV_VAR)
    if from_env:
        return Path(from_env)
    local = Path.cwd() / "sql" / "migrations"
    if local.is_dir():
        return local
    return packaged_migrations_dir()


def split_statements(sql_text: str) -> list[str]:
    """Split a SQL file into statements on `;`, respecting quotes and comments.

    [DEVIATION, 2026-09-20, E2-05] This used to be `sql_text.split(";")`, a
    documented limitation that turned out to be the *first* thing anyone would
    hit: `schema.sql`'s own trigger functions are `CREATE FUNCTION ... $$ ...
    ; ... $$` bodies, so any migration touching them — and `init-db` applying
    the schema at all — would be shredded mid-body. A literal semicolon inside
    an ordinary string literal broke it too.

    Deliberately a small scanner, not a SQL parser (Non-goals rules one out):
    it tracks single-quoted strings with their `''` escape, dollar-quoted
    bodies including tagged `$tag$` ones, `--` line comments and `/* */` block
    comments, and splits on any `;` outside all of them. That is the whole
    grammar a statement splitter needs, and nothing here tries to understand
    the statements themselves.
    """
    statements: list[str] = []
    current: list[str] = []
    i = 0
    length = len(sql_text)
    while i < length:
        ch = sql_text[i]
        rest = sql_text[i:]

        if rest.startswith("--"):
            end = sql_text.find("\n", i)
            end = length if end == -1 else end
            current.append(sql_text[i:end])
            i = end
            continue

        if rest.startswith("/*"):
            end = sql_text.find("*/", i + 2)
            end = length if end == -1 else end + 2
            current.append(sql_text[i:end])
            i = end
            continue

        if ch == "'":
            end = i + 1
            while end < length:
                if sql_text[end] == "'":
                    if end + 1 < length and sql_text[end + 1] == "'":
                        end += 2
                        continue
                    end += 1
                    break
                end += 1
            current.append(sql_text[i:end])
            i = end
            continue

        if ch == "$":
            tag = _dollar_tag_at(sql_text, i)
            if tag is not None:
                close = sql_text.find(tag, i + len(tag))
                end = length if close == -1 else close + len(tag)
                current.append(sql_text[i:end])
                i = end
                continue

        if ch == ";":
            statements.append("".join(current))
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    statements.append("".join(current))
    return [stmt.strip() for stmt in statements if stmt.strip() and not _is_only_comments(stmt)]


def _dollar_tag_at(sql_text: str, index: int) -> str | None:
    """Return the dollar-quote tag starting at `index` (e.g. "$$", "$fn$"), or None."""
    end = sql_text.find("$", index + 1)
    if end == -1:
        return None
    body = sql_text[index + 1 : end]
    if body and not (body[0].isalpha() or body[0] == "_"):
        return None
    if not all(c.isalnum() or c == "_" for c in body):
        return None
    return sql_text[index : end + 1]


def _is_only_comments(statement: str) -> bool:
    """Report whether `statement` holds nothing but comments and whitespace."""
    stripped = re.sub(r"/\*.*?\*/", "", statement, flags=re.DOTALL)
    stripped = re.sub(r"--[^\n]*", "", stripped)
    return not stripped.strip()


# Kept as the private name the rest of this package already imports.
_split_statements = split_statements


def _ensure_bookkeeping_table(engine: Engine) -> None:
    """Create SCHEMA_MIGRATIONS if this database predates it.

    Mirrors schema.sql's own definition. Without this, `migrate` against a
    database created before the table existed raised a raw ProgrammingError
    that no CLI handler caught, and nothing could bootstrap it.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE IF NOT EXISTS SCHEMA_MIGRATIONS ("
                "VERSION VARCHAR PRIMARY KEY, "
                "APPLIED_AT TIMESTAMPTZ NOT NULL DEFAULT now())"
            )
        )


def _pending_migrations(engine: Engine, migrations_dir: Path) -> list[Path]:
    files = sorted(p for p in migrations_dir.glob("*.sql") if p.is_file())
    with engine.connect() as conn:
        applied = set(conn.execute(text("SELECT VERSION FROM SCHEMA_MIGRATIONS")).scalars().all())
    return [f for f in files if f.name not in applied]


def mark_packaged_migrations_applied(engine: Engine) -> list[str]:
    """Record the *packaged* migrations as applied, without executing any of them.

    [ADDITION, 2026-09-22, E2-83] `setup` on a fresh database used to call
    init_db and then apply_pending_migrations, under a comment claiming the
    migrations were "recorded rather than meaningfully re-run". They were not:
    apply_pending_migrations genuinely runs each file's statements and then
    records it, with no record-only path. So every fresh install executed all
    three migrations on top of a schema that already contained everything they
    add. That worked only because all three happen to be written re-runnably,
    an invariant nothing enforced -- the first migration written as a plain
    `ALTER TABLE x ADD COLUMN y` would break `setup` on every new environment,
    and the first one carrying a data backfill would apply it twice.

    Scoped to the migrations that ship *inside the package*, deliberately, and
    not to whatever `resolve_migrations_dir` happens to pick. schema.sql is
    the authoritative full definition of the engine's own schema, so the
    engine's own migrations are by definition already reflected in it. A
    team's `./sql/migrations/` holds *their* changes, which schema.sql knows
    nothing about -- recording those unexecuted would silently skip them,
    which is a worse bug than the one this fixes.
    """
    directory = packaged_migrations_dir()
    if not directory.is_dir():
        return []
    names = sorted(p.name for p in directory.glob("*.sql") if p.is_file())
    if not names:
        return []
    _ensure_bookkeeping_table(engine)
    with engine.begin() as conn:
        for name in names:
            # ON CONFLICT DO NOTHING: this is also reachable for a database
            # where some of them are already recorded, and re-recording one is
            # not an error worth failing a fresh install over.
            conn.execute(
                text(
                    "INSERT INTO SCHEMA_MIGRATIONS (VERSION) VALUES (:version) "
                    "ON CONFLICT (VERSION) DO NOTHING"
                ),
                {"version": name},
            )
    return names


def apply_pending_migrations(engine: Engine, migrations_dir: Path | str | None = None) -> list[str]:
    """Apply every not-yet-applied migration file, in filename order.

    Returns the filenames actually applied (empty if already fully
    up to date). Raises MigrationError, naming the offending file, on the
    first failure — nothing after it is attempted.
    """
    resolved = resolve_migrations_dir(migrations_dir)
    if not resolved.is_dir():
        raise MigrationError(
            f"migrations directory {str(resolved)!r} does not exist — pass "
            f"--migrations-dir, set ${MIGRATIONS_DIR_ENV_VAR}, or run from a directory "
            "containing sql/migrations/"
        )
    _ensure_bookkeeping_table(engine)

    applied: list[str] = []
    # One advisory lock for the whole run: two concurrent `migrate` calls
    # would otherwise both read the same pending list and both try to apply
    # it, colliding only on the VERSION primary key — after each had already
    # run the file's DDL.
    with engine.begin() as lock_conn:
        lock_conn.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": _ADVISORY_LOCK_KEY})
        for path in _pending_migrations(engine, resolved):
            try:
                with engine.begin() as conn:
                    for statement in _split_statements(path.read_text(encoding="utf-8")):
                        # [DEVIATION, 2026-09-22, E2-79] exec_driver_sql, not
                        # execute(text(...)). _split_statements is carefully
                        # quote-aware -- it has to be, for schema.sql's $$ ... $$
                        # trigger bodies -- and text() then ran its *own*, not
                        # quote-aware, :name bind-parameter scan over the same
                        # text. So a migration containing an ordinary string
                        # literal like ':name' or 'docs/#:ref' (seeding a
                        # CFG_TASK_PARAMETERS value, a COMMENT ON, a CHECK
                        # regex) failed with a message about a bind parameter
                        # its author never wrote. The three shipped migrations
                        # escape it only by luck, since SQLAlchemy's own
                        # lookbehind protects a digit before a colon.
                        #
                        # These are whole DDL statements from trusted files
                        # with no parameters to bind, so SQLAlchemy's parsing
                        # buys nothing here.
                        conn.exec_driver_sql(statement)
                    conn.execute(
                        text("INSERT INTO SCHEMA_MIGRATIONS (VERSION) VALUES (:version)"),
                        {"version": path.name},
                    )
            except Exception as exc:
                raise MigrationError(f"{path.name} failed to apply: {exc}") from exc
            applied.append(path.name)
    return applied
