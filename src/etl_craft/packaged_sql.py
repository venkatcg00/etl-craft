"""Access to the SQL files that ship inside the installed package.

[ADDITION, 2026-09-20, E2-13] The built wheel used to contain 24 `.py` files
and nothing else — no `sql/`, no `py.typed`, no license. Two consequences
that between them made `uv add etl-craft` a dead end:

  * `sql/schema.sql` is the single authoritative full definition of the Engine
    DB, and it existed only in the git checkout. An installed package had no
    way to create the schema at all, and there was no `init-db` verb either.
  * `etl-craft migrate` resolved its directory package-relative, landing above
    `site-packages`, where nothing exists. `Path.glob` on a missing directory
    yields nothing without error, so it printed "already up to date" and
    skipped a team's migrations silently.

`sql/` therefore moved to `src/etl_craft/sql/` — uv_build packages
`src/<module>/` only — and is read through `importlib.resources` rather than
`__file__` arithmetic, so it works the same from a checkout, a wheel, or a
zipimport.
"""

from __future__ import annotations

from importlib import resources
from pathlib import Path

SCHEMA_FILENAME = "schema.sql"
MIGRATIONS_DIRNAME = "migrations"


def packaged_sql_dir() -> Path:
    """Return the directory holding the packaged SQL files."""
    return Path(str(resources.files("etl_craft") / "sql"))


def packaged_schema_path() -> Path:
    """Return the packaged `schema.sql` — the full definition for a fresh install."""
    return packaged_sql_dir() / SCHEMA_FILENAME


def packaged_migrations_dir() -> Path:
    """Return the packaged `migrations/` directory."""
    return packaged_sql_dir() / MIGRATIONS_DIRNAME


def read_packaged_schema() -> str:
    """Read the packaged `schema.sql`'s contents."""
    path = packaged_schema_path()
    if not path.is_file():
        raise FileNotFoundError(
            f"packaged schema not found at {str(path)!r} — the installed package is "
            "missing its sql/ data files"
        )
    return path.read_text(encoding="utf-8")
