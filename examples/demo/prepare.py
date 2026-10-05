"""Prepare the demo's databases, with the Python etl-craft is installed in.

    python prepare.py warehouse   # the schemas the demo writes, in warehouse.duckdb
    python prepare.py metadata    # the pipelines, as CFG_ rows, in engine.db (after setup)

The team creates warehouse schemas and authors CFG_ rows as project migrations. This script
prepares both from warehouse_schemas.sql and metadata/support_insights.sql, then runs migrate.
Pass metadata/remote_mode.sql after `metadata` to load it too.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

from etl_craft.cli import main

HERE = Path(__file__).resolve().parent


def warehouse() -> None:
    """Create the demo's schemas in warehouse.duckdb."""
    sql = (HERE / "warehouse_schemas.sql").read_text(encoding="utf-8")
    with duckdb.connect(str(HERE / "warehouse.duckdb")) as conn:
        conn.execute(sql)
    print("created the schemas in warehouse.duckdb")


def metadata(files: list[str]) -> None:
    """Load the demo's metadata into engine.db, which `etl-craft setup` created."""
    database = HERE / "engine.db"
    if not database.exists():
        raise SystemExit("engine.db does not exist yet: run `etl-craft setup` first")
    migrations = HERE / "migrations"
    migrations.mkdir(exist_ok=True)
    for name in files or ["metadata/support_insights.sql"]:
        source = HERE / name
        existing = sorted(migrations.glob(f"*_{source.name}"))
        destination = (
            existing[0]
            if existing
            else migrations / (f"{len(list(migrations.glob('*.sql'))) + 1:04d}_{source.name}")
        )
        sql = source.read_text(encoding="utf-8")
        if destination.exists():
            if destination.read_text(encoding="utf-8") != sql:
                raise SystemExit(
                    f"{name} differs from {destination}; keep existing migrations unchanged "
                    "and add a new project migration for the edit"
                )
        else:
            destination.write_text(sql, encoding="utf-8")
    result = main(["--config", str(HERE / "craft-connector.yml"), "migrate"])
    if result:
        raise SystemExit(result)


if __name__ == "__main__":
    step, *rest = sys.argv[1:] or [""]
    if step == "warehouse":
        warehouse()
    elif step == "metadata":
        metadata(rest)
    else:
        raise SystemExit("usage: python prepare.py warehouse | metadata [file.sql ...]")
