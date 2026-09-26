"""Prepare the demo's databases, with the Python etl-craft is installed in.

    python prepare.py warehouse   # the schemas the demo writes, in warehouse.duckdb
    python prepare.py metadata    # the pipelines, as CFG_ rows, in engine.db (after setup)

etl-craft never creates warehouse schemas and never writes CFG_ rows: your team does both. This
script does them for the demo, from warehouse_schemas.sql and metadata/support_insights.sql.
Pass metadata/remote_mode.sql after `metadata` to load it too.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import duckdb

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
    with sqlite3.connect(database) as conn:
        for name in files or ["metadata/support_insights.sql"]:
            conn.executescript((HERE / name).read_text(encoding="utf-8"))
            print(f"loaded {name} into engine.db")


if __name__ == "__main__":
    step, *rest = sys.argv[1:] or [""]
    if step == "warehouse":
        warehouse()
    elif step == "metadata":
        metadata(rest)
    else:
        raise SystemExit("usage: python prepare.py warehouse | metadata [file.sql ...]")
