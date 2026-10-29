"""Install a tagged schema together with its packaged migration ledger."""

from pathlib import Path

from etl_craft.engine import migrations
from etl_craft.engine.queries import run_script

SCHEMAS = Path(__file__).parent / "schemas"


def install(db, release):
    kind = "postgres" if db.engine.dialect.name == "postgresql" else "sqlite"
    with db.engine.begin() as conn:
        run_script(
            conn, db.dialect.split_statements((SCHEMAS / release / f"{kind}.sql").read_text())
        )
        if release in {"0.2.0", "0.3.0"}:
            for migration in migrations.migration_streams(db.engine)[0].files:
                if migration.version < ("0007" if release == "0.2.0" else "0019"):
                    migrations._record(conn, migration)


def seed_offset(conn, task_id, offset):
    """Seed the released scalar/partition tracker without calling current-schema services."""
    from sqlalchemy import text

    conn.execute(
        text(
            "INSERT INTO AUD_TASK_OFFSET_TRACKER (TASK_ID,OFFSET_TYPE,OFFSET_VALUE) "
            "VALUES (:task,:kind,:value)"
        ),
        {"task": task_id, "kind": offset.offset_type, "value": offset.offset_value},
    )
