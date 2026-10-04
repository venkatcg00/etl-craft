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
        if release == "0.2.0":
            for migration in migrations.migration_streams(db.engine)[0].files:
                if migration.version < "0007":
                    migrations._record(conn, migration)
