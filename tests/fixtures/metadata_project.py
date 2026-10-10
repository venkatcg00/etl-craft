"""A new Engine DB, as ``etl-craft setup`` creates it, in a project holding every file the
configuration examples in ``docs/examples/migrations/`` name.

The project has a DuckDB warehouse, email settings, the examples' SQL files and ingestion
scripts, and empty ``migrations/`` and ``config/`` folders. ``load_examples`` applies the
example migrations in order, as a team's ``etl-craft migrate`` would.
"""

import shutil
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig, load_config
from etl_craft.engine.migrations import apply_pending_migrations
from etl_craft.engine.schema import init_db

EXAMPLES = Path(__file__).parents[2] / "docs" / "examples" / "migrations"
SCRIPT = "def run(task):\n    return None\n"
SCRIPTS = ("fetch_orders", "validate_feed", "publish", "publish_to_partner")
REGIONS = ("uk", "eu", "us")


@dataclass(frozen=True)
class MetadataProject:
    """The project's Engine DB, its loaded configuration and its directory."""

    engine: Engine
    config: ConnectorConfig
    root: Path

    @property
    def migrations(self) -> Path:
        return self.root / "migrations"

    @property
    def config_dir(self) -> Path:
        return self.root / "config"

    def load_examples(self, last: str = "9999") -> list[str]:
        """Apply the example migrations whose names sort up to ``last``; return their names."""
        for example in sorted(EXAMPLES.glob("*.sql")):
            if example.name[: len(last)] <= last:
                shutil.copy(example, self.migrations)
        return apply_pending_migrations(self.engine, self.migrations)

    def rows(self, sql: str, **params: object) -> list[tuple[object, ...]]:
        with self.engine.connect() as conn:
            return [tuple(row) for row in conn.execute(text(sql), params)]


@pytest.fixture
def metadata_project(empty_engine_db, tmp_path, monkeypatch) -> MetadataProject:
    engine_db = empty_engine_db
    init_db(engine_db.engine)
    profile = engine_db.config.engine
    schema = "public" if profile.jdbc_url.startswith("jdbc:postgresql") else "main"
    block = {"jdbc_url": profile.jdbc_url, "schema": schema}
    if profile.auth_mode != "none":
        block |= {
            "user": profile.user,
            "auth_mode": profile.auth_mode,
            "secret": profile.secret_var,
        }
    root = (engine_db.config.config_path or tmp_path / "craft-connector.yml").parent
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {
            "Mode": "local",
            "Email": {"host": "localhost", "port": 1025, "from_address": "etl@x.io"},
        },
        "Engine": {"dev": block},
        "Warehouse": {"dev": {"jdbc_url": "jdbc:duckdb:analytics.duckdb", "schema": "main"}},
    }
    (root / "craft-connector.yml").write_text(yaml.safe_dump(raw, sort_keys=False), "utf-8")
    sql = root / "sql_files" / "sales"
    sql.mkdir(parents=True)
    for region in REGIONS:
        (sql / f"orders_{region}.sql").write_text(
            f"SELECT order_id, customer_id, amount, ordered_at FROM lnd.orders_{region}", "utf-8"
        )
    scripts = root / "ingestion_scripts" / "sales"
    scripts.mkdir(parents=True)
    for name in SCRIPTS:
        (scripts / f"{name}.py").write_text(SCRIPT, "utf-8")
    (root / "migrations").mkdir()
    monkeypatch.chdir(root)
    return MetadataProject(engine_db.engine, load_config(root / "craft-connector.yml"), root)
