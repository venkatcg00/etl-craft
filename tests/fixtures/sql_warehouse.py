"""A warehouse, an Engine DB and a project directory for running SQL tasks, on each warehouse.

``sql_world`` is parametrized over DuckDB (a file, no services), PostgreSQL, Trino over Iceberg
and DuckDB over Iceberg; each case carries its suite marker and works in a schema of its own.
``SqlWorld.task(...)`` adds a SQL task to pipeline P and returns the ``TaskContext`` a task
process would build for it.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl_craft.config import ConnectionProfile, ConnectionSection, ConnectorConfig, SourceConfig
from etl_craft.core.enums import Mode
from etl_craft.engine import runlog
from etl_craft.handlers import sql
from etl_craft.handlers.registry import HandlerResult, TaskContext
from etl_craft.warehouse.connection import build_warehouse_engine
from fixtures.engine_db import apply_schema, sqlite_engine_db
from fixtures.metadata import add_pipeline, add_task
from fixtures.services import (
    MINIO_PASSWORD,
    MINIO_USER,
    POSTGRES_DB,
    POSTGRES_PASSWORD,
    POSTGRES_USER,
    require,
)

SECRET_VAR = "ETL_CRAFT_TEST_SQL_WAREHOUSE_SECRET"


@dataclass
class SqlWorld:
    """A warehouse schema to write into, and the Engine DB rows SQL tasks are defined in."""

    kind: str
    config: ConnectorConfig
    engine_db: Engine
    warehouse: Engine
    catalog: str
    schema: str
    pipeline_id: int
    pipeline_run_id: int
    refresh_type: str = "INCREMENTAL"
    tasks: dict[str, int] = field(default_factory=dict)

    @property
    def project(self) -> Path:
        """The project directory, holding ``sql_files/``."""
        return self.config.project_dir

    def name(self, table: str) -> str:
        """Return ``catalog.schema.table``."""
        return f"{self.catalog}.{self.schema}.{table}"

    def execute(self, sql_text: str, **params: Any) -> None:
        """Run a statement on the warehouse, committed."""
        with self.warehouse.begin() as conn:
            conn.execute(text(sql_text), params)

    def rows(self, sql_text: str, **params: Any) -> list[tuple[Any, ...]]:
        """Return every row ``sql_text`` selects, as tuples."""
        with self.warehouse.connect() as conn:
            return [tuple(row) for row in conn.execute(text(sql_text), params)]

    def columns(self, table: str) -> list[str]:
        """Return a table's column names in order, lower case, from a SELECT of no rows."""
        with self.warehouse.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {self.name(table)} WHERE 1 = 0"))
            return [str(key).lower() for key in result.keys()]  # noqa: SIM118 - result, not dict

    def tables(self) -> list[str]:
        """Return the tables in the schema, lower case."""
        rows = self.rows(
            "SELECT table_name FROM information_schema.tables WHERE lower(table_schema) = "
            "lower(:schema)",
            schema=self.schema,
        )
        return sorted(str(row[0]).lower() for row in rows)

    def task(self, code: str, *, run_id: int | None = None, **params: str) -> TaskContext:
        """Add (or reuse) SQL task ``code`` with ``params``; return its task context.

        A bare ``TARGET_OBJECT`` table is written as ``<schema>.<table>``; a qualified one is kept.
        """
        if "TARGET_OBJECT" in params and "." not in params["TARGET_OBJECT"]:
            params["TARGET_OBJECT"] = f"{self.schema}.{params['TARGET_OBJECT']}"
        with self.engine_db.begin() as conn:
            if code not in self.tasks:
                self.tasks[code] = add_task(conn, self.pipeline_id, code, "SQL", **params)
            else:
                conn.execute(
                    text("DELETE FROM CFG_TASK_PARAMETERS WHERE TASK_ID = :t"),
                    {"t": self.tasks[code]},
                )
                for name, value in params.items():
                    conn.execute(
                        text(
                            "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, "
                            "PARAMETER_VALUE) VALUES (:t, :n, :v)"
                        ),
                        {"t": self.tasks[code], "n": name, "v": value},
                    )
            pipeline_run_id = run_id or self.pipeline_run_id
            binding = runlog.find_or_create_task_run(conn, self.tasks[code], pipeline_run_id)
        return TaskContext(
            config=self.config,
            pipeline_id=self.pipeline_id,
            pipeline_code="P",
            task_id=self.tasks[code],
            task_code=code,
            pipeline_run_id=pipeline_run_id,
            task_run_id=binding.task_run_id,
            attempt=1,
            handler="SQL",
            refresh_type=self.refresh_type,
            task_params=params,
        )

    def run(self, code: str, **params: str) -> HandlerResult:
        """Define task ``code`` with ``params`` and run it."""
        return sql.run(self.task(code, **params), self.engine_db)

    def setup(self, table: str, select: str, writer: str) -> HandlerResult:
        """Create ``table`` from ``select`` with the audit columns ``writer`` needs."""
        return self.run(
            f"setup_{table}",
            SQL_ACTION="SETUP_TABLE",
            TARGET_OBJECT=table,
            SOURCE_SQL=select,
            SETUP_FOR=writer,
        )

    def finish(self, code: str, status: str = "SUCCESS") -> None:
        """Record task ``code`` ended under the current run."""
        with self.engine_db.begin() as conn:
            binding = runlog.find_or_create_task_run(conn, self.tasks[code], self.pipeline_run_id)
            runlog.finish_task_run(conn, binding.task_run_id, status=status)

    def new_run(self) -> int:
        """End the current pipeline run and start another; return its id."""
        with self.engine_db.begin() as conn:
            runlog.finalize_pipeline_run(conn, self.pipeline_run_id, "SUCCESS")
            self.pipeline_run_id = runlog.find_or_create_active_run(conn, self.pipeline_id)
        return self.pipeline_run_id


def _config(tmp_path: Path, profile: ConnectionProfile) -> ConnectorConfig:
    project = tmp_path / "etl-craft"
    (project / "sql_files").mkdir(parents=True, exist_ok=True)
    engine = ConnectionProfile("ENGINE", "dev", "jdbc:sqlite:engine.db", "", "none")
    return ConnectorConfig(
        mode=Mode.LOCAL,
        source=SourceConfig(type="environment"),
        engine=ConnectionSection("dev", {"dev": engine}),
        warehouse=ConnectionSection("dev", {"dev": profile}),
        config_path=project / "craft-connector.yml",
    )


def _profile(
    jdbc_url: str, user: str = "", auth_mode: str = "none", **extra: str
) -> ConnectionProfile:
    return ConnectionProfile("WAREHOUSE", "dev", jdbc_url, user, auth_mode, dict(extra))


@pytest.fixture(
    params=[
        pytest.param("duckdb", marks=pytest.mark.warehouse_duckdb),
        pytest.param("postgres", marks=pytest.mark.warehouse_postgres),
        pytest.param("trino_iceberg", marks=pytest.mark.warehouse_trino_iceberg),
        pytest.param("duckdb_iceberg", marks=pytest.mark.warehouse_duckdb_iceberg),
    ]
)
def sql_world(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[SqlWorld]:
    """A fresh schema on each warehouse in turn, with an Engine DB and pipeline P."""
    kind = request.param
    schema = f"t_{uuid.uuid4().hex[:10]}"
    if kind == "duckdb":
        config = _config(tmp_path, _profile(f"jdbc:duckdb:{tmp_path / 'warehouse.duckdb'}"))
        catalog = "warehouse"
    elif kind == "postgres":
        pg = require("postgres")
        monkeypatch.setenv(SECRET_VAR, POSTGRES_PASSWORD)
        config = _config(
            tmp_path,
            _profile(
                f"jdbc:postgresql://{pg.address}/{POSTGRES_DB}",
                POSTGRES_USER,
                "password",
                secret_var=SECRET_VAR,
            ),
        )
        catalog = POSTGRES_DB
    elif kind == "trino_iceberg":
        trino = require("trino")
        config = _config(
            tmp_path, _profile(f"jdbc:trino://{trino.address}/iceberg/{schema}", "etl")
        )
        config = replace(config, warehouse_table_format="iceberg")
        catalog = "iceberg"
    else:
        rest = require("iceberg_rest")
        minio = require("minio")
        config = _config(
            tmp_path,
            _profile(
                "jdbc:duckdb:",
                catalog="lake",
                catalog_uri=rest.http_url,
                iceberg_warehouse="s3://warehouse/",
                s3_endpoint=minio.address,
                s3_region="us-east-1",
                s3_url_style="path",
                s3_use_ssl="false",
                s3_key_id=MINIO_USER,
                s3_secret=MINIO_PASSWORD,
            ),
        )
        config = replace(config, warehouse_table_format="iceberg")
        catalog = "lake"
    engine_db = sqlite_engine_db(config.project_dir).engine
    apply_schema(engine_db)
    with engine_db.begin() as conn:
        pipeline_id = add_pipeline(conn, "P", refresh_type="INCREMENTAL")
        run_id = runlog.find_or_create_active_run(conn, pipeline_id)
    warehouse = build_warehouse_engine(config)
    with warehouse.begin() as conn:
        conn.execute(
            text(
                f"CREATE SCHEMA {catalog}.{schema}"
                if kind != "postgres"
                else f"CREATE SCHEMA {schema}"
            )
        )
    # From here on the profile names its schema, as a real one does.
    warehouse.dispose()
    profile = replace(config.warehouse.active, schema=schema)
    config = replace(config, warehouse=ConnectionSection("dev", {"dev": profile}))
    warehouse = build_warehouse_engine(config)
    world = SqlWorld(kind, config, engine_db, warehouse, catalog, schema, pipeline_id, run_id)
    try:
        yield world
    finally:
        _drop_schema(world)
        warehouse.dispose()
        engine_db.dispose()


def _drop_schema(world: SqlWorld) -> None:
    if world.kind == "postgres":
        world.execute(f"DROP SCHEMA IF EXISTS {world.schema} CASCADE")
        return
    for table in world.tables():
        world.execute(f"DROP TABLE IF EXISTS {world.name(table)}")
    if world.kind == "duckdb":
        world.execute(f"DROP SCHEMA IF EXISTS {world.catalog}.{world.schema} CASCADE")
    else:
        world.execute(f"DROP SCHEMA IF EXISTS {world.catalog}.{world.schema}")
