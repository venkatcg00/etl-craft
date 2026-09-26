"""The Support Insights demo, from ``examples/demo``, run by an installed ``etl-craft``.

``installed_cli(installer)`` installs the wheel under test (``ETL_CRAFT_TEST_WHEEL``, set by
``scripts/run_suite.py --wheel``) into a clean virtual environment, with pip or with uv, once
per session. ``Demo`` is one copy of the demo project, for one Engine DB and one warehouse:
it writes the project's ``craft-connector.yml``, makes the warehouse schemas the demo writes,
loads its metadata, runs the installed command line, and reads the warehouse, the Engine DB
and the emails Mailpit received.

Each demo works in databases of its own: a SQLite file or a new PostgreSQL database for the
Engine DB, and a DuckDB file or a new PostgreSQL database for the warehouse. The Iceberg REST
catalog is shared, so the demo's namespaces there are emptied before it starts.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import urllib.parse
import urllib.request
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig, load_config
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.connection import engine_db
from etl_craft.engine.queries import run_script
from etl_craft.warehouse.connection import build_warehouse_engine
from fixtures.services import (
    MINIO_PASSWORD,
    MINIO_USER,
    POSTGRES_PASSWORD,
    POSTGRES_USER,
    require,
)

REPO = Path(__file__).resolve().parents[2]
DEMO = REPO / "examples" / "demo"
SCHEMAS = ("lnd", "prs", "ds", "cdc", "pre_dm", "dm", "aud")
PASSWORD_VAR = "ETL_CRAFT_DEMO_SECRET"
S3_VAR = "ETL_CRAFT_DEMO_S3_SECRET"
ENGINES = ("sqlite", "postgres")
WAREHOUSES = ("duckdb", "postgres", "duckdb_iceberg", "trino_iceberg")


def installed_cli(installer: str, root: Path) -> Path:
    """Install the wheel under test into a new virtual environment; return its ``etl-craft``."""
    wheel = os.environ.get("ETL_CRAFT_TEST_WHEEL")
    if not wheel:
        message = "the demo runs from the built wheel: make suite SUITE=e2e-pip WHEEL=dist/..."
        if os.environ.get("ETL_CRAFT_REQUIRE_SERVICES") == "1":
            pytest.fail(message, pytrace=False)
        pytest.skip(message)
    venv = root / f"venv-{installer}"
    target = f"{wheel}[trino]"
    if installer == "pip":
        subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
        python = _venv_bin(venv, "python")
        subprocess.run(
            [str(python), "-m", "pip", "install", "--quiet", target], check=True, timeout=900
        )
    else:
        uv = shutil.which("uv")
        assert uv, "uv is not on PATH"
        subprocess.run([uv, "venv", "--quiet", "--python", sys.executable, str(venv)], check=True)
        python = _venv_bin(venv, "python")
        subprocess.run(
            [uv, "pip", "install", "--quiet", "--python", str(python), target],
            check=True,
            timeout=900,
        )
    return _venv_bin(venv, "etl-craft")


def _venv_bin(venv: Path, name: str) -> Path:
    folder = venv / ("Scripts" if os.name == "nt" else "bin")
    return folder / (f"{name}.exe" if os.name == "nt" else name)


@dataclass
class Demo:
    """One copy of the demo project, its databases, and the installed command line."""

    cli: Path
    root: Path
    engine_kind: str
    warehouse_kind: str
    mode: str = "local"
    tag: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    catalog: str = ""
    cleanup: list[Any] = field(default_factory=list)

    # Building it

    def build(self) -> Demo:
        shutil.copytree(
            DEMO, self.root, ignore=shutil.ignore_patterns("*.db", "*.duckdb", ".flaky-*")
        )
        smtp = require("mailpit_smtp")
        raw = {
            "Secrets": {"Source_type": "environment"},
            "Orchestration": {
                "Mode": self.mode,
                "Task_timeout_seconds": 600,
                "Max_parallel_tasks": 4,
                "Enforce_sla": True,
                "Email": {
                    "host": smtp.host,
                    "port": smtp.port,
                    "from_address": "etl-craft@example.com",
                    "from_name": "Support Insights",
                    "use_tls": False,
                },
            },
            "Engine": {"dev": self._engine_profile()},
            "Warehouse": self._warehouse_section(),
            "Cloning": {"dev": {"Enabled": True, "Scope": "all"}},
        }
        (self.root / "craft-connector.yml").write_text(
            yaml.safe_dump(raw, sort_keys=False), encoding="utf-8"
        )
        self._make_schemas()
        return self

    def _engine_profile(self) -> dict[str, Any]:
        if self.engine_kind == "sqlite":
            return {"jdbc_url": "jdbc:sqlite:engine.db", "schema": "main"}
        return {
            "jdbc_url": f"jdbc:postgresql://{self._new_database('engine')}",
            "schema": "public",
            "user": POSTGRES_USER,
            "auth_mode": "password",
            "secret": PASSWORD_VAR,
        }

    def _warehouse_section(self) -> dict[str, Any]:
        kind = self.warehouse_kind
        if kind == "duckdb":
            self.catalog = "warehouse"
            return {"dev": {"jdbc_url": "jdbc:duckdb:warehouse.duckdb", "schema": "aud"}}
        if kind == "postgres":
            address = self._new_database("warehouse")
            self.catalog = address.rsplit("/", 1)[1]
            profile = {
                "jdbc_url": f"jdbc:postgresql://{address}",
                "schema": "aud",
                "user": POSTGRES_USER,
                "auth_mode": "password",
                "secret": PASSWORD_VAR,
            }
            return {"dev": profile}
        if kind == "trino_iceberg":
            trino = require("trino")
            self.catalog = "iceberg"
            profile = {
                "jdbc_url": f"jdbc:trino://{trino.address}/iceberg/aud",
                "schema": "aud",
                "user": "etl",
                "auth_mode": "none",
            }
            return {"Name": "Trino", "Table_format": "iceberg", "dev": profile}
        catalog = require("iceberg_rest")
        minio = require("minio")
        self.catalog = "lake"
        profile = {
            "jdbc_url": "jdbc:duckdb:",
            "schema": "aud",
            "catalog": "lake",
            "catalog_uri": catalog.http_url,
            "iceberg_warehouse": "s3://warehouse/",
            "s3_endpoint": minio.address,
            "s3_region": "us-east-1",
            "s3_url_style": "path",
            "s3_use_ssl": "false",
            "s3_key_id": MINIO_USER,
            "s3_secret": S3_VAR,
        }
        return {"Name": "DuckDB", "Table_format": "iceberg", "dev": profile}

    def _new_database(self, role: str) -> str:
        pg = require("postgres")
        name = f"demo_{role}_{self.tag}"
        admin = {
            "host": pg.host,
            "port": pg.port,
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "dbname": "postgres",
            "autocommit": True,
        }
        with psycopg.connect(**admin) as conn:
            conn.execute(f'CREATE DATABASE "{name}"')

        def drop() -> None:
            with psycopg.connect(**admin) as conn:
                conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')

        self.cleanup.append(drop)
        return f"{pg.address}/{name}"

    def _make_schemas(self) -> None:
        """Make the schemas the demo writes, as a team does before its first run."""
        if self.warehouse_kind in ("duckdb_iceberg", "trino_iceberg"):
            _empty_namespaces(SCHEMAS)
            return
        with self.warehouse() as engine, engine.begin() as conn:
            for schema in SCHEMAS:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    def environment(self) -> dict[str, str]:
        env = {**os.environ, PASSWORD_VAR: POSTGRES_PASSWORD, S3_VAR: MINIO_PASSWORD}
        env.pop("ETL_CRAFT_CONFIG", None)
        env.pop("ETL_CRAFT_MIGRATIONS_DIR", None)
        return env

    @property
    def config(self) -> ConnectorConfig:
        os.environ.setdefault(PASSWORD_VAR, POSTGRES_PASSWORD)
        os.environ.setdefault(S3_VAR, MINIO_PASSWORD)
        return load_config(self.root / "craft-connector.yml")

    def seed(self) -> None:
        """Load the demo's metadata, with email addresses of this demo's own."""
        sql = (self.root / "metadata" / "support_insights.sql").read_text(encoding="utf-8")
        sql = sql.replace("@example.com", f"+{self.tag}@example.com")
        engine = engine_db(self.config)
        try:
            with engine.begin() as conn:
                run_script(conn, for_engine(engine).split_statements(sql))
        finally:
            engine.dispose()

    def close(self) -> None:
        for step in reversed(self.cleanup):
            step()

    # Running it

    def run(self, *args: str, timeout: float = 900) -> subprocess.CompletedProcess[str]:
        """Run the installed ``etl-craft`` in the project directory."""
        return subprocess.run(
            [str(self.cli), *args],
            cwd=self.root,
            env=self.environment(),
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    def ok(self, *args: str) -> str:
        """Run a command that must succeed; return what it printed."""
        done = self.run(*args)
        assert done.returncode == 0, (
            f"etl-craft {' '.join(args)}: {done.returncode}\n{done.stdout}\n{done.stderr[-4000:]}"
        )
        return done.stdout

    def popen(self, *args: str) -> subprocess.Popen[str]:
        return subprocess.Popen(
            [str(self.cli), *args],
            cwd=self.root,
            env=self.environment(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    # Reading what it did

    def warehouse(self) -> _Closing:
        return _Closing(build_warehouse_engine(self.config))

    def rows(self, sql: str) -> list[tuple[Any, ...]]:
        """Run ``sql`` on the warehouse; ``{c}`` is replaced by the catalog, as ``{c}.dm.x``."""
        with self.warehouse() as engine, engine.connect() as conn:
            return [tuple(row) for row in conn.execute(text(sql.format(c=self.catalog)))]

    def engine_rows(self, sql: str, **params: Any) -> list[tuple[Any, ...]]:
        engine = engine_db(self.config)
        try:
            with engine.connect() as conn:
                return [tuple(row) for row in conn.execute(text(sql), params)]
        finally:
            engine.dispose()

    def task_runs(self, pipeline: str, pipeline_run_id: int) -> dict[str, tuple[str, int]]:
        """Return each task's status and attempt count under a run."""
        rows = self.engine_rows(
            "SELECT t.TASK_CODE AS task_code, r.STATUS AS status, r.ATTEMPT_COUNT AS attempts "
            "FROM AUD_TASK_RUN_LOG r JOIN CFG_TASKS t ON t.TASK_ID = r.TASK_ID "
            "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID "
            "WHERE p.PIPELINE_CODE = :p AND r.PIPELINE_RUN_ID = :r",
            p=pipeline,
            r=pipeline_run_id,
        )
        return {code: (status, int(attempts)) for code, status, attempts in rows}

    def latest_run(self, pipeline: str) -> tuple[int, str]:
        (row,) = self.engine_rows(
            "SELECT r.PIPELINE_RUN_ID AS id, r.STATUS AS status FROM AUD_PIPELINES_RUN_LOG r "
            "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = r.PIPELINE_ID WHERE p.PIPELINE_CODE = :p "
            "ORDER BY r.PIPELINE_RUN_ID DESC LIMIT 1",
            p=pipeline,
        )
        return int(row[0]), str(row[1])

    def emails(self) -> list[tuple[str, list[str]]]:
        """Return the subject and recipients of every email sent to this demo's addresses."""
        api = require("mailpit_api")
        query = urllib.parse.urlencode({"query": f"to:{self.tag}", "limit": "200"})
        with urllib.request.urlopen(f"{api.http_url}/api/v1/search?{query}", timeout=10) as reply:
            found = json.load(reply)["messages"]
        return sorted((m["Subject"], sorted(t["Address"] for t in m["To"])) for m in found)


class _Closing:
    """A warehouse engine for a ``with`` block, disposed at its end."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def __enter__(self) -> Engine:
        return self.engine

    def __exit__(self, *exc: object) -> None:
        self.engine.dispose()


def _empty_namespaces(names: Sequence[str]) -> None:
    """Create the demo's namespaces in the shared Iceberg REST catalog, emptied of tables."""
    catalog = require("iceberg_rest")
    base = f"{catalog.http_url}/v1/namespaces"
    for name in names:
        try:
            with urllib.request.urlopen(f"{base}/{name}/tables", timeout=10) as reply:
                tables = json.load(reply).get("identifiers", [])
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
            _request("POST", base, {"namespace": [name]})
            continue
        for table in tables:
            _request("DELETE", f"{base}/{name}/tables/{table['name']}?purgeRequested=true")


def _request(method: str, url: str, body: dict[str, Any] | None = None) -> None:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=30):
        pass


SMOKE = ("sqlite", "duckdb")
"""The one Engine DB and warehouse a smoke installer runs: the install is what it checks."""


def demos(markers: dict[str, Any], smoke: frozenset[str] = frozenset()) -> Iterator[Any]:
    """Every installer, Engine DB and warehouse, each case with its suite marker.

    An installer in ``smoke`` runs only the ``SMOKE`` pair: how the package was installed does
    not change how it behaves, so one pair proves that install works. ``ETL_CRAFT_E2E_WAREHOUSE``
    keeps one warehouse, so CI can run the warehouses side by side.
    """
    only = os.environ.get("ETL_CRAFT_E2E_WAREHOUSE")
    for installer, mark in markers.items():
        for engine in ENGINES:
            for warehouse in WAREHOUSES:
                if only and warehouse != only:
                    continue
                if installer in smoke and (engine, warehouse) != SMOKE:
                    continue
                yield pytest.param(
                    installer, engine, warehouse, marks=mark, id=f"{installer}-{engine}-{warehouse}"
                )
