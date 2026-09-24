"""A fresh, empty Engine DB with the packaged schema applied, on SQLite or PostgreSQL.

``engine_db`` is parametrized over both dialects, and each case carries its suite marker. The
PostgreSQL case creates its own database on the local service and drops it afterwards, so
tests never share state.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import psycopg
import pytest
from sqlalchemy.engine import Engine

from etl_craft.config import (
    ConnectionProfile,
    ConnectionSection,
    ConnectorConfig,
    SourceConfig,
)
from etl_craft.core.enums import Mode
from etl_craft.dialects.engine import EngineDialect, build_engine, for_engine
from fixtures.services import POSTGRES_PASSWORD, POSTGRES_USER, require

SECRET_VAR = "ETL_CRAFT_TEST_ENGINE_SECRET"


@dataclass(frozen=True)
class EngineDb:
    """A test Engine DB: its dialect, an engine connected to it, and the config naming it."""

    dialect: EngineDialect
    engine: Engine
    config: ConnectorConfig


def engine_config(profile: ConnectionProfile, config_path: Path | None = None) -> ConnectorConfig:
    """Return a config whose Engine section is ``profile``."""
    return ConnectorConfig(
        mode=Mode.LOCAL,
        source=SourceConfig(type="environment"),
        engine=ConnectionSection(profile.name, {profile.name: profile}),
        config_path=config_path,
    )


def apply_schema(engine: Engine) -> int:
    """Apply the dialect's packaged schema in one transaction; return the statement count."""
    dialect = for_engine(engine)
    statements = dialect.split_statements(dialect.schema_path().read_text(encoding="utf-8"))
    with engine.begin() as conn:
        dialect.begin_ddl_transaction(conn)
        for statement in statements:
            conn.exec_driver_sql(statement)
    return len(statements)


def sqlite_engine_db(directory: Path) -> EngineDb:
    """Return an Engine DB in a SQLite file under ``directory``, named relative to its config."""
    profile = ConnectionProfile("ENGINE", "test", "jdbc:sqlite:engine.db", "", "none")
    config = engine_config(profile, directory / "craft-connector.yml")
    engine = build_engine(config)
    return EngineDb(for_engine(engine), engine, config)


@pytest.fixture
def postgres_database(monkeypatch: pytest.MonkeyPatch) -> Iterator[EngineDb]:
    """Create a database of its own on the local PostgreSQL service, and drop it afterwards."""
    service = require("postgres")
    name = f"etl_craft_test_{uuid.uuid4().hex[:12]}"
    admin = {
        "host": service.host,
        "port": service.port,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "dbname": "postgres",
        "autocommit": True,
    }
    with psycopg.connect(**admin) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')
    monkeypatch.setenv(SECRET_VAR, POSTGRES_PASSWORD)
    profile = ConnectionProfile(
        "ENGINE",
        "test",
        f"jdbc:postgresql://{service.host}:{service.port}/{name}",
        POSTGRES_USER,
        "password",
        {"secret_var": SECRET_VAR},
    )
    config = engine_config(profile)
    engine = build_engine(config)
    try:
        yield EngineDb(for_engine(engine), engine, config)
    finally:
        engine.dispose()
        with psycopg.connect(**admin) as conn:
            conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


@pytest.fixture(
    params=[
        pytest.param("sqlite", marks=pytest.mark.engine_sqlite),
        pytest.param("postgresql", marks=pytest.mark.engine_postgres),
    ]
)
def empty_engine_db(request: pytest.FixtureRequest, tmp_path: Path) -> EngineDb:
    """An Engine DB with nothing in it, on each dialect in turn."""
    if request.param == "sqlite":
        db = sqlite_engine_db(tmp_path)
        request.addfinalizer(db.engine.dispose)
        return db
    postgres: EngineDb = request.getfixturevalue("postgres_database")
    return postgres


@pytest.fixture
def engine_db(empty_engine_db: EngineDb) -> EngineDb:
    """An Engine DB with the packaged schema applied, on each dialect in turn."""
    apply_schema(empty_engine_db.engine)
    return empty_engine_db
