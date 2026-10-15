"""A fresh, empty Engine DB with the packaged schema applied, on SQLite or PostgreSQL.

``engine_db`` is parametrized over both dialects, and each case carries its suite marker. The
PostgreSQL case creates its own database on the local service and drops it afterwards, so
tests never share state.
"""

from __future__ import annotations

import shutil
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path

import psycopg
import pytest
from sqlalchemy.engine import Engine

from etl_craft.config import (
    ConnectionProfile,
    ConnectorConfig,
    parse_config,
)
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.dialects.engine import EngineDialect, build_engine, for_engine
from fixtures.services import POSTGRES_PASSWORD, POSTGRES_USER, require

SECRET_VAR = "ETL_CRAFT_TEST_ENGINE_SECRET"
ENGINE_CASES = [
    pytest.param("sqlite", marks=pytest.mark.engine_sqlite),
    pytest.param("postgresql", marks=pytest.mark.engine_postgres),
]


@dataclass(frozen=True)
class EngineDb:
    """A test Engine DB: its dialect, an engine connected to it, and the config naming it."""

    dialect: EngineDialect
    engine: Engine
    config: ConnectorConfig


def engine_config(profile: ConnectionProfile, config_path: Path | None = None) -> ConnectorConfig:
    """Return a config whose Engine section is ``profile``."""
    config = parse_config(
        {
            "Secrets": {"Source_type": "environment"},
            "Orchestration": {"Mode": "local"},
            "Engine": {
                profile.name: {
                    "jdbc_url": profile.jdbc_url,
                    "user": profile.user,
                    "auth_mode": profile.auth_mode,
                    "schema": profile.schema
                    or ("main" if profile.jdbc_url.startswith("jdbc:sqlite:") else "public"),
                    **(
                        {"secret": profile.extra["secret_var"]}
                        if "secret_var" in profile.extra
                        else {}
                    ),
                    **{key: value for key, value in profile.extra.items() if key != "secret_var"},
                },
            },
        },
        config_path or Path("craft-connector.yml"),
    )
    return replace(config, config_path=config_path)


def apply_schema(engine: Engine) -> int:
    """Apply the dialect's packaged schema in one transaction, as ``init-db`` does.

    Return the statement count. The change triggers and the ``AUD_METADATA_CHANGES`` view come
    from the trigger refresh, which ``init-db`` runs after the schema.
    """
    dialect = for_engine(engine)
    statements = dialect.split_statements(dialect.schema_path().read_text(encoding="utf-8"))
    with engine.begin() as conn:
        for statement in statements:
            conn.exec_driver_sql(statement)
        dialect.refresh_metadata_triggers(conn)
    return len(statements)


def sqlite_engine_db(directory: Path) -> EngineDb:
    """Return an Engine DB in a SQLite file under ``directory``, named relative to its config."""
    profile = ConnectionProfile("ENGINE", "test", "jdbc:sqlite:engine.db", "", "none")
    config = engine_config(profile, directory / "craft-connector.yml")
    engine = build_engine(config)
    return EngineDb(for_engine(engine), engine, config)


@contextmanager
def postgres_engine_db(
    monkeypatch: pytest.MonkeyPatch, template: str | None = None
) -> Iterator[EngineDb]:
    """Create a database of its own on the local PostgreSQL service, and drop it afterwards."""
    service = require("postgres")
    name = f"etl_craft_test_{uuid.uuid4().hex[:12]}"
    with postgres_admin() as conn:
        sql = psycopg.sql.SQL("CREATE DATABASE {}").format(psycopg.sql.Identifier(name))
        if template is not None:
            sql += psycopg.sql.SQL(" TEMPLATE {}").format(psycopg.sql.Identifier(template))
        conn.execute(sql)
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
        with postgres_admin() as conn:
            conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


def postgres_admin():
    """Connect outside test databases for creating, cloning and dropping them."""
    service = require("postgres")
    return psycopg.connect(
        host=service.host,
        port=service.port,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname="postgres",
        autocommit=True,
    )


@pytest.fixture
def postgres_database(monkeypatch: pytest.MonkeyPatch) -> Iterator[EngineDb]:
    """An empty PostgreSQL database for initialization and upgrade tests."""
    with postgres_engine_db(monkeypatch) as db:
        yield db


@pytest.fixture(scope="session")
def sqlite_schema_template(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build and checkpoint one immutable schema for independent SQLite copies."""
    directory = tmp_path_factory.mktemp("sqlite-schema")
    db = sqlite_engine_db(directory)
    try:
        with acting_as(SYSTEM_ACTOR):
            apply_schema(db.engine)
    finally:
        db.engine.dispose()
    return directory / "engine.db"


def prepared_sqlite_engine_db(directory: Path, template: Path) -> EngineDb:
    """Copy a closed schema file before opening the test's independent connections."""
    shutil.copyfile(template, directory / "engine.db")
    return sqlite_engine_db(directory)


@pytest.fixture(scope="session")
def postgres_schema_template() -> Iterator[str]:
    """Keep a disconnected PostgreSQL template until its independent clones are dropped."""
    patch = pytest.MonkeyPatch()
    try:
        with postgres_engine_db(patch) as db:
            with acting_as(SYSTEM_ACTOR):
                apply_schema(db.engine)
                name = db.engine.url.database
                assert name is not None
            db.engine.dispose()
            with postgres_admin() as conn:
                conn.execute(
                    psycopg.sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(
                        psycopg.sql.Identifier(name)
                    )
                )
            patch.undo()
            yield name
    finally:
        patch.undo()


@pytest.fixture(params=ENGINE_CASES)
def empty_engine_db(request: pytest.FixtureRequest, tmp_path: Path) -> EngineDb:
    """An Engine DB with nothing in it, on each dialect in turn."""
    if request.param == "sqlite":
        db = sqlite_engine_db(tmp_path)
        request.addfinalizer(db.engine.dispose)
        return db
    postgres: EngineDb = request.getfixturevalue("postgres_database")
    return postgres


@pytest.fixture
def engine_db(empty_engine_db: EngineDb, request: pytest.FixtureRequest) -> EngineDb:
    """An isolated Engine DB copied from its dialect's immutable packaged schema."""
    db = empty_engine_db
    db.engine.dispose()
    if db.config.engine.jdbc_url.startswith("jdbc:sqlite:"):
        template = request.getfixturevalue("sqlite_schema_template")
        shutil.copyfile(template, db.config.project_dir / "engine.db")
    else:
        template = request.getfixturevalue("postgres_schema_template")
        name = db.engine.url.database
        assert name is not None
        with postgres_admin() as conn:
            conn.execute(
                psycopg.sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    psycopg.sql.Identifier(name)
                )
            )
            conn.execute(
                psycopg.sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                    psycopg.sql.Identifier(name), psycopg.sql.Identifier(template)
                )
            )
    return db
