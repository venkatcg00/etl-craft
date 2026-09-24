"""Connecting to each local warehouse, and queueing writers of a single-writer one."""

import threading
import uuid
from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.config import ConnectionProfile, ConnectionSection, ConnectorConfig, SourceConfig
from etl_craft.core.enums import Mode, TableFormat
from etl_craft.core.errors import LockTimeoutError
from etl_craft.warehouse.connection import (
    build_warehouse_engine,
    open_warehouse,
    single_writer_lock,
    verify_iceberg_catalog,
    warehouse_dialect,
)
from fixtures.engine_db import apply_schema, sqlite_engine_db
from fixtures.services import (
    MINIO_PASSWORD,
    MINIO_USER,
    POSTGRES_DB,
    POSTGRES_PASSWORD,
    POSTGRES_USER,
    require,
    service,
)

SECRET_VAR = "ETL_CRAFT_TEST_WAREHOUSE_SECRET"


def warehouse_config(jdbc_url, auth_mode="none", table_format=TableFormat.NATIVE, **fields):
    user = fields.pop("user", "")
    engine = ConnectionProfile("ENGINE", "dev", "jdbc:sqlite:e.db", "", "none")
    warehouse = ConnectionProfile("WAREHOUSE", "dev", jdbc_url, user, auth_mode, fields)
    return ConnectorConfig(
        mode=Mode.LOCAL,
        source=SourceConfig(type="environment"),
        engine=ConnectionSection("dev", {"dev": engine}),
        warehouse=ConnectionSection("dev", {"dev": warehouse}),
        warehouse_table_format=table_format,
    )


def scalar(engine, sql):
    with engine.connect() as conn:
        return conn.execute(text(sql)).scalar_one()


# DuckDB: one file, one writer at a time


@pytest.mark.warehouse_duckdb
def test_a_duckdb_file_warehouse(tmp_path):
    config = warehouse_config(f"jdbc:duckdb:{tmp_path / 'warehouse.duckdb'}")
    assert warehouse_dialect(config).key == "duckdb"
    with open_warehouse(config) as engine, engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA staging"))
        conn.execute(text("CREATE TABLE staging.t AS SELECT 42 AS answer"))
    # A new engine reads what the previous one wrote: the file persists.
    with open_warehouse(config) as engine:
        assert scalar(engine, "SELECT answer FROM warehouse.staging.t") == 42


@pytest.fixture(
    params=[
        pytest.param("sqlite", marks=[pytest.mark.warehouse_duckdb]),
        pytest.param("postgresql", marks=[pytest.mark.warehouse_duckdb]),
    ]
)
def queue_engine_db(request, tmp_path):
    """The Engine DB that holds a single-writer warehouse's queue, on each dialect."""
    if request.param == "sqlite":
        db = sqlite_engine_db(tmp_path)
        request.addfinalizer(db.engine.dispose)
    else:
        db = request.getfixturevalue("postgres_database")
    apply_schema(db.engine)
    return db.engine


@pytest.mark.warehouse_duckdb
def test_writers_of_a_duckdb_file_queue_and_a_bounded_wait_says_why(queue_engine_db, tmp_path):
    config = warehouse_config(f"jdbc:duckdb:{tmp_path / 'warehouse.duckdb'}")
    holding, release = threading.Event(), threading.Event()

    def hold_the_warehouse():
        with open_warehouse(config, queue_engine_db):
            holding.set()
            release.wait(30)

    holder = threading.Thread(target=hold_the_warehouse)
    holder.start()
    try:
        assert holding.wait(10)
        with (
            pytest.raises(LockTimeoutError, match="only one writing process at a time"),
            open_warehouse(config, queue_engine_db, wait_seconds=0.5),
        ):
            pass  # pragma: no cover - the lock is never granted
        # A task that writes through its own script queues the same way.
        with (
            pytest.raises(LockTimeoutError),
            single_writer_lock(config, queue_engine_db, wait_seconds=0.5),
        ):
            pass  # pragma: no cover - the lock is never granted
    finally:
        release.set()
        holder.join(10)
    with open_warehouse(config, queue_engine_db, wait_seconds=5) as engine:
        assert scalar(engine, "SELECT 1") == 1


# PostgreSQL


@pytest.mark.warehouse_postgres
def test_a_postgres_warehouse_with_a_password(monkeypatch):
    pg = require("postgres")
    monkeypatch.setenv(SECRET_VAR, POSTGRES_PASSWORD)
    config = warehouse_config(
        f"jdbc:postgresql://{pg.address}/{POSTGRES_DB}?application_name=etl-craft-test",
        "password",
        user=POSTGRES_USER,
        secret_var=SECRET_VAR,
    )
    engine = build_warehouse_engine(config)
    try:
        assert engine.url.password is None
        assert scalar(engine, "SELECT current_setting('application_name')") == "etl-craft-test"
        # A PostgreSQL warehouse is not single-writer: nothing is queued.
        with single_writer_lock(config, None):
            pass
        assert verify_iceberg_catalog(config, engine) is None
    finally:
        engine.dispose()


# Trino over the Iceberg REST catalog


@pytest.fixture
def trino_config():
    trino = require("trino")
    return warehouse_config(f"jdbc:trino://{trino.address}/iceberg/etl_craft_test", user="etl")


@pytest.mark.warehouse_trino_iceberg
def test_trino_writes_and_reads_an_iceberg_table(trino_config):
    assert warehouse_dialect(trino_config).key == "trino_iceberg"
    table = f"iceberg.etl_craft_test.t_{uuid.uuid4().hex[:8]}"
    engine = build_warehouse_engine(trino_config)
    try:
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS iceberg.etl_craft_test"))
            conn.execute(text(f"CREATE TABLE {table} AS SELECT 7 AS id"))
            try:
                assert conn.execute(text(f"SELECT id FROM {table}")).scalar_one() == 7
            finally:
                conn.execute(text(f"DROP TABLE {table}"))
        assert verify_iceberg_catalog(trino_config, engine) is None
    finally:
        engine.dispose()


@pytest.mark.warehouse_trino_iceberg
def test_a_trino_catalog_that_is_not_iceberg_is_reported(trino_config):
    trino = service("trino")
    active = trino_config.warehouse.active
    config = replace(
        trino_config,
        warehouse=ConnectionSection(
            "dev", {"dev": replace(active, jdbc_url=f"jdbc:trino://{trino.address}/system/runtime")}
        ),
    )
    engine = build_warehouse_engine(config)
    try:
        assert "not an Iceberg catalog" in verify_iceberg_catalog(config, engine)
        missing = replace(
            config,
            warehouse=ConnectionSection(
                "dev", {"dev": replace(active, jdbc_url=f"jdbc:trino://{trino.address}/nope/x")}
            ),
        )
        assert "does not exist" in verify_iceberg_catalog(missing, engine)
    finally:
        engine.dispose()


# DuckDB over the Iceberg REST catalog


def duckdb_iceberg_config(auth_mode="none", **fields):
    catalog = require("iceberg_rest")
    minio = require("minio")
    return warehouse_config(
        "jdbc:duckdb:",
        auth_mode,
        TableFormat.ICEBERG,
        catalog="lake",
        catalog_uri=catalog.http_url,
        iceberg_warehouse="s3://warehouse/",
        s3_endpoint=minio.address,
        s3_region="us-east-1",
        s3_url_style="path",
        s3_use_ssl="false",
        s3_key_id=MINIO_USER,
        s3_secret=MINIO_PASSWORD,
        **fields,
    )


def round_trip(config):
    schema = f"s_{uuid.uuid4().hex[:8]}"
    engine = build_warehouse_engine(config)
    dialect = warehouse_dialect(config)
    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA lake.{schema}"))
            conn.execute(text(f"CREATE TABLE lake.{schema}.t AS SELECT 1 AS id, 'a' AS code"))
            dialect.load_table_metadata(conn, schema, "t")
            columns = (
                conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        f"WHERE table_schema = '{schema}' AND table_name = 't' "
                        "ORDER BY ordinal_position"
                    )
                )
                .scalars()
                .all()
            )
            assert columns == ["id", "code"]
            assert conn.execute(text(f"SELECT code FROM lake.{schema}.t")).scalar_one() == "a"
            conn.execute(text(f"DROP TABLE lake.{schema}.t"))
            conn.execute(text(f"DROP SCHEMA lake.{schema}"))
    finally:
        engine.dispose()


@pytest.mark.warehouse_duckdb_iceberg
def test_duckdb_attaches_the_iceberg_catalog():
    config = duckdb_iceberg_config()
    assert warehouse_dialect(config).key == "duckdb_iceberg"
    round_trip(config)


@pytest.mark.warehouse_duckdb_iceberg
def test_duckdb_attaches_the_iceberg_catalog_with_oauth(monkeypatch):
    # The local REST catalog implements the client-credentials grant, so this is a real login.
    catalog = require("iceberg_rest")
    monkeypatch.setenv(SECRET_VAR, "secret")
    round_trip(
        duckdb_iceberg_config(
            "oauth",
            client_id="etl-craft",
            token_url=f"{catalog.http_url}/v1/oauth/tokens",
            scope="catalog",
            secret_var=SECRET_VAR,
        )
    )


@pytest.mark.warehouse_duckdb_iceberg
def test_duckdb_iceberg_settings_are_checked_before_attaching():
    config = duckdb_iceberg_config()
    active = config.warehouse.active
    broken = replace(active, extra={**active.extra, "catalog_uri": ""})
    engine = build_warehouse_engine(
        replace(config, warehouse=ConnectionSection("dev", {"dev": broken}))
    )
    try:
        with pytest.raises(Exception, match="needs catalog_uri"):
            engine.connect()
    finally:
        engine.dispose()
    quoted = replace(active, extra={**active.extra, "s3_region": "us'east"})
    engine = build_warehouse_engine(
        replace(config, warehouse=ConnectionSection("dev", {"dev": quoted}))
    )
    try:
        with pytest.raises(Exception, match="must not contain a quote"):
            engine.connect()
    finally:
        engine.dispose()
