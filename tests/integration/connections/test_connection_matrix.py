"""The connection matrix: every resource with every auth mode that can be verified locally.

Each case writes a ``craft-connector.yml`` as a team would, loads it, and connects the way the
engine does: ``doctor``'s checks for the Engine DB and the warehouse (the connection, the
profile's schema, and a query), and a real email for the relay, since ``doctor`` only greets
it. The auth modes this matrix and the cloud suites exercise are exactly the ones ``doctor``
calls verified; any other is usable with a warning.
"""

import json
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import pytest
import yaml
from sqlalchemy import text

from etl_craft.config import load_config
from etl_craft.config.auth import EMAIL_VERIFIED_AUTH_MODES, engine_for_jdbc_url, warehouse_by_key
from etl_craft.handlers.mail import send_email
from etl_craft.services.doctor import Status, run_checks
from etl_craft.warehouse.connection import build_warehouse_engine
from fixtures.services import (
    CERTS_DIR,
    MINIO_PASSWORD,
    MINIO_USER,
    POSTGRES_DB,
    POSTGRES_PASSWORD,
    POSTGRES_USER,
    require,
)

pytestmark = pytest.mark.connections

SQLITE_ENGINE = {"jdbc_url": "jdbc:sqlite:engine.db", "schema": "main"}

MATRIX = {
    ("Engine DB", "SQLite"): {"none"},
    ("Engine DB", "PostgreSQL"): {"password", "key_file"},
    ("Warehouse", "postgres"): {"password", "key_file"},
    ("Warehouse", "duckdb"): {"none"},
    ("Warehouse", "duckdb_iceberg"): {"none", "oauth"},
    ("Warehouse", "trino_iceberg"): {"none"},
    ("Email", "SMTP"): {"none", "password"},
}
"""What this suite connects with, by resource."""

CLOUD = {
    ("Warehouse", "databricks"): {"token"},
    ("Warehouse", "databricks_iceberg"): {"token"},
    ("Warehouse", "snowflake"): {"password", "token"},
    ("Warehouse", "snowflake_iceberg"): {"password", "token"},
}
"""What the cloud suites connect with, run with the team's own accounts."""


def write(tmp_path: Path, **sections) -> Path:
    """Write a craft-connector.yml with one ``dev`` profile per section."""
    settings = sections.pop("warehouse_settings", {})
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local", **sections.pop("orchestration", {})},
        "Engine": {"dev": sections.pop("engine", SQLITE_ENGINE)},
        **{name: {"dev": block} for name, block in sections.items()},
    }
    if "Warehouse" in raw:
        raw["Warehouse"] = {**settings, **raw["Warehouse"]}
    path = tmp_path / "craft-connector.yml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return path


def checked(config, prefix: str) -> dict[str, tuple[Status, str]]:
    """Run doctor's connection checks; return those about ``prefix``, and none may fail."""
    checks = run_checks(config, engine_state=False)
    failed = [f"{c.name}: {c.detail}" for c in checks if c.status is Status.FAIL]
    assert failed == [], failed
    return {c.name: (c.status, c.detail) for c in checks if c.name.startswith(prefix)}


def client_key(tmp_path: Path) -> dict[str, str]:
    """The test client certificate, with its key readable by this user only, as libpq wants."""
    assert (CERTS_DIR / "client.crt").is_file(), "run `make certs` to create the test certificates"
    key = tmp_path / "client.key"
    key.write_bytes((CERTS_DIR / "client.key").read_bytes())
    key.chmod(0o600)
    return {"key_file": str(key), "cert_file": str(CERTS_DIR / "client.crt")}


def tls_url(service) -> str:
    query = urllib.parse.urlencode(
        {"sslmode": "verify-full", "sslrootcert": str(CERTS_DIR / "ca.crt")}
    )
    return f"jdbc:postgresql://{service.address}/{POSTGRES_DB}?{query}"


# The Engine DB


def engine_profile(mode: str, tmp_path: Path, monkeypatch) -> dict:
    if mode == "sqlite-none":
        return SQLITE_ENGINE
    if mode == "password":
        pg = require("postgres")
        monkeypatch.setenv("ETL_CRAFT_MATRIX_ENGINE_SECRET", POSTGRES_PASSWORD)
        return {
            "jdbc_url": f"jdbc:postgresql://{pg.address}/{POSTGRES_DB}",
            "schema": "public",
            "user": POSTGRES_USER,
            "auth_mode": "password",
            "secret": "ETL_CRAFT_MATRIX_ENGINE_SECRET",
        }
    pg = require("postgres_tls")
    return {
        "jdbc_url": tls_url(pg),
        "schema": "public",
        "user": POSTGRES_USER,
        "auth_mode": "key_file",
        **client_key(tmp_path),
    }


@pytest.mark.parametrize("mode", ["sqlite-none", "password", "key_file"])
def test_the_engine_db(mode, tmp_path, monkeypatch):
    config = load_config(write(tmp_path, engine=engine_profile(mode, tmp_path, monkeypatch)))
    checks = checked(config, "Engine DB")
    assert checks["Engine DB connection"][0] is Status.OK
    assert "Engine DB auth" not in checks, "a verified auth mode is not warned about"


def test_a_key_file_that_is_not_the_servers_client_fails_with_the_reason(tmp_path, monkeypatch):
    profile = engine_profile("key_file", tmp_path, monkeypatch)
    profile["jdbc_url"] = profile["jdbc_url"].replace("verify-full", "require")
    del profile["cert_file"]
    config = load_config(write(tmp_path, engine=profile))
    checks = {c.name: c for c in run_checks(config, engine_state=False)}
    assert checks["Engine DB connection"].status is Status.FAIL
    assert "certificate" in checks["Engine DB connection"].detail.lower()


# The warehouse


def warehouse_profile(case: str, tmp_path: Path, monkeypatch) -> tuple[dict, dict]:
    """Return the Warehouse section's profile and its section-level settings."""
    if case == "postgres-password":
        pg = require("postgres")
        monkeypatch.setenv("ETL_CRAFT_MATRIX_WAREHOUSE_SECRET", POSTGRES_PASSWORD)
        return {
            "jdbc_url": f"jdbc:postgresql://{pg.address}/{POSTGRES_DB}",
            "schema": "public",
            "user": POSTGRES_USER,
            "auth_mode": "password",
            "secret": "ETL_CRAFT_MATRIX_WAREHOUSE_SECRET",
        }, {}
    if case == "postgres-key_file":
        return {
            "jdbc_url": tls_url(require("postgres_tls")),
            "schema": "public",
            "user": POSTGRES_USER,
            "auth_mode": "key_file",
            **client_key(tmp_path),
        }, {}
    if case == "duckdb-none":
        return {"jdbc_url": f"jdbc:duckdb:{tmp_path / 'wh.duckdb'}", "schema": "main"}, {}
    if case == "trino_iceberg-none":
        trino = require("trino")
        schema = f"m_{uuid.uuid4().hex[:8]}"
        profile = {"jdbc_url": f"jdbc:trino://{trino.address}/iceberg/{schema}", "schema": schema}
        profile |= {"user": "etl", "auth_mode": "none"}
        return profile, {"Name": "Trino", "Table_format": "iceberg"}
    catalog = require("iceberg_rest")
    minio = require("minio")
    monkeypatch.setenv("ETL_CRAFT_MATRIX_S3_SECRET", MINIO_PASSWORD)
    profile = {
        "jdbc_url": "jdbc:duckdb:",
        "schema": f"m_{uuid.uuid4().hex[:8]}",
        "catalog": "lake",
        "catalog_uri": catalog.http_url,
        "iceberg_warehouse": "s3://warehouse/",
        "s3_endpoint": minio.address,
        "s3_region": "us-east-1",
        "s3_url_style": "path",
        "s3_use_ssl": "false",
        "s3_key_id": MINIO_USER,
        "s3_secret": "ETL_CRAFT_MATRIX_S3_SECRET",
    }
    if case == "duckdb_iceberg-oauth":
        monkeypatch.setenv("ETL_CRAFT_MATRIX_WAREHOUSE_SECRET", "secret")
        profile |= {
            "auth_mode": "oauth",
            "client_id": "etl-craft",
            "token_url": f"{catalog.http_url}/v1/oauth/tokens",
            "scope": "catalog",
            "secret": "ETL_CRAFT_MATRIX_WAREHOUSE_SECRET",
        }
    return profile, {"Name": "DuckDB", "Table_format": "iceberg"}


def namespace(name: str, method: str) -> None:
    """Create or drop a namespace in the Iceberg REST catalog, as its owners would."""
    catalog = require("iceberg_rest")
    url = f"{catalog.http_url}/v1/namespaces"
    body = None
    if method == "POST":
        body = json.dumps({"namespace": [name]}).encode()
    else:
        url += f"/{name}"
    request = urllib.request.Request(
        url, data=body, method=method, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=10):
        pass


def create_schema(config, schema: str) -> None:
    """Make the profile's schema, as a team does before pointing etl-craft at it."""
    engine = build_warehouse_engine(config)
    try:
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    finally:
        engine.dispose()


def drop_schema(config, schema: str) -> None:
    engine = build_warehouse_engine(config)
    try:
        with engine.begin() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema}"))
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "case",
    [
        "postgres-password",
        "postgres-key_file",
        "duckdb-none",
        "duckdb_iceberg-none",
        "duckdb_iceberg-oauth",
        "trino_iceberg-none",
    ],
)
def test_the_warehouse(case, tmp_path, monkeypatch):
    profile, settings = warehouse_profile(case, tmp_path, monkeypatch)
    config = load_config(write(tmp_path, Warehouse=profile, warehouse_settings=settings))
    schema = profile["schema"]
    if case.startswith("trino"):
        create_schema(config, f"iceberg.{schema}")
    elif case.startswith("duckdb_iceberg"):
        namespace(schema, "POST")
    try:
        checks = checked(config, "Warehouse")
        assert checks["Warehouse connection"][0] is Status.OK
        assert "Warehouse auth" not in checks, "a verified auth mode is not warned about"
    finally:
        if case.startswith("trino"):
            drop_schema(config, f"iceberg.{schema}")
        elif case.startswith("duckdb_iceberg"):
            namespace(schema, "DELETE")


# The email relay


@pytest.mark.parametrize("mode", ["none", "password"])
def test_the_email_relay(mode, tmp_path, monkeypatch):
    smtp = require("mailpit_smtp")
    api = require("mailpit_api")
    email = {
        "host": smtp.host,
        "port": smtp.port,
        "from_address": "etl@example.com",
        "use_tls": False,
        "auth_mode": mode,
    }
    if mode == "password":
        monkeypatch.setenv("ETL_CRAFT_MATRIX_EMAIL_SECRET", "relay-password")
        email |= {"user": "etl@example.com", "secret": "ETL_CRAFT_MATRIX_EMAIL_SECRET"}
    config = load_config(write(tmp_path, orchestration={"Email": email}))
    checks = checked(config, "Email")
    assert checks["Email relay"][0] is Status.OK and "Email auth" not in checks
    subject = f"matrix {mode} {uuid.uuid4().hex[:8]}"
    send_email(config, ["team@example.com"], subject, "<p>connection matrix</p>")
    query = urllib.parse.urlencode({"query": f'subject:"{subject}"'})
    with urllib.request.urlopen(f"{api.http_url}/api/v1/search?{query}", timeout=10) as reply:
        assert len(json.load(reply)["messages"]) == 1


# What doctor calls verified


def test_doctor_calls_verified_exactly_what_the_matrix_and_the_cloud_suites_exercise():
    verified = {
        ("Engine DB", "SQLite"): engine_for_jdbc_url("jdbc:sqlite:x.db").verified_auth_modes,
        ("Engine DB", "PostgreSQL"): engine_for_jdbc_url(
            "jdbc:postgresql://h/d"
        ).verified_auth_modes,
        ("Email", "SMTP"): EMAIL_VERIFIED_AUTH_MODES,
    }
    for section, key in {**MATRIX, **CLOUD}:
        if section == "Warehouse":
            verified[(section, key)] = warehouse_by_key(key).verified_auth_modes
    assert verified == {**MATRIX, **CLOUD}
