"""The local services work as the other suites expect. Each test skips when its service is down."""

from __future__ import annotations

import json
import smtplib
import time
import urllib.error
import urllib.request
import uuid
from email.message import EmailMessage
from typing import Any

import psycopg
import pytest

from fixtures.services import (
    CERTS_DIR,
    MINIO_BUCKET,
    POSTGRES_DB,
    POSTGRES_PASSWORD,
    POSTGRES_USER,
    Service,
    require,
)

pytestmark = pytest.mark.harness


def http_json(request: urllib.request.Request | str, timeout: float = 30) -> Any:
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def http_status(url: str) -> int:
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return int(response.status)
    except urllib.error.HTTPError as error:
        return error.code


# -- PostgreSQL ---------------------------------------------------------------------------


def test_postgres_accepts_a_password_login():
    pg = require("postgres")
    with psycopg.connect(
        host=pg.host,
        port=pg.port,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
        connect_timeout=5,
    ) as conn:
        assert conn.execute("SELECT 1").fetchone() == (1,)


def test_postgres_tls_accepts_a_client_certificate():
    pg = require("postgres_tls")
    assert (CERTS_DIR / "client.crt").is_file(), "run `make certs` to create the test certificates"
    with psycopg.connect(
        host=pg.host,
        port=pg.port,
        user=POSTGRES_USER,
        dbname=POSTGRES_DB,
        sslmode="verify-full",
        sslrootcert=str(CERTS_DIR / "ca.crt"),
        sslcert=str(CERTS_DIR / "client.crt"),
        sslkey=str(CERTS_DIR / "client.key"),
        connect_timeout=5,
    ) as conn:
        row = conn.execute("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()").fetchone()
        assert row == (True,)


@pytest.mark.parametrize("sslmode", ["disable", "require"])
def test_postgres_tls_refuses_logins_without_a_client_certificate(sslmode):
    pg = require("postgres_tls")
    with pytest.raises(psycopg.OperationalError):
        psycopg.connect(
            host=pg.host,
            port=pg.port,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB,
            sslmode=sslmode,
            connect_timeout=5,
        )


# -- MinIO, the Iceberg REST catalog and Trino --------------------------------------------


def test_minio_is_live():
    minio = require("minio")
    assert http_status(f"{minio.http_url}/minio/health/live") == 200


def test_iceberg_rest_catalog_serves_its_configuration():
    catalog = require("iceberg_rest")
    config = http_json(f"{catalog.http_url}/v1/config?warehouse=s3://{MINIO_BUCKET}/")
    assert {"defaults", "overrides"} <= set(config)


def trino_query(trino: Service, sql: str, deadline_seconds: float = 120) -> list[list[Any]]:
    """Run one statement through Trino's HTTP protocol and return its rows."""
    headers = {"X-Trino-User": POSTGRES_USER}
    request = urllib.request.Request(
        f"{trino.http_url}/v1/statement", data=sql.encode(), headers=headers, method="POST"
    )
    payload = http_json(request)
    rows: list[list[Any]] = []
    deadline = time.monotonic() + deadline_seconds
    while True:
        if "error" in payload:
            raise AssertionError(f"{sql}: {payload['error'].get('message')}")
        rows.extend(payload.get("data", []))
        next_uri = payload.get("nextUri")
        if not next_uri:
            return rows
        if time.monotonic() > deadline:
            raise AssertionError(f"{sql}: no result within {deadline_seconds}s")
        payload = http_json(urllib.request.Request(next_uri, headers=headers))


def test_trino_is_ready():
    trino = require("trino")
    assert http_json(f"{trino.http_url}/v1/info")["starting"] is False


def test_trino_writes_an_iceberg_table_to_minio_and_reads_it_back():
    # The table's data and metadata land in the warehouse bucket, so this also proves the
    # bucket exists and the catalog can write to it.
    trino = require("trino")
    table = f"iceberg.harness.t_{uuid.uuid4().hex[:12]}"
    trino_query(trino, "CREATE SCHEMA IF NOT EXISTS iceberg.harness")
    try:
        trino_query(trino, f"CREATE TABLE {table} AS SELECT 1 AS x")
        assert trino_query(trino, f"SELECT x FROM {table}") == [[1]]
    finally:
        trino_query(trino, f"DROP TABLE IF EXISTS {table}")


# -- Mailpit ------------------------------------------------------------------------------


def delivered_subjects(api: Service) -> list[str]:
    listing = http_json(f"{api.http_url}/api/v1/messages?limit=200")
    return [message["Subject"] for message in listing["messages"]]


@pytest.mark.parametrize("login", [False, True], ids=["no-auth", "password"])
def test_mailpit_delivers_mail_sent_with_and_without_a_login(login):
    smtp = require("mailpit_smtp")
    api = require("mailpit_api")
    subject = f"harness {uuid.uuid4().hex}"
    message = EmailMessage()
    message["From"] = "etl-craft@example.com"
    message["To"] = "ops@example.com"
    message["Subject"] = subject
    message.set_content("sent by the service harness")
    with smtplib.SMTP(smtp.host, smtp.port, timeout=10) as server:
        if login:
            server.login("etl-craft", "any-password")
        server.send_message(message)

    deadline = time.monotonic() + 10
    while subject not in delivered_subjects(api):
        assert time.monotonic() < deadline, f"{subject!r} never reached mailpit"
        time.sleep(0.2)
