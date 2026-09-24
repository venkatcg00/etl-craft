"""Authentication types: none, password, token, key_file, oauth, sso, sts (2026-09-24).

Per explicit instruction: "sso, oauth, sts, key files should be supported but we
cannot test them without elaborate support, so, say these can be used but
success is not guaranteed". What *can* be proven here is proven:

* how each dialect hands each credential to its driver -- down to the real
  driver dialect's own create_connect_args where one is installed, so a
  misnamed parameter fails here rather than at a customer;
* the OAuth client-credentials exchange, against a local HTTP stand-in and
  against the repo's own Iceberg REST catalog, which implements the grant;
* an AWS RDS IAM token, which boto3 signs locally with no AWS call;
* `doctor` warning for every mode that has not run against a live service.
"""

from __future__ import annotations

import json
import os
import smtplib
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs

import pytest
from sqlalchemy import text
from sqlalchemy.engine import URL

import etl_craft.warehouse as warehouse_module
from etl_craft import credentials
from etl_craft.config import (
    CloningConfig,
    ConnectionProfile,
    ConnectionSection,
    ConnectorConfig,
    EmailConfig,
    EmailProfile,
    SourceConfig,
)
from etl_craft.db import ConnectionError_
from etl_craft.dialects import warehouse_dialects
from etl_craft.dialects.engine_dialects import for_name
from etl_craft.dialects.engine_dialects.postgres import _AUTH_REGISTRY as ENGINE_AUTH
from etl_craft.doctor import _auth_check, _settings_check
from etl_craft.email_alert import _login_xoauth2
from etl_craft.warehouse import WAREHOUSE_AUTH_REGISTRY, build_warehouse_engine

ICEBERG_REST_URL = os.environ.get("ETL_CRAFT_TEST_ICEBERG_REST_URL", "http://localhost:58181")


def _profile(auth_mode: str, jdbc_url: str, user: str = "etl", **extra) -> ConnectionProfile:
    return ConnectionProfile(
        section="WAREHOUSE",
        name="dev",
        jdbc_url=jdbc_url,
        user=user,
        auth_mode=auth_mode,
        extra=extra,
    )


@pytest.fixture
def captured(monkeypatch):
    """Capture what the warehouse creator hands the driver, instead of connecting."""
    seen: dict = {}

    def fake_connect(url, extra=None):
        seen["url"] = url
        seen["args"] = extra or {}
        return object()

    monkeypatch.setattr(warehouse_module, "_dbapi_connect", fake_connect)
    return seen


@pytest.fixture
def minted(monkeypatch):
    """Replace the OAuth exchange with a recorder that returns a fixed token."""
    calls: list[tuple] = []

    def fake_token(token_url, client_id, client_secret, scope=None):
        calls.append((token_url, client_id, client_secret, scope))
        return f"access-token-{len(calls)}"

    monkeypatch.setattr(credentials, "client_credentials_token", fake_token)
    return calls


# -- the OAuth client-credentials exchange ---------------------------------------


class _TokenEndpoint(BaseHTTPRequestHandler):
    status = 200
    body: dict = {"access_token": "t0k3n", "token_type": "Bearer"}
    requests: list[dict] = []

    def do_POST(self):  # noqa: N802 - the stdlib's name
        length = int(self.headers["Content-Length"])
        form = parse_qs(self.rfile.read(length).decode())
        type(self).requests.append({key: value[0] for key, value in form.items()})
        payload = json.dumps(type(self).body).encode()
        self.send_response(type(self).status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):  # keep test output clean
        return None


@pytest.fixture
def token_endpoint():
    server = HTTPServer(("127.0.0.1", 0), _TokenEndpoint)
    _TokenEndpoint.status = 200
    _TokenEndpoint.body = {"access_token": "t0k3n", "token_type": "Bearer"}
    _TokenEndpoint.requests = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/token", _TokenEndpoint
    server.shutdown()


def test_client_credentials_token_posts_the_grant_and_returns_the_access_token(token_endpoint):
    url, endpoint = token_endpoint
    assert credentials.client_credentials_token(url, "cid", "csecret", "sql") == "t0k3n"
    assert endpoint.requests == [
        {
            "grant_type": "client_credentials",
            "client_id": "cid",
            "client_secret": "csecret",
            "scope": "sql",
        }
    ]


def test_client_credentials_token_reports_a_refusal_without_echoing_the_secret(token_endpoint):
    url, endpoint = token_endpoint
    endpoint.status = 401
    endpoint.body = {"error": "invalid_client", "error_description": "bad secret"}
    with pytest.raises(ConnectionError_, match="HTTP 401 .invalid_client: bad secret") as info:
        credentials.client_credentials_token(url, "cid", "hunter2")
    assert "hunter2" not in str(info.value)


def test_client_credentials_token_needs_an_access_token_in_the_answer(token_endpoint):
    url, endpoint = token_endpoint
    endpoint.body = {"token_type": "Bearer"}
    with pytest.raises(ConnectionError_, match="had no access_token"):
        credentials.client_credentials_token(url, "cid", "s")


def test_client_credentials_token_reports_an_unreachable_endpoint():
    with pytest.raises(ConnectionError_, match="failed"):
        credentials.client_credentials_token("http://127.0.0.1:9/token", "cid", "s")


def _iceberg_rest_reachable() -> bool:
    import urllib.request

    try:
        urllib.request.urlopen(f"{ICEBERG_REST_URL}/v1/config?warehouse=s3://warehouse/", timeout=3)
    except OSError:
        return False
    return True


@pytest.mark.skipif(not _iceberg_rest_reachable(), reason="Iceberg REST catalog not running")
def test_client_credentials_token_against_a_real_oauth_endpoint():
    # The repo's own Iceberg REST catalog implements the client-credentials
    # grant: a real server, not a stand-in, on the other end.
    token = credentials.client_credentials_token(
        f"{ICEBERG_REST_URL}/v1/oauth/tokens", "etl-craft", "secret", "catalog"
    )
    assert token


# -- AWS RDS IAM (sts) ------------------------------------------------------------


@pytest.fixture
def fake_aws_credentials(monkeypatch):
    # generate_db_auth_token signs locally: no AWS call is made, so fake
    # credentials produce a real, well-formed token.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    monkeypatch.delenv("AWS_PROFILE", raising=False)


def test_aws_rds_auth_token_is_signed_for_the_host_and_user(fake_aws_credentials):
    token = credentials.aws_rds_auth_token("db.example.com", 5432, "etl", "us-east-1")
    assert token.startswith("db.example.com:5432/?")
    assert "DBUser=etl" in token
    assert "X-Amz-Signature=" in token


def test_aws_rds_auth_token_signs_as_the_assumed_role(fake_aws_credentials, monkeypatch):
    import boto3

    assumed = []
    real_client = boto3.session.Session.client

    class FakeSts:
        def assume_role(self, RoleArn, RoleSessionName):  # noqa: N803 - boto3's names
            assumed.append(RoleArn)
            return {
                "Credentials": {
                    "AccessKeyId": "ASIAROLE",
                    "SecretAccessKey": "role-secret",
                    "SessionToken": "role-session",
                }
            }

    def client(self, name, *args, **kwargs):
        return FakeSts() if name == "sts" else real_client(self, name, *args, **kwargs)

    monkeypatch.setattr(boto3.session.Session, "client", client)
    token = credentials.aws_rds_auth_token(
        "db.example.com", 5432, "etl", "us-east-1", "arn:aws:iam::1:role/etl"
    )
    assert assumed == ["arn:aws:iam::1:role/etl"]
    assert "X-Amz-Security-Token=role-session" in token
    assert "ASIAROLE" in token


# -- PostgreSQL: the Engine DB and a Postgres warehouse -----------------------------


@pytest.fixture
def psycopg_calls(monkeypatch):
    import psycopg

    calls: list[dict] = []
    monkeypatch.setattr(psycopg, "connect", lambda **kwargs: calls.append(kwargs) or object())
    return calls


def _engine(auth_mode: str, url: str = "jdbc:postgresql://db:5432/etl", **extra):
    return ConnectionProfile(
        section="ENGINE", name="dev", jdbc_url=url, user="etl", auth_mode=auth_mode, extra=extra
    )


def test_postgres_token_is_the_password(psycopg_calls):
    ENGINE_AUTH["token"](_engine("token"), "bearer")()
    assert psycopg_calls[0]["password"] == "bearer"


def test_postgres_oauth_mints_a_fresh_token_per_connection(psycopg_calls, minted):
    creator = ENGINE_AUTH["oauth"](
        _engine("oauth", client_id="cid", token_url="https://idp/token", scope="s"), "csecret"
    )
    creator()
    creator()
    assert minted == [("https://idp/token", "cid", "csecret", "s")] * 2
    assert [call["password"] for call in psycopg_calls] == ["access-token-1", "access-token-2"]


def test_postgres_sts_uses_an_rds_token_over_tls(psycopg_calls, monkeypatch):
    seen = []

    def fake_rds(host, port, user, region, role_arn=None):
        seen.append((host, port, user, region, role_arn))
        return "rds-token"

    monkeypatch.setattr(credentials, "aws_rds_auth_token", fake_rds)
    ENGINE_AUTH["sts"](_engine("sts", region="eu-west-1"), "")()
    assert seen == [("db", 5432, "etl", "eu-west-1", None)]
    assert psycopg_calls[0]["password"] == "rds-token"
    assert psycopg_calls[0]["sslmode"] == "require"
    # A URL that names its own sslmode keeps it.
    ENGINE_AUTH["sts"](
        _engine("sts", "jdbc:postgresql://db/etl?sslmode=verify-full", region="r"), ""
    )()
    assert psycopg_calls[1]["sslmode"] == "verify-full"


def test_postgres_sso_hands_libpq_its_oauth_settings(psycopg_calls):
    ENGINE_AUTH["sso"](_engine("sso", issuer="https://idp", client_id="cid", scope="openid"), "")()
    call = psycopg_calls[0]
    assert call["oauth_issuer"] == "https://idp"
    assert call["oauth_client_id"] == "cid"
    assert call["oauth_scope"] == "openid"
    assert "password" not in call and "oauth_client_secret" not in call


def test_postgres_key_file_sends_the_certificate_too(psycopg_calls):
    ENGINE_AUTH["key_file"](_engine("key_file", key_file="/k.pem", cert_file="/c.pem"), "pp")()
    assert psycopg_calls[0]["sslkey"] == "/k.pem"
    assert psycopg_calls[0]["sslcert"] == "/c.pem"
    assert psycopg_calls[0]["sslpassword"] == "pp"


def test_postgres_auth_fields_are_checked_before_connecting():
    with pytest.raises(ConnectionError_, match="requires a `region:`"):
        ENGINE_AUTH["sts"](_engine("sts"), "")


def test_postgres_warehouse_authenticates_exactly_like_the_engine_db(captured, minted):
    url = "jdbc:postgresql://wh:5432/analytics?sslmode=require"
    WAREHOUSE_AUTH_REGISTRY["oauth"](
        _profile("oauth", url, client_id="cid", token_url="https://idp/token"), "csecret"
    )()
    assert captured["args"] == {"password": "access-token-1"}
    assert captured["url"].password is None
    WAREHOUSE_AUTH_REGISTRY["key_file"](
        _profile("key_file", url, key_file="/k.pem", cert_file="/c.pem"), "pp"
    )()
    assert captured["args"] == {"sslkey": "/k.pem", "sslpassword": "pp", "sslcert": "/c.pem"}


# -- Trino -------------------------------------------------------------------------

TRINO = "jdbc:trino://trino.internal:8443/iceberg/analytics"


def _trino_auth(url: URL):
    """What the real trino dialect makes of the URL the creator built."""
    from trino.sqlalchemy.dialect import TrinoDialect

    _, kwargs = TrinoDialect().create_connect_args(url)
    return kwargs.get("auth")


def test_trino_token_is_a_jwt_without_a_user(captured):
    from trino.auth import JWTAuthentication

    WAREHOUSE_AUTH_REGISTRY["token"](_profile("token", TRINO, user=""), "jwt")()
    assert isinstance(_trino_auth(captured["url"]), JWTAuthentication)


def test_trino_oauth_sends_the_minted_token_as_a_jwt(captured, minted):
    from trino.auth import JWTAuthentication

    WAREHOUSE_AUTH_REGISTRY["oauth"](
        _profile("oauth", TRINO, user="", client_id="cid", token_url="https://idp/token"), "cs"
    )()
    auth = _trino_auth(captured["url"])
    assert isinstance(auth, JWTAuthentication)
    assert captured["url"].query["access_token"] == "access-token-1"


def test_trino_sso_and_key_file_select_the_clients_own_classes(captured):
    from trino.auth import CertificateAuthentication, OAuth2Authentication

    WAREHOUSE_AUTH_REGISTRY["sso"](_profile("sso", TRINO), "")()
    assert isinstance(_trino_auth(captured["url"]), OAuth2Authentication)
    WAREHOUSE_AUTH_REGISTRY["key_file"](
        _profile("key_file", TRINO, key_file="/k.pem", cert_file="/c.pem"), ""
    )()
    assert isinstance(_trino_auth(captured["url"]), CertificateAuthentication)


# -- Databricks --------------------------------------------------------------------

DATABRICKS = "jdbc:databricks://adb-1.azuredatabricks.net:443/default;httpPath=/sql/1.0/w/1"


def test_databricks_oauth_uses_the_workspace_token_endpoint_by_default(captured, minted):
    from databricks.sqlalchemy import DatabricksDialect

    WAREHOUSE_AUTH_REGISTRY["oauth"](_profile("oauth", DATABRICKS, user="", client_id="sp"), "s")()
    assert minted == [("https://adb-1.azuredatabricks.net/oidc/v1/token", "sp", "s", "all-apis")]
    _, kwargs = DatabricksDialect().create_connect_args(captured["url"])
    assert kwargs["access_token"] == "access-token-1"


def test_databricks_sso_asks_the_connector_for_its_browser_login(captured):
    WAREHOUSE_AUTH_REGISTRY["sso"](_profile("sso", DATABRICKS, user=""), "")()
    assert captured["args"] == {"auth_type": "databricks-oauth"}
    assert captured["url"].password is None


# -- Snowflake ---------------------------------------------------------------------

SNOWFLAKE = "jdbc:snowflake://org-acct.snowflakecomputing.com/?db=ANALYTICS&schema=PUBLIC"


def test_snowflake_oauth_is_the_connectors_own_client_credentials_flow(captured, minted):
    WAREHOUSE_AUTH_REGISTRY["oauth"](
        _profile("oauth", SNOWFLAKE, client_id="cid", token_url="https://idp/token", scope="r"),
        "csecret",
    )()
    assert captured["args"] == {
        "authenticator": "OAUTH_CLIENT_CREDENTIALS",
        "oauth_client_id": "cid",
        "oauth_client_secret": "csecret",
        "oauth_token_request_url": "https://idp/token",
        "oauth_scope": "r",
    }
    # The connector runs the exchange itself; the engine mints nothing.
    assert minted == []


def test_snowflake_sso_and_sts_name_the_connectors_authenticators(captured):
    WAREHOUSE_AUTH_REGISTRY["sso"](_profile("sso", SNOWFLAKE), "")()
    assert captured["args"] == {"authenticator": "externalbrowser"}
    WAREHOUSE_AUTH_REGISTRY["sts"](_profile("sts", SNOWFLAKE), "")()
    assert captured["args"] == {
        "authenticator": "WORKLOAD_IDENTITY",
        "workload_identity_provider": "AWS",
    }


def test_snowflake_connector_accepts_every_argument_the_dialect_sends():
    # Every connect argument named for Snowflake is one the installed
    # connector declares -- a typo would otherwise surface only at a customer.
    from snowflake.connector.connection import DEFAULT_CONFIGURATION

    for mode in ("oauth", "sso", "sts", "key_file"):
        dialect = warehouse_dialects.for_key("snowflake")
        profile = _profile(
            mode, SNOWFLAKE, client_id="c", token_url="https://idp/t", scope="s", key_file="/k"
        )
        presented = dialect.present(profile, "secret", {"host": "h", "port": None})
        assert set(presented.connect_args) <= set(DEFAULT_CONFIGURATION), mode


# -- engines and doctor ------------------------------------------------------------


def test_a_minted_credential_recycles_pooled_connections(monkeypatch):
    profile = _profile("oauth", "jdbc:postgresql://wh/analytics", client_id="c", token_url="u")
    monkeypatch.setenv(profile.secret_var, "s")
    config = _warehouse_config(profile)
    engine = build_warehouse_engine(config)
    assert engine.pool._recycle == credentials.MINTED_CREDENTIAL_POOL_RECYCLE_SECONDS


def _warehouse_config(profile: ConnectionProfile) -> ConnectorConfig:
    return ConnectorConfig(
        mode="local",
        source=SourceConfig(type="environment"),
        postgres=ConnectionSection(
            active_profile="dev",
            profiles={
                "dev": ConnectionProfile(
                    section="ENGINE",
                    name="dev",
                    jdbc_url="jdbc:sqlite:e.db",
                    user="",
                    auth_mode="none",
                )
            },
        ),
        cloning=CloningConfig(),
        warehouse=ConnectionSection(active_profile="dev", profiles={"dev": profile}),
    )


def test_doctor_warns_for_every_mode_not_run_against_a_live_service():
    # Per explicit instruction: "say these can be used but success is not
    # guaranteed" -- a warning, never a failure.
    for key in ("postgres", "snowflake", "databricks", "trino_iceberg", "duckdb_iceberg"):
        dialect = warehouse_dialects.for_key(key)
        for mode in dialect.auth_modes:
            results = _auth_check("Warehouse", mode, dialect.verified_auth_modes, key)
            if mode in dialect.verified_auth_modes:
                assert results == []
            else:
                (result,) = results
                assert result.ok and result.marker == "WARN"
                assert "success is not guaranteed" in result.detail
    assert for_name("postgresql").verified_auth_modes == frozenset({"password"})


def test_doctor_names_a_value_used_as_written_that_looks_like_a_variable(tmp_path, monkeypatch):
    from etl_craft.config import load_config

    path = tmp_path / "craft-connector.yml"
    path.write_text(
        "Secrets:\n  Source_type: environment\n  Profile: dev\n\n"
        "Orchestration:\n  Mode: local\n\n"
        "Engine:\n  dev:\n    jdbc_url: jdbc:sqlite:e.db\n\n"
        "Warehouse:\n  dev:\n    jdbc_url: jdbc:postgresql://wh/a\n"
        "    user: WAREHOUSE_USER\n    auth_mode: password\n    secret: WAREHOUSE_SECRET\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("WAREHOUSE_USER", raising=False)
    monkeypatch.setenv("WAREHOUSE_SECRET", "x")
    results = _settings_check(load_config(path))
    warnings = [r for r in results if r.warning]
    assert len(warnings) == 1
    assert "Warehouse.dev.user is 'WAREHOUSE_USER'" in warnings[0].detail
    # `dev`, `local` and the literal URL are ordinary values, not suspects.


# -- Email: SMTP XOAUTH2 -------------------------------------------------------------


def test_email_oauth_logs_in_with_xoauth2(monkeypatch, minted):
    from etl_craft.execution import TaskExecutionContext

    monkeypatch.setenv("EMAIL_SECRET", "csecret")
    email = EmailProfile(
        section="EMAIL",
        name="dev",
        host="smtp.office365.com",
        port=587,
        from_address="etl@example.com",
        auth_mode="oauth",
        user="etl@example.com",
        extra={"secret_var": "EMAIL_SECRET", "client_id": "cid", "token_url": "https://idp/t"},
    )
    config = _warehouse_config(_profile("password", "jdbc:postgresql://wh/a"))
    config = ConnectorConfig(
        **{**config.__dict__, "email": EmailConfig(active_profile="dev", profiles={"dev": email})}
    )
    ctx = TaskExecutionContext(
        config=config,
        task_run_id=1,
        pipeline_run_id=1,
        handler="EMAIL_ALERT",
        task_params={},
        pipeline_code="P",
        task_code="T",
        refresh_type="FULL",
        force=False,
        task_id=1,
        pipeline_id=1,
    )

    class FakeSmtp:
        def __init__(self):
            self.auth_calls = []

        def ehlo_or_helo_if_needed(self):
            return None

        def auth(self, mechanism, authobject, *, initial_response_ok=True):
            self.auth_calls.append((mechanism, authobject(), initial_response_ok))

    server = FakeSmtp()
    _login_xoauth2(server, ctx, "etl@example.com")  # type: ignore[arg-type]
    assert minted == [("https://idp/t", "cid", "csecret", None)]
    assert server.auth_calls == [
        ("XOAUTH2", "user=etl@example.com\x01auth=Bearer access-token-1\x01\x01", True)
    ]
    assert smtplib.SMTP.auth  # the real signature this fake mirrors


# -- DuckDB over Iceberg: the catalog's own OAuth, against a real catalog ------------


@pytest.mark.skipif(not _iceberg_rest_reachable(), reason="Iceberg REST catalog not running")
def test_duckdb_iceberg_oauth_attaches_the_real_catalog(monkeypatch):
    profile = _profile(
        "oauth",
        "jdbc:duckdb:",
        user="",
        client_id="etl-craft",
        token_url=f"{ICEBERG_REST_URL}/v1/oauth/tokens",
        scope="catalog",
        catalog="lake",
        catalog_uri=ICEBERG_REST_URL,
        iceberg_warehouse="s3://warehouse/",
        s3_endpoint="localhost:59000",
        s3_region="us-east-1",
        s3_url_style="path",
        s3_use_ssl="false",
        s3_key_id="minioadmin",
        s3_secret="minioadmin",
        secret_var="ETL_CRAFT_TEST_ICEBERG_CLIENT_SECRET",
    )
    monkeypatch.setenv("ETL_CRAFT_TEST_ICEBERG_CLIENT_SECRET", "secret")
    config = ConnectorConfig(
        **{
            **_warehouse_config(profile).__dict__,
            "warehouse_table_format": "iceberg",
        }
    )
    engine = build_warehouse_engine(config)
    try:
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS lake.oauthtest"))
            conn.execute(text("DROP TABLE IF EXISTS lake.oauthtest.t"))
            conn.execute(text("CREATE TABLE lake.oauthtest.t AS SELECT 1 AS id"))
            assert conn.execute(text("SELECT id FROM lake.oauthtest.t")).scalar_one() == 1
            conn.execute(text("DROP TABLE lake.oauthtest.t"))
    finally:
        engine.dispose()
