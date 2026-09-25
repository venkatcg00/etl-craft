"""Engine DB dialects without a database server: the registry, the catalog, SQLite files, auth."""

import re
import sqlite3
from pathlib import Path

import pytest

from etl_craft.config import ConnectionProfile
from etl_craft.core.errors import ConfigurationError
from etl_craft.dialects.engine import all_dialects, build_engine, for_jdbc_url, for_name
from etl_craft.dialects.engine import sqlite as sqlite_dialect
from etl_craft.dialects.engine.base import SHARED_QUERIES
from etl_craft.dialects.engine.postgres import (
    PostgresEngineDialect,
    parse_postgres_url,
    postgres_creator,
    psycopg_auth_kwargs,
)
from etl_craft.dialects.engine.sqlite import SqliteEngineDialect, resolve_sqlite_path
from fixtures.engine_db import engine_config

pytestmark = pytest.mark.unit

POSTGRES = for_name("postgresql")
SQLITE = for_name("sqlite")


# The registry


@pytest.mark.parametrize(
    ("name", "expected"),
    [("postgresql", "postgresql"), ("postgresql+psycopg", "postgresql"), ("sqlite", "sqlite")],
)
def test_for_name(name, expected):
    assert for_name(name).name == expected


def test_an_unsupported_engine_db_is_refused():
    with pytest.raises(ConfigurationError, match="not a supported Engine DB"):
        for_name("mysql")
    with pytest.raises(ConfigurationError, match="not a supported Engine DB URL"):
        for_jdbc_url("jdbc:mysql://h/db")


def test_for_jdbc_url():
    assert isinstance(for_jdbc_url("jdbc:postgresql://h/db"), PostgresEngineDialect)
    assert isinstance(for_jdbc_url("jdbc:sqlite:e.db"), SqliteEngineDialect)


def test_every_dialect_ships_its_schema_and_migration_stream():
    for dialect in all_dialects():
        assert dialect.schema_path().is_file()
        assert dialect.migrations_dir().is_dir()
        assert dialect.auth_modes == dialect.spec.auth_modes


# The query catalog


def test_every_query_resolves_on_every_dialect():
    names = set().union(*(dialect.query_names() for dialect in all_dialects()))
    assert names
    for dialect in all_dialects():
        assert dialect.query_names() == names, dialect.name


def catalog_files():
    yield from sorted(SHARED_QUERIES.glob("*.sql"))
    for dialect in all_dialects():
        yield from sorted(dialect.queries_dir().glob("*.sql"))


@pytest.mark.parametrize(
    "path", list(catalog_files()), ids=lambda path: f"{path.parent.parent.name}/{path.name}"
)
def test_every_selected_column_is_aliased_in_lower_case(path):
    sql = path.read_text(encoding="utf-8")
    aliases = re.findall(r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)", sql)
    body = "\n".join(line for line in sql.splitlines() if not line.startswith("--"))
    if body.lstrip().upper().startswith("SELECT"):
        assert aliases, "a catalog query names its columns with AS"
    assert [alias for alias in aliases if alias != alias.lower()] == []


def test_a_dialect_query_overrides_the_shared_one(tmp_path, monkeypatch):
    shared, own = tmp_path / "shared", tmp_path / "own" / "queries"
    shared.mkdir()
    own.mkdir(parents=True)
    (shared / "both.sql").write_text("SELECT 'shared' AS source", encoding="utf-8")
    (shared / "only_shared.sql").write_text("-- :param in a comment\nSELECT 1 AS one\n", "utf-8")
    (own / "both.sql").write_text("SELECT 'own' AS source", encoding="utf-8")
    monkeypatch.setattr("etl_craft.dialects.engine.base.SHARED_QUERIES", shared)

    class Dialect(SqliteEngineDialect):
        directory = tmp_path / "own"

    dialect = Dialect()
    assert dialect.query("both") == "SELECT 'own' AS source"
    assert dialect.query("only_shared") == "SELECT 1 AS one"
    assert dialect.query_names() == {"both", "only_shared"}
    with pytest.raises(LookupError, match="no Engine DB query named 'missing' for sqlite"):
        dialect.query("missing")


# The schemas define the same tables and columns


def schema_columns(path: Path) -> dict[str, list[str]]:
    tables: dict[str, list[str]] = {}
    for match in re.finditer(r"CREATE TABLE (\w+) \((.*?)\n\);", path.read_text("utf-8"), re.S):
        columns = []
        for line in match[2].splitlines():
            first = line.strip().split(" ", 1)[0]
            if first and first.isupper() and first not in {"CONSTRAINT", "CHECK", "OR", "AND"}:
                columns.append(first)
        tables[match[1]] = columns
    return tables


def test_both_schemas_define_the_same_tables_and_columns_in_order():
    postgres = schema_columns(POSTGRES.schema_path())
    sqlite = schema_columns(SQLITE.schema_path())
    assert len(postgres) == 17
    assert postgres == sqlite


def without_comments(statement):
    return "\n".join(line for line in statement.splitlines() if not line.startswith("--")).strip()


def test_the_schemas_split_into_whole_statements():
    postgres = [
        without_comments(s)
        for s in POSTGRES.split_statements(POSTGRES.schema_path().read_text("utf-8"))
    ]
    functions = [s for s in postgres if s.startswith("CREATE OR REPLACE FUNCTION")]
    assert len(functions) == 2
    assert all(s.endswith("LANGUAGE plpgsql") for s in functions)
    sqlite = [
        without_comments(s)
        for s in SQLITE.split_statements(SQLITE.schema_path().read_text("utf-8"))
    ]
    triggers = [s for s in sqlite if s.startswith("CREATE TRIGGER")]
    assert triggers
    assert all(s.endswith("END") for s in triggers)


def test_the_sqlite_splitter_keeps_trigger_bodies_whole():
    statements = SQLITE.split_statements(
        "CREATE TABLE t (a INT); -- a; comment\n"
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN UPDATE t SET a = 1; DELETE FROM t; END;\n"
        "INSERT INTO t VALUES (';');\n-- trailing comment"
    )
    assert len(statements) == 3
    assert statements[1].endswith("END")
    assert statements[2] == "INSERT INTO t VALUES (';')"


# SQLite files


def test_a_relative_sqlite_path_resolves_beside_the_config(tmp_path):
    config_path = tmp_path / "project" / "craft-connector.yml"
    assert resolve_sqlite_path("jdbc:sqlite:data/engine.db", config_path) == str(
        tmp_path.resolve() / "project" / "data" / "engine.db"
    )
    assert resolve_sqlite_path("jdbc:sqlite:/abs/engine.db", config_path) == "/abs/engine.db"
    assert resolve_sqlite_path("jdbc:sqlite:engine.db") == "engine.db"


@pytest.mark.parametrize("jdbc_url", ["jdbc:sqlite:", "jdbc:sqlite::memory:"])
def test_an_in_memory_sqlite_engine_db_is_refused(jdbc_url):
    with pytest.raises(ConfigurationError, match="in-memory"):
        resolve_sqlite_path(jdbc_url)


def test_sqlite_needs_no_authentication(tmp_path):
    profile = ConnectionProfile("ENGINE", "dev", "jdbc:sqlite:e.db", "u", "password")
    with pytest.raises(ConfigurationError, match="auth_mode must be 'none'"):
        build_engine(engine_config(profile, tmp_path / "craft-connector.yml"))


def test_sqlite_connections_enforce_foreign_keys_and_use_wal(tmp_path):
    profile = ConnectionProfile("ENGINE", "dev", "jdbc:sqlite:sub/e.db", "", "none")
    engine = build_engine(engine_config(profile, tmp_path / "craft-connector.yml"))
    try:
        with engine.connect() as conn:
            assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
            assert conn.exec_driver_sql("PRAGMA journal_mode").scalar_one() == "wal"
        assert (tmp_path / "sub" / "e.db").is_file()
    finally:
        engine.dispose()


def test_an_old_sqlite_library_is_refused(monkeypatch):
    monkeypatch.setattr(sqlite3, "sqlite_version_info", (3, 30, 0))
    with pytest.raises(ConfigurationError, match=r"needs 3\.35\.0 or newer"):
        sqlite_dialect.sqlite_creator("/tmp/unused.db")


def test_a_lock_needs_a_database_file():
    class NoFile:
        class url:  # noqa: N801 - mimics Engine.url
            database = None

    with pytest.raises(Exception, match="has no file path"), SQLITE.lock(NoFile(), 1, "x"):  # type: ignore[arg-type]
        pass  # pragma: no cover - the lock is never taken


def test_sqlite_timestamps_are_stored_as_utc_text_and_read_back_aware():
    from datetime import UTC, datetime, timedelta, timezone

    local = datetime(2026, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=5)))
    assert sqlite_dialect._adapt_datetime(local) == "2026-01-01 07:00:00.000000+00:00"
    assert sqlite_dialect._adapt_datetime(datetime(2026, 1, 1)) == (
        "2026-01-01 00:00:00.000000+00:00"
    )
    assert sqlite_dialect._convert_timestamp(b"2026-01-01 07:00:00") == datetime(
        2026, 1, 1, 7, tzinfo=UTC
    )


# PostgreSQL URLs and authentication


def test_parse_postgres_url():
    url = parse_postgres_url("jdbc:postgresql://db/etl?sslmode=require")
    assert (url.host, url.port, url.database, url.query) == (
        "db",
        5432,
        "etl",
        {"sslmode": "require"},
    )
    assert parse_postgres_url("jdbc:postgresql://db:6543/etl").port == 6543


@pytest.mark.parametrize("jdbc_url", ["jdbc:postgresql://db/", "jdbc:mysql://db/etl"])
def test_parse_postgres_url_refuses_another_shape(jdbc_url):
    with pytest.raises(ConfigurationError, match="not a recognized jdbc:postgresql"):
        parse_postgres_url(jdbc_url)


def profile(auth_mode, url="jdbc:postgresql://db:5432/etl", **extra):
    return ConnectionProfile("ENGINE", "dev", url, "etl", auth_mode, extra)


@pytest.fixture
def psycopg_calls(monkeypatch):
    import psycopg

    calls = []
    monkeypatch.setattr(psycopg, "connect", lambda **kwargs: calls.append(kwargs) or object())
    return calls


def connect(auth_mode, secret="", url="jdbc:postgresql://db:5432/etl", **extra):
    engine_profile = profile(auth_mode, url, **extra)
    postgres_creator(engine_profile, secret, parse_postgres_url(url))()


def test_password_and_token_are_the_password(psycopg_calls):
    connect("password", "pw")
    connect("token", "bearer")
    assert [call["password"] for call in psycopg_calls] == ["pw", "bearer"]
    assert psycopg_calls[0] | {"password": None} == {
        "host": "db",
        "port": 5432,
        "dbname": "etl",
        "user": "etl",
        "password": None,
    }


def test_the_url_query_reaches_the_driver_and_wins(psycopg_calls):
    connect("password", "pw", "jdbc:postgresql://db/etl?sslmode=require&application_name=x")
    assert psycopg_calls[0]["sslmode"] == "require"
    assert psycopg_calls[0]["application_name"] == "x"


def test_oauth_mints_a_fresh_token_per_connection(psycopg_calls, monkeypatch):
    from etl_craft.dialects import credentials

    minted = []

    def fake_token(token_url, client_id, client_secret, scope=None):
        minted.append((token_url, client_id, client_secret, scope))
        return f"access-token-{len(minted)}"

    monkeypatch.setattr(credentials, "client_credentials_token", fake_token)
    creator = postgres_creator(
        profile("oauth", client_id="cid", token_url="https://idp/token", scope="s"),
        "csecret",
        parse_postgres_url("jdbc:postgresql://db/etl"),
    )
    creator()
    creator()
    assert minted == [("https://idp/token", "cid", "csecret", "s")] * 2
    assert [call["password"] for call in psycopg_calls] == ["access-token-1", "access-token-2"]


def test_sts_uses_an_rds_token_over_tls(psycopg_calls, monkeypatch):
    from etl_craft.dialects import credentials

    seen = []

    def fake_rds(host, port, user, region, role_arn=None):
        seen.append((host, port, user, region, role_arn))
        return "rds-token"

    monkeypatch.setattr(credentials, "aws_rds_auth_token", fake_rds)
    connect("sts", region="eu-west-1")
    assert seen == [("db", 5432, "etl", "eu-west-1", None)]
    assert psycopg_calls[0]["password"] == "rds-token"
    assert psycopg_calls[0]["sslmode"] == "require"
    connect("sts", url="jdbc:postgresql://db/etl?sslmode=verify-full", region="r")
    assert psycopg_calls[1]["sslmode"] == "verify-full"


def test_sso_hands_libpq_its_oauth_settings(psycopg_calls):
    connect("sso", issuer="https://idp", client_id="cid", scope="openid")
    call = psycopg_calls[0]
    assert (call["oauth_issuer"], call["oauth_client_id"], call["oauth_scope"]) == (
        "https://idp",
        "cid",
        "openid",
    )
    assert "password" not in call
    assert "oauth_client_secret" not in call
    connect("sso", "client-secret", issuer="https://idp", client_id="cid")
    assert psycopg_calls[1]["oauth_client_secret"] == "client-secret"


def test_key_file_sends_the_certificate_and_passphrase(psycopg_calls):
    connect("key_file", "pp", key_file="/k.pem", cert_file="/c.pem")
    connect("key_file", key_file="/k.pem")
    first, second = psycopg_calls
    assert (first["sslkey"], first["sslcert"], first["sslpassword"]) == ("/k.pem", "/c.pem", "pp")
    assert second["sslpassword"] is None
    assert "sslcert" not in second


def test_an_unknown_auth_mode_is_refused():
    with pytest.raises(ConfigurationError, match="not available for PostgreSQL"):
        psycopg_auth_kwargs("kerberos", user="u", secret="", extra={}, host="h", port=1)


def test_build_engine_checks_the_auth_mode_and_its_fields(tmp_path):
    with pytest.raises(ConfigurationError, match="not valid for a PostgreSQL Engine DB"):
        build_engine(engine_config(profile("none")))
    with pytest.raises(ConfigurationError, match="requires a `region:`"):
        build_engine(engine_config(profile("sts")))


def test_a_minted_credential_recycles_pooled_connections():
    engine = build_engine(engine_config(profile("sts", region="eu-west-1")))
    try:
        assert engine.pool._recycle == 600
        assert engine.url.password is None
        assert str(engine.url) == "postgresql+psycopg://etl@db:5432/etl"
    finally:
        engine.dispose()
