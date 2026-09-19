"""Tests for etl_craft.db — JDBC parsing and auth_mode wiring, no live DB required."""

import pytest

from etl_craft.config import ConnectionProfile
from etl_craft.db import AUTH_REGISTRY, ConnectionError_, parse_jdbc_postgres


def test_parse_jdbc_postgres_with_explicit_port():
    parts = parse_jdbc_postgres("jdbc:postgresql://myhost:6543/mydb")
    assert parts == {"host": "myhost", "port": 6543, "database": "mydb"}


def test_parse_jdbc_postgres_default_port():
    parts = parse_jdbc_postgres("jdbc:postgresql://myhost/mydb")
    assert parts == {"host": "myhost", "port": 5432, "database": "mydb"}


def test_parse_jdbc_postgres_rejects_non_jdbc_url():
    with pytest.raises(ConnectionError_):
        parse_jdbc_postgres("postgresql://myhost:5432/mydb")


def profile(auth_mode: str, **extra) -> ConnectionProfile:
    return ConnectionProfile(
        section="POSTGRES",
        name="dev",
        jdbc_url="jdbc:postgresql://localhost:5432/etl_craft",
        user="etl_engine",
        auth_mode=auth_mode,
        extra=extra,
    )


def test_password_creator_returns_callable_that_calls_psycopg_connect(monkeypatch):
    calls = {}

    class FakeConnection:
        pass

    def fake_connect(**kwargs):
        calls.update(kwargs)
        return FakeConnection()

    import psycopg

    monkeypatch.setattr(psycopg, "connect", fake_connect)

    creator = AUTH_REGISTRY["password"](profile("password"), "s3cr3t")
    conn = creator()
    assert isinstance(conn, FakeConnection)
    assert calls == {
        "host": "localhost",
        "port": 5432,
        "dbname": "etl_craft",
        "user": "etl_engine",
        "password": "s3cr3t",
    }


def test_key_file_creator_requires_key_file_in_extra():
    with pytest.raises(ConnectionError_):
        AUTH_REGISTRY["key_file"](profile("key_file"), "passphrase")


def test_key_file_creator_returns_callable_using_sslkey(monkeypatch):
    calls = {}

    class FakeConnection:
        pass

    def fake_connect(**kwargs):
        calls.update(kwargs)
        return FakeConnection()

    import psycopg

    monkeypatch.setattr(psycopg, "connect", fake_connect)

    creator = AUTH_REGISTRY["key_file"](profile("key_file", key_file="/etc/key.pem"), "passphrase")
    creator()
    assert calls["sslkey"] == "/etc/key.pem"
    assert calls["sslpassword"] == b"passphrase"


def test_token_and_sso_creators_are_not_implemented():
    with pytest.raises(NotImplementedError):
        AUTH_REGISTRY["token"](profile("token"), "unused")
    with pytest.raises(NotImplementedError):
        AUTH_REGISTRY["sso"](profile("sso"), "unused")
