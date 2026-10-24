"""The optional LISTEN address changes only the PostgreSQL connection address."""

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from etl_craft.config import parse_config
from etl_craft.core.errors import ConfigurationError
from etl_craft.dialects.engine import for_name

pytestmark = pytest.mark.unit


def config(**values):
    return parse_config(
        {
            "Secrets": {"Source_type": "ENVIRONMENT"},
            "Orchestration": {},
            "Engine": {
                "dev": {
                    "Jdbc_url": "jdbc:postgresql://pooled.example/engine",
                    "Schema": "craft",
                    "User": "etl",
                    "Auth_mode": "PASSWORD",
                    "Secret": "ENGINE_PASSWORD",
                    **values,
                }
            },
        },
        Path("craft-connector.yml"),
    )


@pytest.mark.parametrize("listen", [None, "jdbc:postgresql://direct.example/engine"])
def test_listener_reuses_authentication_and_defaults_to_the_regular_url(monkeypatch, listen):
    monkeypatch.setenv("ENGINE_PASSWORD", "not-a-real-password")
    cfg = config(**({} if listen is None else {"Listen_jdbc_url": listen}))
    seen = []

    def creator(profile, secret, url):
        seen.append((profile, secret, url))
        return lambda: None

    module = "etl_craft.dialects.engine.postgres"
    monkeypatch.setattr(module + ".postgres_creator", creator)
    monkeypatch.setattr(
        module + ".NullPool", lambda **kwargs: SimpleNamespace(connect=lambda: "raw")
    )
    monkeypatch.setattr(module + ".Connection", lambda engine, connection: (engine, connection))
    engine = SimpleNamespace(dialect=SimpleNamespace(default_isolation_level="READ COMMITTED"))
    assert for_name("postgresql").session_connection(engine, cfg) == (engine, "raw")
    assert seen[0][0] == replace(cfg.engine, jdbc_url=listen or cfg.engine.jdbc_url)
    assert seen[0][0].user == "etl"
    assert seen[0][0].secret_var == "ENGINE_PASSWORD"
    assert seen[0][1] == "not-a-real-password"


def test_sqlite_refuses_a_listen_url_naming_the_setting():
    with pytest.raises(ConfigurationError, match=r"Engine.dev.Listen_jdbc_url.*remove"):
        config(
            Jdbc_url="jdbc:sqlite:engine.db",
            Schema="main",
            Auth_mode="NONE",
            Listen_jdbc_url="jdbc:postgresql://direct/engine",
        )


@pytest.mark.parametrize("url", ["jdbc:sqlite:other.db", "not-a-jdbc-url"])
def test_bad_listen_urls_name_the_setting_and_remedy(monkeypatch, url):
    monkeypatch.setenv("ENGINE_PASSWORD", "not-a-real-password")
    with pytest.raises(ConfigurationError, match=r"Listen_jdbc_url.*expected PostgreSQL"):
        config(Listen_jdbc_url=url)


def test_listener_url_credentials_are_not_echoed(monkeypatch):
    monkeypatch.setenv("ENGINE_PASSWORD", "not-a-real-password")
    with pytest.raises(ConfigurationError) as caught:
        config(Listen_jdbc_url="jdbc:postgresql://host/engine?password=must-not-appear")
    assert "must-not-appear" not in str(caught.value)
    assert "listen_jdbc_url" in str(caught.value).lower()
