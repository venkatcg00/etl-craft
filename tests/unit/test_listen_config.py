"""The optional LISTEN address changes only the PostgreSQL connection address."""

from dataclasses import replace
from pathlib import Path

import pytest

from etl_craft.config import parse_config
from etl_craft.core.errors import ConfigurationError
from etl_craft.engine.notifications import listener_engine

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

    def build(actual_config, profile, **kwargs):
        seen.append((actual_config, profile, kwargs))
        return "not-connected"

    monkeypatch.setattr("etl_craft.engine.notifications.build_engine", build)
    assert listener_engine(cfg) == "not-connected"
    assert seen[0][0] is cfg
    assert seen[0][1] == replace(cfg.engine, jdbc_url=listen or cfg.engine.jdbc_url)
    assert seen[0][1].user == "etl"
    assert seen[0][1].secret_var == "ENGINE_PASSWORD"


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
