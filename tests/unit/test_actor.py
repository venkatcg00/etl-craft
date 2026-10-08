"""Actor identity validation and command scopes do not need database services."""

import pytest

from etl_craft.core.actor import (
    SYSTEM_ACTOR,
    Actor,
    ActorKind,
    acting_as,
    current_actor,
    resolve_actor,
)
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.text import public_url_query
from etl_craft.engine.audit import mask_arguments

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("name", ["", " ", "x" * 129, "alice\nadmin", "alice\x00", "alice\u200b"])
def test_invalid_names_are_refused(monkeypatch, name):
    with pytest.raises(ConfigurationError, match="ETL_CRAFT_ACTOR"):
        Actor(name, ActorKind.HUMAN)
    if "\x00" in name:
        return
    monkeypatch.setenv("ETL_CRAFT_ACTOR", name)
    with pytest.raises(ConfigurationError, match="ETL_CRAFT_ACTOR"):
        resolve_actor()


def test_local_identity_and_override(monkeypatch):
    monkeypatch.delenv("ETL_CRAFT_ACTOR", raising=False)
    monkeypatch.delenv("ETL_CRAFT_ACTOR_KIND", raising=False)
    monkeypatch.setattr("getpass.getuser", lambda: "alice")
    monkeypatch.setattr("socket.gethostname", lambda: "laptop")
    assert resolve_actor() == Actor("alice@laptop", ActorKind.HUMAN)
    monkeypatch.setenv("ETL_CRAFT_ACTOR", "airflow:run")
    monkeypatch.setenv("ETL_CRAFT_ACTOR_KIND", "ORCHESTRATOR")
    assert resolve_actor() == Actor("airflow:run", ActorKind.ORCHESTRATOR)
    monkeypatch.setenv("ETL_CRAFT_ACTOR_KIND", "unknown")
    with pytest.raises(ConfigurationError, match="ETL_CRAFT_ACTOR_KIND"):
        resolve_actor()


def test_actor_scope_restores_after_failure():
    actor = Actor("alice", ActorKind.HUMAN)
    with pytest.raises(RuntimeError), acting_as(actor):
        assert current_actor() == actor
        raise RuntimeError("stop")
    assert current_actor() == SYSTEM_ACTOR


def test_sensitive_argument_values_are_masked_recursively():
    result = mask_arguments(
        {
            "token": "secret",
            "nested": {"password": "secret", "other": "keep"},
            "handler": "callable",
        }
    )
    assert result == {"token": "[REDACTED]", "nested": {"password": "[REDACTED]", "other": "keep"}}


def test_missing_local_user_names_the_actor_remedy(monkeypatch):
    monkeypatch.delenv("ETL_CRAFT_ACTOR", raising=False)

    def missing_user():
        raise KeyError("no login")

    monkeypatch.setattr("getpass.getuser", missing_user)
    with pytest.raises(ConfigurationError, match="set ETL_CRAFT_ACTOR"):
        resolve_actor()


@pytest.mark.parametrize("key", ["api_key", "pwd", "passwd", "private_key", "credential", "TOKEN"])
def test_audit_and_logged_urls_share_credential_labels(key):
    values = {key: "secret", "sslmode": "require"}
    assert public_url_query(values) == {"sslmode": "require"}
    assert mask_arguments({"nested": values}) == {
        "nested": {key: "[REDACTED]", "sslmode": "require"}
    }
