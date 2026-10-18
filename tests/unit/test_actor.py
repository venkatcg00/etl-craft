"""Actor identity validation and command scopes do not need database services."""

import pytest

from etl_craft.core.actor import (
    SYSTEM_ACTOR,
    Actor,
    ActorKind,
    acting_as,
    current_actor,
)
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.text import public_url_query
from etl_craft.engine.audit import mask_arguments

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("name", ["", " ", "x" * 129, "alice\nadmin", "alice\x00", "alice\u200b"])
def test_invalid_names_are_refused(name):
    with pytest.raises(ConfigurationError, match="actor name"):
        Actor(name, ActorKind.HUMAN)


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


@pytest.mark.parametrize("key", ["api_key", "pwd", "passwd", "private_key", "credential", "TOKEN"])
def test_audit_and_logged_urls_share_credential_labels(key):
    values = {key: "secret", "sslmode": "require"}
    assert public_url_query(values) == {"sslmode": "require"}
    assert mask_arguments({"nested": values}) == {
        "nested": {key: "[REDACTED]", "sslmode": "require"}
    }
