"""The OIDC device grant against an actual local TLS issuer."""

import pytest

from etl_craft.core.errors import ConfigurationError
from etl_craft.services.oidc import device_subject

pytestmark = pytest.mark.unit


def test_device_sign_in_checks_tls_and_returns_only_a_verified_subject(issuer):
    auth, state = issuer
    instructions = []
    assert device_subject(auth, instructions.append) == "known-subject"
    assert "ABCD-EFGH" in instructions[0]
    assert "private-device-code" not in instructions[0]
    assert state["requests"][0][1]["scope"] == ["openid"]
    assert state["requests"][1][1]["grant_type"] == ["urn:ietf:params:oauth:grant-type:device_code"]


def test_discovery_cannot_substitute_another_issuer_or_redirect(issuer):
    auth, state = issuer
    state["wrong_issuer"] = True
    with pytest.raises(ConfigurationError, match="discovery issuer"):
        device_subject(auth, lambda _: None)
    state["redirect"] = True
    with pytest.raises(ConfigurationError, match="HTTP 302"):
        device_subject(auth, lambda _: None)


def test_tls_verification_is_mandatory(issuer, monkeypatch):
    auth, _ = issuer
    monkeypatch.delenv("SSL_CERT_FILE")
    with pytest.raises(ConfigurationError, match="connectivity and TLS"):
        device_subject(auth, lambda _: None)
