"""The OIDC device grant against an actual local TLS issuer."""

import pytest

from etl_craft.core.errors import ConfigurationError
from etl_craft.services.oidc import poll_device, start_device

pytestmark = pytest.mark.unit


def test_device_sign_in_checks_tls_and_returns_only_a_verified_subject(issuer):
    auth, state = issuer
    grant = start_device(auth)
    assert grant.user_code == "ABCD-EFGH"
    assert grant.verification_uri.endswith("/verify")
    assert poll_device(auth, grant.device_code, grant.nonce) == "known-subject"
    assert state["requests"][0][1]["scope"] == ["openid"]
    assert state["requests"][1][1]["grant_type"] == ["urn:ietf:params:oauth:grant-type:device_code"]
    assert state["requests"][1][1]["device_code"] == ["private-device-code"]


def test_polling_waits_while_pending_and_backs_off_when_asked(issuer):
    auth, state = issuer
    grant = start_device(auth)
    state["errors"] = ["authorization_pending", "slow_down"]
    assert poll_device(auth, grant.device_code, grant.nonce) == 0
    assert poll_device(auth, grant.device_code, grant.nonce) == 5
    assert poll_device(auth, grant.device_code, grant.nonce) == "known-subject"


def test_a_refused_or_expired_code_fails_and_a_foreign_nonce_is_rejected(issuer):
    auth, state = issuer
    grant = start_device(auth)
    state["errors"] = ["access_denied"]
    with pytest.raises(ConfigurationError, match="refused or expired"):
        poll_device(auth, grant.device_code, grant.nonce)
    with pytest.raises(ConfigurationError, match="nonce: value differs"):
        poll_device(auth, grant.device_code, "another-sign-in")


def test_discovery_cannot_substitute_another_issuer_or_redirect(issuer):
    auth, state = issuer
    state["wrong_issuer"] = True
    with pytest.raises(ConfigurationError, match="discovery issuer"):
        start_device(auth)
    state["redirect"] = True
    with pytest.raises(ConfigurationError, match="HTTP 302"):
        start_device(auth)


def test_tls_verification_is_mandatory(issuer, monkeypatch):
    auth, _ = issuer
    monkeypatch.delenv("SSL_CERT_FILE")
    with pytest.raises(ConfigurationError, match="connectivity and TLS"):
        start_device(auth)
