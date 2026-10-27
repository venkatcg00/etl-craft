"""The OIDC device grant against an actual local TLS issuer."""

from dataclasses import replace

import pytest

from etl_craft.core.errors import ConfigurationError
from etl_craft.services.oidc import IdentityClaims, poll_device, start_device

pytestmark = pytest.mark.unit


def test_device_sign_in_checks_tls_and_returns_only_a_verified_subject(issuer):
    auth, state = issuer
    grant = start_device(auth)
    assert grant.user_code == "ABCD-EFGH"
    assert grant.verification_uri.endswith("/verify")
    assert poll_device(auth, grant.device_code, grant.nonce) == IdentityClaims("known-subject")
    assert state["requests"][0][1]["scope"] == ["openid"]
    assert state["requests"][1][1]["grant_type"] == ["urn:ietf:params:oauth:grant-type:device_code"]
    assert state["requests"][1][1]["device_code"] == ["private-device-code"]


def test_polling_waits_while_pending_and_backs_off_when_asked(issuer):
    auth, state = issuer
    grant = start_device(auth)
    state["errors"] = ["authorization_pending", "slow_down"]
    assert poll_device(auth, grant.device_code, grant.nonce) == 0
    assert poll_device(auth, grant.device_code, grant.nonce) == 5
    assert poll_device(auth, grant.device_code, grant.nonce) == IdentityClaims("known-subject")


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


def test_configured_group_claim_is_returned_only_after_subject_validation(issuer):
    auth, state = issuer
    auth = replace(auth, oidc_groups_claim="groups")
    state["extra_claims"] = {"groups": ["sales", "unmatched"]}
    grant = start_device(auth)
    assert poll_device(auth, grant.device_code, grant.nonce) == IdentityClaims(
        "known-subject", ("sales", "unmatched")
    )
    with pytest.raises(ConfigurationError, match="nonce"):
        poll_device(auth, grant.device_code, "another-login")


@pytest.mark.parametrize("groups", [None, "sales", ["sales", 7], [""], {"sales": True}])
def test_configured_group_claim_refuses_missing_or_malformed_values(issuer, groups):
    auth, state = issuer
    auth = replace(auth, oidc_groups_claim="groups")
    state["extra_claims"] = {} if groups is None else {"groups": groups}
    grant = start_device(auth)
    with pytest.raises(ConfigurationError, match=r"groups.*expected a list of group names"):
        poll_device(auth, grant.device_code, grant.nonce)


def test_oidc_clock_leeway_and_discovery_cache(issuer, monkeypatch):
    from etl_craft.services import oidc

    auth, state = issuer
    clock = [1000.0]
    monkeypatch.setattr(oidc.time, "monotonic", lambda: clock[0])
    original = oidc._request
    discoveries = []

    def counted(url, data=None):
        if url.endswith("openid-configuration"):
            discoveries.append(url)
        return original(url, data)

    monkeypatch.setattr(oidc, "_request", counted)
    grant = start_device(auth)
    state["extra_claims"] = {
        "iat": oidc.time.time() + 119,
        "nbf": oidc.time.time() + 119,
        "exp": oidc.time.time() - 119,
    }
    assert poll_device(auth, grant.device_code, grant.nonce).subject == "known-subject"
    assert len(discoveries) == 1
    clock[0] += 601
    start_device(auth)
    assert len(discoveries) == 2
    state["extra_claims"] = {"exp": oidc.time.time() - 121}
    with pytest.raises(ConfigurationError, match="exp"):
        poll_device(auth, grant.device_code, state["nonce"])


def test_oidc_refuses_non_https_token_endpoint_and_does_not_cache_failure(issuer, monkeypatch):
    from etl_craft.services import oidc

    auth, _state = issuer
    original = oidc._request
    invalid = [True]

    def discovery(url, data=None):
        value = original(url, data)
        if url.endswith("openid-configuration") and invalid[0]:
            value["token_endpoint"] = "http://localhost/token"
        return value

    monkeypatch.setattr(oidc, "_request", discovery)
    with pytest.raises(ConfigurationError, match="HTTPS URL"):
        start_device(auth)
    assert auth.oidc_issuer not in oidc._DISCOVERY
    invalid[0] = False
    assert start_device(auth).device_code
