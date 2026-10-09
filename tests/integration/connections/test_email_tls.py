"""SMTP certificate verification and implicit TLS against a real local relay."""

import ssl
from dataclasses import replace

import pytest

from etl_craft.config import EmailProfile, load_config
from etl_craft.core.errors import HandlerError
from etl_craft.execution.connections import probe_email_relay
from etl_craft.handlers.mail import send_email
from etl_craft.services.doctor import Status, run_checks

pytestmark = pytest.mark.connections


def config(tmp_path, relay, mode, *, trusted=True, host=None):
    path = tmp_path / "craft-connector.yml"
    path.write_text(
        "Secrets:\n  Source_type: environment\nOrchestration:\n  Mode: local\n"
        "Engine:\n  dev:\n    jdbc_url: jdbc:sqlite:engine.db\n    schema: main\n",
        encoding="utf-8",
    )
    profile = EmailProfile(
        "EMAIL",
        "dev",
        host or relay.host,
        relay.port,
        "etl@example.com",
        tls_mode=mode,
        ca_file=relay.ca_file if trusted else None,
    )
    return replace(load_config(path), email=profile)


@pytest.mark.parametrize("mode", ["starttls", "ssl"])
@pytest.mark.parametrize("problem", ["untrusted", "hostname"])
def test_an_untrusted_or_wrong_hostname_certificate_is_refused(tmp_path, tls_relay, mode, problem):
    relay = tls_relay(mode)
    settings = config(
        tmp_path,
        relay,
        mode,
        trusted=problem != "untrusted",
        host="127.0.0.1" if problem == "hostname" else None,
    )
    with pytest.raises(HandlerError, match="SSLCertVerificationError") as error:
        send_email(settings, ["ops@example.com"], "subject", "body")
    assert isinstance(error.value.__cause__, ssl.SSLCertVerificationError)
    assert "CERTIFICATE_VERIFY_FAILED" in probe_email_relay(settings)
    assert not relay.messages


@pytest.mark.parametrize("mode", ["starttls", "ssl"])
def test_a_matching_certificate_and_custom_ca_succeed(tmp_path, tls_relay, mode):
    relay = tls_relay(mode)
    settings = config(tmp_path, relay, mode)
    assert probe_email_relay(settings) is None
    assert send_email(settings, ["ops@example.com"], "subject", "body") is None
    assert len(relay.messages) == 1
    assert b"Subject: subject" in relay.messages[0]
    checks = {check.name: check for check in run_checks(settings, engine_state=False)}
    assert checks["Email TLS"].status == Status.OK
    assert mode in checks["Email TLS"].detail


def test_doctor_and_delivery_refuse_login_without_tls(tmp_path, tls_relay):
    relay = tls_relay()
    settings = config(tmp_path, relay, "none")
    profile = replace(settings.email, auth_mode="password", user="etl@example.com")
    settings = replace(settings, email=profile)
    checks = {check.name: check for check in run_checks(settings, engine_state=False)}
    assert checks["Email TLS"].status == Status.FAIL
    assert (
        "password" in checks["Email TLS"].detail and "starttls or ssl" in checks["Email TLS"].detail
    )
    with pytest.raises(HandlerError, match="starttls or ssl"):
        send_email(settings, ["ops@example.com"], "subject", "body")
    assert b"AUTH" not in relay.commands


def test_a_missing_ca_file_names_the_resolved_file(tmp_path, tls_relay):
    relay = tls_relay()
    settings = config(tmp_path, relay, "starttls")
    missing = tmp_path / "no-ca.pem"
    profile = replace(settings.email, ca_file=missing)
    settings = replace(settings, email=profile)
    assert str(missing) in probe_email_relay(settings)
    with pytest.raises(HandlerError, match=r"no-ca\.pem"):
        send_email(settings, ["ops@example.com"], "subject", "body")
