"""SMTP delivery diagnostics, subjects and verified TLS contexts."""

import logging
import smtplib
import ssl
from dataclasses import replace
from unittest.mock import MagicMock

import pytest

from etl_craft.config import (
    ConnectionProfile,
    ConnectorConfig,
    EmailProfile,
    SourceConfig,
)
from etl_craft.core.errors import HandlerError
from etl_craft.handlers import mail

pytestmark = pytest.mark.unit


def config(**kwargs):
    profile = EmailProfile("EMAIL", "dev", "smtp.example.com", 587, "etl@example.com", **kwargs)
    engine = ConnectionProfile("ENGINE", "dev", "jdbc:sqlite:e.db", "main", "none")
    return ConnectorConfig(
        mode="local",
        source=SourceConfig(type="environment"),
        engine=engine,
        email=profile,
    )


@pytest.fixture
def smtp(monkeypatch):
    relay = MagicMock()
    relay.__enter__.return_value = relay
    relay.send_message.return_value = {}
    monkeypatch.setattr(mail.smtplib, "SMTP", lambda *args, **kwargs: relay)
    return relay


def test_starttls_verifies_the_certificate_and_hostname(smtp):
    mail.send_email(config(), ["ops@example.com"], "subject", "<p>body</p>")
    context = smtp.starttls.call_args.kwargs["context"]
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname


def test_partial_recipient_refusal_is_a_warning(smtp, caplog):
    smtp.send_message.return_value = {"bad@example.com": (550, b"no such recipient")}
    with caplog.at_level(logging.WARNING):
        warning = mail.send_email(
            config(tls_mode="none"), ["bad@example.com", "ops@example.com"], "subject", "body"
        )
    assert "bad@example.com" in warning and "550" in warning
    assert "no such recipient" in warning
    assert warning in caplog.text


def test_total_recipient_refusal_fails(smtp):
    smtp.send_message.side_effect = smtplib.SMTPRecipientsRefused(
        {"bad@example.com": (550, b"no such recipient")}
    )
    with pytest.raises(HandlerError, match=r"smtp\.example\.com:587 failed at send.*550"):
        mail.send_email(config(tls_mode="none"), ["bad@example.com"], "subject", "body")


def test_a_header_error_names_the_transport(smtp):
    broken = config()
    profile = replace(broken.email, from_name="ETL\r\nBcc: attacker@example.com")
    broken = replace(broken, email=profile)
    with pytest.raises(HandlerError, match=r"smtp\.example\.com:587 failed at build headers"):
        mail.send_email(broken, ["ops@example.com"], "subject", "body")
    smtp.send_message.assert_not_called()


@pytest.mark.parametrize(
    ("written", "expected"),
    [
        ("error\r\n detail\t with\v spaces", "error detail with spaces"),
        ("error\x00\x07\x1b\x7fmessage", "errormessage"),
        ("  subject  ", "subject"),
        ("x" * 200, "x" * 200),
        ("x" * 201, "x" * 199 + "…"),
    ],
)
def test_subjects_are_one_safe_line(smtp, written, expected):
    mail.send_email(config(tls_mode="none"), ["ops@example.com"], written, "body")
    message = smtp.send_message.call_args.args[0]
    assert str(message["Subject"]) == expected
