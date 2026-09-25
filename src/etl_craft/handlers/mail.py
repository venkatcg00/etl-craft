"""Sending email as the ``Email`` settings of ``craft-connector.yml`` say.

Every email is HTML, sent from the profile's ``from_address`` under its ``from_name``, when set,
in one of two ways:

- ``transport: smtp`` (the default): through the SMTP relay at ``host:port``, with STARTTLS when
  ``use_tls`` is on, logged in to by ``auth_mode``: ``none``, ``password``, or ``oauth`` (SMTP
  XOAUTH2 with a client-credentials token, for relays that refuse passwords);
- ``transport: sendmail``: handed to the host's ``sendmail`` program (``sendmail_path``), the
  one ``mailx`` and ``mail`` use, which delivers it through the host's own mail system.

A failure names the relay or program and what went wrong.
"""

from __future__ import annotations

import html
import logging
import os
import smtplib
import subprocess
from collections.abc import Sequence
from email.message import EmailMessage
from email.utils import formataddr, parseaddr
from pathlib import Path

from etl_craft.config import ConnectorConfig, resolve_secret
from etl_craft.config.model import EmailProfile
from etl_craft.core.enums import AuthMode
from etl_craft.core.errors import EtlCraftError, HandlerError
from etl_craft.dialects import credentials

logger = logging.getLogger(__name__)

SMTP_TIMEOUT_SECONDS = 30.0
"""How long the relay may take to answer each step."""

STYLE = (
    "body{font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;color:#1f2328}"
    "table{border-collapse:collapse;margin:0.5em 0}"
    "th,td{border:1px solid #d0d7de;padding:4px 10px;text-align:left}"
    "details{margin:0.4em 0}"
    "summary{cursor:pointer;font-weight:600}"
)


def parse_recipients(value: str | None, setting: str) -> list[str]:
    """Split a ``|``-separated address list; ``HandlerError`` naming a malformed address."""
    addresses = [part.strip() for part in (value or "").split("|") if part.strip()]
    if not addresses:
        raise HandlerError(f"{setting} is required: one or more addresses separated by '|'")
    bad = [a for a in addresses if "@" not in parseaddr(a)[1] or " " in parseaddr(a)[1]]
    if bad:
        raise HandlerError(f"{setting} has address(es) that are not valid: {', '.join(bad)}")
    return addresses


def page(body: str) -> str:
    """Wrap an HTML fragment in the page every email uses."""
    return f"<html><head><style>{STYLE}</style></head><body>{body}</body></html>"


def paragraph(text: str) -> str:
    """Return ``text`` as an escaped HTML paragraph."""
    return f"<p>{html.escape(text)}</p>"


def send_email(
    config: ConnectorConfig, recipients: Sequence[str], subject: str, body_html: str
) -> None:
    """Send one HTML email; ``HandlerError`` naming the relay when it cannot be sent."""
    if config.email is None:
        raise HandlerError(
            "no Email settings in craft-connector.yml; sending email needs Orchestration's Email "
            "block with the relay's host, port and from address"
        )
    profile = config.email.active
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = formataddr((profile.from_name, profile.from_address))
    message["To"] = ", ".join(recipients)
    message.set_content("This email is HTML; open it in a client that shows HTML.")
    message.add_alternative(body_html, subtype="html")
    if profile.transport == "sendmail":
        _send_with_sendmail(profile, message, recipients)
        logger.info(
            "sent %r to %d recipient(s) through %s",
            subject,
            len(recipients),
            profile.sendmail_path,
        )
        return
    relay = f"{profile.host}:{profile.port}"
    step = "connect"
    try:
        with smtplib.SMTP(profile.host, profile.port, timeout=SMTP_TIMEOUT_SECONDS) as server:
            if profile.use_tls:
                step = "start TLS"
                server.starttls()
            step = "log in"
            _log_in(server, config, profile)
            step = "send"
            refused = server.send_message(message, to_addrs=list(recipients))
    except (smtplib.SMTPException, OSError, EtlCraftError) as error:
        raise HandlerError(
            f"email to {', '.join(recipients)} was not sent: the relay {relay} failed at "
            f"{step}: {type(error).__name__}: {error}"
        ) from error
    if refused:
        raise HandlerError(
            f"the relay {relay} refused recipient(s) {', '.join(sorted(refused))}; the others "
            "were sent the email"
        )
    logger.info("sent %r to %d recipient(s) through %s", subject, len(recipients), relay)


def sendmail_problem(profile: EmailProfile) -> str | None:
    """Return why the profile's ``sendmail`` program cannot be run, or ``None``."""
    program = Path(profile.sendmail_path)
    if not program.is_file():
        return f"the sendmail program {program} does not exist; install one, or set sendmail_path"
    if not os.access(program, os.X_OK):
        return f"the sendmail program {program} is not executable"
    return None


def _send_with_sendmail(
    profile: EmailProfile, message: EmailMessage, recipients: Sequence[str]
) -> None:
    problem = sendmail_problem(profile)
    if problem is not None:
        raise HandlerError(f"email to {', '.join(recipients)} was not sent: {problem}")
    command = [profile.sendmail_path, "-t", "-oi", "-f", profile.from_address]
    if profile.from_name:
        command += ["-F", profile.from_name]
    try:
        finished = subprocess.run(
            command,
            input=message.as_bytes(),
            capture_output=True,
            timeout=SMTP_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise HandlerError(
            f"email to {', '.join(recipients)} was not sent: {profile.sendmail_path} failed: "
            f"{type(error).__name__}: {error}"
        ) from error
    if finished.returncode != 0:
        output = (finished.stderr or finished.stdout).decode("utf-8", "replace").strip()
        raise HandlerError(
            f"email to {', '.join(recipients)} was not sent: {profile.sendmail_path} exited "
            f"{finished.returncode}: {output or '(no output)'}"
        )


def _log_in(server: smtplib.SMTP, config: ConnectorConfig, profile: EmailProfile) -> None:
    if profile.auth_mode == AuthMode.NONE:
        return
    if not profile.user:
        raise HandlerError(
            f"Email profile {profile.name!r} has auth_mode {profile.auth_mode} but no user"
        )
    secret = resolve_secret(config, profile)
    if profile.auth_mode == AuthMode.PASSWORD:
        server.login(profile.user, secret)
        return
    token = credentials.client_credentials_token(
        str(profile.extra["token_url"]),
        str(profile.extra["client_id"]),
        secret,
        profile.extra.get("scope"),
    )
    server.ehlo_or_helo_if_needed()
    xoauth2 = f"user={profile.user}\x01auth=Bearer {token}\x01\x01"
    server.auth("XOAUTH2", lambda challenge=None: xoauth2, initial_response_ok=True)


def sla_recipients(config: ConnectorConfig, pipeline_recipients: Sequence[str] | None) -> list[str]:
    """Return who hears about a pipeline's SLA: its ``EMAIL_RECIPIENTS``, else the DAG default.

    ``HandlerError`` when neither is set, naming both places.
    """
    recipients = list(pipeline_recipients or config.dag_defaults.email_recipients or [])
    if not recipients:
        raise HandlerError(
            "no one to send the SLA email to: set EMAIL_RECIPIENTS in the pipeline's "
            "PIPELINE_PARAMETERS, or the email recipients in Orchestration's DAG defaults"
        )
    return parse_recipients("|".join(recipients), "the SLA email recipients")


def send_sla_lapse_email(
    config: ConnectorConfig,
    pipeline_recipients: Sequence[str] | None,
    *,
    pipeline_code: str,
    pipeline_run_id: int,
    sla_hours: float,
    elapsed_hours: float,
) -> None:
    """Email that a run is past its pipeline's SLA; only when ``Enforce_sla`` is on."""
    if not config.limits.enforce_sla:
        return
    recipients = sla_recipients(config, pipeline_recipients)
    subject = f"[etl-craft] {pipeline_code}: SLA of {sla_hours:g} h missed"
    body = (
        f'<p style="color:#cf222e;font-weight:600">{html.escape(pipeline_code)}: SLA missed</p>'
        + paragraph(
            f"pipeline_run_id={pipeline_run_id} has been running {elapsed_hours:.2f} h, past "
            f"its SLA of {sla_hours:g} h."
        )
    )
    send_email(config, recipients, subject, page(body))
