"""Configuration email."""

from __future__ import annotations

from pathlib import Path

from etl_craft.config.auth import EMAIL_AUTH_FIELDS
from etl_craft.config.loader_helpers import (
    Fields,
    Profiled,
    auth_extra,
    bounded_number,
    reject_unknown,
    relative_to_config,
)
from etl_craft.config.model import DEFAULT_SENDMAIL_PATH, EmailProfile
from etl_craft.config.resolve import Resolver
from etl_craft.core.enums import AuthMode, EmailTransport, TlsMode, enum_value
from etl_craft.core.errors import ConfigurationError

EMAIL_KEYS = frozenset(
    {"host", "port", "from_address", "auth_mode", "user", "secret", "scope"}
    | {"client_id", "token_url", "transport", "sendmail_path", "from_name", "tls_mode", "ca_file"}
)

_SMTP_ONLY_EMAIL_KEYS = frozenset(
    EMAIL_KEYS - {"from_address", "from_name", "transport", "sendmail_path"}
)


def parse_email(orchestration: Profiled, path: Path, resolver: Resolver) -> EmailProfile | None:
    """Read the selected orchestration profile's optional email relay."""
    block = orchestration.settings.get("Email")
    if block is None:
        return None
    where = orchestration.where("Email")
    if not isinstance(block, dict):
        raise ConfigurationError(f"{path}: {where} must be a mapping")
    reject_unknown(block, EMAIL_KEYS, where, path)
    fields = Fields(block, where, orchestration.profile, resolver, path)
    transport = fields.value("transport") or "smtp"
    transport = enum_value(
        EmailTransport,
        transport,
        ConfigurationError(
            f"{path}: {where}.transport resolved to {transport!r}, which is not one of "
            f"{', '.join(EmailTransport)}{fields.hint('transport')}"
        ),
    )
    name = orchestration.profile or "default"
    if transport == "sendmail":
        smtp_only = sorted(set(block) & _SMTP_ONLY_EMAIL_KEYS)
        if smtp_only:
            raise ConfigurationError(
                f"{path}: {where} sends through sendmail, so {', '.join(smtp_only)} would be "
                "ignored; remove them, or set transport: smtp"
            )
        return _sendmail(fields, name, path)
    if "sendmail_path" in block:
        raise ConfigurationError(
            f"{path}: {where}.sendmail_path applies only with transport: sendmail"
        )
    port = bounded_number(
        fields.resolver.resolve(
            block.get("port"), f"{where}.port", profile=fields.profile, field_name="port"
        ),
        f"{where}.port",
        path,
        minimum=1,
        maximum=65535,
        hint=fields.hint("port"),
    )
    auth_mode = fields.value("auth_mode") or AuthMode.NONE
    if auth_mode not in EMAIL_AUTH_FIELDS:
        raise ConfigurationError(
            f"{path}: {where}.auth_mode resolved to {auth_mode!r}, which is not one of "
            f"{sorted(EMAIL_AUTH_FIELDS)}{fields.hint('auth_mode')}"
        )
    user = fields.value("user") or ""
    extra = auth_extra(fields, auth_mode, EMAIL_AUTH_FIELDS[auth_mode], user=user)
    tls_mode = fields.value("tls_mode") or "starttls"
    tls_mode = enum_value(
        TlsMode,
        tls_mode,
        ConfigurationError(
            f"{path}: {where}.tls_mode resolved to {tls_mode!r}, which is not one of "
            f"none, starttls, ssl{fields.hint('tls_mode')}"
        ),
    )
    ca_file = fields.value("ca_file")
    if ca_file and tls_mode == "none":
        raise ConfigurationError(
            f"{path}: {where}.ca_file={ca_file!r} would be ignored with tls_mode=none; "
            "set tls_mode to starttls or ssl, or remove ca_file"
        )
    return EmailProfile(
        section="EMAIL",
        name=name,
        host=fields.value("host", required=True) or "",
        port=port,
        from_address=fields.value("from_address", required=True) or "",
        auth_mode=auth_mode,
        user=user or None,
        extra=extra,
        from_name=fields.value("from_name") or "",
        tls_mode=tls_mode,
        ca_file=relative_to_config(ca_file, path) if ca_file else None,
    )


def _sendmail(fields: Fields, name: str, path: Path) -> EmailProfile:
    return EmailProfile(
        section="EMAIL",
        name=name,
        host="",
        port=0,
        from_address=fields.value("from_address", required=True) or "",
        from_name=fields.value("from_name") or "",
        transport="sendmail",
        sendmail_path=str(
            relative_to_config(fields.value("sendmail_path") or DEFAULT_SENDMAIL_PATH, path)
        ),
    )
