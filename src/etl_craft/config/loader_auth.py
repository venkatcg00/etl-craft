"""Account authentication settings."""

from pathlib import Path
from urllib.parse import urlsplit

from etl_craft.config.loader_helpers import profiled_settings, reject_unknown, whole_number
from etl_craft.config.model import AuthConfig
from etl_craft.config.resolve import Resolver
from etl_craft.core.errors import ConfigurationError

AUTH_KEYS = frozenset(
    {"Session_hours", "Refresh_days", "Oidc_issuer", "Oidc_client_id", "Airflow_connection_id"}
)


def parse_auth(
    raw: dict[str, object], profile: str | None, path: Path, resolver: Resolver
) -> AuthConfig:
    """Validate login lifetimes and the optional registered OIDC client."""
    settings = profiled_settings("Auth", raw, profile, path, resolver, nested=frozenset())
    reject_unknown(settings.settings, AUTH_KEYS, "Auth", path)
    hours = whole_number(settings, "Session_hours", 12, path, minimum=1, maximum=8760)
    refresh_days = whole_number(settings, "Refresh_days", 30, path, minimum=1, maximum=365)
    issuer = settings.text("Oidc_issuer")
    client = settings.text("Oidc_client_id")
    if bool(issuer) != bool(client):
        raise ConfigurationError(f"{path}: set both Auth.Oidc_issuer and Auth.Oidc_client_id")
    if issuer:
        try:
            url = urlsplit(issuer)
        except ValueError as error:
            raise ConfigurationError(
                f"{path}: Auth.Oidc_issuer is malformed; "
                "expected a credential-free HTTPS issuer URL; correct the setting"
            ) from error
        if (
            not issuer.isprintable()
            or url.scheme != "https"
            or not url.hostname
            or url.query
            or url.fragment
            or url.username
            or url.password
        ):
            raise ConfigurationError(
                f"{path}: Auth.Oidc_issuer is invalid; expected a credential-free HTTPS issuer URL "
                "without a query or fragment; correct the setting"
            )
    return AuthConfig(
        hours, issuer, client, settings.text("Airflow_connection_id"), refresh_days=refresh_days
    )
