"""Explicit managed PostgreSQL selection and connection requirements."""

import re
from urllib.parse import urlsplit

from etl_craft.config.model import CloningConfig, ConnectionProfile
from etl_craft.core.enums import AuthMode, ManagedProvider
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.text import mask_arguments, parse_jdbc_url

MANAGED_FIELDS = frozenset({"managed_provider", "account", "instance", "workspace_url", "endpoint"})


def validate_managed_engine(profile: ConnectionProfile) -> None:
    """Reject incomplete provider contracts before any credential exchange or connection."""
    where = f"Engine.{profile.name}"
    value = profile.extra.get("managed_provider", ManagedProvider.NONE)
    if value not in tuple(ManagedProvider):
        raise ConfigurationError(
            f"{where}.Managed_provider={value!r}; expected NONE, SNOWFLAKE or DATABRICKS; "
            "set Managed_provider to the Engine DB platform"
        )
    provider = profile.managed_provider
    required = {
        ManagedProvider.NONE: (),
        ManagedProvider.SNOWFLAKE: ("account", "instance"),
        ManagedProvider.DATABRICKS: ("workspace_url", "endpoint"),
    }[provider]
    for key in MANAGED_FIELDS - {"managed_provider"}:
        value = profile.extra.get(key)
        if (key in required and not value) or (key not in required and value):
            raise ConfigurationError(
                f"{where}.{key}={value!r}; expected {'a value' if key in required else 'absent'} "
                f"for Managed_provider={provider}; correct the provider fields"
            )
    if provider == ManagedProvider.NONE:
        return
    if profile.auth_mode != AuthMode.OAUTH:
        raise ConfigurationError(
            f"{where}.Auth_mode={profile.auth_mode!r}; expected OAUTH for {provider}; "
            "configure Client_id, Secret and Token_url for the issuing platform"
        )
    for address in (profile.jdbc_url, profile.listen_jdbc_url):
        if address is None:
            continue
        url = parse_jdbc_url(address)
        mode = url.query.get("sslmode")
        if url.scheme != "postgresql" or mode not in {"require", "verify-ca", "verify-full"}:
            raise ConfigurationError(
                f"{where}: managed PostgreSQL sslmode={mode!r}; expected TLS on Jdbc_url and "
                "Listen_jdbc_url; use jdbc:postgresql://host/database?sslmode=verify-full"
            )
    _validate_issuer(profile)


def _validate_issuer(profile: ConnectionProfile) -> None:
    """Validate credential destinations and the selected Lakebase resource shape."""
    where = f"Engine.{profile.name}"
    provider = profile.managed_provider
    for key in (
        "token_url",
        *(("workspace_url",) if provider == ManagedProvider.DATABRICKS else ()),
    ):
        value = str(profile.extra.get(key, ""))
        try:
            endpoint_url = urlsplit(value)
            valid = (
                endpoint_url.scheme == "https"
                and bool(endpoint_url.hostname)
                and not endpoint_url.username
                and not endpoint_url.password
                and not endpoint_url.query
                and not endpoint_url.fragment
                and (endpoint_url.port is None or endpoint_url.port > 0)
                and not any(character.isspace() or ord(character) < 32 for character in value)
            )
        except ValueError:
            valid = False
        if not valid:
            shown_address = mask_arguments({"url": value})["url"]
            raise ConfigurationError(
                f"{where}.{key.capitalize()}={shown_address!r}; "
                "expected a credential-free HTTPS URL; "
                f"correct {key.capitalize()}"
            )
    if provider == ManagedProvider.DATABRICKS:
        workspace = str(profile.extra["workspace_url"])
        if urlsplit(workspace).path not in {"", "/"} or not re.fullmatch(
            r"projects/[^/]+/branches/[^/]+/endpoints/[^/]+", str(profile.extra["endpoint"])
        ):
            raise ConfigurationError(
                f"{where}: Workspace_url={workspace!r}, Endpoint={profile.extra['endpoint']!r}; "
                "expected Workspace_url at the HTTPS origin and Endpoint in "
                "projects/<project>/branches/<branch>/endpoints/<endpoint> form; "
                "copy them from Lakebase connection details"
            )


def managed_cloning_problem(profile: ConnectionProfile, cloning: CloningConfig) -> str | None:
    """Explain the managed-platform cloning refusal to the loader and doctor."""
    if profile.managed_provider != ManagedProvider.NONE and cloning.enabled:
        return (
            f"Cloning.Enabled=True with managed Engine DB {profile.managed_provider}; "
            "expected cloning disabled because audit tables are platform-accessible; "
            "set Cloning.Enabled=False"
        )
    return None
