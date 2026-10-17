"""Configuration docs."""

from __future__ import annotations

import re
from pathlib import Path

from etl_craft.config.docs_site import cidr_ranges
from etl_craft.config.loader_helpers import profiled_settings, reject_unknown, relative_to_config
from etl_craft.config.model import DocsSiteConfig
from etl_craft.config.resolve import Resolver
from etl_craft.core.cron import parse as parse_cron
from etl_craft.core.errors import ConfigurationError, MetadataError
from etl_craft.core.text import is_env_name

DOCS_SITE_KEYS = frozenset(
    {"Schedule", "Output", "Authtoken", "Domain", "Allowed_ips", "External_url", "Trusted_proxies"}
)


def parse_docs_site(
    raw: dict[str, object], global_profile: str | None, path: Path, resolver: Resolver
) -> DocsSiteConfig:
    """Read and validate catalog publication settings for the selected profile."""
    profiled = profiled_settings(
        "Docs_site", raw, global_profile, path, resolver, nested=frozenset()
    )
    if not profiled.settings:
        return DocsSiteConfig()
    reject_unknown(
        profiled.settings,
        DOCS_SITE_KEYS,
        "Docs_site",
        path,
    )
    schedule = (profiled.text("Schedule") or "").strip() or None
    if schedule is not None:
        try:
            parse_cron(schedule)
        except MetadataError as error:
            raise ConfigurationError(
                f"{path}: {profiled.where('Schedule')} must be a valid cron expression; {error}"
            ) from error
    output = profiled.text("Output")
    authtoken = profiled.settings.get("Authtoken")
    if authtoken is not None and not (
        isinstance(authtoken, str) and is_env_name(authtoken.strip())
    ):
        raise ConfigurationError(
            f"{path}: {profiled.where('Authtoken')} must be the name of a variable holding the "
            "ngrok authtoken — a secret is never written into craft-connector.yml"
        )
    domain = (profiled.text("Domain") or "").strip().lower() or None
    if domain is not None and not re.fullmatch(r"[a-z0-9]([a-z0-9.-]*[a-z0-9])?", domain):
        raise ConfigurationError(
            f"{path}: {profiled.where('Domain')} must be a host name, such as "
            f"docs.example.com, without a scheme or path; got {domain!r}"
        )
    return DocsSiteConfig(
        schedule=schedule,
        output=relative_to_config(output, path) if output else None,
        authtoken_var=authtoken.strip() if isinstance(authtoken, str) else None,
        domain=domain,
        allowed_ips=cidr_ranges(profiled.value("Allowed_ips"), profiled.where("Allowed_ips"), path),
        external_url=profiled.text("External_url"),
        trusted_proxies=cidr_ranges(
            profiled.value("Trusted_proxies"), profiled.where("Trusted_proxies"), path
        ),
    )
