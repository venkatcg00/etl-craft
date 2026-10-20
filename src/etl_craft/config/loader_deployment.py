"""The declared deployment shape: ``Deployment.Profile``."""

from __future__ import annotations

import ipaddress
from pathlib import Path

from etl_craft.config.auth import EngineSpec
from etl_craft.config.loader_helpers import mapping, reject_unknown
from etl_craft.config.model import parse_api_address
from etl_craft.config.resolve import Resolver
from etl_craft.core.enums import DeploymentProfile, enum_value
from etl_craft.core.errors import ConfigurationError

DEPLOYMENT_KEYS = frozenset({"Profile"})


def parse_deployment(
    raw: dict[str, object],
    path: Path,
    resolver: Resolver,
    *,
    engine: EngineSpec,
    api_address: str,
) -> DeploymentProfile | None:
    """Read the optional profile; ``None`` keeps sizes from the Orchestration settings.

    The section has no profile blocks: its ``Profile`` names the deployment profile, and a
    value that differs per environment comes from a variable.
    """
    section = mapping(raw, "Deployment", path, required=False)
    reject_unknown(section, DEPLOYMENT_KEYS, "Deployment", path)
    written = resolver.text(section.get("Profile"), "Deployment.Profile")
    if written is None:
        return None
    profile = enum_value(
        DeploymentProfile,
        written,
        ConfigurationError(
            f"{path}: Deployment.Profile={written!r}; expected one of "
            f"{', '.join(DeploymentProfile)}{resolver.hint('Deployment.Profile')}"
        ),
    )
    if profile == DeploymentProfile.CONTROLLER_AGENTS and not engine.shared_by_hosts:
        raise ConfigurationError(
            f"{path}: Deployment.Profile is CONTROLLER_AGENTS, but the Engine DB is "
            f"{engine.display_name}; "
            "agents share one PostgreSQL Engine DB. Use a PostgreSQL Engine profile, or choose "
            "LAPTOP or CONTROLLER"
        )
    host, _ = parse_api_address(api_address)
    if profile == DeploymentProfile.LAPTOP and not _loopback(host):
        raise ConfigurationError(
            f"{path}: Deployment.Profile is LAPTOP, but Orchestration.Api_address={api_address!r} "
            "listens beyond this machine; a laptop controller binds to loopback. Use 127.0.0.1, "
            "or choose the CONTROLLER profile"
        )
    return profile


def _loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False
