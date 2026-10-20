"""Configuration cloning."""

from __future__ import annotations

from pathlib import Path

from etl_craft.config.loader_helpers import parse_bool, profiled_settings, reject_unknown
from etl_craft.config.model import CloningConfig
from etl_craft.config.resolve import Resolver
from etl_craft.core.enums import CloningScope, enum_value
from etl_craft.core.errors import ConfigurationError

CLONING_KEYS = frozenset({"Enabled", "Scope", "External_volume", "Base_location"})


def parse_cloning(
    raw: dict[str, object], global_profile: str | None, path: Path, resolver: Resolver
) -> CloningConfig:
    """Read the optional cloning section for the selected profile."""
    profiled = profiled_settings("Cloning", raw, global_profile, path, resolver, nested=frozenset())
    if not profiled.settings:
        return CloningConfig()
    reject_unknown(
        profiled.settings,
        CLONING_KEYS,
        "Cloning",
        path,
    )
    scope = profiled.text("Scope") or CloningScope.CFG
    scope = enum_value(
        CloningScope,
        scope,
        ConfigurationError(
            f"{path}: Cloning.Scope must be one of {', '.join(CloningScope)}, got {scope!r}"
        ),
    )
    enabled = parse_bool(profiled.value("Enabled"), profiled.where("Enabled"), path, default=False)
    return CloningConfig(
        enabled=enabled and scope != CloningScope.NONE,
        scope=scope,
        external_volume=profiled.text("External_volume") or "",
        base_location=profiled.text("Base_location") or "",
    )
