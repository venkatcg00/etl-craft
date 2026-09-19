"""Load and validate craft-connector.yml, the engine's only source of connection config."""

# Per CLAUDE.md: no secrets are ever stored in this file directly, and every
# connection — including the Engine DB, "even on the Airflow side" — resolves
# through it, never through an orchestrator's own connection store.
#
# [CHOICE] CLAUDE.md's own craft-connector.yml example renders section names
# in bracketed INI-style headers (`[Execution]`, `[Postgres]`, ...) inside a
# yaml code fence — that's illustrative grouping, not literal YAML syntax (a
# bare `[Execution]` line isn't a valid YAML top-level construct). This
# loader instead expects those as plain nested top-level mapping keys
# (`Execution:`, `Postgres:`, ...), which is the direct, literal YAML
# rendering of the same structure. Flagging since this is an interpretive
# translation, not something CLAUDE.md pins down byte-for-byte.
#
# [ADDITION] Per-profile secret material (the password/token/passphrase an
# auth_mode needs) is looked up via an env var name, resolved through the
# [Source] section's file-or-environment mechanism. CLAUDE.md establishes
# *that* secrets live outside this file but doesn't name the lookup
# convention, so: a profile may set `secret_var: SOME_NAME` explicitly;
# otherwise it defaults to `ETL_CRAFT_{SECTION}_{PROFILE}_SECRET` (e.g.
# `ETL_CRAFT_POSTGRES_DEV_SECRET`). Confirm this is the right convention
# before other tooling (e.g. `configure`) starts writing profiles that rely
# on it.
#
# [CHOICE] CLAUDE.md open question #1 (Data DB section name — "Data Db"/
# "[Data Db 1]" explicitly rejected as too Informatica-shaped, no
# replacement settled) is resolved here as `Warehouse`: it's the term
# CLAUDE.md itself already uses throughout ("Data DB / warehouse"), reads
# as a plain noun rather than a product-shaped label, and is a one-line
# rename in _parse_config below if a different name is preferred. Unlike
# [Postgres], [Warehouse] is optional at parse time — `list`/`graph`/
# `set-execution-mode`/`configure`/`run --init-only` and friends never
# touch the Data DB, so a file without one still loads; anything that
# genuinely needs a Data DB connection (warehouse.build_data_engine, the
# not-yet-built `validate`/cloning) raises its own clear error if absent.

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path("craft-connector.yml")

VALID_MODES = frozenset({"local", "orchestrator"})
VALID_SOURCE_TYPES = frozenset({"file", "environment"})
VALID_AUTH_MODES = frozenset({"password", "token", "sso", "key_file"})
VALID_CLONING_SCOPES = frozenset({"cfg", "aud", "all"})


class ConfigError(Exception):
    """Raised when craft-connector.yml is missing, malformed, or fails validation."""


@dataclass(frozen=True)
class ConnectionProfile:
    """One named environment profile (dev/sit/uat/prod) under a connection section."""

    section: str
    name: str
    jdbc_url: str
    user: str
    auth_mode: str
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def secret_var(self) -> str:
        """The env var name holding this profile's secret material."""
        override = self.extra.get("secret_var")
        if override:
            return str(override)
        return f"ETL_CRAFT_{self.section}_{self.name}_SECRET".upper()


@dataclass(frozen=True)
class ConnectionSection:
    """An [Postgres]-shaped block: an active profile name plus all named profiles."""

    active_profile: str
    profiles: dict[str, ConnectionProfile]

    @property
    def active(self) -> ConnectionProfile:
        """Return the profile currently selected by active_profile."""
        return self.profiles[self.active_profile]


@dataclass(frozen=True)
class SourceConfig:
    """The [Source] section: where secret values referenced elsewhere live."""

    type: str
    path: str | None = None


@dataclass(frozen=True)
class CloningConfig:
    """The [Cloning] section: merge-style copy of Engine DB tables into the Data DB."""

    enabled: bool = False
    scope: str = "cfg"


@dataclass(frozen=True)
class ConnectorConfig:
    """The fully parsed, validated contents of craft-connector.yml."""

    mode: str
    source: SourceConfig
    postgres: ConnectionSection
    cloning: CloningConfig
    warehouse: ConnectionSection | None = None


def load_config(path: Path | str = DEFAULT_CONFIG_PATH) -> ConnectorConfig:
    """Read, parse, and validate craft-connector.yml at `path`."""
    path = Path(path)
    if not path.is_file():
        raise ConfigError(f"craft-connector.yml not found at {path}")
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path} is not valid YAML: {exc}") from exc
    return _parse_config(raw, path)


def _parse_config(raw: dict[str, Any], path: Path) -> ConnectorConfig:
    execution = _require_section(raw, "Execution", path)
    mode = execution.get("Mode")
    if mode not in VALID_MODES:
        raise ConfigError(
            f"{path}: Execution.Mode must be one of {sorted(VALID_MODES)}, got {mode!r}"
        )

    source_raw = _require_section(raw, "Source", path)
    source = _parse_source(source_raw, path)

    postgres_raw = _require_section(raw, "Postgres", path)
    postgres = _parse_connection_section("POSTGRES", postgres_raw, path)

    warehouse_raw = raw.get("Warehouse")
    if warehouse_raw is None:
        warehouse = None
    elif not isinstance(warehouse_raw, dict):
        raise ConfigError(f"{path}: Warehouse section must be a mapping if present")
    else:
        warehouse = _parse_connection_section("WAREHOUSE", warehouse_raw, path)

    cloning = _parse_cloning(raw.get("Cloning") or {}, path)

    return ConnectorConfig(
        mode=mode, source=source, postgres=postgres, cloning=cloning, warehouse=warehouse
    )


def _require_section(raw: dict[str, Any], name: str, path: Path) -> dict[str, Any]:
    section = raw.get(name)
    if not isinstance(section, dict):
        raise ConfigError(f"{path}: missing or invalid {name!r} section")
    return section


def _parse_source(raw: dict[str, Any], path: Path) -> SourceConfig:
    source_type = raw.get("Type")
    if source_type not in VALID_SOURCE_TYPES:
        raise ConfigError(
            f"{path}: Source.Type must be one of {sorted(VALID_SOURCE_TYPES)}, got {source_type!r}"
        )
    source_path = raw.get("Path")
    if source_type == "file" and not source_path:
        raise ConfigError(f"{path}: Source.Path is required when Source.Type == 'file'")
    return SourceConfig(type=source_type, path=source_path)


def _parse_connection_section(
    section_name: str, raw: dict[str, Any], path: Path
) -> ConnectionSection:
    active_profile = raw.get("Active_profile")
    profiles_raw = raw.get("Profiles")
    if not active_profile or not isinstance(profiles_raw, dict):
        raise ConfigError(f"{path}: {section_name} needs Active_profile and a Profiles mapping")

    profiles: dict[str, ConnectionProfile] = {}
    for profile_name, profile_raw in profiles_raw.items():
        profiles[profile_name] = _parse_profile(section_name, profile_name, profile_raw or {}, path)

    if active_profile not in profiles:
        raise ConfigError(
            f"{path}: {section_name}.Active_profile {active_profile!r} has no matching "
            "entry in Profiles"
        )
    return ConnectionSection(active_profile=active_profile, profiles=profiles)


def _parse_profile(
    section_name: str, profile_name: str, raw: dict[str, Any], path: Path
) -> ConnectionProfile:
    jdbc_url = raw.get("jdbc_url")
    user = raw.get("user")
    auth_mode = raw.get("auth_mode")
    if not jdbc_url or not user or auth_mode not in VALID_AUTH_MODES:
        raise ConfigError(
            f"{path}: {section_name}.Profiles.{profile_name} needs jdbc_url, user, "
            f"and auth_mode in {sorted(VALID_AUTH_MODES)}"
        )
    extra = {k: v for k, v in raw.items() if k not in {"jdbc_url", "user", "auth_mode"}}
    return ConnectionProfile(
        section=section_name,
        name=profile_name,
        jdbc_url=jdbc_url,
        user=user,
        auth_mode=auth_mode,
        extra=extra,
    )


def _parse_cloning(raw: dict[str, Any], path: Path) -> CloningConfig:
    if not raw:
        return CloningConfig()
    enabled = bool(raw.get("Enabled", False))
    scope = raw.get("Scope", "cfg")
    if scope not in VALID_CLONING_SCOPES:
        raise ConfigError(
            f"{path}: Cloning.Scope must be one of {sorted(VALID_CLONING_SCOPES)}, got {scope!r}"
        )
    return CloningConfig(enabled=enabled, scope=scope)


def resolve_secret(config: ConnectorConfig, profile: ConnectionProfile) -> str:
    """Resolve `profile`'s secret material via the [Source] section."""
    var_name = profile.secret_var
    if config.source.type == "environment":
        value = os.environ.get(var_name)
    else:
        value = _load_dotenv_file(config.source.path).get(var_name)
    if value is None:
        raise ConfigError(f"secret {var_name!r} not found (Source.Type={config.source.type!r})")
    return value


def _load_dotenv_file(path: str | None) -> dict[str, str]:
    """Parse a minimal .env-style file: KEY=VALUE per line, '#' comments, blank lines ignored."""
    if not path:
        raise ConfigError("Source.Path is required when Source.Type == 'file'")
    values: dict[str, str] = {}
    for line in Path(path).read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        values[key.strip()] = value.strip().strip("'\"")
    return values
