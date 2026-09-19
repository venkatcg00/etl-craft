"""Write craft-connector.yml — `set-execution-mode` and `configure --env`."""

# config.py only ever reads craft-connector.yml; this module is the only
# place that writes it. Per the sign-off on how these writes should behave:
# always a full parse + re-serialize (via PyYAML), never a surgical text
# edit. Simple and robust, but note the real cost: every write touches the
# whole file, not just the changed field — comments and exact formatting in
# a hand-edited craft-connector.yml don't survive a `set-execution-mode` or
# `configure --env` call. Worth knowing before either command runs against
# a file a human has been curating by hand.
#
# [ADDITION] `configure --env`'s env-var names (ETL_CRAFT_MODE,
# ETL_CRAFT_POSTGRES_JDBC_URL, ...) aren't specified anywhere in CLAUDE.md —
# only that the command exists and is "non-interactive setup from an env
# file". Confirm this naming scheme before it's relied on elsewhere (e.g. a
# generated onboarding doc, a reference-implementation repo's env file).
#
# [CHOICE] Re-running `configure --env` against an existing
# craft-connector.yml merges: it adds/updates just the one Postgres profile
# named in the env file (preserving any other profiles already there, e.g.
# from a prior run against a different environment) and sets it as
# Active_profile, but wholesale-replaces Execution/Source/Cloning, since
# those are singular/global rather than per-profile. Not specified in
# CLAUDE.md; this is the interpretation that makes "configure once per
# environment, reuse the same file" actually work.

from __future__ import annotations

from pathlib import Path

import yaml

from etl_craft.config import (
    DEFAULT_CONFIG_PATH,
    VALID_AUTH_MODES,
    VALID_CLONING_SCOPES,
    VALID_MODES,
    VALID_SOURCE_TYPES,
    ConfigError,
    _load_dotenv_file,
)


def set_execution_mode(mode: str, path: Path | str = DEFAULT_CONFIG_PATH) -> None:
    """Update Execution.Mode in an existing craft-connector.yml, leaving everything else as-is."""
    if mode not in VALID_MODES:
        raise ConfigError(f"mode must be one of {sorted(VALID_MODES)}, got {mode!r}")
    path = Path(path)
    raw = _read_raw_yaml(path)
    if not isinstance(raw.get("Execution"), dict):
        raise ConfigError(f"{path}: missing or invalid 'Execution' section — run `configure` first")
    raw["Execution"]["Mode"] = mode
    _write_raw_yaml(path, raw)


def configure_from_env(env_path: Path | str, path: Path | str = DEFAULT_CONFIG_PATH) -> None:
    """Build (or update) craft-connector.yml from an env file, non-interactively."""
    env_path = Path(env_path)
    path = Path(path)
    if not env_path.is_file():
        raise ConfigError(f"env file not found at {env_path}")
    values = _load_dotenv_file(str(env_path))

    def require(key: str) -> str:
        value = values.get(key)
        if not value:
            raise ConfigError(f"{env_path}: {key} is required")
        return value

    mode = require("ETL_CRAFT_MODE")
    if mode not in VALID_MODES:
        raise ConfigError(
            f"{env_path}: ETL_CRAFT_MODE must be one of {sorted(VALID_MODES)}, got {mode!r}"
        )

    source_type = require("ETL_CRAFT_SOURCE_TYPE")
    if source_type not in VALID_SOURCE_TYPES:
        raise ConfigError(
            f"{env_path}: ETL_CRAFT_SOURCE_TYPE must be one of "
            f"{sorted(VALID_SOURCE_TYPES)}, got {source_type!r}"
        )
    source_path = values.get("ETL_CRAFT_SOURCE_PATH")
    if source_type == "file" and not source_path:
        raise ConfigError(
            f"{env_path}: ETL_CRAFT_SOURCE_PATH is required when ETL_CRAFT_SOURCE_TYPE=file"
        )

    profile_name = require("ETL_CRAFT_POSTGRES_PROFILE")
    jdbc_url = require("ETL_CRAFT_POSTGRES_JDBC_URL")
    user = require("ETL_CRAFT_POSTGRES_USER")
    auth_mode = require("ETL_CRAFT_POSTGRES_AUTH_MODE")
    if auth_mode not in VALID_AUTH_MODES:
        raise ConfigError(
            f"{env_path}: ETL_CRAFT_POSTGRES_AUTH_MODE must be one of "
            f"{sorted(VALID_AUTH_MODES)}, got {auth_mode!r}"
        )

    cloning_scope = values.get("ETL_CRAFT_CLONING_SCOPE", "cfg")
    if cloning_scope not in VALID_CLONING_SCOPES:
        raise ConfigError(
            f"{env_path}: ETL_CRAFT_CLONING_SCOPE must be one of "
            f"{sorted(VALID_CLONING_SCOPES)}, got {cloning_scope!r}"
        )
    cloning_enabled = values.get("ETL_CRAFT_CLONING_ENABLED", "false").strip().lower() == "true"

    raw = _read_raw_yaml(path) if path.is_file() else {}

    execution = {"Mode": mode}
    orchestrator_name = values.get("ETL_CRAFT_ORCHESTRATOR_NAME")
    if orchestrator_name:
        execution["Orchestrator name"] = orchestrator_name
    raw["Execution"] = execution

    source: dict[str, str] = {"Type": source_type}
    if source_path:
        source["Path"] = source_path
    raw["Source"] = source

    postgres = raw.get("Postgres")
    if not isinstance(postgres, dict) or not isinstance(postgres.get("Profiles"), dict):
        postgres = {"Profiles": {}}
    postgres["Active_profile"] = profile_name
    postgres["Profiles"][profile_name] = {
        "jdbc_url": jdbc_url,
        "user": user,
        "auth_mode": auth_mode,
    }
    raw["Postgres"] = postgres

    raw["Cloning"] = {"Enabled": cloning_enabled, "Scope": cloning_scope}

    _write_raw_yaml(path, raw)


def _read_raw_yaml(path: Path) -> dict:
    if not path.is_file():
        raise ConfigError(f"craft-connector.yml not found at {path}")
    try:
        return yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path} is not valid YAML: {exc}") from exc


def _write_raw_yaml(path: Path, raw: dict) -> None:
    path.write_text(yaml.safe_dump(raw, sort_keys=False, default_flow_style=False))
