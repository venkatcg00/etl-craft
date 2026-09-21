"""Write and update craft-connector.yml.

[DEVIATION, 2026-09-20] The interactive `configure` chain is gone, per
explicit instruction ("remove the ineractive setup, lets go in dbt route").
What remains is the non-interactive path, driven by `etl-craft setup`: read
settings from a .env-style file or from the process environment, then write or
merge them into craft-connector.yml. There is no prompt sequence to sit
through, and nothing that behaves differently in CI than on a laptop.

`configure_from_env`'s merge semantics are unchanged: the one Postgres profile
named in the settings is added or updated alongside any others already on
disk and made Active_profile, while Execution/Source/Cloning are replaced
wholesale, since those are singular and global.
"""

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

import os
from collections.abc import Callable
from pathlib import Path

import yaml

from etl_craft.config import (
    AUTH_MODES_WITHOUT_USER,
    DEFAULT_CONFIG_PATH,
    VALID_AUTH_MODES,
    VALID_CLONING_SCOPES,
    VALID_MODES,
    VALID_SOURCE_TYPES,
    ConfigError,
    _load_dotenv_file,
)


def set_execution_mode(mode: str, path: Path | str | None = None) -> None:
    """Update Execution.Mode in an existing craft-connector.yml, leaving everything else as-is."""
    if mode not in VALID_MODES:
        raise ConfigError(f"mode must be one of {sorted(VALID_MODES)}, got {mode!r}")
    path = Path(path) if path is not None else DEFAULT_CONFIG_PATH
    raw = _read_raw_yaml(path)
    if not isinstance(raw.get("Execution"), dict):
        raise ConfigError(f"{path}: missing or invalid 'Execution' section — run `configure` first")
    raw["Execution"]["Mode"] = mode
    _write_raw_yaml(path, raw)


def configure_from_env(env_path: Path | str | None, path: Path | str | None = None) -> None:
    """Build (or update) craft-connector.yml from an env file, or from the environment.

    [DEVIATION, 2026-09-20] `env_path=None` reads the process environment
    instead of a file, per explicit instruction ("the craft connector can take
    values from .env or the environment itself based on the options"). That is
    what makes a container or CI runner — where these are already exported and
    writing a file would be a step backwards — a first-class setup path.
    """
    path = Path(path) if path is not None else DEFAULT_CONFIG_PATH
    if env_path is None:
        values = dict(os.environ)
        origin = "the environment"
    else:
        env_path = Path(env_path)
        if not env_path.is_file():
            raise ConfigError(f"env file not found at {env_path}")
        values = _load_dotenv_file(str(env_path))
        origin = str(env_path)

    def require(key: str) -> str:
        value = values.get(key)
        if not value:
            raise ConfigError(f"{origin}: {key} is required")
        return value

    mode = require("ETL_CRAFT_MODE")
    if mode not in VALID_MODES:
        raise ConfigError(
            f"{origin}: ETL_CRAFT_MODE must be one of {sorted(VALID_MODES)}, got {mode!r}"
        )

    source_type = require("ETL_CRAFT_SOURCE_TYPE")
    if source_type not in VALID_SOURCE_TYPES:
        raise ConfigError(
            f"{origin}: ETL_CRAFT_SOURCE_TYPE must be one of "
            f"{sorted(VALID_SOURCE_TYPES)}, got {source_type!r}"
        )
    source_path = values.get("ETL_CRAFT_SOURCE_PATH")
    if source_type == "file" and not source_path:
        raise ConfigError(
            f"{origin}: ETL_CRAFT_SOURCE_PATH is required when ETL_CRAFT_SOURCE_TYPE=file"
        )

    profile_name = require("ETL_CRAFT_POSTGRES_PROFILE")
    jdbc_url = require("ETL_CRAFT_POSTGRES_JDBC_URL")
    user = require("ETL_CRAFT_POSTGRES_USER")
    auth_mode = require("ETL_CRAFT_POSTGRES_AUTH_MODE")
    if auth_mode not in VALID_AUTH_MODES:
        raise ConfigError(
            f"{origin}: ETL_CRAFT_POSTGRES_AUTH_MODE must be one of "
            f"{sorted(VALID_AUTH_MODES)}, got {auth_mode!r}"
        )

    # [ADDITION, 2026-09-21] The [Warehouse] section, which this command never
    # wrote — so `etl-craft setup`, the one command that is supposed to take a
    # team from nothing to a working deployment, produced a config in which
    # every SQL and BUSINESS_RULES task failed with "no [Warehouse] section
    # configured". Same class of gap as E2-13: the install path stopped short
    # of a working state.
    #
    # Optional, so an Engine-DB-only setup (a team running PYTHON and
    # EMAIL_ALERT tasks) is unchanged and no existing .env file breaks.
    # ETL_CRAFT_WAREHOUSE_JDBC_URL is what turns it on.
    warehouse_url = values.get("ETL_CRAFT_WAREHOUSE_JDBC_URL")
    warehouse_profile = values.get("ETL_CRAFT_WAREHOUSE_PROFILE", profile_name)
    warehouse_user = values.get("ETL_CRAFT_WAREHOUSE_USER", "")
    # [CHOICE] Defaults to `password`, matching [Postgres]. DuckDB sets
    # `none` explicitly — it is a file, with nothing to authenticate to.
    warehouse_auth_mode = values.get("ETL_CRAFT_WAREHOUSE_AUTH_MODE", "password")
    if warehouse_url:
        if warehouse_auth_mode not in VALID_AUTH_MODES:
            raise ConfigError(
                f"{origin}: ETL_CRAFT_WAREHOUSE_AUTH_MODE must be one of "
                f"{sorted(VALID_AUTH_MODES)}, got {warehouse_auth_mode!r}"
            )
        if warehouse_auth_mode not in AUTH_MODES_WITHOUT_USER and not warehouse_user:
            raise ConfigError(
                f"{origin}: ETL_CRAFT_WAREHOUSE_USER is required when "
                f"ETL_CRAFT_WAREHOUSE_AUTH_MODE={warehouse_auth_mode}"
            )

    cloning_scope = values.get("ETL_CRAFT_CLONING_SCOPE", "cfg")
    if cloning_scope not in VALID_CLONING_SCOPES:
        raise ConfigError(
            f"{origin}: ETL_CRAFT_CLONING_SCOPE must be one of "
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

    if warehouse_url:
        # Merged the same way [Postgres] is: add/update just this one profile
        # and make it active, leaving any others already on disk alone.
        warehouse = raw.get("Warehouse")
        if not isinstance(warehouse, dict) or not isinstance(warehouse.get("Profiles"), dict):
            warehouse = {"Profiles": {}}
        warehouse["Active_profile"] = warehouse_profile
        entry: dict[str, str] = {"jdbc_url": warehouse_url, "auth_mode": warehouse_auth_mode}
        if warehouse_user:
            entry["user"] = warehouse_user
        warehouse["Profiles"][warehouse_profile] = entry
        raw["Warehouse"] = warehouse

    raw["Cloning"] = {"Enabled": cloning_enabled, "Scope": cloning_scope}

    _write_raw_yaml(path, raw)


def _required_secret_vars(raw: dict) -> list[tuple[str, str]]:
    """List the (section, env var) pairs the written config will look for at runtime.

    [ADDITION, 2026-09-20, E2-16] `configure` used to write profiles whose
    secrets are looked up as ETL_CRAFT_{SECTION}_{PROFILE}_SECRET and never
    mention that name, so the flow was: answer every prompt, then watch the
    next command fail with "secret 'ETL_CRAFT_POSTGRES_DEV_SECRET' not found".
    The name is derivable from what was just entered, so there is no reason
    not to say it.
    """
    required: list[tuple[str, str]] = []
    for section in ("Postgres", "Warehouse", "Email"):
        block = raw.get(section)
        if not isinstance(block, dict):
            continue
        name = block.get("Active_profile")
        profile = (block.get("Profiles") or {}).get(name)
        if not isinstance(profile, dict):
            continue
        if profile.get("auth_mode") == "none":
            continue
        override = profile.get("secret_var")
        var = override if override else f"ETL_CRAFT_{section}_{name}_SECRET".upper()
        required.append((f"{section}.{name}", var))
    return required


def _report_required_secrets(raw: dict, print_fn: Callable[[str], None]) -> None:
    """Print the exact secret variable names the new configuration expects."""
    required = _required_secret_vars(raw)
    if not required:
        return
    source = raw.get("Source") or {}
    print_fn("")
    print_fn("This configuration expects the following secret(s):")
    for label, var in required:
        print_fn(f"  {label:<24} {var}")
    if source.get("Type") == "file":
        print_fn(f"Add them as KEY=VALUE lines in {source.get('Path')}.")
    else:
        print_fn("Set them in the environment before running any command.")
    print_fn("Run `etl-craft doctor` to check the whole configuration once they are set.")


def _merge_profile_section(existing: object, profile_name: str, profile: dict) -> dict:
    """Merge one named profile into an existing (or new) Active_profile/Profiles section."""
    section = existing if isinstance(existing, dict) else {}
    profiles = section.get("Profiles")
    if not isinstance(profiles, dict):
        profiles = {}
    profiles[profile_name] = profile
    return {"Active_profile": profile_name, "Profiles": profiles}


def _read_raw_yaml(path: Path) -> dict:
    if not path.is_file():
        raise ConfigError(f"craft-connector.yml not found at {path}")
    try:
        return yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path} is not valid YAML: {exc}") from exc


def _write_raw_yaml(path: Path, raw: dict) -> None:
    path.write_text(yaml.safe_dump(raw, sort_keys=False, default_flow_style=False))
