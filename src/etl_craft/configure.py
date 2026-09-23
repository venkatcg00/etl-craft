"""Write and update craft-connector.yml.

[DEVIATION, 2026-09-20] The interactive `configure` chain is gone, per
explicit instruction ("remove the ineractive setup, lets go in dbt route").
What remains is the non-interactive path, driven by `etl-craft setup`: read
settings from a .env-style file or from the process environment, then write or
merge them into craft-connector.yml. There is no prompt sequence to sit
through, and nothing that behaves differently in CI than on a laptop.

New deployments receive the commit-safe manifest format: connection details
are variable *names* in `craft-connector.yml`, and the values stay in the
configured environment or secrets file. Existing legacy manifests remain
editable without being rewritten into a different shape.
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
# Existing legacy files retain their profile mapping when setup updates them.
# A canonical manifest deliberately has one profile label and variable names;
# changing the tier changes that label rather than embedding connection values.

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import yaml

from etl_craft.config import (
    AUTH_MODES_WITHOUT_USER,
    DEFAULT_CONFIG_PATH,
    MODE_ALIASES,
    VALID_CLONING_SCOPES,
    VALID_ENGINE_AUTH_MODES,
    VALID_MODES,
    VALID_SOURCE_TYPES,
    VALID_TABLE_FORMATS,
    VALID_WAREHOUSE_AUTH_MODES,
    ConfigError,
    _load_dotenv_file,
)


def set_execution_mode(mode: str, path: Path | str | None = None) -> None:
    """Update the execution mode without changing a manifest's format."""
    if mode not in VALID_MODES:
        raise ConfigError(f"mode must be one of {sorted(VALID_MODES)}, got {mode!r}")
    path = Path(path) if path is not None else DEFAULT_CONFIG_PATH
    raw = _read_raw_yaml(path)
    runtime_mode = MODE_ALIASES.get(mode, mode)
    if isinstance(raw.get("Orchestration"), dict):
        raw["Orchestration"]["Mode"] = "remote" if runtime_mode == "orchestrator" else runtime_mode
    elif isinstance(raw.get("Execution"), dict):
        raw["Execution"]["Mode"] = runtime_mode
    else:
        raise ConfigError(
            f"{path}: missing or invalid 'Orchestration'/'Execution' section — run `setup` first"
        )
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
    if auth_mode not in VALID_ENGINE_AUTH_MODES:
        raise ConfigError(
            f"{origin}: ETL_CRAFT_POSTGRES_AUTH_MODE must be one of "
            f"{sorted(VALID_ENGINE_AUTH_MODES)}, got {auth_mode!r}"
        )
    engine_key_file = values.get("ETL_CRAFT_POSTGRES_KEY_FILE", "")
    if auth_mode == "key_file" and not engine_key_file:
        raise ConfigError(
            f"{origin}: ETL_CRAFT_POSTGRES_KEY_FILE is required when "
            "ETL_CRAFT_POSTGRES_AUTH_MODE=key_file"
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
    # A canonical manifest may contain variable *names* only. Unlike the
    # former literal-profile format, there is nowhere safe to encode an
    # implicit password value, so a configured warehouse must name its mode.
    warehouse_auth_mode = values.get("ETL_CRAFT_WAREHOUSE_AUTH_MODE", "")
    # The private key's path, for auth_mode=key_file (Snowflake key-pair). The
    # key itself is never written here -- only where to find it.
    warehouse_key_file = values.get("ETL_CRAFT_WAREHOUSE_KEY_FILE", "")
    if warehouse_url:
        if not warehouse_auth_mode:
            raise ConfigError(
                f"{origin}: ETL_CRAFT_WAREHOUSE_AUTH_MODE is required when "
                "ETL_CRAFT_WAREHOUSE_JDBC_URL is set"
            )
        if warehouse_auth_mode not in VALID_WAREHOUSE_AUTH_MODES:
            raise ConfigError(
                f"{origin}: ETL_CRAFT_WAREHOUSE_AUTH_MODE must be one of "
                f"{sorted(VALID_WAREHOUSE_AUTH_MODES)}, got {warehouse_auth_mode!r}"
            )
        if warehouse_auth_mode not in AUTH_MODES_WITHOUT_USER and not warehouse_user:
            raise ConfigError(
                f"{origin}: ETL_CRAFT_WAREHOUSE_USER is required when "
                f"ETL_CRAFT_WAREHOUSE_AUTH_MODE={warehouse_auth_mode}"
            )
        if warehouse_auth_mode == "key_file" and not warehouse_key_file:
            raise ConfigError(
                f"{origin}: ETL_CRAFT_WAREHOUSE_KEY_FILE is required when "
                "ETL_CRAFT_WAREHOUSE_AUTH_MODE=key_file — it is the path to the private key, "
                "which is never stored in craft-connector.yml itself"
            )

    cloning_scope = values.get("ETL_CRAFT_CLONING_SCOPE", "cfg")
    if cloning_scope not in VALID_CLONING_SCOPES:
        raise ConfigError(
            f"{origin}: ETL_CRAFT_CLONING_SCOPE must be one of "
            f"{sorted(VALID_CLONING_SCOPES)}, got {cloning_scope!r}"
        )
    cloning_enabled = values.get("ETL_CRAFT_CLONING_ENABLED", "false").strip().lower() == "true"

    raw = _read_raw_yaml(path) if path.is_file() else {}
    settings = _BootstrapSettings(
        mode=MODE_ALIASES.get(mode, mode),
        source_type=source_type,
        source_path=source_path,
        engine_profile=profile_name,
        engine_auth_mode=auth_mode,
        engine_key_file=engine_key_file,
        warehouse_url=warehouse_url,
        warehouse_profile=warehouse_profile,
        warehouse_auth_mode=warehouse_auth_mode,
        warehouse_key_file=warehouse_key_file,
        cloning_enabled=cloning_enabled,
        cloning_scope=cloning_scope,
        orchestrator_name=values.get("ETL_CRAFT_ORCHESTRATOR_NAME"),
        warehouse_table_format=values.get("ETL_CRAFT_WAREHOUSE_TABLE_FORMAT", "iceberg"),
    )
    if settings.warehouse_table_format not in VALID_TABLE_FORMATS:
        raise ConfigError(
            f"{origin}: ETL_CRAFT_WAREHOUSE_TABLE_FORMAT must be one of "
            f"{sorted(VALID_TABLE_FORMATS)}, got {settings.warehouse_table_format!r}"
        )

    if _is_legacy_manifest(raw):
        _write_legacy_manifest(raw, settings, jdbc_url, user, warehouse_user)
    else:
        _write_canonical_manifest(raw, settings)
    _write_raw_yaml(path, raw)


@dataclass(frozen=True)
class _BootstrapSettings:
    """Validated values used to write either supported configuration shape."""

    mode: str
    source_type: str
    source_path: str | None
    engine_profile: str
    engine_auth_mode: str
    engine_key_file: str
    warehouse_url: str | None
    warehouse_profile: str
    warehouse_auth_mode: str
    warehouse_key_file: str
    cloning_enabled: bool
    cloning_scope: str
    orchestrator_name: str | None
    warehouse_table_format: str


def _is_legacy_manifest(raw: dict) -> bool:
    """Whether an existing file must retain the pre-manifest representation."""
    return bool({"Execution", "Source", "Postgres"}.intersection(raw))


def _write_legacy_manifest(
    raw: dict,
    settings: _BootstrapSettings,
    jdbc_url: str,
    user: str,
    warehouse_user: str,
) -> None:
    """Update an existing legacy file without discarding its other profiles."""
    execution = {"Mode": settings.mode}
    if settings.orchestrator_name:
        execution["Orchestrator name"] = settings.orchestrator_name
    raw["Execution"] = execution

    source: dict[str, str] = {"Type": settings.source_type}
    if settings.source_path:
        source["Path"] = settings.source_path
    raw["Source"] = source

    postgres = raw.get("Postgres")
    if not isinstance(postgres, dict) or not isinstance(postgres.get("Profiles"), dict):
        postgres = {"Profiles": {}}
    engine_profile: dict[str, str] = {
        "jdbc_url": jdbc_url,
        "user": user,
        "auth_mode": settings.engine_auth_mode,
    }
    if settings.engine_key_file:
        engine_profile["key_file"] = settings.engine_key_file
    postgres["Active_profile"] = settings.engine_profile
    postgres["Profiles"][settings.engine_profile] = engine_profile
    raw["Postgres"] = postgres

    if settings.warehouse_url:
        warehouse = raw.get("Warehouse")
        if not isinstance(warehouse, dict) or not isinstance(warehouse.get("Profiles"), dict):
            warehouse = {"Profiles": {}}
        warehouse_profile: dict[str, str] = {
            "jdbc_url": settings.warehouse_url,
            "auth_mode": settings.warehouse_auth_mode,
        }
        if warehouse_user:
            warehouse_profile["user"] = warehouse_user
        if settings.warehouse_key_file:
            warehouse_profile["key_file"] = settings.warehouse_key_file
        warehouse["Active_profile"] = settings.warehouse_profile
        warehouse["Profiles"][settings.warehouse_profile] = warehouse_profile
        warehouse["Table_format"] = settings.warehouse_table_format
        raw["Warehouse"] = warehouse

    raw["Cloning"] = {"Enabled": settings.cloning_enabled, "Scope": settings.cloning_scope}


def _write_canonical_manifest(raw: dict, settings: _BootstrapSettings) -> None:
    """Write a new manifest whose connection values remain outside the repository."""
    orchestration = raw.get("Orchestration")
    if not isinstance(orchestration, dict):
        orchestration = {}
    orchestration["Mode"] = "remote" if settings.mode == "orchestrator" else settings.mode
    if settings.orchestrator_name:
        orchestration["Orchestrator_name"] = settings.orchestrator_name
    raw["Orchestration"] = orchestration

    secrets: dict[str, str] = {"Source_type": settings.source_type}
    if settings.source_path:
        secrets["Source_path"] = settings.source_path
    raw["Secrets"] = secrets

    engine_variables = {
        "jdbc_url": "ETL_CRAFT_POSTGRES_JDBC_URL",
        "user": "ETL_CRAFT_POSTGRES_USER",
        "auth_mode": "ETL_CRAFT_POSTGRES_AUTH_MODE",
        "secret": f"ETL_CRAFT_POSTGRES_{settings.engine_profile.upper()}_SECRET",
    }
    if settings.engine_key_file:
        engine_variables["key_file"] = "ETL_CRAFT_POSTGRES_KEY_FILE"
    raw["Engine"] = {"Profile": settings.engine_profile, "Variables": engine_variables}

    if settings.warehouse_url:
        warehouse_variables = {
            "jdbc_url": "ETL_CRAFT_WAREHOUSE_JDBC_URL",
            "auth_mode": "ETL_CRAFT_WAREHOUSE_AUTH_MODE",
            "secret": f"ETL_CRAFT_WAREHOUSE_{settings.warehouse_profile.upper()}_SECRET",
        }
        # A token carries Databricks' username convention, and a `none`
        # profile needs no username. Every other supported warehouse needs it.
        if settings.warehouse_auth_mode not in AUTH_MODES_WITHOUT_USER:
            warehouse_variables["user"] = "ETL_CRAFT_WAREHOUSE_USER"
        if settings.warehouse_key_file:
            warehouse_variables["key_file"] = "ETL_CRAFT_WAREHOUSE_KEY_FILE"
        warehouse: dict[str, object] = {
            "Table_format": settings.warehouse_table_format,
            "Profile": settings.warehouse_profile,
            "Variables": warehouse_variables,
        }
        name = _warehouse_name(settings.warehouse_url)
        if name:
            warehouse["Name"] = name
        raw["Warehouse"] = warehouse

    raw["Cloning"] = {"Enabled": settings.cloning_enabled, "Scope": settings.cloning_scope}


def _warehouse_name(jdbc_url: str) -> str | None:
    """Return the documented warehouse name for a supported JDBC scheme."""
    scheme = jdbc_url.removeprefix("jdbc:").split(":", 1)[0].lower()
    return {
        "postgresql": "Postgres",
        "duckdb": "DuckDB",
        "databricks": "Databricks",
        "snowflake": "Snowflake",
        "trino": "Trino",
    }.get(scheme)


def _required_secret_vars(raw: dict) -> list[tuple[str, str]]:
    """List the (section, env var) pairs the written config will look for at runtime.

    [ADDITION, 2026-09-20, E2-16] `configure` used to write profiles whose
    secrets are looked up as ETL_CRAFT_{SECTION}_{PROFILE}_SECRET and never
    mention that name, so the flow was: answer every prompt, then watch the
    next command fail with "secret 'ETL_CRAFT_POSTGRES_DEV_SECRET' not found".
    The name is derivable from what was just entered, so there is no reason
    not to say it.
    """
    if isinstance(raw.get("Orchestration"), dict):
        required: list[tuple[str, str]] = []
        for section in ("Engine", "Warehouse", "Email"):
            block = raw.get(section)
            if not isinstance(block, dict):
                continue
            profile = block.get("Profile")
            variables = block.get("Variables")
            secret_var = variables.get("secret") if isinstance(variables, dict) else None
            if isinstance(profile, str) and isinstance(secret_var, str) and secret_var:
                required.append((f"{section}.{profile}", secret_var))
        return required

    required = []
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
    source_value = raw.get("Secrets")
    if not isinstance(source_value, dict):
        source_value = raw.get("Source")
    source = source_value if isinstance(source_value, dict) else {}
    print_fn("")
    print_fn("This configuration expects the following secret(s):")
    for label, var in required:
        print_fn(f"  {label:<24} {var}")
    source_type = source.get("Source_type", source.get("Type"))
    source_path = source.get("Source_path", source.get("Path"))
    if source_type == "file":
        print_fn(f"Add them as KEY=VALUE lines in {source_path}.")
    else:
        print_fn("Set them in the environment before running any command.")
    print_fn("Run `etl-craft doctor` to check the whole configuration once they are set.")


def _read_raw_yaml(path: Path) -> dict:
    if not path.is_file():
        raise ConfigError(f"craft-connector.yml not found at {path}")
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path} is not valid YAML: {exc}") from exc


def _write_raw_yaml(path: Path, raw: dict) -> None:
    path.write_text(
        yaml.safe_dump(raw, sort_keys=False, default_flow_style=False), encoding="utf-8"
    )
