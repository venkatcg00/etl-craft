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

from collections.abc import Callable
from pathlib import Path

import yaml

from etl_craft.config import (
    DEFAULT_CONFIG_PATH,
    VALID_AUTH_MODES,
    VALID_CLONING_SCOPES,
    VALID_EMAIL_AUTH_MODES,
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


def configure_interactive(
    path: Path | str = DEFAULT_CONFIG_PATH,
    *,
    input_fn: Callable[[str], str] = input,
    print_fn: Callable[[str], None] = print,
) -> None:
    """Interactively build (or update) craft-connector.yml by prompting on stdin/stdout.

    [ADDITION] Closes the CLI surface's `configure` row (interactive setup
    chain), previously refused outright with a "not implemented yet"
    message and exit code 2. Mirrors configure_from_env's own section
    shape and merge semantics — Execution/Source/Cloning are wholesale-
    replaced, the one Postgres profile entered is merged in alongside any
    others already on disk — so a team can freely mix an interactive
    session with a later `configure --env` run against the same file
    without either clobbering the other's profiles.

    [ADDITION] Also offers two optional blocks configure_from_env's fixed
    env-var contract never covered at all: [Warehouse] and [Email] — an
    interactive session can naturally ask "do you want to set this up now?"
    in a way a fixed list of required env vars can't. Declining either
    leaves any existing section for it completely untouched, never cleared.
    [CHOICE] The 7 [Orchestrator] Airflow-facing global defaults are
    deliberately not prompted for here — real edge-case tuning is better
    done by hand-editing the YAML (or a future `configure --env` extension)
    than by walking through 7 more prompts most setups would just accept
    the fallback default for anyway.

    `input_fn`/`print_fn` are injectable (default: the real `input`/`print`)
    so this is testable without a real terminal — same "injectable I/O"
    spirit as crosspipe.py's own sleep/now parameters.
    """
    path = Path(path)
    raw = _read_raw_yaml(path) if path.is_file() else {}

    mode = _prompt(input_fn, print_fn, "Execution mode", choices=sorted(VALID_MODES))
    orchestrator_name = _prompt(
        input_fn, print_fn, "Orchestrator name (optional, informational only)", required=False
    )
    execution: dict[str, str] = {"Mode": mode}
    if orchestrator_name:
        execution["Orchestrator name"] = orchestrator_name
    raw["Execution"] = execution

    source_type = _prompt(
        input_fn, print_fn, "Where do secret values live", choices=sorted(VALID_SOURCE_TYPES)
    )
    source: dict[str, str] = {"Type": source_type}
    if source_type == "file":
        source["Path"] = _prompt(input_fn, print_fn, "Path to the .env-style secrets file")
    raw["Source"] = source

    print_fn("-- Postgres (Engine DB, required) --")
    raw["Postgres"] = _merge_profile_section(
        raw.get("Postgres"), *_prompt_connection_profile(input_fn, print_fn)
    )

    if _prompt_yes_no(input_fn, print_fn, "Configure a [Warehouse] (Data DB) connection now?"):
        print_fn("-- Warehouse (Data DB) --")
        raw["Warehouse"] = _merge_profile_section(
            raw.get("Warehouse"), *_prompt_connection_profile(input_fn, print_fn)
        )

    if _prompt_yes_no(input_fn, print_fn, "Configure an [Email] (SMTP) connection now?"):
        print_fn("-- Email (SMTP) --")
        raw["Email"] = _merge_profile_section(
            raw.get("Email"), *_prompt_email_profile(input_fn, print_fn)
        )

    cloning_enabled = _prompt_yes_no(
        input_fn, print_fn, "Enable Cloning (mirror Engine DB tables into the Data DB)?"
    )
    cloning: dict[str, object] = {"Enabled": cloning_enabled}
    if cloning_enabled:
        cloning["Scope"] = _prompt(
            input_fn,
            print_fn,
            "Cloning scope",
            choices=sorted(VALID_CLONING_SCOPES),
            default="cfg",
        )
    raw["Cloning"] = cloning

    _write_raw_yaml(path, raw)


def _merge_profile_section(existing: object, profile_name: str, profile: dict) -> dict:
    """Merge one named profile into an existing (or new) Active_profile/Profiles section."""
    section = existing if isinstance(existing, dict) else {}
    profiles = section.get("Profiles")
    if not isinstance(profiles, dict):
        profiles = {}
    profiles[profile_name] = profile
    return {"Active_profile": profile_name, "Profiles": profiles}


def _prompt_connection_profile(
    input_fn: Callable[[str], str], print_fn: Callable[[str], None]
) -> tuple[str, dict]:
    name = _prompt(input_fn, print_fn, "Profile name (e.g. dev/uat/prod)")
    jdbc_url = _prompt(input_fn, print_fn, "JDBC URL (e.g. jdbc:postgresql://host:5432/db)")
    user = _prompt(input_fn, print_fn, "User")
    auth_mode = _prompt(input_fn, print_fn, "Auth mode", choices=sorted(VALID_AUTH_MODES))
    profile: dict[str, object] = {"jdbc_url": jdbc_url, "user": user, "auth_mode": auth_mode}
    if auth_mode == "key_file":
        profile["key_file"] = _prompt(input_fn, print_fn, "Path to the key file")
    return name, profile


def _prompt_email_profile(
    input_fn: Callable[[str], str], print_fn: Callable[[str], None]
) -> tuple[str, dict]:
    name = _prompt(input_fn, print_fn, "Profile name (e.g. dev/uat/prod)")
    host = _prompt(input_fn, print_fn, "SMTP host")
    port = _prompt(input_fn, print_fn, "SMTP port", default="587")
    from_address = _prompt(input_fn, print_fn, "From address")
    auth_mode = _prompt(
        input_fn, print_fn, "Auth mode", choices=sorted(VALID_EMAIL_AUTH_MODES), default="none"
    )
    profile: dict[str, object] = {
        "host": host,
        "port": int(port),
        "from_address": from_address,
        "auth_mode": auth_mode,
    }
    if auth_mode == "password":
        profile["user"] = _prompt(input_fn, print_fn, "SMTP user")
    return name, profile


def _prompt(
    input_fn: Callable[[str], str],
    print_fn: Callable[[str], None],
    question: str,
    *,
    choices: list[str] | None = None,
    default: str | None = None,
    required: bool = True,
) -> str:
    suffix = f" [{'/'.join(choices)}]" if choices else ""
    if default is not None:
        suffix += f" (default: {default})"
    while True:
        answer = input_fn(f"{question}{suffix}: ").strip()
        if not answer and default is not None:
            return default
        if not answer and not required:
            return ""
        if not answer:
            print_fn("A value is required.")
            continue
        if choices and answer not in choices:
            print_fn(f"Must be one of {choices}.")
            continue
        return answer


def _prompt_yes_no(
    input_fn: Callable[[str], str], print_fn: Callable[[str], None], question: str
) -> bool:
    while True:
        answer = input_fn(f"{question} [y/N]: ").strip().lower()
        if not answer or answer in {"n", "no"}:
            return False
        if answer in {"y", "yes"}:
            return True
        print_fn("Please answer y or n.")


def _read_raw_yaml(path: Path) -> dict:
    if not path.is_file():
        raise ConfigError(f"craft-connector.yml not found at {path}")
    try:
        return yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path} is not valid YAML: {exc}") from exc


def _write_raw_yaml(path: Path, raw: dict) -> None:
    path.write_text(yaml.safe_dump(raw, sort_keys=False, default_flow_style=False))
