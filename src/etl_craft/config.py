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
# [CHOICE] CLAUDE.md open question #1 (warehouse section name — "Data Db"/
# "[Data Db 1]" explicitly rejected as too Informatica-shaped, no
# replacement settled) is resolved here as `Warehouse`: it's the term
# CLAUDE.md itself already uses throughout ("warehouse"), reads
# as a plain noun rather than a product-shaped label, and is a one-line
# rename in _parse_config below if a different name is preferred. Unlike
# [Postgres], [Warehouse] is optional at parse time — `list`/`graph`/
# `set-execution-mode`/`configure`/`run --init-only` and friends never
# touch the warehouse, so a file without one still loads; anything that
# genuinely needs a warehouse connection (warehouse.build_warehouse_engine, the
# not-yet-built `validate`/cloning) raises its own clear error if absent.

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path("craft-connector.yml")
CONFIG_PATH_ENV_VAR = "ETL_CRAFT_CONFIG"
CONFIG_FILENAME = "craft-connector.yml"


def resolve_config_path(explicit: Path | str | None = None) -> Path:
    """Find craft-connector.yml, most-specific first: --config, env var, then upward search.

    [ADDITION, 2026-09-20, E2-06] Every command used to require the process
    cwd to be the directory holding the file, with no flag and no env var to
    say otherwise. That is a poor fit for exactly the deployment CLAUDE.md
    targets: an Airflow BashOperator's cwd is not something a DAG author
    controls reliably, and `generate-yml` emits bare `etl-craft run ...` with
    no `cd`. The same assumption reached into HANDLER=PYTHON, whose scripts
    are documented as resolving config "in the same directory".

    The upward search is the `pyproject.toml`/`.git` pattern, so running from
    a subdirectory of a configured project works the way every other
    developer tool behaves. It stops at the filesystem root and falls back to
    the plain relative path, so the "not found" error still names something a
    reader recognizes.
    """
    if explicit is not None:
        return Path(explicit)
    from_env = os.environ.get(CONFIG_PATH_ENV_VAR)
    if from_env:
        return Path(from_env)
    here = Path.cwd().resolve()
    for directory in (here, *here.parents):
        candidate = directory / CONFIG_FILENAME
        if candidate.is_file():
            return candidate
    return DEFAULT_CONFIG_PATH


# `remote` is the commit-safe connector format's name for orchestration-driven
# execution. The rest of the engine already compares against `orchestrator`,
# so normalise it while reading the config and keep existing deployments valid.
MODE_ALIASES = {"remote": "orchestrator"}
VALID_MODES = frozenset({"local", "orchestrator", "remote"})
VALID_SOURCE_TYPES = frozenset({"file", "environment"})
# [ADDITION, 2026-09-20] "none" joins the list for embedded warehouses like
# DuckDB, which is a file rather than a server: there is no user to be and
# no password to present. [Email] already uses the same value for the same
# reason, so this is an existing vocabulary rather than a new one.
# Keep this union public for callers that need the complete vocabulary. Config
# validation uses the narrower section-specific sets below, so it cannot accept
# a mode whose connector has no implementation.
VALID_AUTH_MODES = frozenset({"none", "password", "token", "sso", "key_file"})
VALID_ENGINE_AUTH_MODES = frozenset({"password", "key_file"})
VALID_WAREHOUSE_AUTH_MODES = frozenset({"none", "password", "key_file", "token"})
# Modes where a `user` is not required in the profile.
#
# `none`: an embedded warehouse is a file — there is nobody to be.
#
# [DEVIATION, 2026-09-22] `token` joined it. A bearer token carries its own
# username convention — Databricks' is the literal string "token" — and
# warehouse._token_creator is where that is known, per dialect. Requiring one
# here too meant two places deciding one rule, and they disagreed: a valid
# Databricks profile was rejected at setup with "ETL_CRAFT_WAREHOUSE_USER is
# required", for a value the creator would have supplied. A profile that
# genuinely needs one (an unknown dialect) still gets a clear error from the
# creator, which is the single place that can actually tell.
AUTH_MODES_WITHOUT_USER = frozenset({"none", "token"})
VALID_CLONING_SCOPES = frozenset({"cfg", "aud", "all"})

# [ADDITION, 2026-09-22] What storage format the engine creates tables in on a
# non-Postgres warehouse.
#
#   iceberg — an Iceberg table, readable by everything else in the lakehouse.
#   native  — the warehouse's own format: Delta on Databricks, a standard
#             table on Snowflake.
#
# [CHOICE] `iceberg` stays the default. "Support non-Iceberg as well" is a
# widening, not a reversal of the default, and flipping it would silently
# change the format of every table an existing pipeline creates.
VALID_TABLE_FORMATS = frozenset({"iceberg", "native"})
DEFAULT_TABLE_FORMAT = "iceberg"
# [ADDITION] EMAIL_ALERT's own, smaller auth vocabulary — per explicit
# instruction, the transport is SMTP. Many internal relays accept anonymous
# submission (no auth_mode concept needed at all); "password" covers the
# other common real case (Gmail/O365-style app-password auth). token/sso
# aren't included: an OAuth2 XOAUTH2 SMTP flow is a real thing some
# providers support, but it's provider-specific in the same way db.py's own
# token/sso stubs are, and no team's e-mail relay has been named yet to
# build a concrete one against — left out rather than stubbed with a third
# NotImplementedError for a mode nothing currently asks for.
VALID_EMAIL_AUTH_MODES = frozenset({"none", "password"})


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
    """The [Cloning] section: merge-style copy of Engine DB tables into the warehouse."""

    enabled: bool = False
    scope: str = "cfg"
    # [ADDITION, 2026-09-22, E2-68] Where a mirrored table's Iceberg storage
    # lives, for warehouses that name it explicitly (Snowflake). Every other
    # target gets these from CFG_TASK_PARAMETERS, but cloning has no task --
    # the mirror is engine-internal machinery -- so [Cloning] is its home.
    external_volume: str = ""
    base_location: str = ""


@dataclass(frozen=True)
class EmailProfile:
    """One named [Email] profile — the SMTP relay email_alert.py sends through.

    [ADDITION] CLAUDE.md's own open question named the transport as still
    undecided ("SMTP creds vs. an API like SES/SendGrid"); resolved per
    explicit instruction to SMTP. Shaped like ConnectionSection's own
    active_profile/Profiles pattern (a team's dev/uat/prod relays can
    genuinely differ), not a single flat section, for the same reason
    [Postgres]/[Warehouse] already work that way.
    """

    section: str
    name: str
    host: str
    port: int
    from_address: str
    auth_mode: str = "none"
    user: str | None = None
    use_tls: bool = True
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def secret_var(self) -> str:
        """The env var name holding this profile's secret material (auth_mode='password' only)."""
        override = self.extra.get("secret_var")
        if override:
            return str(override)
        return f"ETL_CRAFT_{self.section}_{self.name}_SECRET".upper()


@dataclass(frozen=True)
class EmailConfig:
    """The [Email] section: an active profile name plus all named profiles."""

    active_profile: str
    profiles: dict[str, EmailProfile]

    @property
    def active(self) -> EmailProfile:
        """Return the profile currently selected by active_profile."""
        return self.profiles[self.active_profile]


@dataclass(frozen=True)
class OrchestratorConfig:
    """The [Orchestrator] section: global defaults/fallbacks for generate-yml's Airflow fields.

    Per-field settings default to None ("not set globally either" — generate-
    yml falls through to its own final hardcoded default); `global_dag`
    defaults to False per explicit instruction ("defaults to false").
    """

    global_dag: bool = False
    catchup: bool | None = None
    tags: list[str] | None = None
    retries: int | None = None
    retry_delay_minutes: int | None = None
    depends_on_past: bool | None = None
    email_on_failure: bool | None = None
    email_recipients: list[str] | None = None


# [ADDITION, 2026-09-20, E2-17/E2-19] Deployment-wide operational limits.
# Nothing in the engine had a timeout or a parallelism cap: a hung query, a
# wedged ingestion script or an unreachable-but-accepting SMTP relay blocked a
# task forever with its AUD_TASK_RUN_LOG row stuck IN-PROGRESS — which, per
# resolver.NOT_RETRYABLE, makes that task permanently un-retryable without
# manual SQL. And a 40-task wave spawned 40 processes at once, each opening its
# own engines.
#
# [CHOICE] Conservative but real defaults rather than None. A limit nobody sets
# is a limit nobody benefits from, and "six hours" is generous enough that any
# task hitting it is genuinely wedged. Per-task TASK_TIMEOUT_SECONDS overrides
# it; 0 disables it entirely for a task that legitimately runs longer.
DEFAULT_TASK_TIMEOUT_SECONDS = 6 * 60 * 60
DEFAULT_MAX_PARALLEL_TASKS = 8


@dataclass(frozen=True)
class ExecutionLimits:
    """Deployment-wide timeouts and parallelism caps, from [Execution]."""

    task_timeout_seconds: int = DEFAULT_TASK_TIMEOUT_SECONDS
    max_parallel_tasks: int = DEFAULT_MAX_PARALLEL_TASKS
    # SLA_IN_HOURS was read, emitted into the generated YAML, and enforced
    # nowhere (E2-23). Enforcing it engine-side is opt-in: for many teams it
    # really is pass-through metadata for the orchestrator.
    enforce_sla: bool = False


@dataclass(frozen=True)
class ConnectorConfig:
    """The fully parsed, validated contents of craft-connector.yml."""

    mode: str
    source: SourceConfig
    postgres: ConnectionSection
    cloning: CloningConfig
    warehouse: ConnectionSection | None = None
    # The default storage format for tables the engine creates on this
    # warehouse; a task may override it with CFG_TASK_PARAMETERS.TABLE_FORMAT.
    warehouse_table_format: str = DEFAULT_TABLE_FORMAT
    orchestrator: OrchestratorConfig = field(default_factory=OrchestratorConfig)
    email: EmailConfig | None = None
    limits: ExecutionLimits = field(default_factory=ExecutionLimits)
    # [ADDITION, 2026-09-22, E2-78] Where this config was actually read from.
    # orchestrator._run_wave has to pass it on to every task subprocess it
    # spawns: E2-06 added `--config PATH` precisely because an Airflow
    # BashOperator's cwd is not something a DAG author controls, and the
    # spawned tasks were re-resolving a *different* config from their
    # inherited cwd. None when the config was built in memory rather than
    # loaded from a file, as the test helpers do.
    config_path: Path | None = None


def load_config(path: Path | str = DEFAULT_CONFIG_PATH) -> ConnectorConfig:
    """Read, parse, and validate craft-connector.yml at `path`."""
    path = Path(path)
    if not path.is_file():
        raise ConfigError(f"craft-connector.yml not found at {path}")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path} is not valid YAML: {exc}") from exc
    return _parse_config(raw, path)


def _positive_int(section: dict[str, Any], key: str, default: int, path: Path) -> int:
    """Read an optional non-negative integer, rejecting a value that is not one.

    [ADDITION, 2026-09-20, E2-34] `[Orchestrator]` scalars were taken straight
    from `raw.get(...)` with no type check, so `Retries: "three"` flowed
    unexamined into the generated YAML. A setting that means a number should
    say so when it is handed something else.
    """
    value = section.get(key)
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{path}: {key} must be a whole number, got {value!r}")
    if value < 0:
        raise ConfigError(f"{path}: {key} must not be negative, got {value!r}")
    return value


def _parse_config(raw: Any, path: Path) -> ConnectorConfig:
    """Parse either supported connector format without guessing between them.

    The commit-safe manifest format is the public contract. Its sections are
    ``Orchestration``, ``Secrets``, and ``Engine``. Earlier releases wrote
    ``Execution``, ``Source``, and ``Postgres`` with literal connection values.
    Both remain loadable, but mixing their top-level markers is rejected: a
    partial migration must not silently select one set of credentials over the
    other.
    """
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: craft-connector.yml must contain a top-level mapping")

    manifest_markers = {"Orchestration", "Secrets", "Engine"}
    legacy_markers = {"Execution", "Source", "Postgres"}
    has_manifest = bool(manifest_markers.intersection(raw))
    has_legacy = bool(legacy_markers.intersection(raw))
    if has_manifest and has_legacy:
        raise ConfigError(
            f"{path}: mixes the manifest sections {sorted(manifest_markers)} with the legacy "
            f"sections {sorted(legacy_markers)}. Keep one complete format in a file."
        )
    if has_manifest:
        return _parse_manifest_config(raw, path)
    return _parse_legacy_config(raw, path)


def _parse_legacy_config(raw: dict[str, Any], path: Path) -> ConnectorConfig:
    """Parse the pre-manifest format retained for existing deployments."""
    execution = _require_section(raw, "Execution", path)
    mode = _parse_mode(execution, "Execution", path)

    limits = _parse_limits(execution, path)
    source = _parse_legacy_source(_require_section(raw, "Source", path), path)
    postgres = _parse_connection_section("POSTGRES", _require_section(raw, "Postgres", path), path)

    warehouse_raw = raw.get("Warehouse")
    table_format = DEFAULT_TABLE_FORMAT
    if warehouse_raw is None:
        warehouse = None
    elif not isinstance(warehouse_raw, dict):
        raise ConfigError(f"{path}: Warehouse section must be a mapping if present")
    else:
        warehouse = _parse_connection_section("WAREHOUSE", warehouse_raw, path)
        table_format = _parse_table_format(warehouse_raw, path)
        _check_warehouse_name(warehouse_raw.get("Name"), warehouse, path)

    cloning = _parse_cloning(raw.get("Cloning") or {}, path)
    orchestrator = _parse_orchestrator(raw.get("Orchestrator") or {}, path)
    email = _parse_optional_legacy_email(raw.get("Email"), path)
    return ConnectorConfig(
        mode=mode,
        source=source,
        postgres=postgres,
        cloning=cloning,
        warehouse=warehouse,
        warehouse_table_format=table_format,
        orchestrator=orchestrator,
        limits=limits,
        email=email,
        config_path=path,
    )


def _parse_manifest_config(raw: dict[str, Any], path: Path) -> ConnectorConfig:
    """Parse the commit-safe connector manifest used by the shipped examples."""
    orchestration = _require_section(raw, "Orchestration", path)
    mode = _parse_mode(orchestration, "Orchestration", path)
    limits = _parse_limits(orchestration, path)

    source = _parse_manifest_source(_require_section(raw, "Secrets", path), path)
    resolver = _VariableResolver.from_source(source, path)
    postgres = _parse_manifest_connection_section(
        "ENGINE", _require_section(raw, "Engine", path), path, resolver
    )

    warehouse_raw = raw.get("Warehouse")
    table_format = DEFAULT_TABLE_FORMAT
    if warehouse_raw is None:
        warehouse = None
    elif not isinstance(warehouse_raw, dict):
        raise ConfigError(f"{path}: Warehouse section must be a mapping if present")
    else:
        warehouse = _parse_manifest_connection_section("WAREHOUSE", warehouse_raw, path, resolver)
        table_format = _parse_table_format(warehouse_raw, path)
        _check_warehouse_name(warehouse_raw.get("Name"), warehouse, path)

    cloning = _parse_cloning(raw.get("Cloning") or {}, path)
    orchestrator = _parse_orchestrator(raw.get("Dag_defaults") or {}, path)
    email = _parse_optional_manifest_email(raw.get("Email"), path, resolver)
    return ConnectorConfig(
        mode=mode,
        source=source,
        postgres=postgres,
        cloning=cloning,
        warehouse=warehouse,
        warehouse_table_format=table_format,
        orchestrator=orchestrator,
        limits=limits,
        email=email,
        config_path=path,
    )


def _parse_mode(section: dict[str, Any], section_name: str, path: Path) -> str:
    mode = section.get("Mode")
    if not isinstance(mode, str) or mode not in VALID_MODES:
        raise ConfigError(
            f"{path}: {section_name}.Mode must be one of {sorted(VALID_MODES)}, got {mode!r}"
        )
    return MODE_ALIASES.get(mode, mode)


def _parse_limits(section: dict[str, Any], path: Path) -> ExecutionLimits:
    return ExecutionLimits(
        task_timeout_seconds=_positive_int(
            section, "Task_timeout_seconds", DEFAULT_TASK_TIMEOUT_SECONDS, path
        ),
        max_parallel_tasks=_positive_int(
            section, "Max_parallel_tasks", DEFAULT_MAX_PARALLEL_TASKS, path
        ),
        enforce_sla=bool(section.get("Enforce_sla", False)),
    )


def _require_section(raw: dict[str, Any], name: str, path: Path) -> dict[str, Any]:
    section = raw.get(name)
    if not isinstance(section, dict):
        raise ConfigError(f"{path}: missing or invalid {name!r} section")
    return section


def _parse_legacy_source(raw: dict[str, Any], path: Path) -> SourceConfig:
    source_type = raw.get("Type")
    if source_type not in VALID_SOURCE_TYPES:
        raise ConfigError(
            f"{path}: Source.Type must be one of {sorted(VALID_SOURCE_TYPES)}, got {source_type!r}"
        )
    source_path = _source_path(raw.get("Path"), source_type, "Source.Path", path)
    return SourceConfig(type=source_type, path=source_path)


def _parse_manifest_source(raw: dict[str, Any], path: Path) -> SourceConfig:
    source_type = raw.get("Source_type")
    if source_type not in VALID_SOURCE_TYPES:
        raise ConfigError(
            f"{path}: Secrets.Source_type must be one of {sorted(VALID_SOURCE_TYPES)}, "
            f"got {source_type!r}"
        )
    source_path = _source_path(raw.get("Source_path"), source_type, "Secrets.Source_path", path)
    return SourceConfig(type=source_type, path=source_path)


def _source_path(raw_path: Any, source_type: str, field_name: str, config_path: Path) -> str | None:
    """Validate a secret-file path and anchor relative paths at the manifest."""
    if raw_path is None:
        if source_type == "file":
            raise ConfigError(
                f"{config_path}: {field_name} is required when the source type is 'file'"
            )
        return None
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise ConfigError(f"{config_path}: {field_name} must be a non-empty string when present")
    source_path = Path(raw_path).expanduser()
    if source_type == "file" and not source_path.is_absolute():
        source_path = config_path.resolve().parent / source_path
    return str(source_path.resolve()) if source_type == "file" else str(source_path)


@dataclass(frozen=True)
class _VariableResolver:
    """Read named manifest values from the one configured secret source."""

    values: Mapping[str, str]
    origin: str
    path: Path

    @classmethod
    def from_source(cls, source: SourceConfig, path: Path) -> _VariableResolver:
        if source.type == "file":
            return cls(
                values=_load_dotenv_file(source.path),
                origin=f"{source.path} (Secrets.Source_type=file)",
                path=path,
            )
        return cls(
            values=os.environ,
            origin="the process environment (Secrets.Source_type=environment)",
            path=path,
        )

    def value(self, var_name: str, what: str) -> str:
        value = self.values.get(var_name)
        if value is None:
            raise ConfigError(
                f"{self.path}: {what} names the variable {var_name!r}, which is not set in "
                f"{self.origin}"
            )
        return value

    def selected_name(self, var_name: str, profile: str, field_name: str) -> str:
        """Use a tier-specific variable when present, then fall back to the named one."""
        tiered_name = _tiered_variable_name(var_name, profile, field_name)
        if tiered_name and tiered_name in self.values:
            return tiered_name
        return var_name

    def manifest_value(self, var_name: str, profile: str, field_name: str, what: str) -> str:
        selected_name = self.selected_name(var_name, profile, field_name)
        return self.value(selected_name, what)


def _tiered_variable_name(var_name: str, profile: str, field_name: str) -> str | None:
    """Insert ``profile`` before a recognised field suffix in a variable name.

    ``ENGINE_JDBC_URL`` therefore falls back to ``ENGINE_DEV_JDBC_URL`` for a
    ``dev`` profile. The same derivation works for custom prefixes such as
    ``MY_ENGINE_JDBC_URL``. If a name has no recognisable field suffix there is
    no safe way to invent a tiered variant, so the configured name remains the
    only lookup target.
    """
    suffixes = [field_name.upper()]
    if field_name == "from_address":
        # The shipped template calls this variable EMAIL_FROM, while the data
        # field itself is from_address.
        suffixes.append("FROM")
    upper_name = var_name.upper()
    upper_profile = profile.upper()
    for suffix in suffixes:
        plain_suffix = f"_{suffix}"
        tiered_suffix = f"_{upper_profile}{plain_suffix}"
        if upper_name.endswith(tiered_suffix):
            return var_name
        if upper_name.endswith(plain_suffix):
            return f"{var_name[:-len(plain_suffix)]}_{upper_profile}{var_name[-len(plain_suffix):]}"
    return None


def _manifest_profile_name(section_name: str, raw: dict[str, Any], path: Path) -> str:
    """Select a manifest profile label, with the documented environment override."""
    profile = os.environ.get(f"ETL_CRAFT_{section_name}_PROFILE") or raw.get("Profile")
    if not isinstance(profile, str) or not profile.strip():
        raise ConfigError(
            f"{path}: {section_name.title()} needs a non-empty Profile or "
            f"$ETL_CRAFT_{section_name}_PROFILE"
        )
    return profile.strip()


def _manifest_variable_name(
    variables: dict[str, Any], key: str, where: str, *, required: bool
) -> str | None:
    value = variables.get(key)
    if value is None:
        if required:
            raise ConfigError(f"{where} needs {key}, the name of a variable")
        return None
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{where}.{key} must be a non-empty variable name")
    return value.strip()


def _auth_modes_for_section(section_name: str) -> frozenset[str]:
    if section_name in {"POSTGRES", "ENGINE"}:
        return VALID_ENGINE_AUTH_MODES
    if section_name == "WAREHOUSE":
        return VALID_WAREHOUSE_AUTH_MODES
    raise AssertionError(f"unknown connection section {section_name!r}")


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
    valid_auth_modes = _auth_modes_for_section(section_name)
    needs_user = auth_mode not in AUTH_MODES_WITHOUT_USER
    if not jdbc_url or (needs_user and not user) or auth_mode not in valid_auth_modes:
        raise ConfigError(
            f"{path}: {section_name}.Profiles.{profile_name} needs jdbc_url, "
            f"auth_mode in {sorted(valid_auth_modes)}" + (", and user" if needs_user else "")
        )
    extra = {k: v for k, v in raw.items() if k not in {"jdbc_url", "user", "auth_mode"}}
    return ConnectionProfile(
        section=section_name,
        name=profile_name,
        jdbc_url=jdbc_url,
        user=user or "",
        auth_mode=auth_mode,
        extra=extra,
    )


def _parse_manifest_connection_section(
    section_name: str, raw: dict[str, Any], path: Path, resolver: _VariableResolver
) -> ConnectionSection:
    """Resolve one manifest ``Variables`` mapping into the runtime profile."""
    profile_name = _manifest_profile_name(section_name, raw, path)
    variables = raw.get("Variables")
    where = f"{section_name.title()}.Variables"
    if not isinstance(variables, dict):
        raise ConfigError(f"{path}: {section_name.title()} needs a Variables mapping")

    def resolve(key: str, *, required: bool = False) -> str:
        variable_name = _manifest_variable_name(variables, key, where, required=required)
        if variable_name is None:
            return ""
        return resolver.manifest_value(variable_name, profile_name, key, f"{where}.{key}")

    jdbc_url = resolve("jdbc_url", required=True)
    auth_mode = resolve("auth_mode", required=True)
    valid_auth_modes = _auth_modes_for_section(section_name)
    if auth_mode not in valid_auth_modes:
        raise ConfigError(
            f"{path}: {where}.auth_mode resolved to {auth_mode!r}, which is not one of "
            f"{sorted(valid_auth_modes)}"
        )
    user = resolve("user")
    if auth_mode not in AUTH_MODES_WITHOUT_USER and not user:
        raise ConfigError(f"{path}: {where} needs user for auth_mode={auth_mode!r}")

    secret_name = _manifest_variable_name(variables, "secret", where, required=False)
    if secret_name:
        secret_name = resolver.selected_name(secret_name, profile_name, "secret")
    elif auth_mode != "none":
        raise ConfigError(f"{path}: {where} needs secret for auth_mode={auth_mode!r}")

    handled = {"jdbc_url", "user", "auth_mode", "secret"}
    extra: dict[str, str] = {}
    for key in variables:
        if key in handled:
            continue
        variable_name = _manifest_variable_name(variables, key, where, required=True)
        if variable_name is not None:
            extra[key] = resolver.manifest_value(
                variable_name,
                profile_name,
                key,
                f"{where}.{key}",
            )
    if auth_mode == "key_file" and not extra.get("key_file"):
        raise ConfigError(f"{path}: {where} needs key_file for auth_mode='key_file'")
    return ConnectionSection(
        active_profile=profile_name,
        profiles={
            profile_name: ConnectionProfile(
                section=section_name,
                name=profile_name,
                jdbc_url=jdbc_url,
                user=user,
                auth_mode=auth_mode,
                extra={"secret_var": secret_name, **extra} if secret_name else extra,
            )
        },
    )


def _parse_table_format(raw: dict[str, Any], path: Path) -> str:
    """Read the warehouse default rather than silently discarding it."""
    table_format = raw.get("Table_format", DEFAULT_TABLE_FORMAT)
    if not isinstance(table_format, str) or table_format not in VALID_TABLE_FORMATS:
        raise ConfigError(
            f"{path}: Warehouse.Table_format must be one of {sorted(VALID_TABLE_FORMATS)}, "
            f"got {table_format!r}"
        )
    return table_format


WAREHOUSE_NAME_DIALECTS = {
    "postgres": "postgresql",
    "postgresql": "postgresql",
    "databricks": "databricks",
    "snowflake": "snowflake",
    "trino": "trino",
    "duckdb": "duckdb",
}


def _check_warehouse_name(name: Any, warehouse: ConnectionSection, path: Path) -> None:
    """Ensure an optional Warehouse.Name matches its active JDBC URL's dialect."""
    if name is None:
        return
    if not isinstance(name, str):
        raise ConfigError(f"{path}: Warehouse.Name must be a string when present")
    expected = WAREHOUSE_NAME_DIALECTS.get(name.strip().lower())
    if expected is None:
        supported = sorted({value.title() for value in WAREHOUSE_NAME_DIALECTS})
        raise ConfigError(f"{path}: Warehouse.Name {name!r} must be one of {supported}")

    # Imported lazily because warehouse.py imports this module for its profile
    # types. A malformed JDBC URL is a config error at this boundary too: the
    # documented Name check would otherwise claim validation and defer it until
    # the first production task.
    from etl_craft.db import ConnectionError_
    from etl_craft.warehouse import translate_jdbc_url

    try:
        dialect_name, _ = translate_jdbc_url(warehouse.active.jdbc_url)
    except ConnectionError_ as exc:
        raise ConfigError(
            f"{path}: Warehouse.Name cannot be checked because the active JDBC URL is invalid: "
            f"{exc}"
        ) from exc
    actual = dialect_name.split("+", 1)[0]
    if actual != expected:
        raise ConfigError(
            f"{path}: Warehouse.Name is {name!r}, but its active JDBC URL resolves to "
            f"{actual!r}"
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
    return CloningConfig(
        enabled=enabled,
        scope=scope,
        external_volume=str(raw.get("External_volume") or ""),
        base_location=str(raw.get("Base_location") or ""),
    )


def _parse_orchestrator(raw: dict[str, Any], path: Path) -> OrchestratorConfig:
    if not raw:
        return OrchestratorConfig()
    tags = _require_list_if_present(raw, "Tags", path)
    email_recipients = _require_list_if_present(raw, "Email_recipients", path)
    return OrchestratorConfig(
        global_dag=bool(raw.get("Global_dag", False)),
        catchup=raw.get("Catchup"),
        tags=tags,
        retries=raw.get("Retries"),
        retry_delay_minutes=raw.get("Retry_delay_minutes"),
        depends_on_past=raw.get("Depends_on_past"),
        email_on_failure=raw.get("Email_on_failure"),
        email_recipients=email_recipients,
    )


def _parse_optional_legacy_email(raw: Any, path: Path) -> EmailConfig | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: Email section must be a mapping if present")
    return _parse_legacy_email_section(raw, path)


def _parse_legacy_email_section(raw: dict[str, Any], path: Path) -> EmailConfig:
    active_profile = raw.get("Active_profile")
    profiles_raw = raw.get("Profiles")
    if not active_profile or not isinstance(profiles_raw, dict):
        raise ConfigError(f"{path}: Email needs Active_profile and a Profiles mapping")
    profiles = {
        name: _parse_legacy_email_profile(name, profile_raw or {}, path)
        for name, profile_raw in profiles_raw.items()
    }
    if active_profile not in profiles:
        raise ConfigError(
            f"{path}: Email.Active_profile {active_profile!r} has no matching entry in Profiles"
        )
    return EmailConfig(active_profile=active_profile, profiles=profiles)


def _parse_legacy_email_profile(name: str, raw: dict[str, Any], path: Path) -> EmailProfile:
    host = raw.get("host")
    port = raw.get("port")
    from_address = raw.get("from_address")
    if not host or not port or not from_address:
        raise ConfigError(f"{path}: Email.Profiles.{name} needs host, port, and from_address")
    auth_mode = raw.get("auth_mode", "none")
    if auth_mode not in VALID_EMAIL_AUTH_MODES:
        raise ConfigError(
            f"{path}: Email.Profiles.{name}.auth_mode must be one of "
            f"{sorted(VALID_EMAIL_AUTH_MODES)}, got {auth_mode!r}"
        )
    user = raw.get("user")
    if auth_mode == "password" and not user:
        raise ConfigError(f"{path}: Email.Profiles.{name} needs user when auth_mode=password")
    extra = {
        k: v
        for k, v in raw.items()
        if k not in {"host", "port", "from_address", "auth_mode", "user", "use_tls"}
    }
    try:
        parsed_port = int(port)
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            f"{path}: Email.Profiles.{name}.port must be a number, got {port!r}"
        ) from exc
    return EmailProfile(
        section="EMAIL",
        name=name,
        host=host,
        port=parsed_port,
        from_address=from_address,
        auth_mode=auth_mode,
        user=user,
        use_tls=bool(raw.get("use_tls", True)),
        extra=extra,
    )


def _parse_optional_manifest_email(
    raw: Any, path: Path, resolver: _VariableResolver
) -> EmailConfig | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: Email section must be a mapping if present")
    profile_name = _manifest_profile_name("EMAIL", raw, path)
    variables = raw.get("Variables")
    where = "Email.Variables"
    if not isinstance(variables, dict):
        raise ConfigError(f"{path}: Email needs a Variables mapping")

    def resolve(key: str, *, required: bool = False) -> str:
        variable_name = _manifest_variable_name(variables, key, where, required=required)
        if variable_name is None:
            return ""
        return resolver.manifest_value(variable_name, profile_name, key, f"{where}.{key}")

    host = resolve("host", required=True)
    from_address = resolve("from_address", required=True)
    port_raw = resolve("port", required=True)
    try:
        port = int(port_raw)
    except ValueError as exc:
        raise ConfigError(f"{path}: {where}.port resolved to {port_raw!r}, not a number") from exc
    auth_mode = resolve("auth_mode", required=True)
    if auth_mode not in VALID_EMAIL_AUTH_MODES:
        raise ConfigError(
            f"{path}: {where}.auth_mode resolved to {auth_mode!r}, which is not one of "
            f"{sorted(VALID_EMAIL_AUTH_MODES)}"
        )
    user = resolve("user") or None
    if auth_mode == "password" and not user:
        raise ConfigError(f"{path}: {where} needs user for auth_mode='password'")
    use_tls_raw = resolve("use_tls")
    use_tls = _parse_bool(use_tls_raw, f"{where}.use_tls", path, default=True)
    secret_name = _manifest_variable_name(variables, "secret", where, required=False)
    if secret_name:
        secret_name = resolver.selected_name(secret_name, profile_name, "secret")
    elif auth_mode == "password":
        raise ConfigError(f"{path}: {where} needs secret for auth_mode='password'")
    profile = EmailProfile(
        section="EMAIL",
        name=profile_name,
        host=host,
        port=port,
        from_address=from_address,
        auth_mode=auth_mode,
        user=user,
        use_tls=use_tls,
        extra={"secret_var": secret_name} if secret_name else {},
    )
    return EmailConfig(active_profile=profile_name, profiles={profile_name: profile})


def _parse_bool(value: Any, name: str, path: Path, *, default: bool) -> bool:
    """Accept YAML booleans and the standard environment spellings."""
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    raise ConfigError(f"{path}: {name} must be true or false, got {value!r}")


def _require_list_if_present(raw: dict[str, Any], key: str, path: Path) -> list[str] | None:
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, list):
        raise ConfigError(f"{path}: Orchestrator.{key} must be a list of strings if present")
    return value


def resolve_secret(config: ConnectorConfig, profile: ConnectionProfile | EmailProfile) -> str:
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
    """Parse a minimal .env-style file: KEY=VALUE per line, '#' comments, blank lines ignored.

    The supported subset is deliberately small, and is documented in
    docs/configuration.md: no escape sequences, no multi-line values, and `#`
    only as a whole-line comment. A value may be wrapped in one matching pair
    of single or double quotes, which is removed.
    """
    if not path:
        raise ConfigError("Source.Path is required when Source.Type == 'file'")
    try:
        contents = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"could not read secrets file {path!r}: {exc}") from exc
    values: dict[str, str] = {}
    for line in contents.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        values[key.strip()] = _unquote(value.strip())
    return values


def _unquote(value: str) -> str:
    """Remove one matching pair of wrapping quotes, and only that.

    [DEVIATION, 2026-09-22, E2-86] This used to hand the quote characters to
    str.strip, which removes *every* leading and trailing character in the
    set, repeatedly. A secret that legitimately ends in a quote -- not rare in
    a generated password or token -- was silently truncated, and one that both
    began and ended with one lost both. The result is an authentication
    failure with nothing anywhere saying the value had been altered, and
    `doctor` reporting the secret as found, because it was.
    """
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value
