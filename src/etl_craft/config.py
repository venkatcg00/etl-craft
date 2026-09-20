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


VALID_MODES = frozenset({"local", "orchestrator"})
VALID_SOURCE_TYPES = frozenset({"file", "environment"})
VALID_AUTH_MODES = frozenset({"password", "token", "sso", "key_file"})
VALID_CLONING_SCOPES = frozenset({"cfg", "aud", "all"})
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
    """The [Cloning] section: merge-style copy of Engine DB tables into the Data DB."""

    enabled: bool = False
    scope: str = "cfg"


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
    orchestrator: OrchestratorConfig = field(default_factory=OrchestratorConfig)
    email: EmailConfig | None = None
    limits: ExecutionLimits = field(default_factory=ExecutionLimits)


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


def _parse_config(raw: dict[str, Any], path: Path) -> ConnectorConfig:
    execution = _require_section(raw, "Execution", path)
    mode = execution.get("Mode")
    if mode not in VALID_MODES:
        raise ConfigError(
            f"{path}: Execution.Mode must be one of {sorted(VALID_MODES)}, got {mode!r}"
        )

    limits = ExecutionLimits(
        task_timeout_seconds=_positive_int(
            execution, "Task_timeout_seconds", DEFAULT_TASK_TIMEOUT_SECONDS, path
        ),
        max_parallel_tasks=_positive_int(
            execution, "Max_parallel_tasks", DEFAULT_MAX_PARALLEL_TASKS, path
        ),
        enforce_sla=bool(execution.get("Enforce_sla", False)),
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
    orchestrator = _parse_orchestrator(raw.get("Orchestrator") or {}, path)

    email_raw = raw.get("Email")
    if email_raw is None:
        email = None
    elif not isinstance(email_raw, dict):
        raise ConfigError(f"{path}: Email section must be a mapping if present")
    else:
        email = _parse_email_section(email_raw, path)

    return ConnectorConfig(
        mode=mode,
        source=source,
        postgres=postgres,
        cloning=cloning,
        warehouse=warehouse,
        orchestrator=orchestrator,
        limits=limits,
        email=email,
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


def _parse_email_section(raw: dict[str, Any], path: Path) -> EmailConfig:
    active_profile = raw.get("Active_profile")
    profiles_raw = raw.get("Profiles")
    if not active_profile or not isinstance(profiles_raw, dict):
        raise ConfigError(f"{path}: Email needs Active_profile and a Profiles mapping")
    profiles = {
        name: _parse_email_profile(name, profile_raw or {}, path)
        for name, profile_raw in profiles_raw.items()
    }
    if active_profile not in profiles:
        raise ConfigError(
            f"{path}: Email.Active_profile {active_profile!r} has no matching entry in Profiles"
        )
    return EmailConfig(active_profile=active_profile, profiles=profiles)


def _parse_email_profile(name: str, raw: dict[str, Any], path: Path) -> EmailProfile:
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
    return EmailProfile(
        section="EMAIL",
        name=name,
        host=host,
        port=int(port),
        from_address=from_address,
        auth_mode=auth_mode,
        user=user,
        use_tls=bool(raw.get("use_tls", True)),
        extra=extra,
    )


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
