"""Load and validate craft-connector.yml -- the engine's only source of connection config.

[DEVIATION, 2026-09-24] One format, written by the team and only ever read
here. Per explicit instruction: "the craft connector yaml should not be
something that the engine builds. it should be provided by user", with its
sections in this order -- ``Secrets``, ``Orchestration`` (which now carries
the DAG defaults and the Email relay, per environment), ``Engine``,
``Warehouse`` -- plus the optional ``Cloning``. The two earlier layouts
(``Execution``/``Source``/``Postgres`` and ``Orchestration``/``Secrets``/
``Engine`` with ``Variables`` blocks) are refused with a pointer to the
example rather than half-supported: a third parser is exactly the clutter this
change removes, and the project has no released users to migrate.

Every section that varies by environment holds one block per profile
(``dev``/``sit``/``uat``/``prod``, or any names) and a ``Profile`` naming the
active one. Selection, most specific first::

    $ETL_CRAFT_<SECTION>_PROFILE   e.g. ETL_CRAFT_ENGINE_PROFILE=prod
    $ETL_CRAFT_PROFILE             one switch for every section
    <Section>.Profile              in the file
    Secrets.Profile                the file-wide default

A section with a single profile block needs no selection at all.

In ``Engine``, ``Warehouse`` and ``Email`` every value is the *name* of a
variable in the secrets source (the process environment or a .env-style
file), never the value itself -- which is what makes the file safe to commit.
One exception keeps the default deployment variable-free: a ``jdbc_url``
written literally as ``jdbc:...`` is taken as the URL (it carries no
credentials; secrets always go through variables). A tier-specific variable
(``ENGINE_PROD_SECRET``) is preferred over the plain name (``ENGINE_SECRET``)
when both exist.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path("craft-connector.yml")
CONFIG_PATH_ENV_VAR = "ETL_CRAFT_CONFIG"
CONFIG_FILENAME = "craft-connector.yml"
EXAMPLE_PATH = "docs/craft-connector.example.yml"


def resolve_config_path(explicit: Path | str | None = None) -> Path:
    """Find craft-connector.yml, most-specific first: --config, env var, then upward search.

    [ADDITION, 2026-09-20, E2-06] An Airflow BashOperator's cwd is not something
    a DAG author controls, so every command takes `--config`, honours
    `$ETL_CRAFT_CONFIG`, and otherwise searches upward from the cwd the way
    `pyproject.toml`/`.git` discovery works.
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


# `remote` is the file's name for orchestration-driven execution; the rest of
# the engine compares against `orchestrator`.
MODE_ALIASES = {"remote": "orchestrator"}
VALID_MODES = frozenset({"local", "orchestrator", "remote"})
VALID_SOURCE_TYPES = frozenset({"file", "environment"})
VALID_WAREHOUSE_AUTH_MODES = frozenset({"none", "password", "key_file", "token"})
# Modes where a `user` is not required: `none` has nobody to be, and a bearer
# token carries its own username convention (Databricks' is "token").
AUTH_MODES_WITHOUT_USER = frozenset({"none", "token"})
# [DEVIATION, 2026-09-24] `none` joined: a Cloning section may exist, profiled,
# and still clone nothing in one environment.
VALID_CLONING_SCOPES = frozenset({"cfg", "aud", "all", "none"})
VALID_TABLE_FORMATS = frozenset({"iceberg", "native"})
# [DEVIATION, 2026-09-24] `native` is the default now, per explicit
# instruction ("Default native when not set"). Postgres and DuckDB files are
# native anyway; Databricks and Snowflake create their own format unless a
# deployment or task asks for Iceberg; Trino is Iceberg whatever this says,
# because its catalog decides.
DEFAULT_TABLE_FORMAT = "native"
VALID_EMAIL_AUTH_MODES = frozenset({"none", "password"})

# Top-level sections, in the order the file is expected to present them.
SECTIONS = ("Secrets", "Orchestration", "Engine", "Warehouse", "Cloning")
_RETIRED_SECTIONS = {
    "Execution",
    "Source",
    "Postgres",
    "Dag_defaults",
    "Email",
    "Orchestrator",
}
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ConfigError(Exception):
    """Raised when craft-connector.yml is missing, malformed, or fails validation."""


@dataclass(frozen=True)
class ConnectionProfile:
    """The active Engine or Warehouse connection, with its values resolved."""

    section: str
    name: str
    jdbc_url: str
    user: str
    auth_mode: str
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def secret_var(self) -> str:
        """The variable holding this profile's secret material."""
        override = self.extra.get("secret_var")
        if override:
            return str(override)
        return f"ETL_CRAFT_{self.section}_{self.name}_SECRET".upper()


@dataclass(frozen=True)
class ConnectionSection:
    """A connection section: the active profile name plus the resolved active profile."""

    active_profile: str
    profiles: dict[str, ConnectionProfile]

    @property
    def active(self) -> ConnectionProfile:
        """Return the profile currently selected by active_profile."""
        return self.profiles[self.active_profile]


@dataclass(frozen=True)
class SourceConfig:
    """Where the variables named elsewhere in the file are read from."""

    type: str
    path: str | None = None


@dataclass(frozen=True)
class CloningConfig:
    """Merge-style copy of Engine DB tables into the warehouse."""

    enabled: bool = False
    scope: str = "cfg"
    # Where a mirrored table's Iceberg storage lives, for warehouses that name
    # it explicitly (Snowflake). Tasks carry these as CFG_TASK_PARAMETERS;
    # cloning has no task, so its own section is their home.
    external_volume: str = ""
    base_location: str = ""


@dataclass(frozen=True)
class EmailProfile:
    """The SMTP relay EMAIL_ALERT tasks send through."""

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
        """The variable holding the relay password (auth_mode='password' only)."""
        override = self.extra.get("secret_var")
        if override:
            return str(override)
        return f"ETL_CRAFT_{self.section}_{self.name}_SECRET".upper()


@dataclass(frozen=True)
class EmailConfig:
    """The active Email profile."""

    active_profile: str
    profiles: dict[str, EmailProfile]

    @property
    def active(self) -> EmailProfile:
        """Return the profile currently selected by active_profile."""
        return self.profiles[self.active_profile]


@dataclass(frozen=True)
class OrchestratorConfig:
    """DAG defaults for generate-yml, from the active Orchestration profile.

    Per-field settings default to None ("not set" -- generate-yml falls through
    to its own hardcoded default); `global_dag` defaults to False and
    `allow_schedule` to True.
    """

    global_dag: bool = False
    catchup: bool | None = None
    tags: list[str] | None = None
    retries: int | None = None
    retry_delay_minutes: int | None = None
    depends_on_past: bool | None = None
    email_on_failure: bool | None = None
    email_recipients: list[str] | None = None
    # [ADDITION, 2026-09-24] Whether generated DAGs carry their pipeline's
    # RUN_SCHEDULE. False emits `schedule: null`, so a non-production
    # environment can hold every pipeline's definition and run it only when
    # triggered.
    allow_schedule: bool = True


# [ADDITION, 2026-09-20, E2-17/E2-19] Deployment-wide operational limits: a
# hung task otherwise stays IN-PROGRESS -- permanently un-retryable -- and a
# 40-task wave spawns 40 processes at once.
DEFAULT_TASK_TIMEOUT_SECONDS = 6 * 60 * 60
DEFAULT_MAX_PARALLEL_TASKS = 8


@dataclass(frozen=True)
class ExecutionLimits:
    """Deployment-wide timeouts and parallelism caps."""

    task_timeout_seconds: int = DEFAULT_TASK_TIMEOUT_SECONDS
    max_parallel_tasks: int = DEFAULT_MAX_PARALLEL_TASKS
    # Engine-side SLA enforcement is opt-in (E2-23): for many teams
    # SLA_IN_HOURS is pass-through metadata for the orchestrator.
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
    # [ADDITION, 2026-09-22, E2-78] Where this config was read from, so every
    # spawned task re-reads the same file. None for configs built in memory.
    config_path: Path | None = None
    orchestrator_name: str | None = None

    @property
    def engine(self) -> ConnectionSection:
        """The Engine section (historically named `postgres` on this object)."""
        return self.postgres


def load_config(path: Path | str = DEFAULT_CONFIG_PATH) -> ConnectorConfig:
    """Read, parse, and validate craft-connector.yml at `path`."""
    path = Path(path)
    if not path.is_file():
        raise ConfigError(
            f"craft-connector.yml not found at {path} — write one (see {EXAMPLE_PATH}); "
            "etl-craft reads it and never writes it"
        )
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path} is not valid YAML: {exc}") from exc
    return parse_config(raw, path)


def parse_config(raw: Any, path: Path) -> ConnectorConfig:
    """Parse an already-loaded craft-connector.yml mapping."""
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: craft-connector.yml must contain a top-level mapping")
    retired = sorted(_RETIRED_SECTIONS.intersection(raw))
    if not retired and any(isinstance(v, dict) and "Variables" in v for v in raw.values()):
        retired = ["Variables blocks"]
    if retired:
        raise ConfigError(
            f"{path}: this is an earlier craft-connector.yml layout ({', '.join(retired)}). "
            "The current file has Secrets, Orchestration (with the DAG defaults and Email "
            "inside it), Engine, Warehouse and Cloning, each with one block per profile — "
            f"see {EXAMPLE_PATH}"
        )
    unknown = sorted(set(raw) - set(SECTIONS))
    if unknown:
        raise ConfigError(
            f"{path}: unknown top-level section(s) {unknown} — expected {list(SECTIONS)}"
        )
    # The order is part of the format, per the same instruction: a reader
    # finds where secrets come from before anything that names one.
    present = [section for section in raw if section in SECTIONS]
    expected = [section for section in SECTIONS if section in raw]
    if present != expected:
        raise ConfigError(
            f"{path}: sections are in the order {present}; write them as {expected} "
            "(Secrets, Orchestration, Engine, Warehouse, then Cloning)"
        )

    secrets = _mapping(raw, "Secrets", path, required=True)
    source = _parse_source(secrets, path)
    global_profile = _optional_str(secrets, "Profile", "Secrets", path)
    resolver = _VariableResolver.from_source(source, path)

    orchestration = _profiled_settings("Orchestration", raw, global_profile, path, required=True)
    mode = _parse_mode(orchestration.settings, path)
    engine = _parse_engine(raw, global_profile, path, resolver)
    warehouse, table_format = _parse_warehouse(raw, global_profile, path, resolver)
    cloning = _parse_cloning(raw, global_profile, path)
    email = _parse_email(orchestration, path, resolver)

    return ConnectorConfig(
        mode=mode,
        source=source,
        postgres=engine,
        cloning=cloning,
        warehouse=warehouse,
        warehouse_table_format=table_format,
        orchestrator=_parse_dag_defaults(orchestration.settings, path),
        email=email,
        limits=_parse_limits(orchestration.settings, path),
        config_path=path,
        orchestrator_name=_optional_str(orchestration.settings, "Name", "Orchestration", path),
    )


# -- sections and profiles ---------------------------------------------------


@dataclass(frozen=True)
class _Profiled:
    """One section's settings: top-level values overlaid with the active profile's block."""

    section: str
    profile: str | None
    settings: dict[str, Any]


def _mapping(raw: dict[str, Any], name: str, path: Path, *, required: bool) -> dict[str, Any]:
    section = raw.get(name)
    if section is None:
        if required:
            raise ConfigError(f"{path}: missing the {name} section (see {EXAMPLE_PATH})")
        return {}
    if not isinstance(section, dict):
        raise ConfigError(f"{path}: {name} must be a mapping")
    return section


def _profiled_settings(
    name: str,
    raw: dict[str, Any],
    global_profile: str | None,
    path: Path,
    *,
    required: bool = False,
    nested: frozenset[str] = frozenset({"Email"}),
) -> _Profiled:
    """Select `name`'s active profile block and overlay it on the section's own values.

    A mapping-valued key is a profile block unless it is one of `nested` (a
    structured setting that lives inside a profile, like Orchestration's Email).
    """
    section = _mapping(raw, name, path, required=required)
    blocks = {
        key: value
        for key, value in section.items()
        if isinstance(value, dict) and key not in nested
    }
    base = {key: value for key, value in section.items() if key not in blocks and key != "Profile"}
    selected = (
        os.environ.get(f"ETL_CRAFT_{name.upper()}_PROFILE")
        or os.environ.get("ETL_CRAFT_PROFILE")
        or _optional_str(section, "Profile", name, path)
        or global_profile
    )
    if not blocks:
        return _Profiled(section=name, profile=selected, settings=base)
    if selected is None:
        if len(blocks) != 1:
            raise ConfigError(
                f"{path}: {name} has profiles {sorted(blocks)} but none is selected — set "
                f"{name}.Profile, Secrets.Profile, or $ETL_CRAFT_PROFILE"
            )
        selected = next(iter(blocks))
    if selected not in blocks:
        raise ConfigError(f"{path}: {name} has no profile {selected!r} (it has {sorted(blocks)})")
    return _Profiled(section=name, profile=selected, settings={**base, **blocks[selected]})


def _optional_str(section: Mapping[str, Any], key: str, where: str, path: Path) -> str | None:
    value = section.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{path}: {where}.{key} must be a non-empty string")
    return value.strip()


def _reject_unknown(
    settings: Mapping[str, Any], allowed: set[str] | frozenset[str], where: str, path: Path
) -> None:
    unknown = sorted(set(settings) - set(allowed))
    if unknown:
        raise ConfigError(f"{path}: unknown key(s) {unknown} in {where}")


# -- Secrets -----------------------------------------------------------------


def _parse_source(secrets: dict[str, Any], path: Path) -> SourceConfig:
    _reject_unknown(secrets, {"Source_type", "Path", "Profile"}, "Secrets", path)
    source_type = str(secrets.get("Source_type", "")).strip().lower()
    if source_type not in VALID_SOURCE_TYPES:
        raise ConfigError(
            f"{path}: Secrets.Source_type must be one of {sorted(VALID_SOURCE_TYPES)}, "
            f"got {secrets.get('Source_type')!r}"
        )
    raw_path = secrets.get("Path")
    if source_type == "environment":
        if raw_path is not None:
            raise ConfigError(f"{path}: Secrets.Path is only used with Source_type: file")
        return SourceConfig(type=source_type)
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise ConfigError(f"{path}: Secrets.Path is required when Source_type is file")
    source_path = Path(raw_path).expanduser()
    if not source_path.is_absolute():
        # Relative to the manifest, never the cwd, so every task finds it.
        source_path = path.resolve().parent / source_path
    return SourceConfig(type=source_type, path=str(source_path.resolve()))


@dataclass(frozen=True)
class _VariableResolver:
    """Read named values from the one configured secrets source."""

    values: Mapping[str, str]
    origin: str
    path: Path

    @classmethod
    def from_source(cls, source: SourceConfig, path: Path) -> _VariableResolver:
        if source.type == "file":
            return cls(
                values=_load_dotenv_file(source.path),
                origin=f"{source.path} (Secrets.Source_type: file)",
                path=path,
            )
        return cls(
            values=os.environ,
            origin="the process environment (Secrets.Source_type: environment)",
            path=path,
        )

    def selected_name(self, var_name: str, profile: str | None, field_name: str) -> str:
        """Use a tier-specific variable when present, then fall back to the named one."""
        if profile:
            tiered = _tiered_variable_name(var_name, profile, field_name)
            if tiered and tiered in self.values:
                return tiered
        return var_name

    def lookup(self, var_name: str, profile: str | None, field_name: str) -> str | None:
        return self.values.get(self.selected_name(var_name, profile, field_name))


def _tiered_variable_name(var_name: str, profile: str, field_name: str) -> str | None:
    """Insert ``profile`` before a recognised field suffix: ENGINE_SECRET -> ENGINE_DEV_SECRET."""
    suffixes = [field_name.upper()]
    if field_name == "from_address":
        suffixes.append("FROM")
    upper_name = var_name.upper()
    upper_profile = profile.upper()
    for suffix in suffixes:
        plain_suffix = f"_{suffix}"
        if upper_name.endswith(f"_{upper_profile}{plain_suffix}"):
            return var_name
        if upper_name.endswith(plain_suffix):
            return f"{var_name[:-len(plain_suffix)]}_{upper_profile}{var_name[-len(plain_suffix):]}"
    return None


@dataclass(frozen=True)
class _Fields:
    """The fields of one connection profile block, resolved on demand."""

    block: dict[str, Any]
    where: str
    profile: str | None
    resolver: _VariableResolver
    path: Path

    def name_of(self, key: str) -> str | None:
        value = self.block.get(key)
        if value is None:
            return None
        if not isinstance(value, str) or not _ENV_NAME.match(value.strip()):
            raise ConfigError(
                f"{self.path}: {self.where}.{key} must be the name of a variable, got {value!r}"
            )
        return value.strip()

    def value(self, key: str, *, required: bool = False) -> str | None:
        """Resolve `key`'s variable; a literal `jdbc:` URL is taken as written."""
        raw = self.block.get(key)
        if key == "jdbc_url" and isinstance(raw, str) and raw.strip().lower().startswith("jdbc:"):
            return raw.strip()
        name = self.name_of(key)
        if name is None:
            if required:
                raise ConfigError(f"{self.path}: {self.where} needs {key}")
            return None
        resolved = self.resolver.lookup(name, self.profile, key)
        if resolved is None and required:
            raise ConfigError(
                f"{self.path}: {self.where}.{key} names the variable {name!r}, which is not set "
                f"in {self.resolver.origin}"
            )
        return resolved

    def secret_var(self, key: str = "secret") -> str | None:
        name = self.name_of(key)
        return self.resolver.selected_name(name, self.profile, key) if name else None


# -- Orchestration -----------------------------------------------------------

_ORCHESTRATION_KEYS = frozenset(
    {
        "Mode",
        "Name",
        "Task_timeout_seconds",
        "Max_parallel_tasks",
        "Enforce_sla",
        "Global_dag",
        "Catchup",
        "Tags",
        "Retries",
        "Retry_delay_minutes",
        "Depends_on_past",
        "Email_on_failure",
        "Email_recipients",
        "Allow_schedule",
        "Email",
    }
)


def _parse_mode(settings: dict[str, Any], path: Path) -> str:
    _reject_unknown(settings, _ORCHESTRATION_KEYS, "Orchestration", path)
    mode = str(settings.get("Mode", "")).strip().lower()
    if mode not in VALID_MODES:
        raise ConfigError(
            f"{path}: Orchestration.Mode must be local or remote, got {settings.get('Mode')!r}"
        )
    return MODE_ALIASES.get(mode, mode)


def _whole_number(settings: dict[str, Any], key: str, default: int | None, path: Path) -> Any:
    value = settings.get(key)
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfigError(f"{path}: Orchestration.{key} must be a whole number, got {value!r}")
    return value


def _flag(settings: dict[str, Any], key: str, default: bool | None, path: Path) -> Any:
    value = settings.get(key)
    if value is None:
        return default
    return _parse_bool(value, f"Orchestration.{key}", path, default=bool(default))


def _string_list(settings: dict[str, Any], key: str, path: Path) -> list[str] | None:
    value = settings.get(key)
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ConfigError(f"{path}: Orchestration.{key} must be a list of strings")
    return value


def _parse_limits(settings: dict[str, Any], path: Path) -> ExecutionLimits:
    return ExecutionLimits(
        task_timeout_seconds=_whole_number(
            settings, "Task_timeout_seconds", DEFAULT_TASK_TIMEOUT_SECONDS, path
        ),
        max_parallel_tasks=_whole_number(
            settings, "Max_parallel_tasks", DEFAULT_MAX_PARALLEL_TASKS, path
        ),
        enforce_sla=_flag(settings, "Enforce_sla", False, path),
    )


def _parse_dag_defaults(settings: dict[str, Any], path: Path) -> OrchestratorConfig:
    return OrchestratorConfig(
        global_dag=_flag(settings, "Global_dag", False, path),
        catchup=_flag(settings, "Catchup", None, path),
        tags=_string_list(settings, "Tags", path),
        retries=_whole_number(settings, "Retries", None, path),
        retry_delay_minutes=_whole_number(settings, "Retry_delay_minutes", None, path),
        depends_on_past=_flag(settings, "Depends_on_past", None, path),
        email_on_failure=_flag(settings, "Email_on_failure", None, path),
        email_recipients=_string_list(settings, "Email_recipients", path),
        allow_schedule=_flag(settings, "Allow_schedule", True, path),
    )


def _parse_email(
    orchestration: _Profiled, path: Path, resolver: _VariableResolver
) -> EmailConfig | None:
    block = orchestration.settings.get("Email")
    if block is None:
        return None
    where = f"Orchestration.{orchestration.profile or ''}.Email".replace("..", ".")
    if not isinstance(block, dict):
        raise ConfigError(f"{path}: {where} must be a mapping")
    _reject_unknown(
        block,
        {"host", "port", "from_address", "auth_mode", "user", "use_tls", "secret"},
        where,
        path,
    )
    fields = _Fields(block, where, orchestration.profile, resolver, path)
    port_raw = fields.value("port", required=True)
    try:
        port = int(port_raw or "")
    except ValueError as exc:
        raise ConfigError(f"{path}: {where}.port resolved to {port_raw!r}, not a number") from exc
    auth_mode = fields.value("auth_mode") or "none"
    if auth_mode not in VALID_EMAIL_AUTH_MODES:
        raise ConfigError(
            f"{path}: {where}.auth_mode resolved to {auth_mode!r}, which is not one of "
            f"{sorted(VALID_EMAIL_AUTH_MODES)}"
        )
    user = fields.value("user")
    secret_var = fields.secret_var()
    if auth_mode == "password" and (not user or not secret_var):
        raise ConfigError(f"{path}: {where} needs user and secret for auth_mode password")
    name = orchestration.profile or "default"
    profile = EmailProfile(
        section="EMAIL",
        name=name,
        host=fields.value("host", required=True) or "",
        port=port,
        from_address=fields.value("from_address", required=True) or "",
        auth_mode=auth_mode,
        user=user,
        use_tls=_parse_bool(fields.value("use_tls"), f"{where}.use_tls", path, default=True),
        extra={"secret_var": secret_var} if secret_var else {},
    )
    return EmailConfig(active_profile=name, profiles={name: profile})


# -- Engine ------------------------------------------------------------------

_ENGINE_FIELDS = frozenset({"jdbc_url", "user", "auth_mode", "secret", "key_file"})
_ENGINE_NAMES = {"postgres": "postgresql", "postgresql": "postgresql", "sqlite": "sqlite"}


def _connection_block(
    name: str, raw: dict[str, Any], global_profile: str | None, path: Path, *, required: bool
) -> tuple[_Profiled, dict[str, Any]] | None:
    """Return a connection section's selection and its active profile block."""
    section = _mapping(raw, name, path, required=required)
    if not section:
        return None
    profiled = _profiled_settings(name, raw, global_profile, path, required=required)
    if profiled.profile is None or not isinstance(section.get(profiled.profile), dict):
        raise ConfigError(
            f"{path}: {name} needs at least one profile block (dev, prod, ...) holding its "
            f"connection variables — see {EXAMPLE_PATH}"
        )
    return profiled, dict(section[profiled.profile])


def _parse_engine(
    raw: dict[str, Any], global_profile: str | None, path: Path, resolver: _VariableResolver
) -> ConnectionSection:
    from etl_craft.dialects.engine_dialects import for_jdbc_url

    selected = _connection_block("Engine", raw, global_profile, path, required=True)
    assert selected is not None
    profiled, block = selected
    where = f"Engine.{profiled.profile}"
    block.pop("Name", None)  # a per-profile Name is already in profiled.settings
    _reject_unknown(block, _ENGINE_FIELDS, where, path)
    _reject_unknown(
        {k: v for k, v in profiled.settings.items() if k == "Name" or k not in block},
        {"Name"},
        "Engine",
        path,
    )
    fields = _Fields(block, where, profiled.profile, resolver, path)
    jdbc_url = fields.value("jdbc_url", required=True) or ""
    try:
        dialect = for_jdbc_url(jdbc_url)
    except ValueError as exc:
        raise ConfigError(f"{path}: {where}.jdbc_url: {exc}") from exc

    declared = _optional_str(profiled.settings, "Name", "Engine", path)
    if declared and _ENGINE_NAMES.get(declared.lower()) != dialect.name:
        raise ConfigError(
            f"{path}: Engine.Name is {declared!r}, but its jdbc_url is a {dialect.name} URL"
        )

    # SQLite has nothing to authenticate: auth_mode is `none` whether or not
    # the block names one, and any other value is a mistake worth saying.
    default_auth = "none" if dialect.auth_modes == frozenset({"none"}) else None
    auth_mode = fields.value("auth_mode") or default_auth
    if auth_mode not in dialect.auth_modes:
        raise ConfigError(
            f"{path}: {where}.auth_mode resolved to {auth_mode!r}; a {dialect.name} Engine DB "
            f"takes {sorted(dialect.auth_modes)}"
        )
    extra: dict[str, Any] = {}
    user = ""
    if auth_mode != "none":
        user = fields.value("user", required=True) or ""
        secret_var = fields.secret_var()
        if not secret_var:
            raise ConfigError(f"{path}: {where} needs secret for auth_mode {auth_mode}")
        extra["secret_var"] = secret_var
    if auth_mode == "key_file":
        extra["key_file"] = fields.value("key_file", required=True)
    profile = ConnectionProfile(
        section="ENGINE",
        name=profiled.profile or "",
        jdbc_url=jdbc_url,
        user=user,
        auth_mode=auth_mode,
        extra=extra,
    )
    return ConnectionSection(active_profile=profile.name, profiles={profile.name: profile})


# -- Warehouse ---------------------------------------------------------------

_WAREHOUSE_FIELDS = frozenset({"jdbc_url", "user", "auth_mode", "secret", "key_file"})


def _parse_warehouse(
    raw: dict[str, Any], global_profile: str | None, path: Path, resolver: _VariableResolver
) -> tuple[ConnectionSection | None, str]:
    from etl_craft.db import ConnectionError_
    from etl_craft.dialects import warehouse_dialects
    from etl_craft.dialects.warehouse_dialects.duckdb_iceberg import PROFILE_FIELDS
    from etl_craft.warehouse import preferred_connection_url, translate_jdbc_url

    selected = _connection_block("Warehouse", raw, global_profile, path, required=False)
    if selected is None:
        return None, DEFAULT_TABLE_FORMAT
    profiled, block = selected
    where = f"Warehouse.{profiled.profile}"
    # Name and Table_format may sit on the section or on one profile (a dev
    # DuckDB beside a prod Postgres); the profile's own value wins.
    for key in ("Name", "Table_format"):
        block.pop(key, None)
    settings = {
        k: v
        for k, v in profiled.settings.items()
        if k in {"Name", "Table_format"} or k not in block
    }
    _reject_unknown(settings, {"Name", "Table_format"}, "Warehouse", path)
    table_format = str(settings.get("Table_format") or DEFAULT_TABLE_FORMAT).strip().lower()
    if table_format not in VALID_TABLE_FORMATS:
        raise ConfigError(
            f"{path}: Warehouse.Table_format must be native or iceberg, got "
            f"{settings.get('Table_format')!r}"
        )
    declared = _optional_str(settings, "Name", "Warehouse", path)
    expected_dialect = warehouse_dialects.NAMES.get(declared.lower()) if declared else None
    if declared and expected_dialect is None:
        names = sorted(d.display_name for d in warehouse_dialects.ALL if d.display_name)
        raise ConfigError(
            f"{path}: Warehouse.Name {declared!r} must be one of {sorted(set(names))}"
        )
    fields = _Fields(block, where, profiled.profile, resolver, path)

    if "token" in block and expected_dialect in {"databricks", "snowflake"}:
        # The tested connection shape for Databricks and Snowflake: separate
        # fields and a token, assembled into a credential-free URL here.
        dialect = warehouse_dialects.resolve(expected_dialect, "native")
        _reject_unknown(block, set(dialect.preferred_fields) | {"auth_mode"}, where, path)
        if (fields.value("auth_mode") or "token") != "token":
            raise ConfigError(f"{path}: {where} is a token connection; auth_mode must be token")
        parts = {
            key: fields.value(key, required=True) or ""
            for key in dialect.preferred_fields
            if key != "token"
        }
        try:
            jdbc_url = preferred_connection_url(declared or "", parts)
        except ConnectionError_ as exc:
            raise ConfigError(f"{path}: {where}: {exc}") from exc
        profile = ConnectionProfile(
            section="WAREHOUSE",
            name=profiled.profile or "",
            jdbc_url=jdbc_url,
            user=parts.get("user", ""),
            auth_mode="token",
            extra={"secret_var": fields.secret_var("token")},
        )
        return (
            ConnectionSection(active_profile=profile.name, profiles={profile.name: profile}),
            table_format,
        )
    if "token" in block:
        raise ConfigError(
            f"{path}: {where}.token is only for Warehouse.Name Databricks or Snowflake"
        )

    is_duckdb_iceberg = expected_dialect == "duckdb" and table_format == "iceberg"
    allowed = set(_WAREHOUSE_FIELDS) | (set(PROFILE_FIELDS) if is_duckdb_iceberg else set())
    _reject_unknown(block, allowed, where, path)
    jdbc_url = fields.value("jdbc_url", required=True) or ""
    try:
        dialect_name, _ = translate_jdbc_url(jdbc_url)
        dialect = warehouse_dialects.resolve(dialect_name, table_format)
    except (ConnectionError_, warehouse_dialects.UnsupportedWarehouse) as exc:
        raise ConfigError(f"{path}: {where}: {exc}") from exc
    actual = dialect_name.split("+", 1)[0]
    if expected_dialect and actual != expected_dialect:
        raise ConfigError(
            f"{path}: Warehouse.Name is {declared!r}, but its jdbc_url resolves to {actual!r}"
        )
    default_auth = "none" if actual == "duckdb" else None
    auth_mode = fields.value("auth_mode") or default_auth
    if auth_mode not in VALID_WAREHOUSE_AUTH_MODES:
        raise ConfigError(
            f"{path}: {where}.auth_mode resolved to {auth_mode!r}; expected one of "
            f"{sorted(VALID_WAREHOUSE_AUTH_MODES)}"
        )
    user = fields.value("user") or ""
    if auth_mode not in AUTH_MODES_WITHOUT_USER and not user:
        raise ConfigError(f"{path}: {where} needs user for auth_mode {auth_mode}")
    extra: dict[str, Any] = {}
    if auth_mode != "none":
        secret_var = fields.secret_var()
        if not secret_var:
            raise ConfigError(f"{path}: {where} needs secret for auth_mode {auth_mode}")
        extra["secret_var"] = secret_var
    if auth_mode == "key_file":
        # Refused here rather than at the first connection: how a private key
        # reaches a driver is vendor-specific, and only Snowflake's is built.
        if dialect.key_file_connect_args is None:
            raise ConfigError(
                f"{path}: {where}.auth_mode resolved to key_file, which is implemented only for "
                f"a Snowflake warehouse, not {actual!r}"
            )
        extra["key_file"] = fields.value("key_file", required=True)
    if is_duckdb_iceberg:
        for key in PROFILE_FIELDS:
            value = fields.value(key)
            if value is not None:
                extra[key] = value
    profile = ConnectionProfile(
        section="WAREHOUSE",
        name=profiled.profile or "",
        jdbc_url=jdbc_url,
        user=user,
        auth_mode=auth_mode,
        extra=extra,
    )
    return (
        ConnectionSection(active_profile=profile.name, profiles={profile.name: profile}),
        table_format,
    )


# -- Cloning -----------------------------------------------------------------


def _parse_cloning(raw: dict[str, Any], global_profile: str | None, path: Path) -> CloningConfig:
    profiled = _profiled_settings("Cloning", raw, global_profile, path, nested=frozenset())
    settings = profiled.settings
    if not settings:
        return CloningConfig()
    _reject_unknown(
        settings, {"Enabled", "Scope", "External_volume", "Base_location"}, "Cloning", path
    )
    scope = str(settings.get("Scope", "cfg")).strip().lower()
    if scope not in VALID_CLONING_SCOPES:
        raise ConfigError(
            f"{path}: Cloning.Scope must be one of {sorted(VALID_CLONING_SCOPES)}, got {scope!r}"
        )
    enabled = _parse_bool(settings.get("Enabled"), "Cloning.Enabled", path, default=False)
    return CloningConfig(
        enabled=enabled and scope != "none",
        scope=scope,
        external_volume=str(settings.get("External_volume") or ""),
        base_location=str(settings.get("Base_location") or ""),
    )


# -- helpers -----------------------------------------------------------------


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


def resolve_secret(config: ConnectorConfig, profile: ConnectionProfile | EmailProfile) -> str:
    """Resolve `profile`'s secret material from the configured secrets source."""
    var_name = profile.secret_var
    if config.source.type == "environment":
        value = os.environ.get(var_name)
    else:
        value = _load_dotenv_file(config.source.path).get(var_name)
    if value is None:
        raise ConfigError(
            f"secret {var_name!r} not found (Secrets.Source_type: {config.source.type})"
        )
    return value


def _load_dotenv_file(path: str | None) -> dict[str, str]:
    """Parse a minimal .env-style file: KEY=VALUE per line, '#' comments, blank lines ignored.

    No escape sequences, no multi-line values, and `#` only as a whole-line
    comment. A value may be wrapped in one matching pair of quotes, which is
    removed (E2-86).
    """
    if not path:
        raise ConfigError("Secrets.Path is required when Source_type is file")
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
    """Remove one matching pair of wrapping quotes, and only that."""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value
