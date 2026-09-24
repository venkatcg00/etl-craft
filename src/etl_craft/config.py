"""Load and validate craft-connector.yml -- the engine's only source of connection config.

[DEVIATION, 2026-09-24] One format, written by the team and only ever read
here. Per explicit instruction: "the craft connector yaml should not be
something that the engine builds. it should be provided by user", with its
sections in this order -- ``Secrets``, ``Orchestration`` (which carries the DAG
defaults and the Email relay, per environment), ``Engine``, ``Warehouse`` --
plus the optional ``Cloning``. The two earlier layouts (``Execution``/
``Source``/``Postgres`` and ``Orchestration``/``Secrets``/``Engine`` with
``Variables`` blocks) are refused with a pointer to the example rather than
half-supported: the project has no released users to migrate.

**Every value is a variable name or a value** (2026-09-24, per explicit
instruction: "it will store the environment variable names to be mapped either
from .env or environment ... if those file/env does not have it, then use the
set value as actual value"). A setting whose text is a valid variable name, and
that the secrets source (the process environment, or the .env-style file
``Secrets`` names) defines, takes that variable's value; anything else is used
exactly as written. So ``Profile: dev`` is the profile ``dev`` unless a
variable named ``dev`` exists, and ``Profile: ETL_CRAFT_PROFILE`` is whatever
that variable holds. A profile-specific variable wins over the plain name for
that profile (``ENGINE_PROD_SECRET`` over ``ENGINE_SECRET``). Every lookup is
recorded (`ConnectorConfig.settings`), and `doctor` names each value that was
used as written but looks like a variable name -- the one way this rule can
hide a mistake.

**[CHOICE] One exception: secrets.** ``secret``, ``token`` and ``s3_secret``
must name a variable that is set; written text is never used as a secret. The
file is meant to be committed, and falling back would send a mistyped
variable name to the server as a password.

Every section that varies by environment holds one block per profile
(``dev``/``sit``/``uat``/``prod``, or any names). The active one is
``<Section>.Profile``, else ``Secrets.Profile``; a section with a single
profile block needs no selection. ``Secrets.Source_type`` and ``Secrets.Path``
are resolved against the process environment, the only source there is before
the file itself is known.
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

#: The SMTP relay's auth modes and the fields each needs. oauth is SMTP
#: XOAUTH2 with a client-credentials access token (Microsoft 365, Google).
EMAIL_AUTH_FIELDS: dict[str, tuple[str, ...]] = {
    "none": (),
    "password": ("user", "secret"),
    "oauth": ("user", "client_id", "secret", "token_url"),
}
EMAIL_VERIFIED_AUTH_MODES = frozenset({"none", "password"})

#: Profile fields that configure an auth mode rather than the connection.
AUTH_EXTRA_FIELDS = (
    "key_file",
    "cert_file",
    "client_id",
    "token_url",
    "scope",
    "issuer",
    "region",
    "role_arn",
)
# Auth modes that always present a secret; the others (none, sso, sts) need
# one only when the profile names one.
_SECRET_AUTH_MODES = frozenset({"password", "token", "oauth"})

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
# What a missing variable usually looks like when it is used as written:
# upper case with an underscore (ENGINE_USER), unlike a value (dev, local).
_LOOKS_LIKE_A_VARIABLE = re.compile(r"^[A-Z][A-Z0-9]*_[A-Z0-9_]+$")


class ConfigError(Exception):
    """Raised when craft-connector.yml is missing, malformed, or fails validation."""


@dataclass(frozen=True)
class SettingSource:
    """Where one setting's value came from: a variable, or the file as written."""

    where: str
    written: str
    variable: str | None = None

    @property
    def looks_like_a_missing_variable(self) -> bool:
        """Report whether this was used as written but reads like a variable name."""
        return self.variable is None and bool(_LOOKS_LIKE_A_VARIABLE.match(self.written))


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
        """The variable holding the relay password or OAuth client secret."""
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
    """Deployment-wide timeouts, parallelism caps and SLA enforcement."""

    task_timeout_seconds: int = DEFAULT_TASK_TIMEOUT_SECONDS
    max_parallel_tasks: int = DEFAULT_MAX_PARALLEL_TASKS
    # Opt-in (E2-23): for many teams SLA_IN_HOURS is pass-through metadata for
    # the orchestrator. On, each finished run is marked MET or BREACHED
    # against its pipeline's SLA_IN_HOURS (orchestrator.py).
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
    # Where every setting's value came from, in file order (`doctor`).
    settings: tuple[SettingSource, ...] = ()

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
    environment = _Resolver(values=os.environ, origin="the process environment", path=path)
    source = _parse_source(secrets, path, environment)
    resolver = _Resolver.from_source(source, path)
    resolver.sources.extend(environment.sources)
    global_profile = resolver.text(secrets.get("Profile"), "Secrets.Profile")

    orchestration = _profiled_settings("Orchestration", raw, global_profile, path, resolver)
    mode = _parse_mode(orchestration, path)
    limits = _parse_limits(orchestration, path)
    dag_defaults = _parse_dag_defaults(orchestration, path)
    orchestrator_name = orchestration.text("Name")
    email = _parse_email(orchestration, path, resolver)
    engine = _parse_engine(raw, global_profile, path, resolver)
    warehouse, table_format = _parse_warehouse(raw, global_profile, path, resolver)
    cloning = _parse_cloning(raw, global_profile, path, resolver)

    return ConnectorConfig(
        mode=mode,
        source=source,
        postgres=engine,
        cloning=cloning,
        warehouse=warehouse,
        warehouse_table_format=table_format,
        orchestrator=dag_defaults,
        email=email,
        limits=limits,
        config_path=path,
        orchestrator_name=orchestrator_name,
        settings=tuple(resolver.sources),
    )


# -- resolving values ----------------------------------------------------------


@dataclass
class _Resolver:
    """Resolve settings against one source: a variable's value, else the text as written."""

    values: Mapping[str, str]
    origin: str
    path: Path
    sources: list[SettingSource] = field(default_factory=list)

    @classmethod
    def from_source(cls, source: SourceConfig, path: Path) -> _Resolver:
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

    def selected_name(self, var_name: str, profile: str | None, field_name: str | None) -> str:
        """Use a profile-specific variable when it is set, else the named one."""
        if profile and field_name:
            tiered = _tiered_variable_name(var_name, profile, field_name)
            if tiered and tiered in self.values:
                return tiered
        return var_name

    def resolve(
        self, raw: Any, where: str, *, profile: str | None = None, field_name: str | None = None
    ) -> Any:
        """Return `raw` resolved: a set variable's value, else `raw` itself."""
        if isinstance(raw, list):
            return [
                self.resolve(item, f"{where}[{index}]", profile=profile, field_name=field_name)
                for index, item in enumerate(raw)
            ]
        if not isinstance(raw, str):
            return raw
        written = raw.strip()
        if _ENV_NAME.match(written):
            name = self.selected_name(written, profile, field_name)
            if name in self.values:
                self.sources.append(SettingSource(where, written, name))
                return self.values[name]
        self.sources.append(SettingSource(where, written))
        return written

    def text(
        self, raw: Any, where: str, *, profile: str | None = None, field_name: str | None = None
    ) -> str | None:
        """Resolve a setting that must be a non-empty string, or None when absent."""
        if raw is None:
            return None
        value = self.resolve(raw, where, profile=profile, field_name=field_name)
        if not isinstance(value, str) or not value.strip():
            raise ConfigError(f"{self.path}: {where} must be a non-empty string")
        return value.strip()

    def secret_name(
        self, raw: Any, where: str, *, profile: str | None, field_name: str
    ) -> str | None:
        """Return the variable a secret field names, or None when absent.

        A secret is never taken as written: the field must be a variable name.
        Whether that variable is set is checked when the secret is needed
        (`resolve_secret`), and by `doctor`.
        """
        if raw is None:
            return None
        if not isinstance(raw, str) or not _ENV_NAME.match(raw.strip()):
            raise ConfigError(
                f"{self.path}: {where} must be the name of a variable holding the secret — "
                "a secret is never written into craft-connector.yml"
            )
        name = self.selected_name(raw.strip(), profile, field_name)
        self.sources.append(SettingSource(where, raw.strip(), name))
        return name

    def hint(self, where: str) -> str:
        """Say so when `where` was used as written because its variable is not set."""
        for source in reversed(self.sources):
            if source.where == where:
                if source.looks_like_a_missing_variable:
                    return (
                        f" — no variable named {source.written} is set in {self.origin}, so "
                        "the text was used as written"
                    )
                return ""
        return ""


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


# -- sections and profiles ---------------------------------------------------


@dataclass(frozen=True)
class _Profiled:
    """One section's settings: top-level values overlaid with the active profile's block."""

    section: str
    profile: str | None
    settings: dict[str, Any]
    resolver: _Resolver
    path: Path

    def where(self, key: str) -> str:
        return f"{self.section}.{self.profile}.{key}" if self.profile else f"{self.section}.{key}"

    def value(self, key: str) -> Any:
        return self.resolver.resolve(
            self.settings.get(key), self.where(key), profile=self.profile, field_name=key
        )

    def text(self, key: str) -> str | None:
        return self.resolver.text(
            self.settings.get(key), self.where(key), profile=self.profile, field_name=key
        )


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
    resolver: _Resolver,
    *,
    nested: frozenset[str] = frozenset({"Email"}),
) -> _Profiled:
    """Select `name`'s active profile block and overlay it on the section's own values.

    A mapping-valued key is a profile block unless it is one of `nested` (a
    structured setting that lives inside a profile, like Orchestration's Email).
    The active profile is the section's own `Profile`, else `Secrets.Profile`.
    """
    section = _mapping(raw, name, path, required=name == "Orchestration")
    blocks = {
        key: value
        for key, value in section.items()
        if isinstance(value, dict) and key not in nested
    }
    base = {key: value for key, value in section.items() if key not in blocks and key != "Profile"}
    selected = resolver.text(section.get("Profile"), f"{name}.Profile") or global_profile
    if not blocks:
        return _Profiled(name, selected, base, resolver, path)
    if selected is None:
        if len(blocks) != 1:
            raise ConfigError(
                f"{path}: {name} has profiles {sorted(blocks)} but none is selected — set "
                f"{name}.Profile or Secrets.Profile"
            )
        selected = next(iter(blocks))
    if selected not in blocks:
        where = f"{name}.Profile" if "Profile" in section else "Secrets.Profile"
        raise ConfigError(
            f"{path}: {name} has no profile {selected!r} (it has {sorted(blocks)})"
            f"{resolver.hint(where)}"
        )
    return _Profiled(name, selected, {**base, **blocks[selected]}, resolver, path)


def _reject_unknown(
    settings: Mapping[str, Any], allowed: set[str] | frozenset[str], where: str, path: Path
) -> None:
    unknown = sorted(set(settings) - set(allowed))
    if unknown:
        raise ConfigError(f"{path}: unknown key(s) {unknown} in {where}")


# -- Secrets -----------------------------------------------------------------


def _parse_source(secrets: dict[str, Any], path: Path, environment: _Resolver) -> SourceConfig:
    _reject_unknown(secrets, {"Source_type", "Path", "Profile"}, "Secrets", path)
    written_type = environment.text(secrets.get("Source_type"), "Secrets.Source_type")
    source_type = (written_type or "").lower()
    if source_type not in VALID_SOURCE_TYPES:
        raise ConfigError(
            f"{path}: Secrets.Source_type must be one of {sorted(VALID_SOURCE_TYPES)}, "
            f"got {secrets.get('Source_type')!r}{environment.hint('Secrets.Source_type')}"
        )
    raw_path = environment.text(secrets.get("Path"), "Secrets.Path")
    if source_type == "environment":
        if raw_path is not None:
            raise ConfigError(f"{path}: Secrets.Path is only used with Source_type: file")
        return SourceConfig(type=source_type)
    if raw_path is None:
        raise ConfigError(f"{path}: Secrets.Path is required when Source_type is file")
    source_path = Path(raw_path).expanduser()
    if not source_path.is_absolute():
        # Relative to the manifest, never the cwd, so every task finds it.
        source_path = path.resolve().parent / source_path
    return SourceConfig(type=source_type, path=str(source_path.resolve()))


@dataclass(frozen=True)
class _Fields:
    """The fields of one connection profile block, resolved on demand."""

    block: dict[str, Any]
    where: str
    profile: str | None
    resolver: _Resolver
    path: Path

    def value(self, key: str, *, required: bool = False) -> str | None:
        """Resolve `key`: its variable's value, else the text as written."""
        raw = self.block.get(key)
        if raw is None:
            if required:
                raise ConfigError(f"{self.path}: {self.where} needs {key}")
            return None
        value = self.resolver.resolve(
            raw, f"{self.where}.{key}", profile=self.profile, field_name=key
        )
        return str(value).strip() if value is not None else None

    def secret_var(self, key: str = "secret") -> str | None:
        return self.resolver.secret_name(
            self.block.get(key), f"{self.where}.{key}", profile=self.profile, field_name=key
        )

    def hint(self, key: str) -> str:
        return self.resolver.hint(f"{self.where}.{key}")


def _auth_profile_extra(
    fields: _Fields,
    auth_mode: str,
    required: tuple[str, ...],
    *,
    user: str,
    secret_key: str = "secret",
) -> dict[str, Any]:
    """Validate an auth mode's fields and return the profile's `extra` for it."""
    path, where = fields.path, fields.where
    if "user" in required and not user:
        raise ConfigError(f"{path}: {where} needs user for auth_mode {auth_mode}")
    extra: dict[str, Any] = {}
    secret_var = fields.secret_var(secret_key)
    if "secret" in required and not secret_var:
        raise ConfigError(
            f"{path}: {where} needs {secret_key} (a variable name) for auth_mode {auth_mode}"
        )
    if secret_var:
        extra["secret_var"] = secret_var
    for name in AUTH_EXTRA_FIELDS:
        value = fields.value(name)
        if value:
            extra[name] = value
        elif name in required:
            raise ConfigError(f"{path}: {where} needs {name} for auth_mode {auth_mode}")
    return extra


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


def _parse_mode(orchestration: _Profiled, path: Path) -> str:
    _reject_unknown(orchestration.settings, _ORCHESTRATION_KEYS, "Orchestration", path)
    mode = (orchestration.text("Mode") or "").lower()
    if mode not in VALID_MODES:
        raise ConfigError(
            f"{path}: Orchestration.Mode must be local or remote, got "
            f"{orchestration.settings.get('Mode')!r}"
            f"{orchestration.resolver.hint(orchestration.where('Mode'))}"
        )
    return MODE_ALIASES.get(mode, mode)


def _whole_number(settings: _Profiled, key: str, default: int | None, path: Path) -> Any:
    value = settings.value(key)
    if value is None:
        return default
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfigError(
            f"{path}: Orchestration.{key} must be a whole number, got {value!r}"
            f"{settings.resolver.hint(settings.where(key))}"
        )
    return value


def _flag(settings: _Profiled, key: str, default: bool | None, path: Path) -> Any:
    value = settings.value(key)
    if value is None:
        return default
    return _parse_bool(value, settings.where(key), path, default=bool(default))


def _string_list(settings: _Profiled, key: str, path: Path) -> list[str] | None:
    value = settings.value(key)
    if value is None:
        return None
    if isinstance(value, str):
        # A variable (or a plain value) holding a comma-separated list.
        return [item.strip() for item in value.split(",") if item.strip()]
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ConfigError(f"{path}: Orchestration.{key} must be a list of strings")
    return value


def _parse_limits(settings: _Profiled, path: Path) -> ExecutionLimits:
    return ExecutionLimits(
        task_timeout_seconds=_whole_number(
            settings, "Task_timeout_seconds", DEFAULT_TASK_TIMEOUT_SECONDS, path
        ),
        max_parallel_tasks=_whole_number(
            settings, "Max_parallel_tasks", DEFAULT_MAX_PARALLEL_TASKS, path
        ),
        enforce_sla=_flag(settings, "Enforce_sla", False, path),
    )


def _parse_dag_defaults(settings: _Profiled, path: Path) -> OrchestratorConfig:
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


def _parse_email(orchestration: _Profiled, path: Path, resolver: _Resolver) -> EmailConfig | None:
    block = orchestration.settings.get("Email")
    if block is None:
        return None
    where = orchestration.where("Email")
    if not isinstance(block, dict):
        raise ConfigError(f"{path}: {where} must be a mapping")
    _reject_unknown(
        block,
        {"host", "port", "from_address", "auth_mode", "user", "use_tls", "secret", "scope"}
        | {"client_id", "token_url"},
        where,
        path,
    )
    fields = _Fields(block, where, orchestration.profile, resolver, path)
    port_raw = fields.value("port", required=True)
    try:
        port = int(port_raw or "")
    except ValueError as exc:
        raise ConfigError(
            f"{path}: {where}.port resolved to {port_raw!r}, not a number{fields.hint('port')}"
        ) from exc
    auth_mode = fields.value("auth_mode") or "none"
    if auth_mode not in EMAIL_AUTH_FIELDS:
        raise ConfigError(
            f"{path}: {where}.auth_mode resolved to {auth_mode!r}, which is not one of "
            f"{sorted(EMAIL_AUTH_FIELDS)}{fields.hint('auth_mode')}"
        )
    user = fields.value("user") or ""
    extra = _auth_profile_extra(fields, auth_mode, EMAIL_AUTH_FIELDS[auth_mode], user=user)
    name = orchestration.profile or "default"
    profile = EmailProfile(
        section="EMAIL",
        name=name,
        host=fields.value("host", required=True) or "",
        port=port,
        from_address=fields.value("from_address", required=True) or "",
        auth_mode=auth_mode,
        user=user or None,
        use_tls=_parse_bool(fields.value("use_tls"), f"{where}.use_tls", path, default=True),
        extra=extra,
    )
    return EmailConfig(active_profile=name, profiles={name: profile})


# -- Engine ------------------------------------------------------------------

_ENGINE_FIELDS = frozenset({"jdbc_url", "user", "auth_mode", "secret", *AUTH_EXTRA_FIELDS})
_ENGINE_NAMES = {"postgres": "postgresql", "postgresql": "postgresql", "sqlite": "sqlite"}


def _connection_block(
    name: str, raw: dict[str, Any], global_profile: str | None, path: Path, resolver: _Resolver
) -> tuple[_Profiled, dict[str, Any]] | None:
    """Return a connection section's selection and its active profile block."""
    section = _mapping(raw, name, path, required=name == "Engine")
    if not section:
        return None
    profiled = _profiled_settings(name, raw, global_profile, path, resolver)
    if profiled.profile is None or not isinstance(section.get(profiled.profile), dict):
        raise ConfigError(
            f"{path}: {name} needs at least one profile block (dev, prod, ...) holding its "
            f"connection settings — see {EXAMPLE_PATH}"
        )
    return profiled, dict(section[profiled.profile])


def _parse_engine(
    raw: dict[str, Any], global_profile: str | None, path: Path, resolver: _Resolver
) -> ConnectionSection:
    from etl_craft.dialects.engine_dialects import for_jdbc_url

    selected = _connection_block("Engine", raw, global_profile, path, resolver)
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
        raise ConfigError(f"{path}: {where}.jdbc_url: {exc}{fields.hint('jdbc_url')}") from exc

    declared = profiled.text("Name")
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
            f"takes {sorted(dialect.auth_modes)}{fields.hint('auth_mode')}"
        )
    user = (fields.value("user") or "") if auth_mode != "none" else ""
    extra = (
        _auth_profile_extra(fields, auth_mode, dialect.auth_fields[auth_mode], user=user)
        if auth_mode != "none"
        else {}
    )
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

_WAREHOUSE_FIELDS = frozenset({"jdbc_url", "user", "auth_mode", "secret", *AUTH_EXTRA_FIELDS})
# The field whose presence selects the separate-fields connection shape.
_PREFERRED_SHAPE_MARKER = {"databricks": "catalog", "snowflake": "account"}


def _parse_warehouse(
    raw: dict[str, Any], global_profile: str | None, path: Path, resolver: _Resolver
) -> tuple[ConnectionSection | None, str]:
    from etl_craft.db import ConnectionError_
    from etl_craft.dialects import warehouse_dialects
    from etl_craft.dialects.warehouse_dialects.duckdb_iceberg import PROFILE_FIELDS
    from etl_craft.warehouse import preferred_connection_url, translate_jdbc_url

    selected = _connection_block("Warehouse", raw, global_profile, path, resolver)
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
    table_format = (profiled.text("Table_format") or DEFAULT_TABLE_FORMAT).lower()
    if table_format not in VALID_TABLE_FORMATS:
        raise ConfigError(
            f"{path}: Warehouse.Table_format must be native or iceberg, got "
            f"{settings.get('Table_format')!r}"
        )
    declared = profiled.text("Name")
    expected_dialect = warehouse_dialects.NAMES.get(declared.lower()) if declared else None
    if declared and expected_dialect is None:
        names = sorted(d.display_name for d in warehouse_dialects.ALL if d.display_name)
        raise ConfigError(
            f"{path}: Warehouse.Name {declared!r} must be one of {sorted(set(names))}"
        )
    fields = _Fields(block, where, profiled.profile, resolver, path)

    marker = _PREFERRED_SHAPE_MARKER.get(expected_dialect or "")
    if marker and (marker in block or "token" in block):
        # The tested connection shape for Databricks and Snowflake: separate
        # fields, assembled into a credential-free URL here. A `token` field
        # means auth_mode token; any other mode is named in auth_mode.
        assert expected_dialect is not None
        dialect = warehouse_dialects.resolve(expected_dialect, table_format)
        preferred = [name for name in dialect.preferred_fields if name != "token"]
        _reject_unknown(
            block,
            {*dialect.preferred_fields, "auth_mode", "secret", *AUTH_EXTRA_FIELDS},
            where,
            path,
        )
        auth_mode = fields.value("auth_mode") or ("token" if "token" in block else "")
        if auth_mode not in dialect.auth_modes:
            raise ConfigError(
                f"{path}: {where}.auth_mode resolved to {auth_mode!r}; a {declared} warehouse "
                f"takes {sorted(dialect.auth_modes)}{fields.hint('auth_mode')}"
            )
        if auth_mode == "token" and "secret" in block:
            raise ConfigError(f"{path}: {where} names its token under `token`, not `secret`")
        parts = {name: fields.value(name, required=True) or "" for name in preferred}
        try:
            jdbc_url = preferred_connection_url(declared or "", parts)
        except ConnectionError_ as exc:
            raise ConfigError(f"{path}: {where}: {exc}") from exc
        extra = _auth_profile_extra(
            fields,
            auth_mode,
            dialect.auth_fields[auth_mode],
            user=parts.get("user", ""),
            secret_key="token" if auth_mode == "token" else "secret",
        )
        profile = ConnectionProfile(
            section="WAREHOUSE",
            name=profiled.profile or "",
            jdbc_url=jdbc_url,
            user=parts.get("user", ""),
            auth_mode=auth_mode,
            extra=extra,
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
        raise ConfigError(f"{path}: {where}: {exc}{fields.hint('jdbc_url')}") from exc
    actual = dialect_name.split("+", 1)[0]
    if expected_dialect and actual != expected_dialect:
        raise ConfigError(
            f"{path}: Warehouse.Name is {declared!r}, but its jdbc_url resolves to {actual!r}"
        )
    default_auth = "none" if actual == "duckdb" else ""
    auth_mode = fields.value("auth_mode") or default_auth
    if auth_mode not in dialect.auth_modes:
        label = dialect.display_name or actual
        raise ConfigError(
            f"{path}: {where}.auth_mode resolved to {auth_mode!r}; a {label} warehouse takes "
            f"{sorted(dialect.auth_modes)}{fields.hint('auth_mode')}"
        )
    user = fields.value("user") or ""
    extra = _auth_profile_extra(fields, auth_mode, dialect.auth_fields[auth_mode], user=user)
    if is_duckdb_iceberg:
        for key in PROFILE_FIELDS:
            if key == "s3_secret":
                name = fields.secret_var(key)
                if name is None:
                    continue
                value = resolver.values.get(name)
                if value is None:
                    raise ConfigError(
                        f"{path}: {where}.s3_secret names the variable {name!r}, which is not "
                        f"set in {resolver.origin}"
                    )
                extra[key] = value
                continue
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


def _parse_cloning(
    raw: dict[str, Any], global_profile: str | None, path: Path, resolver: _Resolver
) -> CloningConfig:
    profiled = _profiled_settings(
        "Cloning", raw, global_profile, path, resolver, nested=frozenset()
    )
    if not profiled.settings:
        return CloningConfig()
    _reject_unknown(
        profiled.settings,
        {"Enabled", "Scope", "External_volume", "Base_location"},
        "Cloning",
        path,
    )
    scope = (profiled.text("Scope") or "cfg").lower()
    if scope not in VALID_CLONING_SCOPES:
        raise ConfigError(
            f"{path}: Cloning.Scope must be one of {sorted(VALID_CLONING_SCOPES)}, got {scope!r}"
        )
    enabled = _parse_bool(profiled.value("Enabled"), profiled.where("Enabled"), path, default=False)
    return CloningConfig(
        enabled=enabled and scope != "none",
        scope=scope,
        external_volume=profiled.text("External_volume") or "",
        base_location=profiled.text("Base_location") or "",
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


def profile_secret(config: ConnectorConfig, profile: ConnectionProfile | EmailProfile) -> str:
    """Resolve the profile's secret, or return "" when its auth mode presents none.

    none, sts and a secret-less sso have nothing to present, so asking for a
    secret there would mean inventing a variable. A key_file profile's
    passphrase, and sso's optional client secret, are read when the profile
    names one.
    """
    if not profile_needs_secret(profile):
        return ""
    return resolve_secret(config, profile)


def profile_needs_secret(profile: ConnectionProfile | EmailProfile) -> bool:
    """Report whether connecting with `profile` reads a secret variable."""
    return profile.auth_mode in _SECRET_AUTH_MODES or bool(profile.extra.get("secret_var"))


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
