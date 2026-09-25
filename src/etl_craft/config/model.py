"""The parsed contents of ``craft-connector.yml``.

Only the selected profile of each section is kept, with every value already resolved. Secrets
are kept as the names of the variables that hold them, never their values.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from etl_craft.core.enums import AuthMode, CloningScope, Mode, TableFormat

CONFIG_FILENAME = "craft-connector.yml"
EXAMPLE_PATH = "docs/craft-connector.example.yml"
DEFAULT_TASK_TIMEOUT_SECONDS = 6 * 60 * 60
DEFAULT_MAX_PARALLEL_TASKS = 8

# What a missing variable usually looks like when it is used as written: upper case with an
# underscore (ENGINE_USER), unlike an ordinary value (dev, local).
_LOOKS_LIKE_A_VARIABLE = re.compile(r"^[A-Z][A-Z0-9]*_[A-Z0-9_]+$")


@dataclass(frozen=True)
class SettingSource:
    """Where one setting's value came from: a variable, or the text as written.

    ``where`` is the setting's path in the file, such as ``Engine.dev.user``.
    """

    where: str
    written: str
    variable: str | None = None

    @property
    def looks_like_a_missing_variable(self) -> bool:
        """Whether the text was used as written although it reads like a variable name."""
        return self.variable is None and bool(_LOOKS_LIKE_A_VARIABLE.match(self.written))


@dataclass(frozen=True)
class ConnectionProfile:
    """The active Engine or Warehouse connection.

    ``extra`` holds the auth mode's own fields (``key_file``, ``client_id``, ...), the dialect's
    profile fields, and ``secret_var`` when the profile names a secret.
    """

    section: str
    name: str
    jdbc_url: str
    user: str
    auth_mode: str
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def secret_var(self) -> str:
        """The variable holding this profile's secret."""
        return _secret_var(self.section, self.name, self.extra)


@dataclass(frozen=True)
class ConnectionSection:
    """A connection section's selected profile."""

    active_profile: str
    profiles: dict[str, ConnectionProfile]

    @property
    def active(self) -> ConnectionProfile:
        """The selected profile."""
        return self.profiles[self.active_profile]


@dataclass(frozen=True)
class SourceConfig:
    """Where the variables the file names are read from: the environment, or a ``.env`` file."""

    type: str
    path: str | None = None


@dataclass(frozen=True)
class CloningConfig:
    """Copying Engine DB tables into the warehouse.

    ``external_volume`` and ``base_location`` place the copies' Iceberg storage on warehouses
    that name it explicitly (Snowflake).
    """

    enabled: bool = False
    scope: CloningScope = CloningScope.CFG
    external_volume: str = ""
    base_location: str = ""


@dataclass(frozen=True)
class EmailProfile:
    """The SMTP relay email alert tasks send through."""

    section: str
    name: str
    host: str
    port: int
    from_address: str
    auth_mode: str = AuthMode.NONE
    user: str | None = None
    use_tls: bool = True
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def secret_var(self) -> str:
        """The variable holding the relay password or OAuth client secret."""
        return _secret_var(self.section, self.name, self.extra)


@dataclass(frozen=True)
class EmailConfig:
    """The Email block of the selected Orchestration profile."""

    active_profile: str
    profiles: dict[str, EmailProfile]

    @property
    def active(self) -> EmailProfile:
        """The selected profile."""
        return self.profiles[self.active_profile]


@dataclass(frozen=True)
class DagDefaults:
    """Defaults for the DAGs ``generate-yml`` writes.

    ``None`` means not set, so the generator uses its own default. ``allow_schedule`` false
    writes ``schedule: null``, so an environment holds every pipeline but runs one only when
    triggered.
    """

    global_dag: bool = False
    catchup: bool | None = None
    tags: list[str] | None = None
    retries: int | None = None
    retry_delay_minutes: int | None = None
    depends_on_past: bool | None = None
    email_on_failure: bool | None = None
    email_recipients: list[str] | None = None
    allow_schedule: bool = True


@dataclass(frozen=True)
class ExecutionLimits:
    """Deployment-wide limits: a task's time limit (0 for none), parallel tasks and SLA alerts.

    Every run of a pipeline with ``SLA_IN_HOURS`` is marked ``MET`` or ``BREACHED``. With
    ``enforce_sla`` on, a run that misses its SLA also sends an SLA email, through the Email
    settings.
    """

    task_timeout_seconds: int = DEFAULT_TASK_TIMEOUT_SECONDS
    max_parallel_tasks: int = DEFAULT_MAX_PARALLEL_TASKS
    enforce_sla: bool = False


@dataclass(frozen=True)
class ConnectorConfig:
    """Everything ``craft-connector.yml`` configures.

    ``warehouse_table_format`` is the default for tables the engine creates; a task may choose
    another with ``CFG_TASK_PARAMETERS.TABLE_FORMAT``. ``config_path`` is where the file was
    read, so every task process reads the same one. ``settings`` records where each value
    came from, in file order, for ``doctor``.
    """

    mode: Mode
    source: SourceConfig
    engine: ConnectionSection
    cloning: CloningConfig = field(default_factory=CloningConfig)
    warehouse: ConnectionSection | None = None
    warehouse_table_format: TableFormat = TableFormat.NATIVE
    dag_defaults: DagDefaults = field(default_factory=DagDefaults)
    email: EmailConfig | None = None
    limits: ExecutionLimits = field(default_factory=ExecutionLimits)
    config_path: Path | None = None
    orchestrator_name: str | None = None
    settings: tuple[SettingSource, ...] = ()


def _secret_var(section: str, name: str, extra: dict[str, Any]) -> str:
    override = extra.get("secret_var")
    if override:
        return str(override)
    return f"ETL_CRAFT_{section}_{name}_SECRET".upper()
