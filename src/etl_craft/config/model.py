"""The parsed contents of ``craft-connector.yml``.

Only the selected profile of each section is kept, with every value already resolved. Secrets
are kept as the names of the variables that hold them, never their values.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from etl_craft.config.docs_site import external_url
from etl_craft.config.object_models import Artifacts, ObjectStore
from etl_craft.core.enums import (
    AuthMode,
    CloningScope,
    DeploymentProfile,
    EmailTransport,
    GatePolicy,
    ManagedProvider,
    RetryOn,
    SecretsSource,
    TableFormat,
    TlsMode,
)
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.resources import Sizing

CONFIG_FILENAME = "craft-connector.yml"
PROJECT_DIRNAME = "etl-craft"
SQL_SCRIPTS_DIRNAME = "sql_scripts"
PYTHON_SCRIPTS_DIRNAME = "python_scripts"
MIGRATIONS_DIRNAME = "migrations"
EXAMPLE_PATH = "docs/craft-connector.example.yml"
DEFAULT_TASK_TIMEOUT_SECONDS = 6 * 60 * 60
DEFAULT_MAX_PARALLEL_TASKS = 8
DEFAULT_GATE_WAIT_MINUTES = 60
DEFAULT_LOG_DIR = "logs"

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
    profile fields, and ``secret_var`` when the profile names a secret. ``schema`` is where the
    profile's tables live: the Engine DB's own tables, or the warehouse schema cloning writes to.
    An empty ``schema`` leaves the database's default.
    """

    section: str
    name: str
    jdbc_url: str
    user: str
    auth_mode: str
    extra: dict[str, Any] = field(default_factory=dict)
    schema: str = ""
    listen_jdbc_url: str | None = None

    @property
    def managed_provider(self) -> ManagedProvider:
        """Return the explicitly selected managed Engine DB platform."""
        return ManagedProvider(self.extra.get("managed_provider", ManagedProvider.NONE))

    @property
    def secret_var(self) -> str:
        """The variable holding this profile's secret."""
        return _secret_var(self.section, self.name, self.extra)


@dataclass(frozen=True)
class SourceConfig:
    """Where the variables the file names are read from: the environment, or a ``.env`` file."""

    type: SecretsSource
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
class DocsSiteConfig:
    """The catalog site ``generate-docs`` writes and ``publish-docs`` serves.

    ``schedule`` is when the controller writes it again, as a cron expression; ``output`` is
    its folder, ``catalog/`` in the project directory when not set. ``authtoken_var`` names the
    variable holding the ngrok authtoken,
    read only when the site is published; ``domain`` is the team's reserved ngrok domain, and
    ``allowed_ips`` the CIDR ranges ``publish-docs`` serves, every address when empty.
    ``external_url`` selects organization hosting, including its path prefix, without ngrok;
    ``trusted_proxies`` names the source networks allowed to supply catalog visitor and
    API client addresses and the API request protocol.
    """

    schedule: str | None = None
    output: Path | None = None
    authtoken_var: str | None = None
    domain: str | None = None
    allowed_ips: tuple[str, ...] = ()
    external_url: str | None = None
    trusted_proxies: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate the hosting selection for config files and Python callers alike."""
        if self.external_url is not None:
            object.__setattr__(self, "external_url", external_url(self.external_url))
            if self.domain:
                raise ConfigurationError(
                    "Docs_site.External_url and Domain select different hosting providers; "
                    "set External_url for an organization proxy or Domain for ngrok, not both"
                )
            if self.allowed_ips and not self.trusted_proxies:
                raise ConfigurationError(
                    "Docs_site.External_url with Allowed_ips has no trusted proxy; "
                    "set Trusted_proxies to the proxy's source CIDRs so visitor IPs can be checked"
                )


DEFAULT_SENDMAIL_PATH = "/usr/sbin/sendmail"


@dataclass(frozen=True)
class EmailProfile:
    """How email is sent: through an SMTP relay, or the host's own ``sendmail`` program.

    With ``transport`` ``sendmail`` the message is handed to ``sendmail_path`` (the program
    ``mailx`` and ``mail`` use), which delivers it through the host's mail system; ``host``,
    ``port`` and the login settings do not apply. ``from_address`` is the sender either way, and
    ``from_name``, when set, the name shown with it.
    """

    section: str
    name: str
    host: str
    port: int
    from_address: str
    auth_mode: str = AuthMode.NONE
    user: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    transport: str = EmailTransport.SMTP
    sendmail_path: str = DEFAULT_SENDMAIL_PATH
    from_name: str = ""
    tls_mode: str = TlsMode.STARTTLS
    ca_file: Path | None = None

    @property
    def secret_var(self) -> str:
        """The variable holding the relay password or OAuth client secret."""
        return _secret_var(self.section, self.name, self.extra)


@dataclass(frozen=True)
class ExecutionLimits:
    """Deployment-wide limits: a task's time limit, parallel tasks, SLA alerts and gate waits.

    ``task_timeout_seconds`` of 0 means no limit. ``gate_wait_minutes`` is how long a
    cross-pipeline gate waits for a running upstream, 0 for not at all. The local pool runs at
    most its measured number of task processes, further bounded by ``local_python_slots``
    and ``local_warehouse_slots``. ``max_parallel_tasks`` is the internal sized process count,
    independent of each pipeline's database limit; it is not a YAML setting.

    A process that has reached the Engine DB waits up to ``engine_db_outage_seconds`` for it to
    answer again after a restart or failover; 0 fails at the first refused connection.

    Every run of a pipeline with ``SLA_IN_HOURS`` is marked ``MET`` or ``BREACHED``. With
    ``enforce_sla`` on, a run that misses its SLA also sends an SLA email, through the Email
    settings.
    """

    task_timeout_seconds: int = DEFAULT_TASK_TIMEOUT_SECONDS
    max_parallel_tasks: int = DEFAULT_MAX_PARALLEL_TASKS
    enforce_sla: bool = False
    gate_wait_minutes: int = DEFAULT_GATE_WAIT_MINUTES
    shutdown_grace_seconds: int = 60
    engine_db_outage_seconds: int = 120
    local_python_slots: int | None = None
    local_warehouse_slots: int | None = None


@dataclass(frozen=True)
class AuthConfig:
    """Deployment login and orchestration credentials."""

    session_hours: int = 12
    oidc_issuer: str | None = None
    oidc_client_id: str | None = None
    refresh_days: int = 30
    oidc_groups_claim: str | None = None


@dataclass(frozen=True)
class Retention:
    """Periods in days; daily execution is opt-in, in the selected project's time zone."""

    run_history_days: int | None = None
    attempt_log_days: int | None = None
    consumption_days: int | None = None
    interventions_days: int | None = None
    config_history_days: int | None = None
    daily_at: str | None = None

    def __post_init__(self) -> None:
        """Protect programmatic configuration from invalid deletion cutoffs too."""
        for name in (
            "run_history_days",
            "attempt_log_days",
            "consumption_days",
            "interventions_days",
            "config_history_days",
        ):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 36500
            ):
                raise ConfigurationError(
                    f"Retention.{name.capitalize()}={value!r}; expected whole days 1..36500 "
                    "or None; set a valid period or omit it to keep history"
                )
        if self.daily_at is not None and (
            not isinstance(self.daily_at, str)
            or not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", self.daily_at)
        ):
            raise ConfigurationError(
                f"Retention.Daily_at={self.daily_at!r}; expected HH:MM; "
                "set a quoted 24-hour time or omit it for manual purge"
            )


@dataclass(frozen=True)
class ConnectorConfig:
    """Everything ``craft-connector.yml`` configures.

    ``dependency_gates`` is what a local run does with its dependencies on other pipelines.
    ``warehouse_table_format`` is the default for tables the engine creates; a task may choose
    another with ``CFG_TASK_PARAMETERS.TABLE_FORMAT``. ``log_dir`` is where each task attempt's
    log file is written. ``config_path`` is where the file was
    read, so every task process reads the same one. ``settings`` records where each value
    came from, in file order, for ``doctor``.

    The directory holding the file is the project directory (conventionally ``etl-craft/``).
    It holds the project's ``sql_scripts/``, ``python_scripts/`` and ``migrations/``.
    """

    source: SourceConfig
    engine: ConnectionProfile
    cloning: CloningConfig = field(default_factory=CloningConfig)
    docs_site: DocsSiteConfig = field(default_factory=DocsSiteConfig)
    warehouse: ConnectionProfile | None = None
    warehouse_table_format: TableFormat = TableFormat.NATIVE
    max_warehouse_queries: int | None = None
    retries: int | None = None
    email_recipients: list[str] | None = None
    allow_schedule: bool = True
    """Whether this environment's controller starts pipelines on their ``RUN_SCHEDULE``."""
    email: EmailProfile | None = None
    limits: ExecutionLimits = field(default_factory=ExecutionLimits)
    config_path: Path | None = None
    _host_config_path: Path | None = field(default=None, repr=False)
    api_address: str = "127.0.0.1:8730"
    timezone: str = "UTC"
    project: str = "default"
    db_pool_size: int = 10
    dependency_gates: GatePolicy = GatePolicy.ENFORCE
    settings: tuple[SettingSource, ...] = ()
    log_dir: Path = Path(DEFAULT_LOG_DIR)
    auth: AuthConfig = field(default_factory=AuthConfig)
    deployment: DeploymentProfile | None = None
    sizing: Sizing | None = None
    """Set at ``run`` and ``server`` startup from the measured allocation and ``deployment``."""

    artifacts: Artifacts = field(default_factory=Artifacts)
    object_stores: dict[str, ObjectStore] = field(default_factory=dict)
    retention: Retention = field(default_factory=Retention)
    retry_on: RetryOn = RetryOn.TRANSIENT

    @property
    def project_dir(self) -> Path:
        """The directory holding ``craft-connector.yml``, or the current one without a file."""
        if self.config_path is None:
            return Path.cwd()
        return self.config_path.absolute().parent

    @property
    def sql_scripts_dir(self) -> Path:
        """Where ``SCRIPT_FILE`` names are found: ``sql_scripts/`` in the project."""
        return self.project_dir / SQL_SCRIPTS_DIRNAME

    @property
    def python_scripts_dir(self) -> Path:
        """Where Python scripts are found: ``python_scripts/`` in the project."""
        return self.project_dir / PYTHON_SCRIPTS_DIRNAME

    @property
    def migrations_dir(self) -> Path:
        """The project's own Engine DB migrations: ``migrations/`` in the project."""
        return self.project_dir / MIGRATIONS_DIRNAME


def _secret_var(section: str, name: str, extra: dict[str, Any]) -> str:
    override = extra.get("secret_var")
    if override:
        return str(override)
    return f"ETL_CRAFT_{section}_{name}_SECRET".upper()


def parse_api_address(value: str) -> tuple[str, int]:
    """Validate a host and port, including bracketed IPv6 and ephemeral port zero."""
    try:
        address = urlsplit("tcp://" + value)
        host, port = address.hostname, address.port
        if (
            not host
            or port is None
            or address.username is not None
            or address.password is not None
            or address.path
            or address.query
            or address.fragment
            or not re.fullmatch(r"[a-zA-Z0-9.:%_-]+", host)
        ):
            raise ValueError(value)
    except ValueError as error:
        raise ConfigurationError(
            f"Orchestration.Api_address={value!r}; expected host:port, such as 127.0.0.1:8730"
        ) from error
    return host, port
