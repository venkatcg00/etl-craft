"""Closed values for storage, configuration and public operation documents.

String enum members compare equal to their serialized values.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TypeVar

from etl_craft.core.errors import EtlCraftError

E = TypeVar("E", bound=StrEnum)


class GapState(StrEnum):
    """Comparison outcomes displayed in project documentation."""

    DEPLOYED_ONLY = "Deployed only"
    CONFIG_ONLY = "Config only"
    TYPE_MISMATCH = "Type mismatch"
    NULLABILITY_MISMATCH = "Nullability mismatch"
    KIND_MISMATCH = "Object kind mismatch"
    LINEAGE_GAP = "Lineage gap"
    MATCH = "Matched"
    UNCHECKED = "Unchecked"
    UNAVAILABLE = "Unavailable"
    STALE = "Stale"
    NOT_DECLARED = "Not declared"
    EXPECTED = "Expected before first deployment"


class GapAttribute(StrEnum):
    """Attributes with independent schema or lineage coverage."""

    OBJECT = "object"
    KIND = "object kind"
    COLUMN = "column"
    TYPE = "type"
    NULLABILITY = "nullability"
    LINEAGE = "lineage"


def enum_value(enum: type[E], value: str, error: EtlCraftError) -> E:
    """Read an enum value exactly as it is written, or raise the caller's domain diagnostic."""
    for member in enum:
        if member.value == value.strip():
            return member
    raise error


class CatalogRunKind(StrEnum):
    """Mutually exclusive groups of logical runs shown in catalog metrics."""

    SCHEDULED = "SCHEDULED"
    BACKFILL = "BACKFILL"
    REPAIR = "REPAIR"
    OTHER = "OTHER"


class RunStatus(StrEnum):
    """Status of a pipeline run, a task run or a business-rule run.

    ``CANCELLED`` ends a run an operator cancelled, and the tasks it stopped.
    """

    QUEUED = "QUEUED"
    IN_PROGRESS = "IN-PROGRESS"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"


TERMINAL_STATUSES = frozenset(
    {RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.SKIPPED, RunStatus.CANCELLED}
)
"""Statuses of a task that has finished its attempt."""

SETTLED_STATUSES = frozenset({RunStatus.SUCCESS, RunStatus.SKIPPED})
"""Statuses a task never leaves within its run; a retry does not run it again."""

NOT_RETRYABLE_STATUSES = frozenset({RunStatus.SUCCESS, RunStatus.SKIPPED, RunStatus.IN_PROGRESS})
"""Statuses that keep a task out of the ready set; ``FAILED`` and never-run tasks are retried."""

FINISHED_RUN_STATUSES = TERMINAL_STATUSES
"""Statuses of a pipeline run that has ended, a run skipped on purpose included."""

MARKABLE_STATUSES = (RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.SKIPPED)
"""The statuses ``mark`` sets on a task or a run."""


class AttemptStatus(StrEnum):
    """Status of one attempt of a task run in the attempt ledger."""

    QUEUED = "QUEUED"
    CLAIMED = "CLAIMED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"
    LOST = "LOST"
    HANDED_OFF = "HANDED_OFF"


class ControlAction(StrEnum):
    """Run/task dispatch and agent controls."""

    HOLD = "HOLD"
    RELEASE = "RELEASE"
    STOP = "STOP"
    ABORT = "ABORT"
    RECOVER = "RECOVER"
    RESTART = "RESTART"
    RUN_PAUSED = "RUN_PAUSED"
    PAUSE = "PAUSE"
    RESUME = "RESUME"


DEFINITION_CONTROLS = frozenset({ControlAction.PAUSE, ControlAction.RESUME})
"""Controls of a pipeline definition rather than of one run; they are never stored as requests."""


class ControlStatus(StrEnum):
    """Accepted controls remain pending until their actual acknowledgements exist."""

    REQUESTED = "REQUESTED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    COMPLETED = "COMPLETED"


class TriggerKind(StrEnum):
    """What started a pipeline run."""

    SCHEDULE = "SCHEDULE"
    MANUAL = "MANUAL"
    BACKFILL = "BACKFILL"
    ORCHESTRATOR = "ORCHESTRATOR"
    STAND_IN = "STAND_IN"
    PIPELINE_TASK = "PIPELINE_TASK"


class ActorKind(StrEnum):
    """The kind of account responsible for an action."""

    USER = "USER"
    SERVICE = "SERVICE"
    SYSTEM = "SYSTEM"


class TokenKind(StrEnum):
    """What a stored credential is for: automation, a request, or renewing a user's access."""

    AGENT = "AGENT"
    SERVICE = "SERVICE"
    ACCESS = "ACCESS"
    REFRESH = "REFRESH"


class AccountKind(StrEnum):
    """Whether a registered account represents a person or an automated caller."""

    USER = "USER"
    SERVICE = "SERVICE"


class ConnectionUse(StrEnum):
    """Separately bounded Engine DB sessions in one serving process."""

    TASK = "TASK"
    PROBE = "PROBE"
    HEALTH = "HEALTH"


class ManagedProvider(StrEnum):
    """Platforms that mint short-lived PostgreSQL database credentials."""

    NONE = "NONE"
    SNOWFLAKE = "SNOWFLAKE"
    DATABRICKS = "DATABRICKS"


class ProcessRole(StrEnum):
    """Processes that reserve Engine DB capacity; API-only agents reserve none."""

    CONTROLLER = "CONTROLLER"
    UI = "UI"
    RUN = "RUN"


class ApiRole(StrEnum):
    """A deployment account role, each including the ones before it."""

    AGENT = "agent"
    VIEWER = "viewer"
    OPERATOR = "operator"
    DEVELOPER = "developer"
    ADMIN = "admin"


class HttpScheme(StrEnum):
    """HTTP schemes accepted from explicitly trusted proxies."""

    HTTP = "http"
    HTTPS = "https"


class ActionScope(StrEnum):
    """Whether authorization uses deployment or pipeline roles."""

    DEPLOYMENT = "deployment"
    PROJECT = "project"
    PIPELINE = "pipeline"


class ActionName(StrEnum):
    """Every action an interface can request; the value is its audit and role-policy name."""

    BUNDLE_PUBLISH = "bundle publish"
    AGENT_DOCTOR = "agent doctor"

    AGENT_REGISTER = "agent register"
    AGENT_DEREGISTER = "agent deregister"
    AGENT_CLAIM = "agent claim"
    AGENT_RENEW = "agent renew"
    AGENT_REPORT = "agent report"
    AGENT_LOG = "agent log"
    AGENT_CONTROL = "agent control"
    AGENTS = "agents"
    ITEMS = "items"
    ITEM_CONTROL = "item control"

    LOGIN = "login"
    LOGIN_METHODS = "login methods"
    LOGIN_SSO = "login sso"
    LOGIN_SSO_POLL = "login sso poll"
    REFRESH = "refresh"
    LOGOUT = "logout"
    GROUP_LIST = "group list"
    GROUP_ADD = "group add"
    GROUP_DISABLE = "group disable"
    GROUP_ADD_MEMBER = "group add-member"
    GROUP_REMOVE_MEMBER = "group remove-member"
    WHOAMI = "whoami"
    HEALTH = "health"
    PIPELINES = "pipelines"
    PIPELINE = "pipeline"
    GRAPH = "graph"
    STEPS = "steps"
    STATUS = "status"
    RUN_DETAIL = "run detail"
    CAPABILITIES = "capabilities"
    CONTROL_REQUEST = "control-request"
    EXPLAIN = "explain"
    HISTORY = "history"
    AUDIT = "audit"
    ATTEMPT = "attempt"
    LOGS = "logs"
    CATALOG_GAPS = "catalog gaps"
    VALIDATE = "validate"
    DOCTOR = "doctor"
    LINEAGE = "lineage"
    LINEAGE_REFRESH = "lineage refresh"
    CONFIG_PLAN = "config plan"
    CONFIG_EXPORT = "config export"
    CONFIG_APPLY = "config apply"
    RUN = "run"
    TRIGGER_PIPELINE = "trigger-pipeline"
    MARK = "mark"
    CONTROL = "control"
    CONTROL_BULK = "control bulk"
    RECONCILE = "reconcile"
    DATA_EDIT = "data edit"
    DATA_UNLOCK = "data unlock"
    DATA_PROVENANCE = "data provenance"
    ACCOUNT_LIST = "account list"
    ACCOUNT_ADD = "account add"
    ACCOUNT_SET_ROLE = "account set-role"
    ACCOUNT_DISABLE = "account disable"
    ACCOUNT_RESET_PASSWORD = "account reset-password"
    TOKEN_LIST = "token list"
    TOKEN_CREATE = "token create"
    TOKEN_REVOKE = "token revoke"
    GENERATE_DOCS = "generate-docs"
    DOCS_VERSION_CHECK = "docs-version check"
    DOCS_VERSION = "docs-version"
    REHASH_PLAN = "rehash plan"
    REHASH = "rehash"
    UPGRADE_TARGETS = "upgrade-targets"
    PURGE = "purge"
    CLONE = "clone"
    SETUP_GRANTS = "setup grants"
    SETUP = "setup"
    INIT_DB = "init-db"
    MIGRATE = "migrate"
    CONTROLLER = "controller"
    UI = "ui"
    PUBLISH_DOCS = "publish-docs"


class Placement(StrEnum):
    """Where an action may run: through any interface, or only on the controller's host."""

    ANYWHERE = "ANYWHERE"
    CONTROLLER_HOST = "CONTROLLER_HOST"


class Audit(StrEnum):
    """Whether an action records its request, and who records it."""

    NONE = "NONE"
    REQUEST = "REQUEST"
    BY_ACTION = "BY_ACTION"
    """The action records its own request, once the actor it acts as is proven (sign-in)."""


class Transport(StrEnum):
    """How a request reached its operation."""

    LOCAL = "LOCAL"
    HTTP = "HTTP"


class HttpMethod(StrEnum):
    """The HTTP methods the API uses: reads are GET and changes are POST."""

    GET = "GET"
    POST = "POST"


class LineageDirection(StrEnum):
    """Which way a lineage trace follows its table or column."""

    BOTH = "BOTH"
    UPSTREAM = "UPSTREAM"
    DOWNSTREAM = "DOWNSTREAM"


class ActionOutcome(StrEnum):
    """Immutable request and completion outcomes, separate from task and run results."""

    REQUESTED = "REQUESTED"
    REFUSED = "REFUSED"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class InterventionAction(StrEnum):
    """What an operator did to a run, as ``AUD_RUN_INTERVENTIONS.ACTION`` records it."""

    MARK = "MARK"
    NEW_RUN = "NEW_RUN"
    CANCEL = "CANCEL"
    REOPEN = "REOPEN"
    RESET = "RESET"
    GATE_BYPASS = "GATE_BYPASS"
    IGNORE_DEPENDENCIES = "IGNORE_DEPENDENCIES"
    RERUN = "RERUN"


class SlaStatus(StrEnum):
    """Whether a pipeline run finished within its SLA."""

    MET = "MET"
    BREACHED = "BREACHED"


class RefreshType(StrEnum):
    """How a pipeline loads its data."""

    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"


class DependencyType(StrEnum):
    """The upstream outcome a task or pipeline dependency waits for."""

    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    ALWAYS = "ALWAYS"
    HAS_DATA = "HAS_DATA"


class Handler(StrEnum):
    """The handler that runs a task."""

    PYTHON = "PYTHON"
    SQL = "SQL"
    SQL_SCRIPT = "SQL_SCRIPT"
    BUSINESS_RULES = "BUSINESS_RULES"
    EMAIL_ALERT = "EMAIL_ALERT"
    EVENT_WAIT = "EVENT_WAIT"
    RUN_PIPELINE = "RUN_PIPELINE"


class GateOutcome(StrEnum):
    """A dependency gate's judgement; ``WAIT`` is a check still waiting, and is never recorded."""

    SATISFIED = "SATISFIED"
    UNSATISFIED = "UNSATISFIED"
    BYPASSED = "BYPASSED"
    WAIT = "WAIT"


ADMITTED_OUTCOMES = frozenset({GateOutcome.SATISFIED, GateOutcome.BYPASSED})
"""Recorded gate outcomes that let the dependent task or pipeline start."""


class WaitKind(StrEnum):
    """What a persisted gate wait is waiting for."""

    DEPENDENCY = "DEPENDENCY"
    EVENT = "EVENT"
    PIPELINE_RUN = "PIPELINE_RUN"


class RunCondition(StrEnum):
    """How many of a task's dependencies must be satisfied; a NULL condition means ``ALL``."""

    ALL = "ALL"
    ANY = "ANY"
    N = "N"


class OffsetType(StrEnum):
    """The type of an incremental load's offset value."""

    NUMBER = "NUMBER"
    TEXT = "TEXT"
    TIMESTAMP = "TIMESTAMP"
    PARTITIONS = "PARTITIONS"


class SqlAction(StrEnum):
    """The write a SQL task's SELECT is wrapped in; the engine owns every write."""

    CREATE_TABLE = "CREATE_TABLE"
    SETUP_TABLE = "SETUP_TABLE"
    OVERWRITE_TABLE = "OVERWRITE_TABLE"
    APPEND_TABLE = "APPEND_TABLE"
    SCD1_MERGE = "SCD1_MERGE"
    SCD2_MERGE = "SCD2_MERGE"
    DROP_TABLE = "DROP_TABLE"
    DELETE_ROWS = "DELETE_ROWS"
    CREATE_VIEW = "CREATE_VIEW"


class EmailFlavour(StrEnum):
    """The outcome an email alert reports."""

    FAILED = "FAILED"
    COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS"
    SUCCESS = "SUCCESS"


class DeploymentProfile(StrEnum):
    """The deployment shape a team declares in ``Deployment.Profile``; never inferred."""

    LAPTOP = "LAPTOP"
    CONTROLLER = "CONTROLLER"
    CONTROLLER_AGENTS = "CONTROLLER_AGENTS"


class GatePolicy(StrEnum):
    """What a local run does with its dependencies on other pipelines: ``Dependency_gates``.

    ``enforce`` checks them and skips what they do not allow; ``warn`` checks them and runs
    anyway, recording the bypass; ``off`` does not check them, recording that too.
    """

    ENFORCE = "ENFORCE"
    WARN = "WARN"
    OFF = "OFF"


class AuthMode(StrEnum):
    """How a connection authenticates."""

    NONE = "NONE"
    PASSWORD = "PASSWORD"
    TOKEN = "TOKEN"
    KEY_FILE = "KEY_FILE"
    OAUTH = "OAUTH"
    SSO = "SSO"
    STS = "STS"


class SecretsSource(StrEnum):
    """Where ``craft-connector.yml``'s variables come from: the environment or a ``.env`` file."""

    ENVIRONMENT = "ENVIRONMENT"
    FILE = "FILE"


class TableFormat(StrEnum):
    """The table format the warehouse writes; ``native`` is the default."""

    NATIVE = "NATIVE"
    ICEBERG = "ICEBERG"


class CloningScope(StrEnum):
    """Which Engine DB tables a clone copies."""

    CFG = "CFG"
    AUD = "AUD"
    ALL = "ALL"
    NONE = "NONE"


class EmailTransport(StrEnum):
    """How the mail handler delivers a message."""

    SMTP = "SMTP"
    SENDMAIL = "SENDMAIL"


class TlsMode(StrEnum):
    """How an SMTP session starts TLS."""

    NONE = "NONE"
    STARTTLS = "STARTTLS"
    SSL = "SSL"


class YesNo(StrEnum):
    """A Y/N flag column: active, current, backfill, catch-up."""

    YES = "Y"
    NO = "N"

    @classmethod
    def of(cls, value: bool) -> YesNo:
        """Return the flag that records ``value``."""
        return cls.YES if value else cls.NO


class ProtectionEvent(StrEnum):
    """Warehouse evidence for protection and value attribution."""

    ENABLE = "ENABLE"
    BASELINE = "BASELINE"
    WRITE = "WRITE"
    DELETE = "DELETE"
    LOCK = "LOCK"
    UNLOCK = "UNLOCK"


class ControlAcknowledgement(StrEnum):
    """Independent, append-only physical observations from an owning agent."""

    PROCESS = "PROCESS"
    QUERIES = "QUERIES"


class FailureClass(StrEnum):
    """Whether repeating the same work can succeed."""

    TRANSIENT = "TRANSIENT"
    PERMANENT = "PERMANENT"
    UNKNOWN = "UNKNOWN"


class RetryOn(StrEnum):
    """Failure classes eligible for automatic retry within the retry budget."""

    NONE = "NONE"
    TRANSIENT = "TRANSIENT"
    TRANSIENT_AND_UNKNOWN = "TRANSIENT_AND_UNKNOWN"
    ANY = "ANY"

    def permits(self, failure: FailureClass) -> bool:
        """Decide eligibility independently of the number of attempts remaining."""
        return (
            self == RetryOn.ANY
            or (self != RetryOn.NONE and failure == FailureClass.TRANSIENT)
            or (self == RetryOn.TRANSIENT_AND_UNKNOWN and failure == FailureClass.UNKNOWN)
        )


class TriggerMode(StrEnum):
    """Whether a trigger task waits for its child run."""

    WAIT = "WAIT"
    DETACH = "DETACH"


class TaskType(StrEnum):
    """The execution category written on a task definition."""

    INGESTION = "INGESTION"
    ETL = "ETL"


class AgentStatus(StrEnum):
    """Whether an agent accepts claims, is draining, or has departed."""

    ACTIVE = "ACTIVE"
    DRAINING = "DRAINING"
    GONE = "GONE"


class AgentCommand(StrEnum):
    """Durable operator requests acknowledged by their agent."""

    DRAIN = "DRAIN"
    RESUME = "RESUME"
    STOP = "STOP"


class SlotKind(StrEnum):
    """The process capacity an entire attempt consumes."""

    PYTHON = "python"
    WAREHOUSE = "warehouse"


class AgentReport(StrEnum):
    """Fenced task progress sent through the agent report action."""

    START = "START"
    END = "END"
    CANCEL = "CANCEL"
    STATE = "STATE"
    TARGET = "TARGET"
    PUBLISH = "PUBLISH"
    WATERMARK = "WATERMARK"
    ITEMS = "ITEMS"
    ITEM_EVENT = "ITEM_EVENT"
    ITEM_PREPARE = "ITEM_PREPARE"
    BUNDLE = "BUNDLE"
    RULE_BEGIN = "RULE_BEGIN"
    RULE_KEYS = "RULE_KEYS"
    RULE_END = "RULE_END"
    ALERT = "ALERT"
    LOCK = "LOCK"
    UNLOCK = "UNLOCK"


class DatabricksQueryStatus(StrEnum):
    """Databricks Query History states, including compilation before execution."""

    QUEUED = "QUEUED"
    STARTED = "STARTED"
    COMPILING = "COMPILING"
    COMPILED = "COMPILED"
    RUNNING = "RUNNING"
    CANCELED = "CANCELED"
    FAILED = "FAILED"
    FINISHED = "FINISHED"


class CapacitySource(StrEnum):
    """Whether an agent measures its own host or fronts an explicitly declared quota."""

    MEASURED = "MEASURED"
    DECLARED = "DECLARED"


class ItemStatus(StrEnum):
    """Recorded item progress; cancellation is acknowledged only when execution ends."""

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"


class ItemAction(StrEnum):
    """Operator actions on one item or the failed items of one exact task run."""

    CANCEL = "CANCEL"
    RERUN = "RERUN"
    MARK = "MARK"


class ObjectStoreAuth(StrEnum):
    """Authentication supported by an S3-compatible object store."""

    KEYS = "KEYS"
    DEFAULT = "DEFAULT"


class ObjectUrlStyle(StrEnum):
    """S3 bucket addressing."""

    PATH = "PATH"
    VIRTUAL = "VIRTUAL"
