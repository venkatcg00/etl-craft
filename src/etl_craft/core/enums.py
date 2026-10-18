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
    """Read a case-insensitive enum value or raise the caller's domain diagnostic."""
    try:
        return enum(value.strip().lower())
    except ValueError:
        raise error from None


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


class TriggerKind(StrEnum):
    """What started a pipeline run."""

    SCHEDULE = "SCHEDULE"
    MANUAL = "MANUAL"
    BACKFILL = "BACKFILL"
    ORCHESTRATOR = "ORCHESTRATOR"
    STAND_IN = "STAND_IN"


class ActorKind(StrEnum):
    """The kind of account responsible for an action."""

    USER = "USER"
    SERVICE = "SERVICE"
    SYSTEM = "SYSTEM"


class TokenKind(StrEnum):
    """What a stored credential is for: automation, a request, or renewing a user's access."""

    SERVICE = "SERVICE"
    ACCESS = "ACCESS"
    REFRESH = "REFRESH"


class AccountKind(StrEnum):
    """Whether a registered account represents a person or an automated caller."""

    USER = "USER"
    SERVICE = "SERVICE"


class ApiRole(StrEnum):
    """A deployment account role, each including the ones before it."""

    VIEWER = "viewer"
    OPERATOR = "operator"
    DEVELOPER = "developer"
    ADMIN = "admin"


class ActionOutcome(StrEnum):
    """Immutable action-request outcomes, separate from execution results."""

    REQUESTED = "REQUESTED"
    REFUSED = "REFUSED"


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
    BUSINESS_RULES = "BUSINESS_RULES"
    EMAIL_ALERT = "EMAIL_ALERT"
    EVENT_WAIT = "EVENT_WAIT"


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


class Mode(StrEnum):
    """Who schedules the tasks: the engine itself, or an external orchestrator."""

    LOCAL = "local"
    REMOTE = "remote"


class GatePolicy(StrEnum):
    """What a local run does with its dependencies on other pipelines: ``Dependency_gates``.

    ``enforce`` checks them and skips what they do not allow; ``warn`` checks them and runs
    anyway, recording the bypass; ``off`` does not check them, recording that too.
    """

    ENFORCE = "enforce"
    WARN = "warn"
    OFF = "off"


class AuthMode(StrEnum):
    """How a connection authenticates."""

    NONE = "none"
    PASSWORD = "password"
    TOKEN = "token"
    KEY_FILE = "key_file"
    OAUTH = "oauth"
    SSO = "sso"
    STS = "sts"


class TableFormat(StrEnum):
    """The table format the warehouse writes; ``native`` is the default."""

    NATIVE = "native"
    ICEBERG = "iceberg"


class CloningScope(StrEnum):
    """Which Engine DB tables a clone copies."""

    CFG = "cfg"
    AUD = "aud"
    ALL = "all"
    NONE = "none"


class EmailTransport(StrEnum):
    """How the mail handler delivers a message."""

    SMTP = "smtp"
    SENDMAIL = "sendmail"


class TlsMode(StrEnum):
    """How an SMTP session starts TLS."""

    NONE = "none"
    STARTTLS = "starttls"
    SSL = "ssl"


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
