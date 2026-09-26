"""The closed value sets of the Engine DB schema and ``craft-connector.yml``.

Each enum's values are exactly what the Engine DB stores or the configuration file accepts, so
members compare equal to the raw strings read from either.
"""

from __future__ import annotations

from enum import StrEnum


class ActiveFlag(StrEnum):
    """``ACTIVE_FLAG`` on every ``CFG_`` row."""

    YES = "Y"
    NO = "N"


class RunStatus(StrEnum):
    """Status of a pipeline run, a task run or a business-rule run.

    ``CANCELLED`` ends a run an operator cancelled, and the tasks it stopped.
    """

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

FINISHED_RUN_STATUSES = frozenset({RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.CANCELLED})
"""Statuses of a pipeline run that has ended."""

MARKABLE_STATUSES = (RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.SKIPPED)
"""The statuses ``mark`` sets on a task or a run."""


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


class TaskType(StrEnum):
    """What a task does in its pipeline."""

    INGESTION = "INGESTION"
    ETL = "ETL"


class Handler(StrEnum):
    """The handler that runs a task."""

    PYTHON = "PYTHON"
    SQL = "SQL"
    BUSINESS_RULES = "BUSINESS_RULES"
    EMAIL_ALERT = "EMAIL_ALERT"


class RunCondition(StrEnum):
    """How many of a task's dependencies must be satisfied; a NULL condition means ``ALL``."""

    ALL = "ALL"
    ANY = "ANY"
    N = "N"


class BusinessRuleType(StrEnum):
    """What a failing business rule does to its rows."""

    INCOMPLETE = "INCOMPLETE"
    REJECT = "REJECT"
    REPORT = "REPORT"


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
