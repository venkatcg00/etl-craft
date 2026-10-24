"""Closed values match the Engine DB, configuration and public comparison documents."""

import pytest

from etl_craft.core import enums
from etl_craft.core.enums import RunStatus

pytestmark = pytest.mark.unit

EXPECTED = {
    enums.ItemAction: {"MARK", "RERUN", "CANCEL"},
    enums.ItemStatus: {
        "QUEUED",
        "RUNNING",
        "SUCCESS",
        "FAILED",
        "SKIPPED",
        "CANCEL_REQUESTED",
        "CANCELLED",
    },
    enums.CapacitySource: {"MEASURED", "DECLARED"},
    enums.TriggerMode: {"WAIT", "DETACH"},
    enums.TaskType: {"INGESTION", "ETL"},
    enums.FailureClass: {"TRANSIENT", "PERMANENT", "UNKNOWN"},
    enums.RetryOn: {"NONE", "TRANSIENT", "TRANSIENT_AND_UNKNOWN", "ANY"},
    enums.CatalogRunKind: {"SCHEDULED", "BACKFILL", "REPAIR", "OTHER"},
    enums.GapAttribute: {"object", "object kind", "column", "type", "nullability", "lineage"},
    enums.GapState: {
        "Deployed only",
        "Config only",
        "Type mismatch",
        "Nullability mismatch",
        "Object kind mismatch",
        "Lineage gap",
        "Matched",
        "Unchecked",
        "Unavailable",
        "Stale",
        "Not declared",
        "Expected before first deployment",
    },
    enums.YesNo: {"Y", "N"},
    enums.RunStatus: {"QUEUED", "IN-PROGRESS", "SUCCESS", "FAILED", "SKIPPED", "CANCELLED"},
    enums.AttemptStatus: {
        "QUEUED",
        "CLAIMED",
        "RUNNING",
        "SUCCESS",
        "FAILED",
        "TIMED_OUT",
        "CANCELLED",
        "LOST",
        "HANDED_OFF",
    },
    enums.ControlAction: {
        "HOLD",
        "RELEASE",
        "STOP",
        "ABORT",
        "RECOVER",
        "RESTART",
        "RUN_PAUSED",
        "PAUSE",
        "RESUME",
    },
    enums.Placement: {"ANYWHERE", "CONTROLLER_HOST"},
    enums.Audit: {"NONE", "REQUEST", "BY_ACTION"},
    enums.ConnectionUse: {"TASK", "PROBE", "HEALTH"},
    enums.ProcessRole: {"CONTROLLER", "UI", "RUN"},
    enums.ActionScope: {"deployment", "project", "pipeline"},
    enums.Transport: {"LOCAL", "HTTP"},
    enums.HttpMethod: {"GET", "POST"},
    enums.HttpScheme: {"http", "https"},
    enums.LineageDirection: {"BOTH", "UPSTREAM", "DOWNSTREAM"},
    enums.ControlStatus: {"REQUESTED", "ACKNOWLEDGED", "COMPLETED"},
    enums.ControlAcknowledgement: {"PROCESS", "QUERIES"},
    enums.TriggerKind: {
        "SCHEDULE",
        "MANUAL",
        "BACKFILL",
        "ORCHESTRATOR",
        "STAND_IN",
        "PIPELINE_TASK",
    },
    enums.ProtectionEvent: {"ENABLE", "BASELINE", "WRITE", "DELETE", "LOCK", "UNLOCK"},
    enums.ActorKind: {"USER", "SERVICE", "SYSTEM"},
    enums.TokenKind: {"SERVICE", "ACCESS", "REFRESH", "AGENT"},
    enums.AccountKind: {"USER", "SERVICE"},
    enums.ActionOutcome: {"REQUESTED", "REFUSED", "SUCCESS", "FAILURE"},
    enums.ApiRole: {"viewer", "operator", "developer", "admin", "agent"},
    enums.SlaStatus: {"MET", "BREACHED"},
    enums.RefreshType: {"FULL", "INCREMENTAL"},
    enums.DependencyType: {"SUCCESS", "FAILURE", "ALWAYS", "HAS_DATA"},
    enums.Handler: {
        "PYTHON",
        "SQL",
        "SQL_SCRIPT",
        "BUSINESS_RULES",
        "EMAIL_ALERT",
        "EVENT_WAIT",
        "RUN_PIPELINE",
    },
    enums.RunCondition: {"ALL", "ANY", "N"},
    enums.OffsetType: {"NUMBER", "PARTITIONS", "TEXT", "TIMESTAMP"},
    enums.SqlAction: {
        "CREATE_TABLE",
        "SETUP_TABLE",
        "OVERWRITE_TABLE",
        "APPEND_TABLE",
        "SCD1_MERGE",
        "SCD2_MERGE",
        "DROP_TABLE",
        "DELETE_ROWS",
        "CREATE_VIEW",
    },
    enums.EmailFlavour: {"FAILED", "COMPLETED_WITH_ERRORS", "SUCCESS"},
    enums.EmailTransport: {"SMTP", "SENDMAIL"},
    enums.TlsMode: {"NONE", "STARTTLS", "SSL"},
    enums.DeploymentProfile: {"LAPTOP", "CONTROLLER", "CONTROLLER_AGENTS"},
    enums.AuthMode: {"NONE", "PASSWORD", "TOKEN", "KEY_FILE", "OAUTH", "SSO", "STS"},
    enums.TableFormat: {"NATIVE", "ICEBERG"},
    enums.CloningScope: {"CFG", "AUD", "ALL", "NONE"},
    enums.SecretsSource: {"ENVIRONMENT", "FILE"},
    enums.GateOutcome: {"SATISFIED", "UNSATISFIED", "BYPASSED", "WAIT"},
    enums.WaitKind: {"DEPENDENCY", "EVENT", "PIPELINE_RUN"},
    enums.InterventionAction: {
        "MARK",
        "NEW_RUN",
        "CANCEL",
        "REOPEN",
        "RESET",
        "GATE_BYPASS",
        "IGNORE_DEPENDENCIES",
        "RERUN",
    },
    enums.GatePolicy: {"ENFORCE", "WARN", "OFF"},
    enums.AgentStatus: {"ACTIVE", "DRAINING", "GONE"},
    enums.AgentCommand: {"DRAIN", "RESUME", "STOP"},
    enums.ObjectStoreAuth: {"KEYS", "DEFAULT"},
    enums.ObjectUrlStyle: {"PATH", "VIRTUAL"},
    enums.SlotKind: {"python", "warehouse"},
    enums.DatabricksQueryStatus: {
        "QUEUED",
        "STARTED",
        "COMPILING",
        "COMPILED",
        "RUNNING",
        "CANCELED",
        "FAILED",
        "FINISHED",
    },
    enums.AgentReport: {
        "BUNDLE",
        "ITEMS",
        "ITEM_EVENT",
        "ITEM_PREPARE",
        "CANCEL",
        "START",
        "END",
        "STATE",
        "TARGET",
        "PUBLISH",
        "WATERMARK",
        "RULE_BEGIN",
        "RULE_KEYS",
        "RULE_END",
        "ALERT",
        "LOCK",
        "UNLOCK",
    },
}


@pytest.mark.parametrize("enum", EXPECTED, ids=lambda enum: enum.__name__)
def test_enum_values_match_the_stored_values(enum):
    assert {member.value for member in enum} == EXPECTED[enum]


def test_every_enum_in_the_module_is_checked():
    defined = {
        obj
        for obj in vars(enums).values()
        if isinstance(obj, type) and issubclass(obj, enums.StrEnum) and obj is not enums.StrEnum
    }
    # Action names grow with the action table, whose own test covers every member.
    assert defined - {enums.ActionName} == set(EXPECTED)


def test_members_compare_equal_to_the_raw_strings():
    assert RunStatus.IN_PROGRESS == "IN-PROGRESS"
    assert RunStatus("IN-PROGRESS") is RunStatus.IN_PROGRESS
    assert f"{RunStatus.IN_PROGRESS}" == "IN-PROGRESS"
    assert "SKIPPED" in enums.SETTLED_STATUSES


def test_status_groups():
    assert {
        RunStatus.SUCCESS,
        RunStatus.FAILED,
        RunStatus.SKIPPED,
        RunStatus.CANCELLED,
    } == enums.TERMINAL_STATUSES
    assert {RunStatus.SUCCESS, RunStatus.SKIPPED} == enums.SETTLED_STATUSES
    assert {
        RunStatus.SUCCESS,
        RunStatus.SKIPPED,
        RunStatus.IN_PROGRESS,
    } == enums.NOT_RETRYABLE_STATUSES
    assert enums.TERMINAL_STATUSES == enums.FINISHED_RUN_STATUSES


def test_a_failed_task_stays_retryable():
    assert RunStatus.FAILED in enums.TERMINAL_STATUSES
    assert RunStatus.FAILED not in enums.SETTLED_STATUSES
    assert RunStatus.FAILED not in enums.NOT_RETRYABLE_STATUSES


def test_settled_statuses_are_terminal_and_not_retryable():
    assert enums.SETTLED_STATUSES <= enums.TERMINAL_STATUSES
    assert enums.SETTLED_STATUSES <= enums.NOT_RETRYABLE_STATUSES
