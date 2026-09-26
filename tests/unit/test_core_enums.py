"""The enums hold exactly the values of the Engine DB CHECK constraints and the config file."""

import pytest

from etl_craft.core import enums
from etl_craft.core.enums import RunStatus

pytestmark = pytest.mark.unit

EXPECTED = {
    enums.ActiveFlag: {"Y", "N"},
    enums.RunStatus: {"IN-PROGRESS", "SUCCESS", "FAILED", "SKIPPED", "CANCELLED"},
    enums.SlaStatus: {"MET", "BREACHED"},
    enums.RefreshType: {"FULL", "INCREMENTAL"},
    enums.DependencyType: {"SUCCESS", "FAILURE", "ALWAYS", "HAS_DATA"},
    enums.TaskType: {"INGESTION", "ETL"},
    enums.Handler: {"PYTHON", "SQL", "BUSINESS_RULES", "EMAIL_ALERT"},
    enums.RunCondition: {"ALL", "ANY", "N"},
    enums.BusinessRuleType: {"INCOMPLETE", "REJECT", "REPORT"},
    enums.OffsetType: {"NUMBER", "TEXT", "TIMESTAMP"},
    enums.SqlAction: {
        "CREATE_TABLE",
        "SETUP_TABLE",
        "OVERWRITE_TABLE",
        "APPEND_TABLE",
        "SCD1_MERGE",
        "SCD2_MERGE",
        "DROP_TABLE",
        "DELETE_ROWS",
    },
    enums.EmailFlavour: {"FAILED", "COMPLETED_WITH_ERRORS", "SUCCESS"},
    enums.Mode: {"local", "remote"},
    enums.AuthMode: {"none", "password", "token", "key_file", "oauth", "sso", "sts"},
    enums.TableFormat: {"native", "iceberg"},
    enums.CloningScope: {"cfg", "aud", "all", "none"},
    enums.InterventionAction: {"MARK", "NEW_RUN", "CANCEL", "REOPEN", "RESET"},
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
    assert defined == set(EXPECTED)


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
    assert {RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.CANCELLED} == enums.FINISHED_RUN_STATUSES


def test_a_failed_task_stays_retryable():
    assert RunStatus.FAILED in enums.TERMINAL_STATUSES
    assert RunStatus.FAILED not in enums.SETTLED_STATUSES
    assert RunStatus.FAILED not in enums.NOT_RETRYABLE_STATUSES


def test_settled_statuses_are_terminal_and_not_retryable():
    assert enums.SETTLED_STATUSES <= enums.TERMINAL_STATUSES
    assert enums.SETTLED_STATUSES <= enums.NOT_RETRYABLE_STATUSES
