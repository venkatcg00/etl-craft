"""The alert's outcome, its tokens and its recipients."""

import pytest

from etl_craft.core.errors import HandlerError
from etl_craft.engine.repository.runs import TaskStatus
from etl_craft.handlers.email_alert import run_outcome, substitute
from etl_craft.handlers.mail import parse_recipients

pytestmark = pytest.mark.unit


def status(task_id, state, *, handler="SQL", error=None, attempts=1):
    return TaskStatus(task_id, f"t{task_id}", handler, state, error, attempts)


@pytest.mark.parametrize(
    ("statuses", "sla_missed", "expected"),
    [
        ([status(1, "SUCCESS"), status(2, "SUCCESS")], False, "SUCCESS"),
        ([status(1, "SUCCESS"), status(2, "FAILED")], False, "FAILED"),
        ([status(1, "SUCCESS"), status(2, "SKIPPED")], False, "COMPLETED_WITH_ERRORS"),
        ([status(1, "SUCCESS", attempts=2)], False, "COMPLETED_WITH_ERRORS"),
        ([status(1, "SUCCESS", error="warned")], False, "COMPLETED_WITH_ERRORS"),
        ([status(1, "PENDING")], False, "COMPLETED_WITH_ERRORS"),
        ([status(1, "SUCCESS")], True, "COMPLETED_WITH_ERRORS"),
        # This alert and any other alert are left out.
        (
            [
                status(1, "SUCCESS"),
                status(9, "IN-PROGRESS"),
                status(8, "PENDING", handler="EMAIL_ALERT"),
            ],
            False,
            "SUCCESS",
        ),
        ([], False, "SUCCESS"),
    ],
)
def test_the_run_outcome(statuses, sla_missed, expected):
    assert run_outcome(statuses, exclude_task_id=9, sla_missed=sla_missed) == expected


def test_tokens_are_replaced_and_an_unknown_one_fails():
    values = {
        "status": "FAILED",
        "pipeline_id": "97",
        "pipeline_code": "P",
        "task_code": "a",
        "error_message": "",
    }
    assert substitute("$$pipeline_code run $$pipeline_id: $$status", values, "EMAIL_SUBJECT") == (
        "P run 97: FAILED"
    )
    with pytest.raises(HandlerError, match=r"EMAIL_BODY uses unknown token\(s\) \$\$run_date; the"):
        substitute("on $$run_date", values, "EMAIL_BODY")


def test_recipients():
    assert parse_recipients(" a@x.io | Ops <ops@x.io> ", "EMAIL_TO") == ["a@x.io", "Ops <ops@x.io>"]
    with pytest.raises(HandlerError, match="EMAIL_TO is required"):
        parse_recipients(" | ", "EMAIL_TO")
    with pytest.raises(HandlerError, match=r"EMAIL_TO has address.* not valid: team"):
        parse_recipients("a@x.io|team", "EMAIL_TO")
