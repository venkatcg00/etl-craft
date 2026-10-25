"""The installed demo checks exact email semantics against recorded task outcomes."""

import runpy
from pathlib import Path

import pytest

from etl_craft.core.enums import RunStatus

pytestmark = pytest.mark.unit
check = runpy.run_path(str(Path(__file__).parents[1] / "e2e/test_demo.py"))["check_the_emails"]


class DemoEvidence:
    def __init__(self, attempts, status=RunStatus.SUCCESS, error=None):
        self.tasks = [("load", status, attempts, error)]
        self.requested = None

    def latest_run(self, pipeline):
        assert pipeline == "SUPPORT_DM"
        return 7, RunStatus.SUCCESS

    def engine_rows(self, sql, **params):
        assert params == {"run": 7}
        return self.tasks

    def wait_for_email(self, subject):
        self.requested = subject
        return {subject}


@pytest.mark.parametrize(
    ("attempts", "subject"),
    [
        (1, "SUPPORT_DM: SUCCESS"),
        (2, "SUPPORT_DM: COMPLETED_WITH_ERRORS"),
    ],
)
def test_demo_email_uses_recorded_retries(attempts, subject):
    demo = DemoEvidence(attempts)
    check(demo, failed=False)
    assert demo.requested == subject


@pytest.mark.parametrize(
    ("status", "error"),
    [
        (RunStatus.SKIPPED, None),
        (RunStatus.IN_PROGRESS, None),
        (RunStatus.SUCCESS, "unexpected"),
    ],
)
def test_demo_does_not_accept_unexpected_task_outcomes(status, error):
    demo = DemoEvidence(1, status, error)
    with pytest.raises(AssertionError):
        check(demo, failed=False)
    assert demo.requested is None
