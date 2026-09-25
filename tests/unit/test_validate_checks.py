"""The checks ``validate`` shares with the handlers, on their own."""

import pytest

from etl_craft.engine.repository.pipelines import pipeline_parameter_problems
from etl_craft.handlers.email_alert import alert_parameter_problems
from etl_craft.handlers.python_scripts import script_definition_problem
from etl_craft.services.validate import _cycles

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("source", "problem"),
    [
        ("def run(task):\n    pass\n", None),
        ("def run():\n    pass\n", None),
        ("def run(task, extra=1, *args):\n    pass\n", None),
        ("from helpers import run\n", None),
        ("run = lambda task: None\n", None),
        ("async def run(task):\n    pass\n", "s.py: run is async"),
        ("def main(task):\n    pass\n", "SCRIPT_NAME='s.py' defines no run(task) function"),
    ],
)
def test_a_script_is_read_without_running_it(tmp_path, source, problem):
    path = tmp_path / "s.py"
    path.write_text("import sys\nsys.exit(3)\n" + source, "utf-8")
    found = script_definition_problem(path, "s.py")
    assert found == problem if problem is None else found.startswith(problem)


def test_an_alert_is_checked_for_every_outcome_it_can_send_on():
    assert (
        alert_parameter_problems({"EMAIL_TO": "a@x.io", "EMAIL_SUBJECT": "s", "EMAIL_BODY": "b"})
        == []
    )
    assert (
        alert_parameter_problems(
            {
                "EMAIL_TO": "a@x.io",
                "EMAIL_SUBJECT_FAILED": "s",
                "EMAIL_PIPELINES": "ALL",
                "EMAIL_ON_STATUS": "FAILED",
            }
        )
        == []
    )
    assert alert_parameter_problems({"EMAIL_ON_STATUS": "FAILD", "EMAIL_SUBJECT": "$$x"}) == [
        "EMAIL_TO is required: one or more addresses separated by '|'",
        "EMAIL_ON_STATUS names unknown outcome(s) FAILD; the outcomes are "
        "FAILED, COMPLETED_WITH_ERRORS, SUCCESS",
        "no EMAIL_BODY for outcome(s) FAILED, COMPLETED_WITH_ERRORS, SUCCESS: set EMAIL_BODY, "
        "or EMAIL_BODY_<OUTCOME> for each, or leave them out of EMAIL_ON_STATUS",
        "EMAIL_SUBJECT uses unknown token(s) $$x; the tokens are $$status, $$pipeline_id, "
        "$$pipeline_code, $$task_code, $$error_message",
    ]


def test_pipeline_parameters_are_typed():
    assert pipeline_parameter_problems(None) == ([], [])
    assert pipeline_parameter_problems('{"RETRIES": 2, "TAGS": ["a"], "CATCHUP": false}') == (
        [],
        [],
    )
    assert pipeline_parameter_problems({"RETRIES": True, "TAGS": "a", "OWNER": "x"}) == (
        [
            "PIPELINE_PARAMETERS.RETRIES=true must be a whole number, 0 or more",
            'PIPELINE_PARAMETERS.TAGS="a" must be a list of strings',
        ],
        ["OWNER"],
    )
    assert pipeline_parameter_problems("[1]")[0] == [
        "PIPELINE_PARAMETERS must be a JSON object; it is [1]"
    ]
    assert pipeline_parameter_problems("{")[0][0].startswith(
        "PIPELINE_PARAMETERS is not valid JSON"
    )


def test_each_cycle_is_reported_once_from_its_smallest_code():
    edges = {("B", "C"), ("C", "B"), ("A", "B"), ("X", "Y"), ("Y", "Z"), ("Z", "X")}
    assert _cycles(edges) == [["B", "C"], ["X", "Y", "Z"]]
    assert _cycles({("A", "B"), ("B", "C")}) == []
