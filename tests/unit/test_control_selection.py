"""Malformed bulk selections fail before any operation receives them."""

import json

import pytest

from etl_craft.cli.commands.control_requests import read_selection
from etl_craft.core.enums import ControlAction
from etl_craft.core.errors import UsageError

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        ["all"],
        [{"action": "GUESS"}],
        [
            {
                "action": "HOLD",
                "pipeline_code": "P",
                "reason": "reason",
                "request_key": "key",
                "run_id": True,
            }
        ],
    ],
)
def test_malformed_control_selection_is_refused_with_its_file(tmp_path, value):
    path = tmp_path / "selection.json"
    path.write_text(json.dumps(value))
    with pytest.raises(UsageError, match=r"selection\.json"):
        read_selection(path)


def test_control_selection_retains_the_exact_object_and_idempotency_key(tmp_path):
    path = tmp_path / "selection.json"
    path.write_text(
        json.dumps(
            [
                {
                    "pipeline_code": "P",
                    "task_code": "T",
                    "run_id": 12,
                    "action": "HOLD",
                    "reason": "inspect",
                    "request_key": "stable",
                }
            ]
        )
    )
    selected = read_selection(path)
    assert len(selected) == 1
    assert selected[0].action == ControlAction.HOLD
    assert selected[0].task_code == "T" and selected[0].selector.run_id == 12
    assert selected[0].request_key == "stable"
