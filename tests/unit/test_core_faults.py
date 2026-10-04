"""Fault selection is exact, opt-in, and works in a real process."""

import os
import subprocess
import sys

import pytest

from etl_craft.core.errors import InjectedFaultError
from etl_craft.core.faults import fault_point

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("selected", [None, "other", "point:unknown", "point-two"])
def test_unselected_fault_points_do_nothing(monkeypatch, selected):
    monkeypatch.delenv("ETL_CRAFT_FAULT", raising=False)
    if selected is not None:
        monkeypatch.setenv("ETL_CRAFT_FAULT", selected)
    fault_point("point")


def test_selected_fault_names_its_point(monkeypatch):
    monkeypatch.setenv("ETL_CRAFT_FAULT", "point")
    with pytest.raises(InjectedFaultError, match="injected fault at point"):
        fault_point("point")


def test_kill_fault_exits_without_finally_handlers(tmp_path):
    marker = tmp_path / "finally"
    code = (
        "from pathlib import Path\nfrom etl_craft.core.faults import fault_point\n"
        f"try:\n    fault_point('point')\nfinally:\n    Path({str(marker)!r}).touch()\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        env={**os.environ, "ETL_CRAFT_FAULT": "point:kill"},
        timeout=10,
    )
    assert result.returncode == 137
    assert not marker.exists()
