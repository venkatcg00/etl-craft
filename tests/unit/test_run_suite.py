import json
import sys
from pathlib import Path

import pytest

from fixtures.scripts import load

pytestmark = pytest.mark.unit

run_suite = load("run_suite")
suites = load("suites")


def test_an_unknown_suite_is_a_usage_error(capsys):
    assert run_suite.main(["nightly"]) == 2
    assert "unknown suite 'nightly'; known: unit" in capsys.readouterr().err


def test_a_wheel_suite_needs_an_existing_wheel(capsys, tmp_path):
    assert run_suite.main(["package"]) == 2
    assert "pass --wheel PATH" in capsys.readouterr().err
    assert run_suite.main(["package", "--wheel", str(tmp_path / "missing.whl")]) == 2
    assert "wheel not found" in capsys.readouterr().err


def test_the_pytest_command_selects_the_suite_and_writes_evidence(tmp_path):
    suite = suites.load_suites()["package"]
    wheel = tmp_path / "etl_craft.whl"
    command = run_suite.pytest_command(suite, tmp_path / "e.json", wheel, ["-x"])
    assert command[:5] == [sys.executable, "-m", "pytest", "-m", "package"]
    assert f"--evidence={tmp_path / 'e.json'}" in command
    assert "--evidence-suite=package" in command
    assert f"--evidence-wheel={wheel}" in command
    assert command[-1] == "-x"


def test_running_a_suite_records_its_evidence(tmp_path, capsys):
    status = run_suite.main(
        ["unit", "--evidence-dir", str(tmp_path), "--", "-q", "-k", "test_version_flag"]
    )
    assert status == 0
    evidence = json.loads((tmp_path / "unit.json").read_text(encoding="utf-8"))
    assert evidence["suite"] == "unit"
    assert evidence["marker"] == "unit"
    assert evidence["counts"]["passed"] == 1
    assert [Path(test["nodeid"]).name for test in evidence["tests"]] == [
        "test_cli.py::test_version_flag_prints_the_package_version"
    ]
    assert f"evidence for unit: {tmp_path / 'unit.json'}" in capsys.readouterr().out
