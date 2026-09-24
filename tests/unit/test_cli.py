import importlib
import importlib.metadata
import runpy
import subprocess
import sys

import pytest

import etl_craft
from etl_craft.cli import main

pytestmark = pytest.mark.unit


def test_version_flag_prints_the_package_version(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["--version"])
    assert exit_info.value.code == 0
    assert capsys.readouterr().out.strip() == f"etl-craft {etl_craft.__version__}"


def test_the_installed_version_comes_from_package_metadata():
    assert etl_craft.__version__ != "0.0.0+unknown"


def test_no_command_prints_usage_and_exits_2(capsys):
    assert main([]) == 2
    assert capsys.readouterr().out.startswith("usage: etl-craft")


def test_python_dash_m_runs_the_same_command_line():
    result = subprocess.run(
        [sys.executable, "-m", "etl_craft", "--version"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout.strip() == f"etl-craft {etl_craft.__version__}"


def test_running_the_package_as_a_module_calls_main(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["etl_craft", "--version"])
    with pytest.raises(SystemExit) as exit_info:
        runpy.run_module("etl_craft", run_name="__main__")
    assert exit_info.value.code == 0
    assert capsys.readouterr().out.strip() == f"etl-craft {etl_craft.__version__}"


def test_an_uninstalled_source_tree_reports_an_unknown_version(monkeypatch):
    def not_installed(name: str) -> str:
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, "version", not_installed)
    try:
        assert importlib.reload(etl_craft).__version__ == "0.0.0+unknown"
    finally:
        monkeypatch.undo()
        importlib.reload(etl_craft)
    assert etl_craft.__version__ != "0.0.0+unknown"
