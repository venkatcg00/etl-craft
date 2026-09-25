import importlib
import importlib.metadata
import json
import logging
import runpy
import subprocess
import sys
from pathlib import Path

import pytest

import etl_craft
from etl_craft.cli import main
from etl_craft.cli.commands import Command
from etl_craft.core.errors import ConfigurationError, MetadataError, UsageError

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


@pytest.fixture
def restore_logger():
    logger = logging.getLogger("etl_craft")
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


def _record(calls):
    def configure(parser):
        parser.add_argument("--pipeline_code", required=True)

    def run(args, out):
        calls.append(args)
        out.line(f"ran {args.pipeline_code}")
        return 0

    return Command(name="demo", help="A command for the tests.", configure=configure, run=run)


def _raising(error):
    def run(args, out):
        raise error

    return Command(name="fail", help="Always fails.", configure=lambda parser: None, run=run)


@pytest.mark.usefixtures("restore_logger")
def test_a_command_runs_with_its_options_and_the_global_defaults(capsys):
    calls = []
    assert main(["demo", "--pipeline_code", "P1"], commands=[_record(calls)]) == 0
    assert capsys.readouterr().out == "ran P1\n"
    (args,) = calls
    assert (args.config, args.log_level, args.log_format) == (None, "INFO", "text")


@pytest.mark.usefixtures("restore_logger")
@pytest.mark.parametrize(
    "argv",
    [
        ["--config", "c.yml", "--log-level", "debug", "--log-format", "JSON", "demo"],
        ["demo", "--config", "c.yml", "--log-level", "debug", "--log-format", "JSON"],
        ["--config", "c.yml", "demo", "--log-level", "DEBUG", "--log-format", "json"],
    ],
)
def test_global_options_work_before_or_after_the_command(argv):
    calls = []
    assert main([*argv, "--pipeline_code", "P1"], commands=[_record(calls)]) == 0
    (args,) = calls
    assert (args.config, args.log_level, args.log_format) == (Path("c.yml"), "DEBUG", "json")


@pytest.mark.usefixtures("restore_logger")
def test_a_global_option_after_the_command_wins():
    calls = []
    argv = ["--log-level", "ERROR", "demo", "--log-level", "DEBUG", "--pipeline_code", "P1"]
    main(argv, commands=[_record(calls)])
    assert calls[0].log_level == "DEBUG"


@pytest.mark.usefixtures("restore_logger")
def test_the_log_options_configure_logging(capsys):
    def run(args, out):
        logging.getLogger("etl_craft.cli.test").debug("detail")
        logging.getLogger("etl_craft.cli.test").info("progress")
        return 0

    command = Command(name="logs", help="Logs.", configure=lambda parser: None, run=run)
    main(["--log-format", "json", "--log-level", "INFO", "logs"], commands=[command])
    captured = capsys.readouterr()
    assert captured.out == ""
    assert [json.loads(line)["message"] for line in captured.err.splitlines()] == ["progress"]


@pytest.mark.usefixtures("restore_logger")
@pytest.mark.parametrize(
    ("error", "exit_code"),
    [
        (ConfigurationError("craft-connector.yml not found"), 3),
        (UsageError("--pipeline_code is required with --task_code"), 2),
        (MetadataError("no active pipeline 'P9'"), 4),
    ],
)
def test_an_etl_craft_error_becomes_an_error_line_and_its_exit_code(capsys, error, exit_code):
    assert main(["fail"], commands=[_raising(error)]) == exit_code
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == f"error: {error}\n"


@pytest.mark.usefixtures("restore_logger")
def test_any_other_exception_is_logged_and_exits_unexpected(capsys, caplog):
    assert main(["fail"], commands=[_raising(ZeroDivisionError("oops"))]) == 16
    assert capsys.readouterr().err.endswith("error: unexpected ZeroDivisionError: oops\n")
    assert "Traceback" in caplog.text or caplog.records[-1].exc_info is not None


def test_no_command_with_commands_registered_prints_usage_and_exits_2(capsys):
    assert main([], commands=[_record([])]) == 2
    assert capsys.readouterr().out.startswith("usage: etl-craft")


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (["--log-level", "loud", "demo"], "invalid choice: 'LOUD'"),
        (["demo", "--log-format", "xml"], "invalid choice: 'xml'"),
        (["demo"], "the following arguments are required: --pipeline_code"),
        (["nope"], "invalid choice: 'nope'"),
    ],
)
def test_invalid_arguments_exit_2_with_the_reason(capsys, argv, message):
    with pytest.raises(SystemExit) as exit_info:
        main(argv, commands=[_record([])])
    assert exit_info.value.code == 2
    assert message in capsys.readouterr().err


def test_help_lists_the_commands_and_the_global_options(capsys):
    with pytest.raises(SystemExit):
        main(["--help"], commands=[_record([])])
    text = capsys.readouterr().out
    assert "demo" in text
    assert "A command for the tests." in text
    for option in ("--config PATH", "--log-level", "--log-format"):
        assert option in text


def test_a_command_help_includes_the_global_options(capsys):
    with pytest.raises(SystemExit):
        main(["demo", "--help"], commands=[_record([])])
    text = capsys.readouterr().out
    assert "--pipeline_code" in text
    assert "global options" in text
