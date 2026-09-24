import io
import json
import logging
import subprocess
import sys

import pytest

from etl_craft.core import log
from etl_craft.core.errors import UsageError

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def restore_logger():
    logger = logging.getLogger(log.ROOT_LOGGER)
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


def test_importing_the_package_configures_nothing():
    code = (
        "import logging, etl_craft.core.log\n"
        "logger = logging.getLogger('etl_craft')\n"
        "print([type(h).__name__ for h in logger.handlers], logger.level,"
        " logging.getLogger().handlers)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], check=True, capture_output=True, text=True
    )
    assert result.stdout.strip() == "['NullHandler'] 0 []"


def test_an_unconfigured_warning_is_not_printed():
    code = "import logging, etl_craft\nlogging.getLogger('etl_craft.x').warning('quiet')\n"
    result = subprocess.run(
        [sys.executable, "-c", code], check=True, capture_output=True, text=True
    )
    assert result.stderr == ""


def test_text_format():
    stream = io.StringIO()
    log.configure("info", "text", stream)
    logging.getLogger("etl_craft.engine").info("connected to %s", "sqlite")
    logging.getLogger("etl_craft.engine").debug("hidden")
    line = stream.getvalue()
    assert line.endswith(" INFO etl_craft.engine: connected to sqlite\n")
    assert "hidden" not in line


def test_json_format_writes_one_object_per_line():
    stream = io.StringIO()
    log.configure("DEBUG", log.LogFormat.JSON, stream)
    logger = logging.getLogger("etl_craft.execution")
    logger.debug("first")
    logger.warning("task %s failed", "T1", extra={"pipeline_code": "P1", "attempt": 2})
    first, second = (json.loads(line) for line in stream.getvalue().splitlines())
    assert first["message"] == "first"
    assert first["level"] == "DEBUG"
    assert second == {
        "time": second["time"],
        "level": "WARNING",
        "logger": "etl_craft.execution",
        "message": "task T1 failed",
        "pipeline_code": "P1",
        "attempt": 2,
    }
    assert second["time"].endswith("+00:00")


def test_json_format_includes_the_exception_and_stringifies_unknown_values():
    stream = io.StringIO()
    log.configure("INFO", "json", stream)
    try:
        raise ValueError("boom")
    except ValueError:
        logging.getLogger("etl_craft.x").exception("failed", extra={"obj": object()})
    entry = json.loads(stream.getvalue())
    assert entry["exception"].startswith("Traceback")
    assert "ValueError: boom" in entry["exception"]
    assert entry["obj"].startswith("<object object")


def test_json_format_includes_a_requested_stack():
    stream = io.StringIO()
    log.configure("INFO", "json", stream)
    logging.getLogger("etl_craft.x").info("here", stack_info=True)
    assert json.loads(stream.getvalue())["stack"].startswith("Stack (most recent call last)")


def test_configure_again_replaces_its_handler():
    first, second = io.StringIO(), io.StringIO()
    log.configure("INFO", "text", first)
    handler = log.configure("WARNING", "json", second)
    logger = logging.getLogger(log.ROOT_LOGGER)
    assert handler in logger.handlers
    assert sum(isinstance(h, logging.StreamHandler) for h in logger.handlers) == 1
    logger.info("dropped")
    logger.warning("kept")
    assert first.getvalue() == ""
    assert json.loads(second.getvalue())["message"] == "kept"


def test_configure_writes_to_stderr_by_default(capsys):
    log.configure()
    logging.getLogger("etl_craft.x").info("to stderr")
    assert "INFO etl_craft.x: to stderr" in capsys.readouterr().err


def test_records_outside_etl_craft_are_not_handled():
    stream = io.StringIO()
    log.configure("DEBUG", "text", stream)
    logging.getLogger("urllib3").warning("other library")
    assert stream.getvalue() == ""


@pytest.mark.parametrize(("name", "level"), [("debug", 10), ("Warning", 30), (40, 40)])
def test_parse_level(name, level):
    assert log.parse_level(name) == level


def test_an_unknown_level_is_a_usage_error():
    with pytest.raises(UsageError, match="unknown log level 'loud'"):
        log.parse_level("loud")


def test_an_unknown_format_is_a_usage_error():
    with pytest.raises(UsageError, match="unknown log format 'xml'; use one of text, json"):
        log.configure("INFO", "xml")
