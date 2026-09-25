import pytest

from etl_craft.config import (
    ConnectionProfile,
    ConnectionSection,
    ConnectorConfig,
    ExecutionLimits,
    SourceConfig,
)
from etl_craft.core.enums import Mode
from etl_craft.core.errors import HandlerError
from etl_craft.execution.limits import task_timeout_seconds
from etl_craft.handlers import registry
from etl_craft.handlers.registry import HandlerResult, format_task_log

pytestmark = pytest.mark.unit

CONFIG = ConnectorConfig(
    mode=Mode.LOCAL,
    source=SourceConfig("environment"),
    engine=ConnectionSection(
        "d", {"d": ConnectionProfile("ENGINE", "d", "jdbc:sqlite:e", "", "none")}
    ),
    limits=ExecutionLimits(task_timeout_seconds=600),
)


@pytest.mark.parametrize(
    ("params", "expected"),
    [({}, 600), ({"TASK_TIMEOUT_SECONDS": "30"}, 30), ({"TASK_TIMEOUT_SECONDS": "0"}, 0)],
)
def test_task_timeout(params, expected):
    assert task_timeout_seconds(params, CONFIG) == expected


@pytest.mark.parametrize(
    ("value", "message"), [("soon", "not a whole number"), ("-1", "must not be negative")]
)
def test_a_bad_task_timeout(value, message):
    with pytest.raises(HandlerError, match=message):
        task_timeout_seconds({"TASK_TIMEOUT_SECONDS": value}, CONFIG)


def test_format_task_log():
    assert format_task_log(HandlerResult()) is None
    assert format_task_log(HandlerResult(source_count=2, delete_count=0)) == (
        "SOURCE_COUNT = 2\nDELETE_COUNT = 0"
    )
    # The counts first, then the handler's own values.
    assert format_task_log(HandlerResult(target_count=1, variables={"A": 1, "B": None})) == (
        "TARGET_COUNT = 1\nA = 1"
    )


def test_resolve_handler(monkeypatch):
    monkeypatch.setitem(registry.HANDLERS, "SQL", "etl_craft.handlers.registry:format_task_log")
    assert registry.resolve_handler("SQL") is format_task_log
    monkeypatch.delitem(registry.HANDLERS, "EMAIL_ALERT")
    with pytest.raises(HandlerError, match="no handler is installed for HANDLER 'EMAIL_ALERT'"):
        registry.resolve_handler("EMAIL_ALERT")
