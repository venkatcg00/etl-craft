"""Event condition boundaries and safe SQL file-token expansion."""

from dataclasses import replace
from pathlib import Path

import pytest

from etl_craft.config import ConnectionProfile, parse_config
from etl_craft.core.errors import HandlerError
from etl_craft.core.text import substitute_task_tokens
from etl_craft.handlers.event_wait import read_event

pytestmark = pytest.mark.unit


def base_config():
    return parse_config(
        {
            "Secrets": {"Source_type": "environment"},
            "Orchestration": {"Mode": "local"},
            "Engine": {"dev": {"jdbc_url": "jdbc:sqlite:engine.db", "schema": "main"}},
        },
        Path("craft-connector.yml"),
    )


@pytest.mark.parametrize(
    "params,problem",
    [
        ({}, "exactly one"),
        ({"EVENT_FILE": "a", "EVENT_SQL": "SELECT 1"}, "exactly one"),
        ({"EVENT_FILE": "a\x00"}, "NUL"),
        ({"EVENT_FILE": "a", "EVENT_SETTLE_SECONDS": "-1"}, "finite number"),
        ({"EVENT_FILE": "a", "EVENT_TIMEOUT_MINUTES": "nan"}, "finite number"),
        ({"EVENT_FILE": "a", "EVENT_TIMEOUT_MINUTES": "inf"}, "finite number"),
        ({"EVENT_FILE": "a", "EVENT_TIMEOUT_MINUTES": "bad"}, "finite number"),
        ({"EVENT_FILE": "a", "EVENT_TIMEOUT_MINUTES": "1e308"}, "datetime range"),
        ({"EVENT_FILE": "a", "EVENT_ON_TIMEOUT": "IGNORE"}, "FAIL or SKIP"),
        ({"EVENT_SQL": "SELECT 1"}, "Warehouse section"),
        ({"EVENT_FILE": "a", "EVENT_POLL_SECONDS": "0"}, ">= 1"),
        ({"EVENT_FILE": "a", "EVENT_POLL_SECONDS": "nan"}, "finite number"),
        ({"EVENT_FILE": "a", "EVENT_POLL_SECONDS": "1e308"}, "datetime range"),
    ],
)
def test_invalid_event_definitions(params, problem):
    with pytest.raises(HandlerError, match=problem):
        read_event(base_config(), params)


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM t",
        "SELECT 1; SELECT 2",
        "WITH gone AS (DELETE FROM t RETURNING *) SELECT * FROM gone",
        "SELECT * INTO target FROM source",
    ],
)
def test_event_query_cannot_write(sql):
    config = replace(
        base_config(),
        warehouse=ConnectionProfile("WAREHOUSE", "test", "jdbc:duckdb:test.db", "", "none"),
    )
    with pytest.raises(HandlerError, match="read-only SELECT"):
        read_event(config, {"EVENT_SQL": sql})


def test_file_tokens_escape_paths_and_leave_strings_and_comments_untouched():
    sql = substitute_task_tokens(
        "SELECT $$event_files, '$$event_files' -- $$event_files\n",
        pipeline_run_id=1,
        refresh_type="INCREMENTAL",
        pipeline_run_id_substitution=False,
        filter_enabled=False,
        event_files_literal="'/tmp/a''b.csv', '/tmp/c.csv'",
    )
    assert sql == "SELECT '/tmp/a''b.csv', '/tmp/c.csv', '$$event_files' -- $$event_files\n"


def test_runtime_file_token_without_evidence_is_refused():
    with pytest.raises(HandlerError, match="no successful file EVENT_WAIT"):
        substitute_task_tokens(
            "SELECT $$event_files",
            pipeline_run_id=1,
            refresh_type="INCREMENTAL",
            pipeline_run_id_substitution=False,
            filter_enabled=False,
            event_files_literal="",
        )


def test_indefinite_wait_and_default_probe_interval():
    event = read_event(
        base_config(), {"EVENT_FILE": "inbox/*.csv", "EVENT_TIMEOUT_MINUTES": " none "}
    )
    assert event.timeout_seconds is None
    assert event.poll_seconds == 60
    assert read_event(base_config(), {"EVENT_FILE": "a"}).timeout_seconds == 3600
