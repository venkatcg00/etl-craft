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
            "Secrets": {"Source_type": "ENVIRONMENT"},
            "Orchestration": {},
            "Engine": {"dev": {"Jdbc_url": "jdbc:sqlite:engine.db", "Schema": "main"}},
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
        warehouse=ConnectionProfile("WAREHOUSE", "test", "jdbc:duckdb:test.db", "", "NONE"),
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
        event_literals={"event_files": "'/tmp/a''b.csv', '/tmp/c.csv'"},
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
            event_literals={"event_files": ""},
        )


def test_indefinite_wait_and_default_probe_interval():
    event = read_event(
        base_config(), {"EVENT_FILE": "inbox/*.csv", "EVENT_TIMEOUT_MINUTES": " none "}
    )
    assert event.timeout_seconds is None
    assert event.poll_seconds == 60
    assert read_event(base_config(), {"EVENT_FILE": "a"}).timeout_seconds == 3600


@pytest.mark.parametrize("maximum", ["0", "-1", "1.5", "nan", "1e2", "", "\u0661"])
def test_event_max_rows_requires_a_positive_whole_number(maximum):
    with pytest.raises(HandlerError) as caught:
        read_event(base_config(), {"EVENT_FILE": "inbox/*", "EVENT_MAX_ROWS": maximum})
    for part in ("EVENT_MAX_ROWS", repr(maximum), "positive whole number", "correct"):
        assert part in str(caught.value)


def test_a_file_wait_cannot_declare_a_signal_key_column():
    with pytest.raises(HandlerError) as caught:
        read_event(base_config(), {"EVENT_FILE": "inbox/*", "EVENT_KEY_COLUMN": "event_id"})
    for part in (
        "EVENT_FILE='inbox/*'",
        "EVENT_KEY_COLUMN='event_id'",
        "EVENT_SQL result",
        "remove",
    ):
        assert part in str(caught.value)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count(*) AS ready FROM signals",
        "(SELECT count(*) AS ready FROM signals)",
        "SELECT sum(x) AS ready FROM signals WHERE 1=0",
        "WITH events AS (SELECT * FROM signals) SELECT count(*) FROM events",
        "SELECT coalesce(max(x), 0) AS ready FROM signals",
    ],
)
def test_unconditional_aggregates_cannot_satisfy_an_event(sql):
    config = replace(
        base_config(),
        warehouse=ConnectionProfile("WAREHOUSE", "test", "jdbc:duckdb:test.db", "", "NONE"),
    )
    with pytest.raises(HandlerError) as caught:
        read_event(config, {"EVENT_SQL": sql})
    for part in ("EVENT_SQL", "aggregate", "always returns a row", "HAVING count(*) > 0"):
        assert part in str(caught.value)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count(*) AS ready FROM signals HAVING count(*) > 0",
        "SELECT x, count(*) FROM signals GROUP BY x",
        "SELECT count(*) OVER () AS ready FROM signals",
        "SELECT * FROM signals WHERE x > (SELECT count(*) FROM other)",
        "WITH counts AS (SELECT count(*) AS n FROM signals) SELECT * FROM counts WHERE n>0",
    ],
)
def test_conditional_aggregates_and_windows_remain_valid(sql):
    config = replace(
        base_config(),
        warehouse=ConnectionProfile("WAREHOUSE", "test", "jdbc:duckdb:test.db", "", "NONE"),
    )
    assert read_event(config, {"EVENT_SQL": sql}).sql == sql
