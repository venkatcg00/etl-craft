"""ALLOW_EMPTY=false refuses an empty replacement before the target changes."""

import pytest

from etl_craft.core.errors import SqlGuardError

EMPTY = "SELECT 1 AS id WHERE 1 = 0"


@pytest.mark.parametrize("action", ["CREATE_TABLE", "OVERWRITE_TABLE"])
def test_allow_empty_false_refuses_an_empty_select_and_keeps_the_target(sql_world, action):
    w = sql_world
    if action == "OVERWRITE_TABLE":
        w.setup("daily", "SELECT 1 AS id", action)
    params = {"SQL_ACTION": action, "TARGET_OBJECT": "daily"}
    w.run("daily", SELECT_QUERY="SELECT 1 AS id", **params)
    with pytest.raises(SqlGuardError) as refused:
        w.run("daily", SELECT_QUERY=EMPTY, ALLOW_EMPTY="false", **params)
    message = str(refused.value)
    assert f"task daily: {action} " in message
    assert "returned no rows and ALLOW_EMPTY is false" in message
    assert "set ALLOW_EMPTY=true" in message
    assert w.rows(f"SELECT id FROM {w.name('daily')}") == [(1,)]

    w.run("daily", SELECT_QUERY="SELECT 2 AS id", ALLOW_EMPTY="false", **params)
    assert w.rows(f"SELECT id FROM {w.name('daily')}") == [(2,)]

    result = w.run("daily", SELECT_QUERY=EMPTY, ALLOW_EMPTY="true", **params)
    assert result.insert_count in (0, None)
    assert w.rows(f"SELECT id FROM {w.name('daily')}") == []


def test_allow_empty_false_does_not_create_a_missing_target(sql_world):
    w = sql_world
    with pytest.raises(SqlGuardError, match="ALLOW_EMPTY is false"):
        w.run(
            "daily",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="daily",
            SELECT_QUERY=EMPTY,
            ALLOW_EMPTY="false",
        )
    assert not [table for table in w.tables() if table.lower() == "daily"]
