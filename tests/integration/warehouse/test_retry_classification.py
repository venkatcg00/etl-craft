"""Actual SQL syntax and connection failures on every local warehouse."""

from fixtures.retry_warehouse import check_connection_retry, check_syntax_error, save_project


def test_sql_syntax_fails_once_with_driver_evidence(sql_world):
    save_project(sql_world)
    check_syntax_error(sql_world)


def test_dropped_connection_is_retried_and_succeeds(sql_world, monkeypatch):
    save_project(sql_world)
    check_connection_retry(sql_world, monkeypatch)


def test_none_policy_does_not_retry_a_dropped_connection(sql_world, monkeypatch):
    from etl_craft.core.enums import RetryOn

    save_project(sql_world)
    check_connection_retry(sql_world, monkeypatch, RetryOn.NONE)
