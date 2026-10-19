"""Classify installed drivers' real exceptions, including wrapped errors."""

from dataclasses import replace
from pathlib import Path

import duckdb
import psycopg
import pytest
from databricks.sql import exc as databricks
from snowflake.connector import errors as snowflake
from sqlalchemy.exc import DBAPIError
from trino import exceptions as trino

from etl_craft.config import parse_config
from etl_craft.core.enums import FailureClass, RetryOn
from etl_craft.core.errors import ConfigurationError, HandlerError, PermanentError, TransientError
from etl_craft.dialects.warehouse import for_key
from etl_craft.execution.failures import classify_failure
from etl_craft.execution.retries import task_retry_policy
from unit.test_execution_limits import CONFIG

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "key,error,kind,code",
    [
        ("postgres", psycopg.errors.SyntaxError("syntax"), FailureClass.PERMANENT, "42601"),
        ("postgres", psycopg.errors.UndefinedTable("table"), FailureClass.PERMANENT, "42P01"),
        (
            "postgres",
            psycopg.errors.InsufficientPrivilege("grant"),
            FailureClass.PERMANENT,
            "42501",
        ),
        ("postgres", psycopg.errors.SerializationFailure("retry"), FailureClass.TRANSIENT, "40001"),
        ("postgres", psycopg.errors.LockNotAvailable("timeout"), FailureClass.TRANSIENT, "55P03"),
        ("postgres", psycopg.errors.QueryCanceled("timeout"), FailureClass.TRANSIENT, "57014"),
        ("postgres", psycopg.OperationalError("network"), FailureClass.TRANSIENT, None),
        ("postgres", psycopg.DataError("value"), FailureClass.PERMANENT, None),
        ("postgres", psycopg.IntegrityError("key"), FailureClass.PERMANENT, None),
        ("postgres", psycopg.NotSupportedError("operation"), FailureClass.PERMANENT, None),
        ("duckdb", duckdb.ParserException("syntax"), FailureClass.PERMANENT, None),
        ("duckdb", duckdb.CatalogException("missing"), FailureClass.PERMANENT, None),
        ("duckdb", duckdb.BinderException("column"), FailureClass.PERMANENT, None),
        ("duckdb", duckdb.TransactionException("conflict"), FailureClass.TRANSIENT, None),
        ("duckdb", duckdb.IOException("lock"), FailureClass.TRANSIENT, None),
        ("duckdb", duckdb.OutOfMemoryException("memory"), FailureClass.TRANSIENT, None),
        (
            "snowflake",
            snowflake.ProgrammingError(msg="compile", errno=1003, sqlstate="42000"),
            FailureClass.PERMANENT,
            "42000",
        ),
        (
            "snowflake",
            snowflake.OperationalError(msg="compile", errno=1003),
            FailureClass.PERMANENT,
            "1003",
        ),
        (
            "snowflake",
            snowflake.InterfaceError(msg="network", errno=250003),
            FailureClass.TRANSIENT,
            "250003",
        ),
        (
            "databricks",
            databricks.ServerOperationError("parse error SQLSTATE: 42601"),
            FailureClass.PERMANENT,
            "42601",
        ),
        (
            "databricks",
            databricks.ServerOperationError("SQLSTATE: 40001"),
            FailureClass.TRANSIENT,
            "40001",
        ),
        ("databricks", databricks.RequestError("network"), FailureClass.TRANSIENT, None),
        (
            "databricks",
            databricks.RequestError("refused", context={"http-code": 403}),
            FailureClass.PERMANENT,
            "403",
        ),
        (
            "databricks",
            databricks.RequestError("limit", context={"http-code": 429}),
            FailureClass.TRANSIENT,
            "429",
        ),
        ("databricks", databricks.ServerOperationError("unclassified"), FailureClass.UNKNOWN, None),
    ],
)
@pytest.mark.parametrize("wrapped", [False, True])
def test_driver_failures(key, error, kind, code, wrapped):
    original = error
    if wrapped:
        error = DBAPIError("SELECT", {}, error)
    failure = for_key(key).classify_failure(error)
    assert failure.kind == kind
    assert failure.source == type(original).__name__
    assert failure.code == code


@pytest.mark.parametrize(
    "error_type,exception,kind",
    [
        ("USER_ERROR", trino.TrinoUserError, FailureClass.PERMANENT),
        ("EXTERNAL", trino.TrinoExternalError, FailureClass.TRANSIENT),
        ("INSUFFICIENT_RESOURCES", trino.TrinoQueryError, FailureClass.TRANSIENT),
        ("INTERNAL_ERROR", trino.TrinoQueryError, FailureClass.UNKNOWN),
    ],
)
def test_trino_driver_error_types(error_type, exception, kind):
    error = exception({"errorType": error_type, "errorCode": 7, "message": "failed"})
    failure = for_key("trino_iceberg").classify_failure(error)
    assert failure.kind == kind
    assert failure.code == "7"


@pytest.mark.parametrize(
    "error,kind",
    [
        (ConfigurationError("bad config"), FailureClass.PERMANENT),
        (TransientError("network"), FailureClass.TRANSIENT),
        (PermanentError("source"), FailureClass.PERMANENT),
        (LookupError("unknown"), FailureClass.UNKNOWN),
    ],
)
def test_handler_wrapper_preserves_script_cause(error, kind):
    wrapper = HandlerError("script failed")
    wrapper.__cause__ = error
    failure = classify_failure(wrapper, CONFIG)
    assert failure.kind == kind
    assert failure.source == type(error).__name__


@pytest.mark.parametrize(
    "policy,allowed",
    [
        (RetryOn.NONE, set()),
        (RetryOn.TRANSIENT, {FailureClass.TRANSIENT}),
        (RetryOn.TRANSIENT_AND_UNKNOWN, {FailureClass.TRANSIENT, FailureClass.UNKNOWN}),
        (RetryOn.ANY, set(FailureClass)),
    ],
)
def test_policy_classes_and_project_override(policy, allowed):
    config = replace(CONFIG, retry_on=policy)
    assert task_retry_policy({}, config).retry_on == policy
    assert task_retry_policy({"RETRY_ON": RetryOn.NONE}, config).retry_on == RetryOn.NONE
    assert {kind for kind in FailureClass if policy.permits(kind)} == allowed


def test_project_retry_policy_validation():
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local", "Retry_on": "occasionally"},
        "Engine": {"dev": {"jdbc_url": "jdbc:sqlite:e.db", "schema": "main"}},
    }
    with pytest.raises(ConfigurationError, match=r"Retry_on.*occasionally.*TRANSIENT"):
        parse_config(raw, Path("craft-connector.yml"))


@pytest.mark.parametrize(
    "state,kind",
    [
        ("08006", FailureClass.TRANSIENT),
        ("53300", FailureClass.TRANSIENT),
        ("57P01", FailureClass.TRANSIENT),
        ("57P02", FailureClass.TRANSIENT),
        ("57P03", FailureClass.TRANSIENT),
        ("22003", FailureClass.PERMANENT),
        ("23505", FailureClass.PERMANENT),
    ],
)
def test_postgres_specific_states_precede_broad_driver_types(state, kind):
    error = psycopg.errors.lookup(state)("failed")
    failure = for_key("postgres").classify_failure(error)
    assert failure.kind == kind
    assert failure.code == state


@pytest.mark.parametrize(
    "error,kind",
    [
        (trino.TrinoAuthError("refused"), FailureClass.PERMANENT),
        (trino.Http503Error("unavailable"), FailureClass.TRANSIENT),
        (trino.TrinoConnectionError("network"), FailureClass.TRANSIENT),
    ],
)
def test_trino_auth_and_transport_errors(error, kind):
    assert for_key("trino_iceberg").classify_failure(error).kind == kind


def test_driver_code_zero_and_nested_network_cause_are_preserved():
    error = trino.TrinoExternalError({"errorType": "EXTERNAL", "errorCode": 0})
    failure = for_key("trino_iceberg").classify_failure(error)
    assert failure.code == "0"
    driver = psycopg.errors.ConnectionFailure("lost")
    driver.__cause__ = ConnectionResetError("socket")
    wrapped = DBAPIError("SELECT", {}, driver)
    failure = classify_failure(wrapped, CONFIG)
    assert failure.kind == FailureClass.TRANSIENT
    assert failure.source == "ConnectionFailure" and failure.code == "08006"


def test_script_database_errors_need_an_explicit_transient_declaration():
    error = DBAPIError("SELECT", {}, psycopg.errors.ConnectionFailure("lost"))
    failure = classify_failure(error, CONFIG, script=True)
    assert failure.kind == FailureClass.UNKNOWN
    assert failure.source == "ConnectionFailure" and failure.code == "08006"


def test_trino_statement_timeout_is_transient_even_as_a_user_error():
    error = trino.TrinoUserError({"errorType": "USER_ERROR", "errorName": "EXCEEDED_TIME_LIMIT"})
    assert for_key("trino_iceberg").classify_failure(error).kind == FailureClass.TRANSIENT
