"""Vendor metadata and immutable stages avoid redundant work without guessing capabilities."""

from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.core.enums import TableFormat
from etl_craft.core.errors import HandlerError
from etl_craft.dialects.warehouse import resolve
from etl_craft.handlers.sql.session import Session

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "schema, expected",
    [("UPPER_SCHEMA", "upper_schema"), ("Mixed", "Mixed"), (None, "server_schema")],
)
def test_snowflake_uses_authenticated_schema_and_still_reads_server_version(schema, expected):
    from etl_craft.dialects.warehouse.snowflake_driver import dialect

    calls = []

    def execute(sql):
        calls.append(str(sql))
        values = ("9.99.1 internal",) if "CURRENT_VERSION" in str(sql) else ("DB", "SERVER_SCHEMA")
        return SimpleNamespace(fetchone=lambda: values)

    connection = SimpleNamespace(
        connection=SimpleNamespace(dbapi_connection=SimpleNamespace(schema=schema)), execute=execute
    )
    driver = dialect()
    assert driver._get_default_schema_name(connection) == expected
    assert len(calls) == (1 if schema is None else 0)
    assert driver._get_server_version_info(connection) == (9, 99, 1)
    assert calls[-1] == "SELECT CURRENT_VERSION()"


@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
@pytest.mark.parametrize(
    "mode", ["temporary", "unsupported", "envelope", "invalid", "disguised", "mutable"]
)
def test_databricks_immutable_stages_fall_back_only_for_capability_refusals(
    monkeypatch, table_format, mode
):
    engine = create_engine("sqlite://")
    calls = []

    def run(session, sql, params=None, *, step):
        calls.append(sql)
        if sql.startswith("CREATE TEMPORARY") and mode in {
            "unsupported",
            "envelope",
            "invalid",
            "disguised",
        }:
            message = (
                "[TEMP_TABLE_CREATION_NOT_SUPPORTED_IN_COMPUTE]"
                if mode == "unsupported"
                else "network error quoting [TEMP_TABLE_CREATION_NOT_SUPPORTED_IN_COMPUTE]"
            )
            if mode == "disguised":
                message = "[USER_RAISED_EXCEPTION] [TEMP_TABLE_CREATION_NOT_SUPPORTED_IN_COMPUTE]"
            if mode == "envelope":
                message = (
                    "[RequestId=964fd9ee-917e-4dc8-846a-bae97061e8fa "
                    "ErrorClass=TEMP_TABLE_CREATION_NOT_SUPPORTED_IN_COMPUTE] unavailable"
                )
            raise HandlerError("stage the SELECT failed") from SQLAlchemyError(message)

    try:
        with engine.connect() as conn:
            session = Session(
                conn,
                resolve("databricks", table_format),
                catalog="catalog",
                action="SCD1_MERGE",
                target_object="schema.target",
                task_run_id=1,
                params={},
            )
            monkeypatch.setattr(Session, "run", run)
            monkeypatch.setattr(conn, "execute", lambda sql: calls.append(str(sql)))
            name = session.scratch("stage")
            try:
                if mode in {"invalid", "disguised"}:
                    with pytest.raises(HandlerError, match="stage the SELECT failed"):
                        session.create_scratch(name, "SELECT 1 AS id", step="stage", immutable=True)
                    assert len(calls) == 1
                    return
                actual = session.create_scratch(
                    name, "SELECT 1 AS id", step="stage", immutable=mode != "mutable"
                )
                if mode == "temporary":
                    assert actual == name.rsplit(".", 1)[-1]
                    assert calls == [f"CREATE TEMPORARY TABLE {actual} AS SELECT 1 AS id"]
                else:
                    assert actual == name
                    assert calls[-1] == f"CREATE TABLE {name} AS SELECT 1 AS id"
                    assert len(calls) == (2 if mode in {"unsupported", "envelope"} else 1)
                session.drop(actual)
            finally:
                session.sweep()
            if mode == "temporary":
                assert len(calls) == 1
            else:
                assert any(sql == f"DROP TABLE IF EXISTS {name}" for sql in calls)
    finally:
        engine.dispose()


@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
@pytest.mark.parametrize(
    "action, rejects, protected, temporary",
    [
        ("CREATE_TABLE", False, False, True),
        ("SETUP_TABLE", False, False, True),
        ("OVERWRITE_TABLE", False, False, True),
        ("APPEND_TABLE", False, False, True),
        ("DELETE_ROWS", False, False, True),
        ("SCD1_MERGE", False, False, False),
        ("SCD2_MERGE", False, False, False),
        ("APPEND_TABLE", True, False, False),
        ("DELETE_ROWS", False, True, False),
    ],
)
def test_action_stages_keep_mutable_work_persistent(
    monkeypatch, table_format, action, rejects, protected, temporary
):
    from datetime import UTC, datetime

    from etl_craft.core.enums import SqlAction
    from etl_craft.handlers.sql.context import ActionContext
    from etl_craft.handlers.sql.spec import RejectLimit, SqlTask

    engine = create_engine("sqlite://")
    calls = []
    try:
        with engine.connect() as conn:
            session = Session(
                conn,
                resolve("databricks", table_format),
                catalog="catalog",
                action=action,
                target_object="schema.target",
                task_run_id=1,
                params={},
            )
            session.protection = object() if protected else None
            monkeypatch.setattr(Session, "run", lambda self, sql, **kwargs: calls.append(sql))
            monkeypatch.setattr(conn, "execute", lambda sql: calls.append(str(sql)))
            task = SqlTask(
                SqlAction(action),
                "schema.target",
                "SELECT 1 AS id",
                "inline",
                reject_limit=RejectLimit(0, False) if rejects else None,
            )
            context = ActionContext(task, None, None, "test", datetime(2026, 1, 1, tzinfo=UTC))
            try:
                stage = context.build_stage(session)
                assert calls[0].startswith(
                    "CREATE TEMPORARY TABLE" if temporary else "CREATE TABLE"
                )
                assert ("." not in stage) == temporary
                assert session.columns(stage) == [("id", "")]
            finally:
                session.sweep()
            assert any(sql.startswith("DROP TABLE") for sql in calls) != temporary
    finally:
        engine.dispose()
