"""Non-default released data, including explicit oracles for normalized and renamed fields."""

import hashlib
import json
import re
from contextlib import contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal

from sqlalchemy import Boolean, Date, DateTime, Integer, Numeric, inspect, text

from etl_craft.core.actor import SYSTEM_ACTOR

STAMP = "2001-02-03 04:05:06+00:00"
END = "2001-02-03 05:05:06+00:00"
COMMON = {
    "PIPELINE_ID": 101,
    "TASK_ID": 201,
    "PIPELINE_RUN_ID": 701,
    "TASK_RUN_ID": 801,
    "DEPENDS_ON_PIPELINE_ID": 102,
    "DEPENDS_ON_TASK_ID": 202,
    "PIPELINE_DEPENDENCY_ID": 401,
    "TASK_DEPENDENCY_ID": 501,
    "BUSINESS_RULE_ID": 601,
    "BUSINESS_RULE_RUN_ID": 1101,
    "ATTEMPT_ID": 1701,
    "CONSUMED_PIPELINE_RUN_ID": 702,
    "CONSUMED_TASK_RUN_ID": 802,
    "LAST_CONSUMED_PIPELINE_RUN_ID": 702,
    "LAST_CONSUMED_TASK_RUN_ID": 802,
    "SELECTED_PIPELINE_RUN_ID": 702,
    "SELECTED_TASK_RUN_ID": 802,
    "ACTIVE_FLAG": "N",
    "CONSUME_REPAIRS": "N",
    "BACKFILL": "Y",
    "REPAIR_PENDING": "Y",
    "CATCHUP": "Y",
    "MAX_CATCHUP_RUNS": 7,
    "OVERLAP_POLICY": "QUEUE",
    "PIPELINE_CODE": "LEGACY",
    "TASK_CODE": "extract",
    "TASK_TYPE": "INGESTION",
    "HANDLER": "PYTHON",
    "REFRESH_TYPE": "INCREMENTAL",
    "RUN_CONDITION": "N",
    "RUN_CONDITION_COUNT": 2,
    "RUN_SCHEDULE": "15 6 * * *",
    "SCHEDULE_TIMEZONE": "Asia/Kolkata",
    "SLA_STATUS": "BREACHED",
    "STATUS": "FAILED",
    "TRIGGER_KIND": "BACKFILL",
    "RUN_KEY": "kept-run",
    "ATTEMPT_COUNT": 2,
    "ATTEMPT_NUMBER": 1,
    "SOURCE_COUNT": 17,
    "TARGET_COUNT": 11,
    "INSERT_COUNT": 7,
    "UPDATE_COUNT": 3,
    "DELETE_COUNT": 5,
    "ROWS_WRITTEN": 15,
    "TASK_LOG": "legacy output\n" * 6000,
    "OUTPUT_REVISION": 7,
    "CONSUMED_REVISION": 7,
    "SELECTED_REVISION": 7,
    "DEPENDENCY_TYPE": "HAS_DATA",
    "OFFSET_TYPE": "TEXT",
    "OFFSET_VALUE": "kept offset",
    "BUSINESS_RULE_TYPE": "REPORT",
    "BUSINESS_RULE_KEY_COLUMN": "id",
    "BUSINESS_RULE_SQL": "SELECT id FROM legacy.raw.target",
    "TARGET_OBJECT": "legacy.raw.target",
    "SOURCE_OBJECT": "legacy.raw.source",
    "TARGET_TABLE": "legacy.raw.target",
    "TARGET_COLUMN": "id",
    "SOURCE_COLUMN": "id",
    "SOURCE_SQL_HASH": "a" * 64,
    "DOCUMENTATION_HASH": "b" * 64,
    "CONFIG_SHA256": "c" * 64,
    "TOKEN_SHA256": "d" * 64,
    "HASH_VERSION": 2,
    "RESULT": "BYPASSED",
    "ACTION": "MARK",
    "FROM_STATUS": "FAILED",
    "TO_STATUS": "SUCCESS",
    "ACTOR_KIND": "HUMAN",
    "REQUESTED_BY_KIND": "HUMAN",
    "STARTED_BY_KIND": "HUMAN",
    "ENDED_BY_KIND": "HUMAN",
    "STOPPED_BY_KIND": "HUMAN",
    "PAUSED_BY_KIND": "HUMAN",
    "RESUMED_BY_KIND": "HUMAN",
    "COMMAND": "run",
    "OUTCOME": "REFUSED",
    "PUBLISHED_URL": "https://docs.example.test/kept",
    "TRANSFORMATION": "id",
    "ROLE": "operator",
    "PARAMETER_NAME": "SCRIPT_NAME",
    "PARAMETER_VALUE": "legacy.py",
    "PIPELINE_PARAMETERS": json.dumps({"TAGS": ["legacy"]}),
    "ARGUMENTS": json.dumps({"retained": 17}),
    "VERSION": 7,
    "RETRYABLE": False,
    "RUN_DATE": "2001-02-03",
    "SCHEDULE_START_DATE": "2000-01-01",
    "END_DATE": END,
    "LAST_CONSUMED_END_DATE": END,
    "EXPIRES_AT": END,
}

# Mutually exclusive dependency fields get separate rows so each column has a non-NULL sentinel.
ROWS = {
    "CFG_PIPELINES": [{"PIPELINE_ID": 101}, {"PIPELINE_ID": 102, "PIPELINE_CODE": "UPSTREAM"}],
    "CFG_TASKS": [{"TASK_ID": 201}, {"TASK_ID": 202, "TASK_CODE": "upstream"}],
    "CFG_TASK_PARAMETERS": [
        {"TASK_PARAMETER_ID": 301},
        {
            "TASK_PARAMETER_ID": 302,
            "TASK_ID": 202,
            "PARAMETER_NAME": "SOURCE_SQL_FILE",
            "PARAMETER_VALUE": "sql_scripts/legacy.sql",
        },
        {
            "TASK_PARAMETER_ID": 303,
            "PARAMETER_NAME": "SOURCE_SQL",
            "PARAMETER_VALUE": "SELECT 17 AS kept",
        },
    ],
    "CFG_PIPELINE_DEPENDENCY": [{"PIPELINE_DEPENDENCY_ID": 401}],
    "CFG_TASK_DEPENDENCY": [{"TASK_DEPENDENCY_ID": 501, "DEPENDS_ON_PIPELINE_ID": 101}],
    "CFG_BUSINESS_RULES": [{"BUSINESS_RULE_ID": 601}],
    "AUD_PIPELINES_RUN_LOG": [
        {"PIPELINE_RUN_ID": 701},
        {"PIPELINE_RUN_ID": 702, "PIPELINE_ID": 102, "RUN_KEY": "kept-upstream"},
    ],
    "AUD_TASK_RUN_LOG": [{"TASK_RUN_ID": 801}, {"TASK_RUN_ID": 802, "TASK_ID": 202}],
    "AUD_TASK_OFFSET_TRACKER": [{}],
    "AUD_RUN_INTERVENTIONS": [{"INTERVENTION_ID": 901}],
    "AUD_PIPELINE_PAUSES": [{"PIPELINE_PAUSE_ID": 1001}],
    "AUD_BUSINESS_RULES_RUN_LOG": [{"BUSINESS_RULE_RUN_ID": 1101}],
    "AUD_BUSINESS_RULES_RESULTS": [{"BUSINESS_RULE_RESULT_ID": 1201, "STATUS": "REPORT"}],
    "AUD_COLUMN_LINEAGE": [{"COLUMN_LINEAGE_ID": 1301}],
    "AUD_DOCS_PUBLICATION": [{"DOCS_PUBLICATION_ID": 1401}],
    "AUD_TASK_DOCUMENTATION": [{"TASK_DOCUMENTATION_ID": 1501}],
    "AUD_TARGET_HASH_VERSION": [{}],
    "AUD_OVERSEERS": [{"OVERSEER_ID": 1601, "VERSION": "0.3.0"}],
    "AUD_GATE_WAITS": [{}],
    "AUD_TASK_EVENTS": [{"EVIDENCE": "{}"}],
    "AUD_EVENT_FILES": [{"FILE_PATH": "/kept/file.csv"}],
    "AUD_EVENT_KEYS": [{"EVENT_KEY": "kept-signal"}],
    "AUD_TASK_ITEMS": [
        {
            "ITEM_KEY": "kept-item",
            "REPORT_ID": "kept-item-report",
            "ITEM_ATTEMPT_NUMBER": 3,
            "ROW_COUNT": 17,
        }
    ],
    "AUD_TASK_ATTEMPTS": [
        {"ATTEMPT_ID": 1701, "RETRYABLE": False},
        {"ATTEMPT_ID": 1702, "ATTEMPT_NUMBER": 2, "RETRYABLE": True},
    ],
    "AUD_GATE_DECISIONS": [
        {"DECISION_ID": 1801, "TASK_DEPENDENCY_ID": None},
        {"DECISION_ID": 1802, "PIPELINE_DEPENDENCY_ID": None},
    ],
    "AUD_DEPENDENCY_CONSUMPTION": [
        {"CONSUMPTION_ID": 1901, "TASK_DEPENDENCY_ID": None},
        {"CONSUMPTION_ID": 1902, "PIPELINE_DEPENDENCY_ID": None},
    ],
    "AUD_ACTIONS": [{"ACTION_ID": 2001}],
    "CFG_API_TOKENS": [{"TOKEN_ID": 2101}],
    "AUD_PIPELINE_DEPENDENCY_TRACKER": [{}],
    "AUD_TASK_DEPENDENCY_TRACKER": [{"DEPENDS_ON_PIPELINE_ID": 101}],
}


@contextmanager
def historical_rows(conn, tables):
    """Load fixed historical audit stamps; restore every original trigger before upgrading."""
    if conn.dialect.name == "sqlite":
        triggers = (
            conn.exec_driver_sql("SELECT name, sql FROM sqlite_schema WHERE type='trigger'")
            .mappings()
            .all()
        )
        for trigger in triggers:
            conn.exec_driver_sql(f'DROP TRIGGER "{trigger["name"]}"')
        try:
            yield
        finally:
            for trigger in triggers:
                conn.exec_driver_sql(trigger["sql"])
    else:
        for table in tables:
            conn.exec_driver_sql(f'ALTER TABLE "{table}" DISABLE TRIGGER USER')
        try:
            yield
        finally:
            for table in tables:
                conn.exec_driver_sql(f'ALTER TABLE "{table}" ENABLE TRIGGER USER')


def _value(column):
    name = column["name"].upper()
    if name in COMMON:
        return COMMON[name]
    if isinstance(column["type"], Boolean):
        return False
    if isinstance(column["type"], DateTime):
        return STAMP
    if isinstance(column["type"], Date):
        return "2001-02-03"
    if isinstance(column["type"], (Integer, Numeric)):
        return 17
    return "kept_" + name.lower()


def _insert(conn, table, columns, row):
    values = {column["name"]: row.get(column["name"].upper(), _value(column)) for column in columns}
    names = ", ".join(values)
    bindings = ", ".join(":" + name for name in values)
    override = (
        " OVERRIDING SYSTEM VALUE"
        if conn.dialect.name == "postgresql" and any(c.get("identity") for c in columns)
        else ""
    )
    conn.execute(text(f'INSERT INTO "{table}" ({names}){override} VALUES ({bindings})'), values)


def seed(engine, directory):
    inspector = inspect(engine)
    tables = {name.upper(): name for name in inspector.get_table_names()}
    assert set(tables) <= set(ROWS) | {"AUD_METADATA_CHANGES", "SCHEMA_MIGRATIONS"}, set(
        tables
    ) - set(ROWS)
    for table in tables.values():
        inspector.get_columns(table)
        inspector.get_pk_constraint(table)
    project = directory / "migrations"
    project.mkdir()
    sql = "SELECT 17;\n"
    (project / "0001_legacy_project.sql").write_text(sql)
    with engine.begin() as conn, historical_rows(conn, tables.values()):
        conn.execute(
            text(
                "INSERT INTO SCHEMA_MIGRATIONS (SOURCE,VERSION,CHECKSUM,APPLIED_AT) "
                "VALUES ('PROJECT','0001_legacy_project.sql',:checksum,:stamp)"
            ),
            {"checksum": hashlib.sha256(sql.encode()).hexdigest(), "stamp": STAMP},
        )
        for name, rows in ROWS.items():
            if name not in tables:
                continue
            table = tables[name]
            columns = inspector.get_columns(table)
            for row in rows:
                _insert(conn, table, columns, row)
        if "AUD_METADATA_CHANGES" in tables:
            _history(conn, tables, inspector)
        if "SCHEMA_MIGRATIONS" in tables:
            conn.execute(text("UPDATE SCHEMA_MIGRATIONS SET APPLIED_AT=:stamp"), {"stamp": STAMP})
    kept = []
    with engine.connect() as conn:
        for table in tables.values():
            rows = [dict(row) for row in conn.execute(text(f'SELECT * FROM "{table}"')).mappings()]
            assert rows, table
            columns = inspector.get_columns(table)
            for column in columns:
                name, default = column["name"], column["default"]
                populated = [row[name] for row in rows if row[name] is not None]
                assert populated, (table, name, "no non-NULL sentinel")
                if default and re.fullmatch(r"'(?:[^']|'')*'|\d+|TRUE|FALSE", str(default), re.I):
                    expected = str(default).strip("'").lower()
                    assert any(str(value).lower() != expected for value in populated), (
                        table,
                        name,
                        "only default values",
                    )
            keys = inspector.get_pk_constraint(table)["constrained_columns"]
            if table.upper() == "AUD_GATE_WAITS":
                keys = ["PIPELINE_RUN_ID", "TASK_ID"]
            assert keys, (table, "declare its natural identity")
            kept.append((table.upper(), tuple(name.upper() for name in keys), rows))
    return kept


def _history(conn, tables, inspector):
    pipeline = dict(
        conn.execute(text("SELECT * FROM CFG_PIPELINES WHERE PIPELINE_ID=101")).mappings().one()
    )
    after = {name.lower(): value for name, value in pipeline.items()}
    before = {**after, "description": "kept_previous_description"}
    table = tables["AUD_METADATA_CHANGES"]
    columns = inspector.get_columns(table)
    for identity, operation, previous, current in [
        (2201, "INSERT", None, before),
        (2202, "UPDATE", before, after),
    ]:
        _insert(
            conn,
            table,
            columns,
            {
                "CHANGE_ID": identity,
                "TABLE_NAME": "CFG_PIPELINES",
                "ROW_KEY": "101",
                "OPERATION": operation,
                "BEFORE_JSON": None
                if previous is None
                else json.dumps(previous, default=_json_value),
                "AFTER_JSON": json.dumps(current, default=_json_value),
                "MIGRATION": "0001_legacy.sql",
                "CHANGED_AT": STAMP if identity == 2201 else END,
            },
        )


def _json_value(value):
    return float(value) if isinstance(value, Decimal) else str(value)


def _normal(value):
    if isinstance(value, str) and value.startswith(("{", "[")):
        value = json.loads(value)
    if isinstance(value, dict):
        return {key: _normal(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normal(item) for item in value]
    if isinstance(value, str) and re.match(r"\d{4}-\d\d-\d\d[ T]", value):
        value = datetime.fromisoformat(value)
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC).astimezone(UTC).isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    return value


def _expression(table, column):
    if table == "AUD_OVERSEERS" and column == "OVERSEER_ID":
        return "t.CONTROLLER_ID"
    if table == "CFG_API_TOKENS" and column == "CREATED_AT":
        return "t.CREATE_DATE"
    if table == "CFG_API_TOKENS" and column == "ROLE":
        return "(SELECT a.ROLE FROM CFG_ACCOUNTS a WHERE a.ACCOUNT_ID=t.ACCOUNT_ID)"
    if table == "CFG_BUSINESS_RULES" and column == "TARGET_TABLE":
        return (
            "(SELECT a.CATALOG_NAME||'.'||a.SCHEMA_NAME||'.'||a.TABLE_NAME "
            "FROM CFG_TABLES a WHERE a.TABLE_ID=t.TARGET_TABLE_ID)"
        )
    objects = {
        "AUD_COLUMN_LINEAGE": {"SOURCE_OBJECT": "SOURCE", "TARGET_OBJECT": "TARGET"},
        "AUD_BUSINESS_RULES_RESULTS": {"TARGET_TABLE": "TARGET"},
        "AUD_TARGET_HASH_VERSION": {"TARGET_OBJECT": "TARGET"},
    }
    if column in objects.get(table, {}):
        prefix = objects[table][column]
        return f"t.{prefix}_CATALOG_NAME||'.'||t.{prefix}_SCHEMA_NAME||'.'||t.{prefix}_TABLE_NAME"
    return "t." + column


def _assert_fields(actual, expected, location):
    expected, actual = _normal(expected), _normal(actual)
    if isinstance(expected, dict):
        assert isinstance(actual, dict), location
        for name, value in expected.items():
            assert name in actual, (location, name)
            _assert_fields(actual[name], value, (location, name))
    else:
        assert actual == expected, (location, actual, expected)


def assert_retained(engine, kept, upgrade_window):
    with engine.connect() as conn:
        for table, keys, rows in sorted(kept, key=lambda item: item[0] != "AUD_TASK_ATTEMPTS"):
            if table == "AUD_COLUMN_LINEAGE":
                # 0031 deliberately discards derived cache; history starts at recomputation.
                assert (
                    conn.execute(text("SELECT COUNT(*) FROM AUD_COLUMN_LINEAGE")).scalar_one() == 0
                )
                continue
            if table.endswith("_DEPENDENCY_TRACKER"):
                _tracker(conn, table, rows)
                continue
            destination = "AUD_CONTROLLERS" if table == "AUD_OVERSEERS" else table
            for row in rows:
                original = {name.upper(): value for name, value in row.items()}
                # 0047 renames script parameters and makes script paths relative to the project.
                renamed = table == "CFG_TASK_PARAMETERS" and original.get("PARAMETER_NAME") in {
                    "SCRIPT_NAME",
                    "SOURCE_SQL_FILE",
                    "SOURCE_SQL",
                }
                if renamed:
                    name, value = original["PARAMETER_NAME"], original["PARAMETER_VALUE"]
                    original["PARAMETER_NAME"], original["PARAMETER_VALUE"] = {
                        "SCRIPT_NAME": ("SCRIPT_FILE", _in_folder("python_scripts/", value)),
                        "SOURCE_SQL_FILE": ("SCRIPT_FILE", _in_folder("sql_scripts/", value)),
                        "SOURCE_SQL": ("SELECT_QUERY", value),
                    }[name]
                columns = ", ".join(
                    f"{_expression(table, name)} AS {name.lower()}" for name in original
                )
                where = " AND ".join(
                    f"{_expression(table, name)}=:key{i}" for i, name in enumerate(keys)
                )
                parameters = {f"key{i}": original[name] for i, name in enumerate(keys)}
                found = (
                    conn.execute(
                        text(f"SELECT {columns} FROM {destination} t WHERE {where}"), parameters
                    )
                    .mappings()
                    .all()
                )
                assert len(found) == 1, (table, parameters, original, found)
                actual = found[0]
                if renamed:
                    assert actual["updated_by"] == SYSTEM_ACTOR.name
                    assert (
                        _normal(upgrade_window[0])
                        <= _normal(actual["updated_date"])
                        <= _normal(upgrade_window[1])
                    )
                    original["UPDATED_BY"] = SYSTEM_ACTOR.name
                    original["UPDATED_DATE"] = actual["updated_date"]
                _assert_fields(
                    dict(actual),
                    {name.lower(): value for name, value in original.items()},
                    (table, parameters),
                )


def _in_folder(folder, value):
    """0047 adds the folder to a script path that lacks it, and never doubles it."""
    return value if value.startswith(folder) else folder + value


def _tracker(conn, table, rows):
    task = table == "AUD_TASK_DEPENDENCY_TRACKER"
    moved = {
        "LAST_CONSUMED_PIPELINE_RUN_ID": "t.CONSUMED_PIPELINE_RUN_ID",
        "LAST_CONSUMED_TASK_RUN_ID": "t.CONSUMED_TASK_RUN_ID",
        "LAST_UPDATED_TIMESTAMP": "t.CONSUMED_AT",
        "LAST_CONSUMED_END_DATE": "r.END_DATE",
        "DEPENDS_ON_TASK_ID": "s.TASK_ID",
    }
    key = "TASK_DEPENDENCY_ID" if task else "PIPELINE_DEPENDENCY_ID"
    for row in rows:
        columns = ", ".join(
            f"{moved.get(name.upper(), 't.' + name.upper())} AS {name.lower()}" for name in row
        )
        join = " JOIN AUD_TASK_RUN_LOG s ON s.TASK_RUN_ID=t.CONSUMED_TASK_RUN_ID" if task else ""
        actual = (
            conn.execute(
                text(
                    f"SELECT {columns} FROM AUD_DEPENDENCY_CONSUMPTION t "
                    "JOIN AUD_PIPELINES_RUN_LOG r "
                    f"ON r.PIPELINE_RUN_ID=t.CONSUMED_PIPELINE_RUN_ID{join} WHERE t.{key}=:id"
                ),
                {"id": next(value for name, value in row.items() if name.upper() == key)},
            )
            .mappings()
            .one()
        )
        _assert_fields(dict(actual), {name.lower(): value for name, value in row.items()}, table)
