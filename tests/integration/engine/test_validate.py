"""``validate``: every problem in the metadata, found without running anything, both Engine DBs."""

import json
import logging

import pytest
import yaml
from sqlalchemy import text

from etl_craft.cli import main
from etl_craft.config import load_config
from etl_craft.core.errors import ExitCode, MetadataError
from etl_craft.services.doctor import Status
from etl_craft.services.validate import validate
from fixtures.metadata import add_dependency, add_pipeline, add_pipeline_dependency, add_task
from fixtures.metadata import insert as insert_row

GOOD_SCRIPT = "def run(task):\n    return None\n"


@pytest.fixture(autouse=True)
def restore_logger():
    logger = logging.getLogger("etl_craft")
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


@pytest.fixture
def project(engine_db, tmp_path, monkeypatch):
    """A project with a DuckDB warehouse, email, a SQL file and an ingestion script."""
    profile = engine_db.config.engine.active
    schema = "public" if profile.jdbc_url.startswith("jdbc:postgresql") else "main"
    block = {"jdbc_url": profile.jdbc_url, "schema": schema}
    if profile.auth_mode != "none":
        block |= {
            "user": profile.user,
            "auth_mode": profile.auth_mode,
            "secret": profile.secret_var,
        }
    root = (engine_db.config.config_path or tmp_path / "craft-connector.yml").parent
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {
            "Mode": "local",
            "Email": {"host": "localhost", "port": 1025, "from_address": "etl@x.io"},
        },
        "Engine": {"dev": block},
        "Warehouse": {"dev": {"jdbc_url": "jdbc:duckdb:analytics.duckdb", "schema": "main"}},
    }
    (root / "craft-connector.yml").write_text(yaml.safe_dump(raw, sort_keys=False), "utf-8")
    (root / "sql_files").mkdir(exist_ok=True)
    (root / "sql_files" / "orders.sql").write_text("SELECT id, amount FROM raw.orders", "utf-8")
    (root / "ingestion_scripts").mkdir(exist_ok=True)
    (root / "ingestion_scripts" / "load.py").write_text(GOOD_SCRIPT, "utf-8")
    monkeypatch.chdir(root)
    return engine_db.engine, load_config(root / "craft-connector.yml"), root


def add_rule(conn, pipeline, task, name, *, key="id", target="sales.orders", sql=None):
    return insert_row(
        conn,
        "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
        "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE, "
        "SEQUENCE_NUMBER) VALUES (:name, :p, :t, :rule_sql, 'REJECT', :key, :target, 1)",
        "BUSINESS_RULE_ID",
        name=name,
        p=pipeline,
        t=task,
        rule_sql=sql or "SELECT 1 WHERE t.amount < 0",
        key=key,
        target=target,
    )


def good_pipeline(conn, code="SALES"):
    """ingest, then stage (from a file) and setup, then load, rules and alert."""
    p = add_pipeline(conn, code)
    ingest = add_task(conn, p, "ingest", "PYTHON", SCRIPT_NAME="load.py", INPUT_PARAMS='{"a": 1}')
    setup = add_task(
        conn,
        p,
        "setup",
        SQL_ACTION="SETUP_TABLE",
        TARGET_OBJECT="sales.orders",
        SOURCE_SQL_FILE="orders.sql",
    )
    load = add_task(
        conn,
        p,
        "load",
        SQL_ACTION="SCD1_MERGE",
        TARGET_OBJECT="sales.orders",
        SOURCE_SQL="SELECT id, amount FROM staging.orders",
        MERGE_KEY="id",
        MERGE_COMPARE_COLUMNS="amount",
        DOCUMENTATION="Merges orders.",
    )
    rules = add_task(conn, p, "rules", "BUSINESS_RULES")
    add_rule(conn, p, rules, "negative amount")
    alert = add_task(
        conn,
        p,
        "alert",
        "EMAIL_ALERT",
        EMAIL_TO="ops@x.io",
        EMAIL_SUBJECT="$$pipeline_code: $$status",
        EMAIL_BODY="Run $$pipeline_id finished.",
    )
    add_dependency(conn, p, setup, ingest)
    add_dependency(conn, p, load, setup)
    add_dependency(conn, p, rules, load, "HAS_DATA")
    add_dependency(conn, p, alert, rules, "ALWAYS")
    return p


def found(report):
    return [(f.where, f.status, f.message) for f in report.findings]


def test_a_clean_project_has_no_findings(project):
    engine, config, _ = project
    with engine.begin() as conn:
        good_pipeline(conn)
    report = validate(engine, config)
    assert found(report) == []
    assert (report.pipelines, report.tasks, report.failed) == (1, 5, False)


def test_task_definitions_are_checked_with_the_handlers_own_rules(project):
    engine, config, root = project
    (root / "ingestion_scripts" / "two.py").write_text("def run(a, b):\n    pass\n", "utf-8")
    (root / "ingestion_scripts" / "broken.py").write_text("def run(:\n", "utf-8")
    (root / "ingestion_scripts" / "none.py").write_text("x = 1\n", "utf-8")
    with engine.begin() as conn:
        p = add_pipeline(conn, "BAD")
        add_task(conn, p, "no_action", TARGET_OBJECT="s.t", SOURCE_SQL="SELECT 1 AS a")
        add_task(
            conn,
            p,
            "writes",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="s.t",
            SOURCE_SQL="DELETE FROM s.t",
        )
        add_task(
            conn,
            p,
            "no_file",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="s.u",
            SOURCE_SQL_FILE="missing.sql",
        )
        add_task(
            conn,
            p,
            "typo",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="s.v",
            SOURCE_SQL="SELECT 1 AS a",
            MERGE_KEYS="a",
            TASK_TIMEOUT_SECONDS="soon",
        )
        add_task(
            conn,
            p,
            "orphan_setup",
            SQL_ACTION="SETUP_TABLE",
            TARGET_OBJECT="s.w",
            SOURCE_SQL="SELECT 1 AS a",
        )
        add_task(
            conn,
            p,
            "iceberg_path",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="s.x",
            SOURCE_SQL="SELECT 1 AS a",
            EXTERNAL_LOCATION="s3://b/x",
        )
        add_task(conn, p, "two_args", "PYTHON", SCRIPT_NAME="two.py")
        add_task(conn, p, "syntax", "PYTHON", SCRIPT_NAME="broken.py")
        add_task(conn, p, "no_run", "PYTHON", SCRIPT_NAME="none.py", INPUT_PARAMS="[1]")
        add_task(conn, p, "no_script", "PYTHON", SCRIPT_NAME="gone.py")
        add_task(conn, p, "no_rules", "BUSINESS_RULES")
        add_task(
            conn,
            p,
            "rebuild",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="s.y",
            SOURCE_SQL="SELECT 1 AS a",
        )
        rules = add_task(conn, p, "rules", "BUSINESS_RULES")
        add_rule(conn, p, rules, "on row id", key="ROW_ID", target="s.y")
        add_rule(conn, p, rules, "two statements", target="s.y", sql="SELECT 1; SELECT 2")
        add_task(
            conn,
            p,
            "alert",
            "EMAIL_ALERT",
            EMAIL_TO="ops",
            EMAIL_SUBJECT_FAILED="x $$when",
            EMAIL_ON_STATUS="FAILED|SUCCESS",
        )
        for code in (
            "no_action",
            "writes",
            "no_file",
            "typo",
            "orphan_setup",
            "iceberg_path",
            "two_args",
            "syntax",
            "no_run",
            "no_script",
            "no_rules",
            "rules",
        ):
            upstream = conn.execute(
                text("SELECT TASK_ID FROM CFG_TASKS WHERE TASK_CODE = :c AND PIPELINE_ID = :p"),
                {"c": code, "p": p},
            ).scalar_one()
            alert = conn.execute(
                text("SELECT TASK_ID FROM CFG_TASKS WHERE TASK_CODE = 'alert'")
            ).scalar_one()
            add_dependency(conn, p, alert, upstream, "ALWAYS")
    report = validate(engine, config)
    by_task = {}
    for f in report.findings:
        by_task.setdefault(f.where, []).append((f.status, f.message))
    assert report.failed
    fail = Status.FAIL

    def only(task):
        ((status, message),) = by_task[f"BAD.{task}"]
        assert status is fail
        return message

    assert only("no_action").startswith("SQL_ACTION is required")
    assert "read-only" in only("writes")
    assert "missing.sql" in only("no_file")
    assert sorted(by_task["BAD.typo"]) == [
        (fail, "CFG_TASK_PARAMETERS.TASK_TIMEOUT_SECONDS='soon' is not a whole number"),
        (
            Status.WARN,
            "SQL task parameter MERGE_KEYS is not read by etl-craft, so it has no "
            "effect; did you mean MERGE_KEY, MERGE_COMPARE_COLUMNS, "
            "MERGE_DEDUPE_ORDER",
        ),
    ]
    assert "set SETUP_FOR" in only("orphan_setup")
    assert "EXTERNAL_LOCATION does not apply to DuckDB" in only("iceberg_path")
    assert only("two_args") == "two.py: run takes 2 arguments; it takes the task, or nothing"
    assert only("syntax").startswith("SCRIPT_NAME='broken.py' has a syntax error at line 1")
    assert "INPUT_PARAMS must be a JSON object" in only("no_run")
    assert "gone.py" in only("no_script")
    assert "no active row in CFG_BUSINESS_RULES" in only("no_rules")
    rules_messages = [m for _, m in by_task["BAD.rules"]]
    assert any(
        "BUSINESS_RULE_KEY_COLUMN is ROW_ID, but BAD.rebuild rebuilds s.y" in m
        for m in rules_messages
    )
    assert any("must be one correlated SELECT" in m for m in rules_messages)
    alert_messages = [m for _, m in by_task["BAD.alert"]]
    assert "EMAIL_TO has address(es) that are not valid: ops" in alert_messages
    assert any(m.startswith("no EMAIL_SUBJECT for outcome(s) SUCCESS") for m in alert_messages)
    assert any(m.startswith("no EMAIL_BODY for outcome(s) FAILED, SUCCESS") for m in alert_messages)
    assert any("unknown token(s) $$when" in m for m in alert_messages)
    assert "BAD.rebuild" not in by_task


def test_dependencies_and_pipeline_settings(project):
    engine, config, _ = project
    with engine.begin() as conn:
        good = good_pipeline(conn, "GOOD")
        up = add_pipeline(conn, "UP")
        off = add_task(conn, up, "off", "PYTHON", SCRIPT_NAME="load.py")
        down = add_pipeline(conn, "DOWN")
        waits = add_task(conn, down, "waits", "PYTHON", SCRIPT_NAME="load.py")
        rules_data = add_task(conn, down, "after_rules", "PYTHON", SCRIPT_NAME="load.py")
        alert = add_task(
            conn,
            down,
            "alert",
            "EMAIL_ALERT",
            EMAIL_TO="ops@x.io",
            EMAIL_SUBJECT="s",
            EMAIL_BODY="b",
        )
        add_dependency(conn, down, waits, off, upstream_pipeline=up)
        rules = conn.execute(
            text("SELECT TASK_ID FROM CFG_TASKS WHERE TASK_CODE = 'rules'")
        ).scalar_one()
        add_dependency(conn, down, rules_data, rules, "HAS_DATA", upstream_pipeline=good)
        add_dependency(conn, down, alert, waits, "SUCCESS")
        conn.execute(text("UPDATE CFG_TASKS SET ACTIVE_FLAG = 'N' WHERE TASK_ID = :t"), {"t": off})
        add_pipeline_dependency(conn, down, good)
        add_pipeline_dependency(conn, good, down)
        gone = add_pipeline(conn, "GONE")
        add_pipeline_dependency(conn, down, gone)
        conn.execute(
            text("UPDATE CFG_PIPELINES SET ACTIVE_FLAG = 'N' WHERE PIPELINE_ID = :p"), {"p": gone}
        )
        conn.execute(
            text("UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS = :v WHERE PIPELINE_ID = :p"),
            {"p": down, "v": json.dumps({"RETRIES": "3", "TAG": ["x"]})},
        )
        bad_code = add_pipeline(conn, "bad code")
        add_task(conn, bad_code, "x.y", "PYTHON", SCRIPT_NAME="load.py")
    report = validate(engine, config)
    got = found(report)
    fail, warn = Status.FAIL, Status.WARN
    assert (
        "DOWN.waits",
        fail,
        "depends on UP.off, whose task is inactive and no longer runs, so once its last run is "
        "consumed the dependency is never satisfied again; deactivate the dependency too, or "
        "reactivate it",
    ) in got
    assert (
        "DOWN.after_rules",
        fail,
        "has a HAS_DATA dependency on GOOD.rules, which is a BUSINESS_RULES task and reports no "
        "target rows, so the dependency is never satisfied and the task is always skipped; use "
        "SUCCESS",
    ) in got
    assert (
        "DOWN.alert",
        fail,
        "reports on the whole run but does not depend on after_rules, so it can run before they "
        "finish and report on a run still in progress; add an ALWAYS dependency on each",
    ) in got
    assert ("DOWN.alert", warn) == got[[g[0] for g in got].index("DOWN.alert") + 1][:2]
    assert (
        "DOWN",
        fail,
        "pipeline dependencies form a cycle (DOWN -> GOOD -> DOWN), so none of these pipelines "
        "can start",
    ) in got
    assert (
        "DOWN",
        fail,
        'PIPELINE_PARAMETERS.RETRIES="3" must be a whole number, 0 or more',
    ) in got
    assert (
        "DOWN",
        warn,
        "PIPELINE_PARAMETERS TAG is not read by etl-craft, so it has no effect; did you mean TAGS",
    ) in got
    assert any(w == "DOWN" and "GONE, which is inactive" in m for w, _, m in got)
    assert any(
        w == "bad code" and m.startswith("PIPELINE_CODE='bad code' may hold") for w, _, m in got
    )
    assert any(w == "bad code.x.y" and m.startswith("TASK_CODE='x.y'") for w, _, m in got)
    # The rules task reports no rows, but its own pipeline is clean apart from the cycle.
    assert [f for f in got if f[0].startswith("GOOD.")] == []


def test_a_cycle_inside_a_pipeline(project):
    engine, config, _ = project
    with engine.begin() as conn:
        p = add_pipeline(conn, "LOOP")
        a = add_task(conn, p, "a", "PYTHON", SCRIPT_NAME="load.py")
        b = add_task(conn, p, "b", "PYTHON", SCRIPT_NAME="load.py")
        add_dependency(conn, p, a, b)
        add_dependency(conn, p, b, a)
    (finding,) = validate(engine, config).findings
    assert finding.where == "LOOP" and "cycle" in finding.message


def test_the_command(project, capsys):
    engine, _, _ = project
    with engine.begin() as conn:
        good_pipeline(conn)
        p = add_pipeline(conn, "OTHER")
        add_task(conn, p, "bad", "PYTHON")
    assert main(["validate", "--pipeline_code", "SALES"]) == ExitCode.SUCCESS
    assert (
        capsys.readouterr().out == "checked 1 pipeline(s) and 5 task(s): 0 failed, 0 warning(s)\n"
    )
    assert main(["validate"]) == ExitCode.FAILURE
    assert capsys.readouterr().out.splitlines() == [
        "[FAIL] OTHER.bad: SCRIPT_NAME is required for HANDLER=PYTHON: a .py file under "
        "ingestion_scripts/",
        "checked 2 pipeline(s) and 6 task(s): 1 failed, 0 warning(s)",
    ]
    assert main(["validate", "--pipeline_code", "NOPE"]) == ExitCode.METADATA
    with pytest.raises(MetadataError):
        validate(engine, load_config("craft-connector.yml"), "NOPE")
