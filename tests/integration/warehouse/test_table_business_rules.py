"""Table checks publish one bounded result, without changing row flag semantics."""

import logging
from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.errors import HandlerError
from etl_craft.services.operations.context import OperationContext
from fixtures.actions import act
from fixtures.business_rules import add_rule, run_rules
from fixtures.metadata import add_pipeline, add_task, start_run
from fixtures.sql_warehouse import warehouses
from integration.warehouse.test_business_rules import world as world


@pytest.fixture
def table_world(world, engine_db):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "rules", "BUSINESS_RULES")
        run_id = start_run(conn, pipeline)
    world = replace(
        world,
        engine_db=engine_db.engine,
        config=replace(world.config, engine=engine_db.config.engine),
        pipeline_id=pipeline,
        pipeline_run_id=run_id,
        tasks={"rules": task},
    )
    world.execute(f"UPDATE {world.name('orders')} SET PIPELINE_RUN_ID={run_id}")
    return world


def result_row(w, rule):
    with w.engine_db.connect() as conn:
        row = conn.execute(
            text(
                "SELECT STATUS AS status,PASSED AS passed,OBSERVED AS observed,DETAIL AS detail "
                "FROM AUD_BUSINESS_RULES_RUN_LOG WHERE BUSINESS_RULE_ID=:rule"
            ),
            {"rule": rule},
        ).one()
        assert (
            conn.execute(text("SELECT COUNT(*) FROM AUD_BUSINESS_RULES_RESULTS")).scalar_one() == 0
        )
        return row


@pytest.mark.parametrize("scoped", [True, False])
@pytest.mark.parametrize("rule_type", ["REJECT", "REPORT"])
@pytest.mark.parametrize("passed", ["Y", "N"])
def test_table_rule_types_and_scopes_store_one_result_without_row_flags(
    table_world, scoped, rule_type, passed
):
    w = table_world
    w.execute(f"UPDATE {w.name('orders')} SET PIPELINE_RUN_ID=0 WHERE order_id=10")
    source = "$$rows_in_scope" if scoped else w.name("orders")
    count = 2 if scoped else 3
    expected = count if passed == "Y" else count + 1
    sql = (
        f"SELECT CASE WHEN COUNT(*)={expected} THEN 'Y' ELSE 'N' END AS PASSED, "
        f"CAST(COUNT(*) AS VARCHAR) AS OBSERVED, 'row count' AS DETAIL FROM {source}"
    )
    rule = add_rule(w, "table_count", sql, level="TABLE", key=None, rule_type=rule_type)
    if passed == "N" and rule_type == "REJECT":
        with pytest.raises(HandlerError, match=r"table_count.*task P.rules.*PASSED=N"):
            run_rules(w)
    else:
        run_rules(w)
    row = result_row(w, rule)
    assert (row.passed, row.observed, row.detail) == (passed, str(count), "row count")
    assert row.status == ("FAILED" if passed == "N" and rule_type == "REJECT" else "SUCCESS")


def test_table_output_failures_are_recorded_and_logged(table_world, caplog):
    w = table_world
    outputs = [
        ("SELECT 'Y' AS PASSED WHERE 1=0", "0 rows"),
        ("SELECT 'Y' AS PASSED UNION ALL SELECT 'N'", "2 rows"),
        ("SELECT 'Y' AS other", "columns"),
        ("SELECT true AS PASSED", "PASSED=True"),
        ("SELECT false AS PASSED", "PASSED=False"),
        ("SELECT 1 AS PASSED", "PASSED=1"),
        ("SELECT 0 AS PASSED", "PASSED=0"),
        ("SELECT '1' AS PASSED", "PASSED='1'"),
        ("SELECT '0' AS PASSED", "PASSED='0'"),
        ("SELECT 'y' AS PASSED", "PASSED='y'"),
        ("SELECT 'n' AS PASSED", "PASSED='n'"),
        ("SELECT NULL AS PASSED", "PASSED=None"),
        ("SELECT 'YES' AS PASSED", "PASSED length=3; limit=1"),
        ("SELECT 'Y' AS PASSED, 1 AS extra", "columns"),
        ("SELECT 'Y' AS PASSED, '" + "x" * 4097 + "' AS OBSERVED", "OBSERVED length=4097"),
        ("SELECT 'Y' AS PASSED, '" + "x" * 4097 + "' AS DETAIL", "DETAIL length=4097"),
    ]
    for index, (sql, expected) in enumerate(outputs):
        with w.engine_db.begin() as conn:
            conn.execute(text("UPDATE CFG_BUSINESS_RULES SET ACTIVE_FLAG='N'"))
        rule = add_rule(w, f"bad_{index}", sql, level="TABLE", key=None)
        with pytest.raises(HandlerError) as caught:
            run_rules(w)
        message = str(caught.value)
        assert expected in message and f"bad_{index}" in message and "P.rules" in message
        assert "BUSINESS_RULE_SQL" in message
        with w.engine_db.connect() as conn:
            stored = conn.execute(
                text(
                    "SELECT STATUS AS status,DETAIL AS detail FROM "
                    "AUD_BUSINESS_RULES_RUN_LOG WHERE BUSINESS_RULE_ID=:rule"
                ),
                {"rule": rule},
            ).one()
        assert stored.status == "FAILED" and expected in stored.detail
        assert expected in caplog.text


def test_table_retry_skips_successful_rules_and_force_rechecks_scoped_rows(table_world, caplog):
    caplog.set_level(logging.INFO, logger="etl_craft.handlers.business_rules")
    w = table_world
    w.execute(f"UPDATE {w.name('orders')} SET PIPELINE_RUN_ID=0 WHERE order_id=10")
    good = add_rule(w, "already_passed", "SELECT 'Y' AS PASSED", level="TABLE", key=None)
    later = add_rule(w, "later", "SELECT 'N' AS PASSED", level="TABLE", key=None, sequence=2)
    with pytest.raises(HandlerError, match="later"):
        run_rules(w)
    with w.engine_db.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_BUSINESS_RULES SET BUSINESS_RULE_SQL='SELECT 1 AS other' "
                "WHERE BUSINESS_RULE_ID=:rule"
            ),
            {"rule": good},
        )
        conn.execute(
            text(
                "UPDATE CFG_BUSINESS_RULES SET BUSINESS_RULE_SQL=:sql WHERE BUSINESS_RULE_ID=:rule"
            ),
            {
                "rule": later,
                "sql": "SELECT CASE WHEN COUNT(*)=2 THEN 'Y' ELSE 'N' END AS PASSED "
                "FROM $$rows_in_scope",
            },
        )
    run_rules(w)
    assert "already succeeded" in caplog.text
    assert result_row(w, good).passed == "Y"
    with w.engine_db.begin() as conn:
        conn.execute(
            text("UPDATE CFG_BUSINESS_RULES SET ACTIVE_FLAG='N' WHERE BUSINESS_RULE_ID=:rule"),
            {"rule": good},
        )
    with pytest.raises(HandlerError, match="PASSED=N"):
        run_rules(w, force=True)
    assert result_row(w, later).passed == "N"


def test_table_results_are_shared_by_run_history_status_explain_and_catalog(
    table_world, tmp_path, monkeypatch, capsys, cli_logging
):
    from etl_craft.services.catalog.collect import build_catalog
    from etl_craft.services.catalog.html import table_url
    from etl_craft.services.catalog.site import write_site

    w = table_world
    rule = add_rule(
        w,
        "reported",
        "SELECT 'N' AS PASSED, '4.7%' AS OBSERVED, '<unsafe>' AS DETAIL",
        level="TABLE",
        key=None,
        rule_type="REPORT",
    )
    with w.engine_db.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASKS SET HANDLER='BUSINESS_RULES' WHERE TASK_ID=:task"),
            {"task": w.tasks["rules"]},
        )
    w.new_run()
    run_rules(w)
    ctx = OperationContext(w.engine_db, w.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    run_id = w.pipeline_run_id
    detail = act(ctx, ActionName.RUN_DETAIL, run_id=run_id)
    assert detail.rules[0].business_rule_id == rule and detail.rules[0].observed == "4.7%"
    history = act(ctx, ActionName.HISTORY, pipeline_code="P")
    assert history.rules[0] == detail.rules[0]
    explained = act(ctx, ActionName.EXPLAIN, pipeline_code="P", task_code="rules", run_id=run_id)
    assert explained.task.rules[0] == detail.rules[0]
    assert "PASSED='N'" in explained.task.rules[0].describe()
    catalog = build_catalog(w.engine_db, w.config)
    assert catalog.rules[rule].observations[0] == detail.rules[0]
    status = act(ctx, ActionName.STATUS, pipeline_code="P", run_id=run_id)
    found = next(t for t in status.tasks if t.task_code == "rules")
    assert found.explanation.task.rules[0].observed == "4.7%"
    folder = tmp_path / "catalog"
    write_site(catalog, w.config, folder)
    page = (folder / table_url(catalog.rules[rule].table)).read_text()
    assert "4.7%" in page and "PASSED" in page and "&lt;unsafe&gt;" in page
    assert "<unsafe>" not in page

    import json

    import yaml

    from etl_craft.cli import main
    from etl_craft.cli.commands import explain as cli_explain
    from etl_craft.cli.commands import status as cli_status
    from fixtures.auth import service_token
    from fixtures.engine_db import profile_block

    monkeypatch.setenv("ETL_CRAFT_TOKEN", service_token(w.engine_db))

    config_path = tmp_path / "render.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "ENVIRONMENT"},
                "Orchestration": {"Project": "default"},
                "Engine": {"dev": profile_block(w.config.engine, w.config.engine.schema)},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    for command, module, document in [
        ("explain", cli_explain, explained),
        ("status", cli_status, status),
    ]:
        monkeypatch.setattr(module, "call", lambda *_, document=document: document)
        args = [
            command,
            "--config",
            str(config_path),
            "--project",
            "default",
            "--pipeline-code",
            "P",
            "--run-id",
            str(run_id),
        ]
        if command == "explain":
            args += ["--task-code", "rules"]
        assert main(args) == 0
        text_output = capsys.readouterr().out
        assert "PASSED='N'" in text_output and "4.7%" in text_output and "<unsafe>" in text_output
        assert main([*args, "--format", "json"]) == 0
        encoded = json.loads(capsys.readouterr().out)
        actual = (
            encoded["task"] if command == "explain" else encoded["tasks"][0]["explanation"]["task"]
        )
        assert actual["rules"][0]["passed"] == "N" and actual["rules"][0]["observed"] == "4.7%"


def test_table_reject_finishes_its_wave_before_refusing_later_waves(table_world):
    w = table_world
    first = add_rule(w, "first", "SELECT 'Y' AS PASSED", level="TABLE", key=None)
    bad = add_rule(w, "bad", "SELECT 'N' AS PASSED", level="TABLE", key=None)
    later = add_rule(w, "not_started", "SELECT 'Y' AS PASSED", level="TABLE", key=None, sequence=2)
    with pytest.raises(HandlerError, match="bad"):
        run_rules(w)
    assert result_row(w, first).passed == "Y" and result_row(w, bad).passed == "N"
    with w.engine_db.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) FROM AUD_BUSINESS_RULES_RUN_LOG WHERE BUSINESS_RULE_ID=:rule"
                ),
                {"rule": later},
            ).scalar_one()
            == 0
        )


@warehouses("postgres")
def test_concurrent_table_checks_publish_one_result(table_world):
    from sqlalchemy import Engine, event

    from etl_craft.handlers import business_rules
    from fixtures.races import two_at_once

    w = table_world
    rule = add_rule(w, "one", "SELECT 'Y' AS PASSED, 'once' AS DETAIL", level="TABLE", key=None)
    context = replace(w.task("rules"), handler="BUSINESS_RULES")
    checks = []

    def observed(conn, cursor, statement, parameters, context, executemany):
        if "'once' AS DETAIL" in statement:
            checks.append(statement)

    event.listen(Engine, "before_cursor_execute", observed)
    try:
        two_at_once(
            lambda: business_rules.run(context, w.engine_db),
            lambda: business_rules.run(context, w.engine_db),
            at=(business_rules, "check_rule"),
        )
    finally:
        event.remove(Engine, "before_cursor_execute", observed)
    assert len(checks) == 1 and result_row(w, rule).passed == "Y"


def test_table_publication_refuses_row_flags(table_world):
    from etl_craft.core.enums import RunStatus, YesNo
    from etl_craft.engine import locks
    from etl_craft.engine.task_store import RuleEnding
    from etl_craft.handlers.registry import task_store

    w = table_world
    rule = add_rule(w, "table_only", "SELECT 'Y' AS PASSED", level="TABLE", key=None)
    store = task_store(replace(w.task("rules"), handler="BUSINESS_RULES"), w.engine_db)
    with locks.business_rule(rule, "table_only").hold(store):
        binding = store.begin_rule(rule)
        with pytest.raises(HandlerError, match="TABLE publication contains row keys"):
            store.end_rule(
                RuleEnding(
                    rule,
                    binding.business_rule_run_id,
                    RunStatus.SUCCESS,
                    new_keys=("forged",),
                    passed=YesNo.YES,
                )
            )
        with pytest.raises(HandlerError, match="expected SUCCESS"):
            store.end_rule(
                RuleEnding(rule, binding.business_rule_run_id, RunStatus.FAILED, passed=YesNo.YES)
            )
        with pytest.raises(HandlerError, match="missing TABLE result"):
            store.end_rule(RuleEnding(rule, binding.business_rule_run_id, RunStatus.SUCCESS))
        store.end_rule(
            RuleEnding(rule, binding.business_rule_run_id, RunStatus.SUCCESS, passed=YesNo.YES)
        )
    assert result_row(w, rule).passed == "Y"
