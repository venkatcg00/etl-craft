"""Every Pipeline Insights panel equals an independent reading of the audit rows."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from etl_craft.core.actor import acting_as
from etl_craft.core.counts import Counts
from etl_craft.core.enums import ActionName, ApiRole, AttemptStatus, RunStatus
from etl_craft.core.errors import UsageError
from etl_craft.core.time import as_utc
from etl_craft.engine import transitions
from etl_craft.services.operations.identity import authenticated_context
from fixtures.actions import act
from fixtures.metadata import add_pipeline, add_task, start_run
from fixtures.run_scene import OWNER, seed


def sales_run(conn, pipeline, task, status, counts):
    run = start_run(conn, pipeline)
    summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
    attempt = transitions.queue_attempt(conn, summary, OWNER)
    lease = datetime.now(UTC) + timedelta(minutes=5)
    transitions.claim_attempt(conn, attempt, OWNER, owner="w1", lease_expires_at=lease)
    transitions.start_attempt(conn, attempt, OWNER, owner="w1", host="worker-1")
    ended = AttemptStatus.SUCCESS if status == RunStatus.SUCCESS else AttemptStatus.FAILED
    transitions.finish_attempt(conn, attempt, ended, OWNER, owner="w1", counts=counts)
    if status == RunStatus.SUCCESS:
        transitions.finish_task_run(conn, summary, status=status, counts=counts)
    transitions.finalize_pipeline_run(conn, run, status, sla_in_hours=None)
    return run


@pytest.fixture
def scene(engine_db):
    ctx, orders_run, _ = seed(engine_db.engine, engine_db.config)
    with acting_as(OWNER), engine_db.engine.begin() as conn:
        sales = add_pipeline(conn, "SALES")
        load = add_task(conn, sales, "LOAD")
        loaded = sales_run(conn, sales, load, RunStatus.SUCCESS, Counts(10, 9, 7, 2, 0, None, 1))
        failed = sales_run(conn, sales, load, RunStatus.FAILED, Counts())
        old = sales_run(conn, sales, load, RunStatus.SUCCESS, Counts(100, 100, 100, 0, 0))
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET START_DATE=:at WHERE PIPELINE_RUN_ID=:r"),
            {"at": datetime.now(UTC) - timedelta(days=30), "r": old},
        )
    return ctx, {"orders": orders_run, "loaded": loaded, "failed": failed, "old": old}


def audited_runs(engine, start):
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT p.PIPELINE_CODE AS code, r.PIPELINE_RUN_ID AS id, r.STATUS AS status, "
                "r.START_DATE AS started, r.END_DATE AS ended, r.SLA_STATUS AS sla "
                "FROM AUD_PIPELINES_RUN_LOG r JOIN CFG_PIPELINES p ON p.PIPELINE_ID=r.PIPELINE_ID "
                "WHERE r.START_DATE >= :start"
            ),
            {"start": start},
        ).all()


def test_run_and_duration_panels_match_the_audit(scene, engine_db):
    ctx, runs = scene
    view = act(ctx, ActionName.INSIGHTS)
    rows = audited_runs(engine_db.engine, view.window_start)
    assert runs["old"] not in {row.id for row in rows}
    for code in ("ORDERS", "SALES"):
        mine = [row for row in rows if row.code == code]
        panel = next(p for p in view.per_pipeline if p.pipeline_code == code)
        finished = [row for row in mine if row.status != RunStatus.IN_PROGRESS]
        succeeded = sum(row.status == RunStatus.SUCCESS for row in mine)
        assert (panel.runs, panel.finished, panel.succeeded) == (
            len(mine),
            len(finished),
            succeeded,
        )
        assert panel.success_rate == (succeeded / len(finished) if finished else None)
        seconds = [(as_utc(r.ended) - as_utc(r.started)).total_seconds() for r in finished]
        if seconds:
            assert panel.average_seconds == pytest.approx(sum(seconds) / len(seconds), abs=0.01)
            assert panel.longest_seconds == pytest.approx(max(seconds), abs=0.01)
        else:
            assert panel.average_seconds is None and panel.longest_seconds is None
        assert panel.sla_breached == sum(row.sla == "BREACHED" for row in mine)
    assert view.total.runs == len(rows)
    assert sum(day.runs for day in view.days) == len(rows)
    latest = [run.pipeline_run_id for run in view.latest]
    assert latest == sorted((row.id for row in rows), key=lambda i: -i)[: len(latest)]


def test_attempt_task_and_failure_panels_keep_unreported_counts_unmeasured(scene, engine_db):
    ctx, _ = scene
    view = act(ctx, ActionName.INSIGHTS)
    orders = next(p for p in view.per_pipeline if p.pipeline_code == "ORDERS")
    assert (orders.attempts, orders.retries, orders.failed_attempts) == (2, 1, 1)
    assert orders.rows_written is None and orders.rows_per_second is None
    sales = next(p for p in view.per_pipeline if p.pipeline_code == "SALES")
    assert sales.attempts == 2 and sales.failed_attempts == 1
    assert sales.rows_written == 9
    load = next(t for t in view.tasks if t.task_code == "LOAD")
    assert (load.runs, load.source_rows, load.inserted, load.updated, load.rejected) == (
        1,
        10,
        7,
        2,
        1,
    )
    extract = next(t for t in view.tasks if t.task_code == "EXTRACT")
    assert extract.source_rows is None and extract.inserted is None
    failures = {(f.pipeline_code, f.task_code, f.status): f.attempts for f in view.failures}
    assert failures == {("ORDERS", "EXTRACT", "FAILED"): 1, ("SALES", "LOAD", "FAILED"): 1}


def test_window_filters_and_visibility(scene, engine_db):
    ctx, runs = scene
    today = datetime.now(UTC).date()
    old = act(ctx, ActionName.INSIGHTS, start=today - timedelta(days=31), end=today)
    assert runs["old"] in {row.pipeline_run_id for row in old.latest}
    only = act(ctx, ActionName.INSIGHTS, pipelines=["SALES"])
    assert only.pipelines == ("SALES",) and [p.pipeline_code for p in only.per_pipeline] == [
        "SALES"
    ]
    with pytest.raises(UsageError, match="at most 92"):
        act(ctx, ActionName.INSIGHTS, start=today - timedelta(days=92), end=today)
    with pytest.raises(UsageError, match="after end"):
        act(ctx, ActionName.INSIGHTS, start=today, end=today - timedelta(days=1))
    token = act(ctx, ActionName.TOKEN_CREATE, name="reader", role=ApiRole.VIEWER)
    reader = authenticated_context(ctx, token.metadata)
    with acting_as(OWNER), engine_db.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_GRANTS (PIPELINE_CODE,GRANTEE_ROLE,PERMISSION) "
                "VALUES ('SALES','operator','CAN_VIEW')"
            )
        )
    hidden = act(reader, ActionName.INSIGHTS)
    assert hidden.pipelines == ("ORDERS",)
    with pytest.raises(UsageError, match="SALES"):
        act(reader, ActionName.INSIGHTS, pipelines=["SALES"])


def test_cli_prints_the_same_document_as_the_api(metadata_project, monkeypatch, capsys):
    import json

    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app
    from etl_craft.cli import main

    p = metadata_project
    ctx, _, _ = seed(p.engine, p.config)
    token = act(ctx, ActionName.TOKEN_CREATE, name="reader", role=ApiRole.VIEWER).token
    monkeypatch.chdir(p.root)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", token)
    assert main(["insights", "--format", "json"]) == 0
    printed = json.loads(capsys.readouterr().out)
    served = TestClient(create_app(ctx)).get(
        "/api/v1/projects/default/insights", headers={"Authorization": f"Bearer {token}"}
    )
    assert served.status_code == 200
    assert printed["schema"] == served.json()["schema"] == "etl-craft/insights/1"
    assert printed["total"] == served.json()["total"]
    assert main(["insights"]) == 0
    assert "ORDERS" in capsys.readouterr().out


def test_generated_catalog_snapshot_matches_the_live_read(scene, engine_db, tmp_path):
    import json
    import re

    from etl_craft.services.catalog.collect import build_catalog
    from etl_craft.services.catalog.site import write_site

    ctx, _ = scene
    folder = write_site(
        build_catalog(engine_db.engine, engine_db.config), engine_db.config, tmp_path / "site"
    ).folder
    snapshot = json.loads((folder / "insights.json").read_text())
    live = act(ctx, ActionName.INSIGHTS)
    assert snapshot["window_start"] == live.window_start.isoformat()
    assert snapshot["total"]["runs"] == live.total.runs
    assert [p["runs"] for p in snapshot["per_pipeline"]] == [p.runs for p in live.per_pipeline]
    page = (folder / "insights.html").read_text()
    assert re.search(r"<td>LOAD</td><td>1</td><td>10</td>", page)
    assert "Not measured" in page
    assert 'href="insights.html"' in (folder / "index.html").read_text()


def test_rules_panel_counts_rule_runs_and_lists_table_rule_observations(scene, engine_db):
    from etl_craft.engine.queries import statement
    from etl_craft.engine.repository.business_rules import begin_rule_run, finish_rule_run
    from fixtures.metadata import add_table, insert

    ctx, runs = scene
    now = datetime.now(UTC)
    with acting_as(OWNER), engine_db.engine.begin() as conn:
        ids = conn.execute(
            text(
                "SELECT t.TASK_RUN_ID AS run, k.PIPELINE_ID AS pipeline_id, k.TASK_ID AS task_id "
                "FROM AUD_TASK_RUN_LOG t JOIN CFG_TASKS k ON k.TASK_ID=t.TASK_ID "
                "WHERE t.PIPELINE_RUN_ID=:r"
            ),
            {"r": runs["loaded"]},
        ).one()
        task_run = ids.run
        rule = insert(
            conn,
            "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
            "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, TARGET_TABLE_ID, SEQUENCE_NUMBER, "
            "BUSINESS_RULE_LEVEL) VALUES ('ROW_COUNT', :p, :t, "
            "'SELECT 1 AS PASSED', 'REPORT', :target, 1, 'TABLE')",
            "BUSINESS_RULE_ID",
            p=ids.pipeline_id,
            t=ids.task_id,
            target=add_table(conn, "mart.sales"),
        )
        bound = begin_rule_run(conn, rule, task_run, now)
        conn.execute(
            statement(conn, "table_rule_result"),
            {
                "business_rule_run_id": bound.business_rule_run_id,
                "passed": "N",
                "observed": "41",
                "detail": "below 50",
            },
        )
        finish_rule_run(conn, bound.business_rule_run_id, RunStatus.SUCCESS, now)
    panel = next(r for r in act(ctx, ActionName.INSIGHTS).rules if r.rule_name == "ROW_COUNT")
    assert (panel.pipeline_code, panel.runs, panel.failed, panel.not_passed) == ("SALES", 1, 0, 1)
    assert [(o.pipeline_run_id, o.observed, o.passed) for o in panel.observations] == [
        (runs["loaded"], "41", "N")
    ]
