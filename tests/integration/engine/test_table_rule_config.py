"""Partial helpers retain a rule's level; every interface validates the effective shape."""

import csv
from dataclasses import replace

import pytest
import yaml
from sqlalchemy import text

from etl_craft.config import ConnectionProfile
from etl_craft.config.object_models import Artifacts
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.errors import MetadataFileError
from etl_craft.engine.repository.business_rules import fetch_business_rules_for_task
from etl_craft.services.config_files import read_config_files, sync_config
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.validate import validate
from etl_craft.warehouse.connection import build_warehouse_engine
from fixtures.actions import act
from fixtures.engine_db import profile_block
from fixtures.metadata import add_pipeline, add_table, add_task, insert


@pytest.fixture
def rule_project(engine_db, tmp_path):
    warehouse = ConnectionProfile(
        "WAREHOUSE",
        "dev",
        f"jdbc:duckdb:{tmp_path / 'warehouse.duckdb'}",
        "",
        "NONE",
        schema="main",
    )
    config = replace(
        engine_db.config,
        warehouse=warehouse,
        config_path=tmp_path / "craft-connector.yml",
        artifacts=Artifacts((tmp_path / "store").as_uri()),
        log_dir=tmp_path / "logs",
    )
    config.config_path.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "ENVIRONMENT"},
                "Orchestration": {"Project": "default", "Log_dir": str(config.log_dir)},
                "Engine": {"dev": profile_block(config.engine, config.engine.schema)},
                "Warehouse": {"dev": profile_block(warehouse, "main")},
                "Artifacts": {"Store": config.artifacts.store},
            },
            sort_keys=False,
        )
    )
    engine = build_warehouse_engine(config)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE main.data (id INTEGER, PIPELINE_RUN_ID BIGINT)"))
    engine.dispose()
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "rules", "BUSINESS_RULES")
        table = add_table(conn, "main.data")
        rule = insert(
            conn,
            "INSERT INTO CFG_BUSINESS_RULES "
            "(BUSINESS_RULE_NAME,PIPELINE_ID,TASK_ID,TARGET_TABLE_ID,"
            "BUSINESS_RULE_SQL,BUSINESS_RULE_TYPE,BUSINESS_RULE_KEY_COLUMN,"
            "BUSINESS_RULE_LEVEL,SEQUENCE_NUMBER) "
            "VALUES ('table check',:pipeline,:task,:target,:rule_sql,'REPORT',NULL,'TABLE',1)",
            "BUSINESS_RULE_ID",
            pipeline=pipeline,
            task=task,
            target=table,
            rule_sql="SELECT 'Y' AS PASSED",
        )
    ctx = OperationContext(
        engine_db.engine, config, SYSTEM_ACTOR, role=ApiRole.ADMIN, project="default", project_id=1
    )
    return ctx, pipeline, task, rule


def patch(tmp_path, **fields):
    directory = tmp_path / "patch"
    directory.mkdir(exist_ok=True)
    with (directory / "business_rules.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=["PIPELINE_CODE", "TASK_CODE", "BUSINESS_RULE_NAME", *fields]
        )
        writer.writeheader()
        writer.writerow(
            {
                "PIPELINE_CODE": "P",
                "TASK_CODE": "rules",
                "BUSINESS_RULE_NAME": "table check",
                **fields,
            }
        )
    return read_config_files(directory)


@pytest.mark.parametrize(
    "fields,expected",
    [
        ({"BUSINESS_RULE_LEVEL": "ROW"}, "ROW needs BUSINESS_RULE_KEY_COLUMN"),
        ({"BUSINESS_RULE_KEY_COLUMN": "id"}, "TABLE has BUSINESS_RULE_KEY_COLUMN"),
        ({"BUSINESS_RULE_TYPE": "INCOMPLETE"}, "TABLE cannot use INCOMPLETE"),
    ],
)
def test_rule_level_refused_by_apply_validate_and_refresh(
    rule_project, tmp_path, monkeypatch, fields, expected
):
    ctx, _pipeline, task, _rule = rule_project
    for apply in (False, True):
        with pytest.raises(MetadataFileError) as caught:
            sync_config(ctx.engine, ctx.config, patch(tmp_path, **fields), apply=apply)
        assert expected in str(caught.value) and "table check" in str(caught.value)
        assert "line 2" in str(caught.value)
    with ctx.engine.connect() as conn:
        actual = fetch_business_rules_for_task(conn, task)[0]
    values = {
        "business_rule_level": fields.get("BUSINESS_RULE_LEVEL", actual.business_rule_level),
        "business_rule_key_column": fields.get(
            "BUSINESS_RULE_KEY_COLUMN", actual.business_rule_key_column
        ),
        "business_rule_type": fields.get("BUSINESS_RULE_TYPE", actual.business_rule_type),
    }
    # Constraints prevent malformed rows; inject the reader result to check both validators too.
    monkeypatch.setattr(
        "etl_craft.services.validate.fetch_business_rules_for_task",
        lambda *_: [replace(actual, **values)],
    )
    found = validate(ctx.engine, ctx.config)
    assert any(expected in f.message and "table check" in f.message for f in found.findings)
    refreshed = act(ctx, ActionName.PIPELINE_REFRESH, pipeline_code="P")
    assert refreshed.failed and any(expected in f.message for f in refreshed.refreshes[0].findings)


def test_partial_rule_level_updates_keep_id_and_omitted_level(rule_project, tmp_path):
    ctx, _, task, rule = rule_project
    unchanged = sync_config(ctx.engine, ctx.config, patch(tmp_path, SEQUENCE_NUMBER=2), apply=True)
    assert not unchanged.failed
    with ctx.engine.connect() as conn:
        after = fetch_business_rules_for_task(conn, task)[0]
        assert after.business_rule_id == rule and after.business_rule_level == "TABLE"
    changed = sync_config(
        ctx.engine,
        ctx.config,
        patch(
            tmp_path,
            BUSINESS_RULE_LEVEL="ROW",
            BUSINESS_RULE_KEY_COLUMN="id",
            BUSINESS_RULE_SQL="SELECT id FROM $$rows_in_scope",
        ),
        apply=True,
    )
    assert not changed.failed
    with ctx.engine.connect() as conn:
        after = fetch_business_rules_for_task(conn, task)[0]
        assert after.business_rule_id == rule and after.business_rule_level == "ROW"


def test_table_reject_stops_downstream_and_records_the_attempt_log(rule_project):
    from etl_craft.core.enums import RunStatus
    from fixtures.metadata import add_dependency

    ctx, pipeline, task, rule = rule_project
    directory = ctx.config.python_scripts_dir
    directory.mkdir()
    (directory / "downstream.py").write_text(
        "def run(task):\n    raise AssertionError('downstream executed')\n"
    )
    with ctx.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_BUSINESS_RULES SET "
                "BUSINESS_RULE_TYPE='REJECT',BUSINESS_RULE_SQL=:sql WHERE "
                "BUSINESS_RULE_ID=:rule"
            ),
            {"rule": rule, "sql": "SELECT 'N' AS PASSED, 'totals differ' AS DETAIL"},
        )
        after = add_task(
            conn,
            pipeline,
            "downstream",
            "PYTHON",
            SCRIPT_KIND="UTILITY",
            SCRIPT_FILE="python_scripts/downstream.py",
        )
        add_dependency(conn, pipeline, after, task)
    done = act(ctx, ActionName.RUN, pipeline_code="P")
    assert done.status == RunStatus.FAILED
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS a JOIN AUD_TASK_RUN_LOG l ON "
                    "l.TASK_RUN_ID=a.TASK_RUN_ID WHERE l.TASK_ID=:task"
                ),
                {"task": after},
            ).scalar_one()
            == 0
        )
        row = conn.execute(
            text(
                "SELECT a.LOG_PATH AS path FROM AUD_TASK_ATTEMPTS a JOIN AUD_TASK_RUN_LOG l ON "
                "l.TASK_RUN_ID=a.TASK_RUN_ID WHERE l.TASK_ID=:task"
            ),
            {"task": task},
        ).one()
        stored = conn.execute(
            text(
                "SELECT PASSED AS passed,DETAIL AS detail FROM AUD_BUSINESS_RULES_RUN_LOG "
                "WHERE BUSINESS_RULE_ID=:rule"
            ),
            {"rule": rule},
        ).one()
    from pathlib import Path

    log = Path(row.path).read_text(encoding="utf-8")
    assert "table check" in log and "P.rules" in log and "PASSED=N" in log
    assert stored.passed == "N" and stored.detail == "totals differ"


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT 1 AS other", "returned columns"),
        ("SELECT 'Y' AS PASSED FROM missing_table", "could not verify TABLE output"),
    ],
)
def test_table_output_validation_findings_keep_last_good_version(rule_project, sql, expected):
    from etl_craft.core.enums import RunStatus
    from etl_craft.engine.refreshes import admitted

    ctx, pipeline, _, rule = rule_project
    good = act(ctx, ActionName.PIPELINE_REFRESH, pipeline_code="P")
    assert not good.failed, good.refreshes[0].findings
    ctx.engine.update_execution_options(refresh_required=True)
    with ctx.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_BUSINESS_RULES SET BUSINESS_RULE_SQL=:sql WHERE BUSINESS_RULE_ID=:rule"
            ),
            {"sql": sql, "rule": rule},
        )
    findings = validate(ctx.engine, ctx.config).findings
    assert any(expected in f.message and "table check" in f.message for f in findings)
    failed = act(ctx, ActionName.PIPELINE_REFRESH, pipeline_code="P")
    assert failed.failed and any(expected in f.message for f in failed.refreshes[0].findings)
    with ctx.engine.connect() as conn:
        assert admitted(conn, pipeline)[1] == good.refreshes[0].metadata_version
    done = act(ctx, ActionName.RUN, pipeline_code="P")
    assert done.status == RunStatus.SUCCESS, done.message


def test_legacy_rule_snapshot_defaults_to_row_without_rewriting_text(rule_project):
    import json

    from etl_craft.core.definitions import Definition, using
    from etl_craft.engine.queries import statement

    ctx, pipeline, task, _ = rule_project
    with ctx.engine.connect() as conn:
        image = json.loads(
            conn.execute(
                statement(conn, "metadata_capture"), {"pipeline_id": pipeline}
            ).scalar_one()
        )
        rules = image["CFG_BUSINESS_RULES"]
        rules["columns"].remove("business_rule_level")
        for rule in rules["rows"]:
            rule.pop("business_rule_level")
            rule["business_rule_key_column"] = "id"
            rule["business_rule_sql"] = "SELECT id FROM $$rows_in_scope"
        legacy = json.dumps(image)
        definition = Definition.read(legacy)
        with using(definition):
            stored = fetch_business_rules_for_task(conn, task)[0]
        assert stored.business_rule_level == "ROW" and stored.business_rule_key_column == "id"
        assert definition.text == legacy


def test_rule_observation_history_is_bounded_and_exact_runs_remain_readable(rule_project):
    from datetime import UTC, datetime

    from etl_craft.core.enums import RunStatus
    from etl_craft.engine import transitions
    from etl_craft.engine.queries import statement
    from etl_craft.engine.repository.business_rules import fetch_rule_observations
    from fixtures.metadata import start_run

    ctx, pipeline, task, rule = rule_project
    runs = []
    with ctx.engine.begin() as conn:
        for number in range(55):
            run = start_run(conn, pipeline)
            runs.append(run)
            binding = transitions.find_or_create_task_run(conn, task, run)
            rule_run = transitions.create_rule_run(conn, rule, binding.task_run_id)
            conn.execute(
                statement(conn, "table_rule_result"),
                {
                    "business_rule_run_id": rule_run,
                    "passed": "Y",
                    "observed": str(number),
                    "detail": None,
                },
            )
            transitions.finish_rule_run(conn, rule_run, RunStatus.SUCCESS, datetime.now(UTC))
            transitions.finish_task_run(conn, binding.task_run_id, status=RunStatus.SUCCESS)
            transitions.finalize_pipeline_run(conn, run, RunStatus.SUCCESS)
    with ctx.engine.connect() as conn:
        recent = fetch_rule_observations(conn, pipeline)
        assert [r.observed for r in recent] == [str(i) for i in range(54, 4, -1)]
        assert fetch_rule_observations(conn, pipeline, runs[0])[0].observed == "0"
    selected = act(ctx, ActionName.HISTORY, pipeline_code="P", run_id=runs[0])
    assert [(r.pipeline_run_id, r.observed) for r in selected.rules] == [(runs[0], "0")]


@pytest.mark.parametrize("level", [None, "TABLE"])
def test_new_config_rule_defaults_to_row_or_accepts_table_without_a_key(
    rule_project, tmp_path, level
):
    ctx, _, task, _ = rule_project
    directory = tmp_path / "insert"
    directory.mkdir()
    values = {
        "PIPELINE_CODE": "P",
        "TASK_CODE": "rules",
        "BUSINESS_RULE_NAME": "new rule",
        "SEQUENCE_NUMBER": 2,
        "BUSINESS_RULE_TYPE": "REPORT",
        "SCHEMA_NAME": "main",
        "TABLE_NAME": "data",
        "BUSINESS_RULE_SQL": "SELECT id FROM $$rows_in_scope",
    }
    if level is None:
        values["BUSINESS_RULE_KEY_COLUMN"] = "id"
    else:
        values.update(BUSINESS_RULE_LEVEL=level, BUSINESS_RULE_SQL="SELECT 'Y' AS PASSED")
    with (directory / "business_rules.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(values))
        writer.writeheader()
        writer.writerow(values)
    done = sync_config(ctx.engine, ctx.config, read_config_files(directory), apply=True)
    assert not done.failed, done.findings
    with ctx.engine.connect() as conn:
        inserted = next(
            r
            for r in fetch_business_rules_for_task(conn, task)
            if r.business_rule_name == "new rule"
        )
    assert inserted.business_rule_level == (level or "ROW")
    assert inserted.business_rule_key_column == (None if level else "id")


def test_table_result_fields_reach_the_existing_api_reads(rule_project):
    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app
    from fixtures.auth import service_token

    ctx, _, _, _ = rule_project
    done = act(ctx, ActionName.RUN, pipeline_code="P")
    assert done.run is not None
    app = create_app(ctx)
    headers = {"Authorization": "Bearer " + service_token(ctx.engine)}
    with TestClient(app) as client:
        response = client.get(
            f"/api/v1/projects/default/runs/{done.run.pipeline_run_id}", headers=headers
        )
        assert response.status_code == 200, response.text
        result = response.json()["rules"][0]
        assert result["passed"] == "Y" and result["observed"] is None
        response = client.get(
            "/api/v1/projects/default/history",
            params={"pipeline_code": "P", "run_id": done.run.pipeline_run_id},
            headers=headers,
        )
        assert response.status_code == 200, response.text
        assert response.json()["rules"][0] == result
