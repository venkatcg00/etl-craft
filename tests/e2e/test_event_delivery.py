"""The optional delivery example and a warehouse event, run from the installed wheel."""

import json
import os
import shutil
import sqlite3
import subprocess

import duckdb
import pytest
import yaml

from fixtures.demo import DEMO, installed_cli
from fixtures.terminal import login_answers, setup_answers, terminal_run

pytestmark = [pytest.mark.e2e_uv, pytest.mark.timeout(180)]


def test_installed_event_pipeline_records_rows_and_consumes_delivery(tmp_path):
    cli = installed_cli("uv", tmp_path / "venvs")
    project = tmp_path / "project"
    project.mkdir()
    raw = {
        "Secrets": {"Source_type": "ENVIRONMENT"},
        "Orchestration": {},
        "Engine": {"dev": {"Jdbc_url": "jdbc:sqlite:engine.db", "Schema": "main"}},
        "Warehouse": {"dev": {"Jdbc_url": "jdbc:duckdb:warehouse.duckdb", "Schema": "main"}},
    }
    (project / "craft-connector.yml").write_text(yaml.safe_dump(raw, sort_keys=False))
    migrations = project / "migrations"
    migrations.mkdir()
    shutil.copy(DEMO / "metadata" / "event_waits.sql", migrations / "0001_delivery.sql")
    (migrations / "0002_ready.sql").write_text(
        "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID,PARAMETER_NAME,PARAMETER_VALUE) "
        "SELECT TASK_ID,'EVENT_SETTLE_SECONDS','0' FROM CFG_TASKS WHERE TASK_CODE='wait';\n"
        "INSERT INTO CFG_TASKS (TASK_CODE,TASK_TYPE,PIPELINE_ID,HANDLER) "
        "SELECT 'row','INGESTION',PIPELINE_ID,'EVENT_WAIT' FROM CFG_PIPELINES;\n"
        "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID,PARAMETER_NAME,PARAMETER_VALUE) "
        "SELECT TASK_ID,'EVENT_SQL','SELECT 1 AS ready' FROM CFG_TASKS WHERE TASK_CODE='row';\n"
        "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID,PARAMETER_NAME,PARAMETER_VALUE) "
        "SELECT TASK_ID,'EVENT_POLL_SECONDS','1' FROM CFG_TASKS WHERE TASK_CODE='row';\n"
        "INSERT INTO CFG_TASKS (TASK_CODE,TASK_TYPE,PIPELINE_ID,HANDLER) "
        "SELECT 'files','ETL',PIPELINE_ID,'SQL' FROM CFG_PIPELINES;\n"
        "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID,PARAMETER_NAME,PARAMETER_VALUE) "
        "SELECT TASK_ID,'SQL_ACTION','CREATE_TABLE' FROM CFG_TASKS WHERE TASK_CODE='files';\n"
        "INSERT INTO CFG_TABLES (SCHEMA_NAME,TABLE_NAME) VALUES ('main','records');\n"
        "UPDATE CFG_TASKS SET TARGET_TABLE_ID=(SELECT TABLE_ID FROM CFG_TABLES "
        "WHERE SCHEMA_NAME='main' AND TABLE_NAME='records') WHERE TASK_CODE='files';\n"
        "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID,PARAMETER_NAME,PARAMETER_VALUE) "
        "SELECT TASK_ID,'SELECT_QUERY','SELECT * FROM read_csv([$$event_files], header=true)' "
        "FROM CFG_TASKS WHERE TASK_CODE='files';\n"
        "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID,TASK_ID,DEPENDS_ON_TASK_ID,DEPENDENCY_TYPE) "
        "SELECT downstream.PIPELINE_ID,downstream.TASK_ID,upstream.TASK_ID,'SUCCESS' "
        "FROM CFG_TASKS downstream JOIN CFG_TASKS upstream "
        "ON upstream.PIPELINE_ID=downstream.PIPELINE_ID "
        "WHERE downstream.TASK_CODE='files' AND upstream.TASK_CODE='wait';\n"
    )
    scripts = project / "python_scripts"
    scripts.mkdir()
    shutil.copy(DEMO / "python_scripts" / "delivery.py", scripts / "delivery.py")
    inbox = project / "inbox"
    inbox.mkdir()
    delivery = inbox / "batch-001.csv"
    delivery.write_text("id\n1\n2\n")
    environment = {**os.environ, "XDG_CONFIG_HOME": str(tmp_path / "person")}
    for verb, answers in (("setup", setup_answers()), ("login", login_answers())):
        done = terminal_run([str(cli), verb], cwd=project, env=environment, answers=answers)
        assert done.returncode == 0, done.stdout
    for args in (("migrate",), ("run", "--pipeline-code", "FILE_DELIVERY")):
        done = subprocess.run(
            [str(cli), *args],
            cwd=project,
            env=environment,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        assert done.returncode == 0, (args, done.stdout, done.stderr)
    with sqlite3.connect(project / "engine.db") as conn:
        assert conn.execute("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG").fetchall() == [("SUCCESS",)]
        assert conn.execute(
            "SELECT r.SOURCE_COUNT FROM AUD_TASK_RUN_LOG r JOIN CFG_TASKS t ON t.TASK_ID=r.TASK_ID "
            "WHERE t.TASK_CODE='consume'"
        ).fetchone() == (2,)
        evidence = {
            row[0]: json.loads(row[1])
            for row in conn.execute(
                "SELECT t.TASK_CODE,e.EVIDENCE FROM AUD_TASK_EVENTS e "
                "JOIN AUD_TASK_RUN_LOG r ON r.TASK_RUN_ID=e.TASK_RUN_ID "
                "JOIN CFG_TASKS t ON t.TASK_ID=r.TASK_ID"
            )
        }
        assert evidence["row"] == {"row": {"ready": 1}}
        assert evidence["wait"]["files"][0]["path"] == str(delivery.resolve())
        assert conn.execute("SELECT FILE_PATH FROM AUD_EVENT_FILES").fetchall() == [
            (str(delivery.resolve()),)
        ]
    with duckdb.connect(str(project / "warehouse.duckdb"), read_only=True) as warehouse:
        assert warehouse.execute("SELECT id FROM main.records ORDER BY id").fetchall() == [
            (1,),
            (2,),
        ]
