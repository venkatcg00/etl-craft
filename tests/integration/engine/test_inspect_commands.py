"""``list``, ``graph``, ``steps`` and ``history`` on the command line, both Engine DBs."""

import logging

import pytest
import yaml
from sqlalchemy import text

from etl_craft.cli import main
from etl_craft.core.errors import ExitCode
from etl_craft.engine import runlog
from fixtures.metadata import (
    add_dependency,
    add_pipeline,
    add_pipeline_dependency,
    add_task,
    start_run,
    task_run,
)


@pytest.fixture(autouse=True)
def restore_logger():
    logger = logging.getLogger("etl_craft")
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


@pytest.fixture
def project(engine_db, tmp_path, monkeypatch):
    """Pipelines UP and SALES, with dependencies of every kind and two runs of SALES."""
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
        "Orchestration": {"Mode": "local"},
        "Engine": {"dev": block},
    }
    (root / "craft-connector.yml").write_text(yaml.safe_dump(raw, sort_keys=False), "utf-8")
    monkeypatch.chdir(root)
    engine = engine_db.engine
    ids = {}
    with engine.begin() as conn:
        ids["UP"] = add_pipeline(conn, "UP")
        ids["publish"] = add_task(conn, ids["UP"], "publish")
        ids["SALES"] = add_pipeline(conn, "SALES", refresh_type="INCREMENTAL", sla_in_hours=2)
        ids["extract"] = add_task(conn, ids["SALES"], "extract", "PYTHON", SCRIPT_NAME="x.py")
        ids["load"] = add_task(
            conn, ids["SALES"], "load", SQL_ACTION="SCD1_MERGE", TARGET_OBJECT="s.orders"
        )
        ids["alert"] = add_task(conn, ids["SALES"], "alert", "EMAIL_ALERT")
        conn.execute(
            text("UPDATE CFG_TASKS SET RUN_CONDITION = 'ANY' WHERE TASK_ID = :t"),
            {"t": ids["alert"]},
        )
        add_dependency(conn, ids["SALES"], ids["load"], ids["extract"])
        add_dependency(conn, ids["SALES"], ids["load"], ids["publish"], upstream_pipeline=ids["UP"])
        add_dependency(conn, ids["SALES"], ids["alert"], ids["load"], "FAILURE")
        add_pipeline_dependency(conn, ids["SALES"], ids["UP"])
        first = start_run(conn, ids["SALES"])
        task_run(conn, ids["extract"], first, "SUCCESS", 10)
        runlog.finalize_pipeline_run(conn, first, "SUCCESS", sla_in_hours=2)
        ids["second"] = start_run(conn, ids["SALES"])
        failed = task_run(conn, ids["extract"], ids["second"], "IN-PROGRESS")
        runlog.finish_task_run(conn, failed, status="FAILED", error_message="source down")
    return ids


def run(capsys, *args):
    code = main(list(args))
    return code, capsys.readouterr().out


def test_list(project, capsys):
    code, out = run(capsys, "list")
    assert code == ExitCode.SUCCESS
    assert out.splitlines() == [
        "PIPELINE_CODE\tPIPELINE_NAME\tREFRESH_TYPE\tRUN_SCHEDULE\tSLA_IN_HOURS\tPAUSED",
        "SALES\tSALES\tINCREMENTAL\t\t2.0\t",
        "UP\tUP\tFULL\t\t\t",
    ]
    assert run(capsys, "pause", "--pipeline_code", "UP", "--reason", "source down")[0] == 0
    code, out = run(capsys, "list")
    assert out.splitlines()[2].startswith("UP\tUP\tFULL\t\t\tpaused since ")
    assert out.splitlines()[2].endswith(": source down")


def test_graph(project, capsys):
    code, out = run(capsys, "graph", "--pipeline_code", "SALES")
    assert code == ExitCode.SUCCESS
    assert out.splitlines() == [
        "Pipeline SALES",
        "Waves, the order that is always safe:",
        "  1: extract",
        "  2: load",
        "  3: alert",
        "May start before their wave (an ANY or N run condition): alert",
        "Task dependencies:",
        "  alert <- load (FAILURE)",
        "  load <- UP.publish (SUCCESS)",
        "  load <- extract (SUCCESS)",
        "Pipeline dependencies:",
        "  UP (SUCCESS)",
    ]


def test_steps(project, capsys):
    code, out = run(capsys, "steps", "--pipeline_code", "SALES")
    assert code == ExitCode.SUCCESS
    assert out.splitlines() == [
        "TASK_CODE\tHANDLER\tTASK_TYPE\tRUN_CONDITION\tPARAMETERS",
        "alert\tEMAIL_ALERT\tETL\tANY\t",
        "extract\tPYTHON\tETL\tALL\tSCRIPT_NAME=x.py",
        "load\tSQL\tETL\tALL\tSQL_ACTION=SCD1_MERGE, TARGET_OBJECT=s.orders",
    ]


def test_history(project, capsys):
    code, out = run(capsys, "history", "--pipeline_code", "SALES")
    lines = out.splitlines()
    assert code == ExitCode.SUCCESS
    assert lines[0] == "PIPELINE_RUN_ID\tSTATUS\tSTART_DATE\tEND_DATE\tSLA_STATUS"
    assert [line.split("\t")[1] for line in lines[1:]] == ["IN-PROGRESS", "SUCCESS"]
    assert lines[2].endswith("\tMET")

    code, out = run(
        capsys, "history", "--pipeline_code", "SALES", "--task_code", "extract", "--limit", "1"
    )
    lines = out.splitlines()
    assert len(lines) == 2
    fields = lines[1].split("\t")
    assert (fields[0], fields[1], fields[2], fields[-1]) == (
        str(project["second"]),
        "FAILED",
        "1",
        "source down",
    )
    assert run(capsys, "history", "--pipeline_code", "UP") == (ExitCode.SUCCESS, "(no runs yet)\n")


def test_unknown_codes_and_limits_are_named(project, capsys):
    assert main(["graph", "--pipeline_code", "SALE"]) == ExitCode.METADATA
    assert "did you mean: SALES" in capsys.readouterr().err
    assert main(["history", "--pipeline_code", "SALES", "--task_code", "lod"]) == ExitCode.METADATA
    assert main(["history", "--pipeline_code", "SALES", "--limit", "0"]) == ExitCode.USAGE
