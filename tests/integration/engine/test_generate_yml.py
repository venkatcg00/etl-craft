"""``generate-yml``: the DAG a pipeline becomes, and the global trigger DAG, both Engine DBs."""

import json
import logging
from dataclasses import replace

import pytest
import yaml
from sqlalchemy import text

from etl_craft.cli import main
from etl_craft.config import DagDefaults, DocsSiteConfig
from etl_craft.core.errors import ConfigurationError, ExitCode
from etl_craft.services.generate_yml import GLOBAL_DAG_ID, docs_dag, global_dag, pipeline_dag
from fixtures.metadata import add_dependency, add_pipeline, add_pipeline_dependency, add_task


@pytest.fixture(autouse=True)
def restore_logger():
    logger = logging.getLogger("etl_craft")
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


@pytest.fixture
def pipelines(engine_db):
    """SALES: extract, then load (ALL of SUCCESS and ALWAYS), then alert (ANY FAILURE)."""
    engine = engine_db.engine
    ids = {}
    with engine.begin() as conn:
        ids["UP"] = add_pipeline(conn, "UP")
        ids["publish"] = add_task(conn, ids["UP"], "publish")
        ids["SALES"] = add_pipeline(conn, "SALES", refresh_type="INCREMENTAL", sla_in_hours=2)
        conn.execute(
            text(
                "UPDATE CFG_PIPELINES SET RUN_SCHEDULE = '0 6 * * *', "
                "PIPELINE_PARAMETERS = :params WHERE PIPELINE_ID = :p"
            ),
            {"p": ids["SALES"], "params": json.dumps({"RETRIES": 3, "TAGS": ["sales"]})},
        )
        for code in ("extract", "setup", "load", "alert"):
            ids[code] = add_task(conn, ids["SALES"], code)
        conn.execute(
            text("UPDATE CFG_TASKS SET RUN_CONDITION = 'ANY' WHERE TASK_ID = :t"),
            {"t": ids["alert"]},
        )
        add_dependency(conn, ids["SALES"], ids["load"], ids["extract"])
        add_dependency(conn, ids["SALES"], ids["load"], ids["setup"], "ALWAYS")
        add_dependency(conn, ids["SALES"], ids["alert"], ids["load"], "FAILURE")
        add_dependency(conn, ids["SALES"], ids["load"], ids["publish"], upstream_pipeline=ids["UP"])
        add_pipeline_dependency(conn, ids["SALES"], ids["UP"])
    return engine


def test_a_pipeline_becomes_one_dag(engine_db, pipelines):
    config = replace(engine_db.config, dag_defaults=DagDefaults(retries=2, catchup=True))
    with pipelines.connect() as conn:
        dag = pipeline_dag(conn, config, "SALES")
        # CREATED_BY is set when the row is inserted, and triggers keep it.
        owner = conn.execute(
            text("SELECT CREATED_BY FROM CFG_PIPELINES WHERE PIPELINE_CODE = 'SALES'")
        ).scalar_one()
    run = "etl-craft run --pipeline_code SALES"
    assert dag == {
        "dag_id": "SALES",
        "description": None,
        "schedule": "0 6 * * *",
        "sla_hours": 2.0,
        "refresh_type": "INCREMENTAL",
        # The pipeline's own settings win, then the DAG defaults, then the built-in ones.
        "catchup": True,
        "tags": ["sales"],
        "default_args": {
            "owner": owner,
            "retries": 3,
            "retry_delay_minutes": 5,
            "depends_on_past": False,
            "email_on_failure": False,
        },
        "tasks": {
            "__init__": {
                "bash_command": f"{run} --init-only",
                "depends_on": [],
                "trigger_rule": "all_success",
            },
            "alert": {
                "bash_command": f"{run} --task_code alert",
                "depends_on": ["load"],
                "trigger_rule": "one_failed",
            },
            "extract": {
                "bash_command": f"{run} --task_code extract",
                "depends_on": ["__init__"],
                "trigger_rule": "all_success",
            },
            # Mixed dependency types: the permissive rule, and the engine decides.
            "load": {
                "bash_command": f"{run} --task_code load",
                "depends_on": ["extract", "setup"],
                "trigger_rule": "all_done",
            },
            "setup": {
                "bash_command": f"{run} --task_code setup",
                "depends_on": ["__init__"],
                "trigger_rule": "all_success",
            },
            "__finalize__": {
                "bash_command": f"{run} --finalize-only",
                "depends_on": ["alert"],
                "trigger_rule": "all_done",
            },
        },
        "pipeline_dependencies": [{"depends_on_pipeline": "UP", "dependency_type": "SUCCESS"}],
        "cross_pipeline_task_dependencies": [
            {
                "task": "load",
                "depends_on_pipeline": "UP",
                "depends_on_task": "publish",
                "dependency_type": "SUCCESS",
            }
        ],
    }


def test_an_environment_without_schedules(engine_db, pipelines):
    config = replace(engine_db.config, dag_defaults=DagDefaults(allow_schedule=False))
    with pipelines.connect() as conn:
        assert pipeline_dag(conn, config, "SALES")["schedule"] is None


def test_the_global_dag(engine_db, pipelines):
    with pipelines.connect() as conn:
        with pytest.raises(ConfigurationError, match="the global DAG is off: set Global_dag"):
            global_dag(conn, engine_db.config)
        dag = global_dag(conn, replace(engine_db.config, dag_defaults=DagDefaults(global_dag=True)))
    assert dag == {
        "dag_id": GLOBAL_DAG_ID,
        "pipelines": {
            "SALES": {
                "trigger_dag_id": "SALES",
                "depends_on": ["UP"],
                "trigger_rule": "all_success",
            },
            "UP": {"trigger_dag_id": "UP", "depends_on": [], "trigger_rule": "all_success"},
        },
    }


def test_the_command_writes_yaml(engine_db, pipelines, tmp_path, monkeypatch, capsys):
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
        "Orchestration": {"Mode": "remote"},
        "Engine": {"dev": block},
        "Docs_site": {"Schedule": "30 1 * * *"},
    }
    (root / "craft-connector.yml").write_text(yaml.safe_dump(raw, sort_keys=False), "utf-8")
    monkeypatch.chdir(root)
    out = tmp_path / "dags" / "sales.yml"
    assert main(["generate-yml", "--pipeline_code", "SALES", "--output", str(out)]) == 0
    assert capsys.readouterr().out == f"wrote {out}\n"
    written = out.read_text("utf-8")
    assert written.startswith("# Generated by `etl-craft generate-yml`.")
    assert yaml.safe_load(written)["dag_id"] == "SALES"
    assert main(["generate-yml", "--global"]) == ExitCode.CONFIGURATION
    assert "Global_dag" in capsys.readouterr().err
    assert main(["generate-yml", "--docs"]) == 0
    docs = yaml.safe_load(capsys.readouterr().out)
    assert (docs["dag_id"], docs["schedule"]) == ("etl_craft_docs", "30 1 * * *")


def test_the_docs_dag_writes_the_catalog_again_on_its_schedule(engine_db):
    with pytest.raises(ConfigurationError, match=r"Docs_site\.Schedule is not set"):
        docs_dag(engine_db.config)
    config = replace(engine_db.config, docs_site=DocsSiteConfig(schedule="0 2 * * *"))
    dag = docs_dag(config)
    assert dag["dag_id"] == "etl_craft_docs" and dag["schedule"] == "0 2 * * *"
    assert dag["tasks"] == {
        "generate_docs": {
            "bash_command": "etl-craft generate-docs",
            "depends_on": [],
            "trigger_rule": "all_success",
        }
    }
    unscheduled = replace(config, dag_defaults=DagDefaults(allow_schedule=False))
    assert docs_dag(unscheduled)["schedule"] is None
