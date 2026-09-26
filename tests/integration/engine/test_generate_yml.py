"""``generate-yml``: the DAG a pipeline becomes, and the global trigger DAG, both Engine DBs."""

import json
import logging
from dataclasses import replace

import pytest
import yaml
from sqlalchemy import text

from etl_craft.cli import main
from etl_craft.config import DagDefaults, DocsSiteConfig
from etl_craft.core.enums import Mode
from etl_craft.core.errors import ConfigurationError, ExitCode, RemoteUnsupportedError
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
        "max_active_runs": 1,
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


def one_type(engine):
    """Make load's dependencies one type, so a remote orchestrator can apply its rule."""
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASK_DEPENDENCY SET DEPENDENCY_TYPE = 'SUCCESS' "
                "WHERE DEPENDENCY_TYPE = 'ALWAYS'"
            )
        )


def test_remote_mode_puts_every_rule_in_the_dag(engine_db, pipelines):
    remote = replace(engine_db.config, mode=Mode.REMOTE)
    with pipelines.connect() as conn, pytest.raises(RemoteUnsupportedError) as refused:
        pipeline_dag(conn, remote, "SALES")
    assert str(refused.value) == (
        "SALES has 1 rule(s) the remote orchestrator does not support, so they cannot be "
        "applied in remote mode: SALES.load: its dependencies have different types (extract: "
        "SUCCESS, setup: ALWAYS, the sensor on UP.publish: SUCCESS); the remote orchestrator "
        "does not support this. An orchestrator applies one trigger rule to every upstream step. "
        "Give them one DEPENDENCY_TYPE, or split the task in two, or run it in local mode "
        "(Orchestration.Mode: local), where etl-craft applies it."
    )
    one_type(pipelines)
    with pipelines.connect() as conn:
        dag = pipeline_dag(conn, remote, "SALES")
    run = "etl-craft run --pipeline_code SALES"
    assert dag["max_active_runs"] == 1
    assert "pipeline_dependencies" not in dag
    assert "cross_pipeline_task_dependencies" not in dag
    assert dag["tasks"] == {
        "__init__": {
            "bash_command": f"{run} --init-only",
            "depends_on": [],
            "trigger_rule": "all_success",
        },
        # The dependency on pipeline UP: a sensor on UP's DAG run, before any task.
        "__wait_for_UP__": {
            "sensor": {
                "external_dag_id": "UP",
                "external_task_id": None,
                "allowed_states": ["success"],
                "failed_states": ["failed"],
            },
            "depends_on": ["__init__"],
            "trigger_rule": "all_success",
        },
        # load's dependency on UP.publish: a sensor on that task, before load.
        "__wait_for_UP.publish__": {
            "sensor": {
                "external_dag_id": "UP",
                "external_task_id": "publish",
                "allowed_states": ["success"],
                "failed_states": ["failed", "upstream_failed", "skipped"],
            },
            "depends_on": ["__wait_for_UP__"],
            "trigger_rule": "all_success",
        },
        "alert": {
            "bash_command": f"{run} --task_code alert",
            "depends_on": ["load"],
            "trigger_rule": "one_failed",
        },
        "extract": {
            "bash_command": f"{run} --task_code extract",
            "depends_on": ["__wait_for_UP__"],
            "trigger_rule": "all_success",
        },
        "load": {
            "bash_command": f"{run} --task_code load",
            "depends_on": ["__wait_for_UP.publish__", "extract", "setup"],
            "trigger_rule": "all_success",
        },
        "setup": {
            "bash_command": f"{run} --task_code setup",
            "depends_on": ["__wait_for_UP__"],
            "trigger_rule": "all_success",
        },
        "__finalize__": {
            "bash_command": f"{run} --finalize-only",
            "depends_on": ["alert"],
            "trigger_rule": "all_done",
        },
    }
    # With the global DAG on, it orders the pipelines, so no sensor waits for UP's DAG.
    ordered = replace(remote, dag_defaults=DagDefaults(global_dag=True))
    with pipelines.connect() as conn:
        tasks = pipeline_dag(conn, ordered, "SALES")["tasks"]
        assert global_dag(conn, ordered)["pipelines"]["SALES"]["depends_on"] == ["UP"]
    assert "__wait_for_UP__" not in tasks and tasks["extract"]["depends_on"] == ["__init__"]
    assert tasks["__wait_for_UP.publish__"]["depends_on"] == ["__init__"]


@pytest.mark.parametrize(
    ("change", "where", "rule"),
    [
        (
            "UPDATE CFG_TASKS SET RUN_CONDITION = 'N', RUN_CONDITION_COUNT = 1 "
            "WHERE TASK_CODE = 'load'",
            "SALES.load",
            "RUN_CONDITION = 'N' (RUN_CONDITION_COUNT = 1)",
        ),
        (
            "UPDATE CFG_TASK_DEPENDENCY SET DEPENDENCY_TYPE = 'HAS_DATA' "
            "WHERE DEPENDS_ON_PIPELINE_ID <> PIPELINE_ID",
            "SALES.load",
            "depends on UP.publish with DEPENDENCY_TYPE = 'HAS_DATA'",
        ),
        (
            "UPDATE CFG_PIPELINE_DEPENDENCY SET DEPENDENCY_TYPE = 'HAS_DATA'",
            "SALES",
            "depends on pipeline UP with DEPENDENCY_TYPE = 'HAS_DATA'",
        ),
    ],
)
def test_remote_mode_names_each_rule_it_cannot_apply(engine_db, pipelines, change, where, rule):
    one_type(pipelines)
    with pipelines.begin() as conn:
        conn.execute(text(change))
    remote = replace(engine_db.config, mode=Mode.REMOTE)
    with pipelines.connect() as conn, pytest.raises(RemoteUnsupportedError) as refused:
        pipeline_dag(conn, remote, "SALES")
    assert f"in remote mode: {where}: {rule}; the remote orchestrator does not support this." in (
        str(refused.value)
    )


def test_the_global_dag_in_remote_mode_needs_one_type_per_pipeline(engine_db, pipelines):
    one_type(pipelines)
    with pipelines.begin() as conn:
        other = add_pipeline(conn, "OTHER")
        add_pipeline_dependency(
            conn,
            conn.execute(
                text("SELECT PIPELINE_ID FROM CFG_PIPELINES WHERE PIPELINE_CODE = 'SALES'")
            ).scalar_one(),
            other,
            "ALWAYS",
        )
    config = replace(engine_db.config, mode=Mode.REMOTE, dag_defaults=DagDefaults(global_dag=True))
    with pipelines.connect() as conn:
        with pytest.raises(
            RemoteUnsupportedError,
            match=r"SALES: its dependencies on other "
            r"pipelines have different types \(OTHER: ALWAYS, UP: SUCCESS\)",
        ):
            global_dag(conn, config)
        # Without the global DAG, each dependency is a sensor of its own.
        tasks = pipeline_dag(conn, replace(config, dag_defaults=DagDefaults()), "SALES")["tasks"]
    assert tasks["__wait_for_OTHER__"]["sensor"]["allowed_states"] == ["success", "failed"]
    assert tasks["extract"]["depends_on"] == ["__wait_for_OTHER__", "__wait_for_UP__"]


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
    code = main(["generate-yml", "--pipeline_code", "SALES", "--output", str(out)])
    assert code == ExitCode.REMOTE_UNSUPPORTED
    assert "SALES.load: its dependencies have different types" in capsys.readouterr().err
    one_type(pipelines)
    assert main(["generate-yml", "--pipeline_code", "SALES", "--output", str(out)]) == 0
    assert capsys.readouterr().out == f"wrote {out}\n"
    written = out.read_text("utf-8")
    assert written.startswith("# Generated by `etl-craft generate-yml`.")
    assert "# Remote mode: this DAG is the only source of truth for scheduling." in written
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
