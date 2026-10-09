"""Contracts between the demo exports and real Airflow operators."""

import os
from datetime import UTC, datetime, timedelta
from graphlib import CycleError
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import yaml
from etl_craft_airflow import (
    BashOperator,
    ExternalTaskSensor,
    TriggerDagRunOperator,
    build_dag,
    load_dags,
)
from jsonschema import ValidationError

pytestmark = pytest.mark.unit

START = datetime(2026, 1, 1, tzinfo=UTC)
ROOT = Path(os.environ["ETL_CRAFT_AIRFLOW_CONTRACTS"])
EXPORTS = sorted(ROOT.glob("*/*.yml"))
assert EXPORTS, "export the demo contracts before testing the factory"


@pytest.mark.parametrize("path", EXPORTS, ids=lambda path: f"{path.parent.name}-{path.stem}")
def test_export_preserves_the_real_airflow_graph(path):
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    dag = build_dag(document, start_date=START)
    assert dag.dag_id == document["dag_id"]
    assert dag.max_active_runs == 1
    steps = document.get("tasks", document.get("pipelines", {}))
    assert set(dag.task_ids) == set(steps)
    for name, step in steps.items():
        task = dag.get_task(name)
        assert task.upstream_task_ids == set(step["depends_on"])
        assert task.trigger_rule == step["trigger_rule"]
        if "sensor" in step:
            assert isinstance(task, ExternalTaskSensor)
            for key, value in step["sensor"].items():
                assert getattr(task, key) == value
            assert task.mode == "reschedule"
        elif "trigger_dag_id" in step:
            assert isinstance(task, TriggerDagRunOperator)
            assert task.trigger_dag_id == step["trigger_dag_id"]
            assert task.wait_for_completion and task.reset_dag_run
            assert task.trigger_run_id == "{{ run_id }}"
            assert task.logical_date == "{{ logical_date }}"
        else:
            assert isinstance(task, BashOperator)
            assert task.bash_command == step["bash_command"]
            assert task.env == step.get("env")
            assert task.append_env == step.get("append_env", False)
            assert not task.skip_on_exit_code
            assert task.retries == document["default_args"]["retries"]
            assert task.retry_delay == timedelta(
                minutes=document["default_args"]["retry_delay_minutes"]
            )


@pytest.fixture
def document():
    return yaml.safe_load((ROOT / "local/CLIENT_ALPHA.yml").read_text(encoding="utf-8"))


def test_unknown_version_and_unknown_fields_are_refused(document):
    document["etl_craft_yaml_version"] = 2
    with pytest.raises(ValidationError):
        build_dag(document, start_date=START)
    document["etl_craft_yaml_version"] = 1
    document["unknown_option"] = True
    with pytest.raises(ValidationError):
        build_dag(document, start_date=START)


def test_missing_dependencies_and_cycles_are_refused(document):
    document["tasks"]["land"]["depends_on"] = ["missing"]
    with pytest.raises(ValueError, match="missing dependencies"):
        build_dag(document, start_date=START)
    document["tasks"]["land"]["depends_on"] = ["land"]
    with pytest.raises(CycleError):
        build_dag(document, start_date=START)


def test_loader_registers_all_demos_and_refuses_duplicates(tmp_path, document):
    assert set(load_dags(ROOT / "local", start_date=START)) == {
        "CLIENT_ALPHA",
        "CLIENT_BETA",
        "SUPPORT_DM",
        "SUPPORT_EXPORT",
        "SUPPORT_BACKFILL",
    }
    for name in ["a.yml", "b.yaml"]:
        (tmp_path / name).write_text(yaml.safe_dump(document), encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate DAG id"):
        load_dags(tmp_path, start_date=START)


@pytest.mark.parametrize("contents", ["etl_craft_yaml_version: 9", "[]", "!unsafe tag", "null"])
def test_loader_refuses_bad_documents(tmp_path, contents):
    (tmp_path / "bad.yml").write_text(contents, encoding="utf-8")
    with pytest.raises(ValueError, match=r"cannot load bad\.yml"):
        load_dags(tmp_path, start_date=START)


def test_start_date_and_timezone_are_preserved(document):
    document["timezone"] = "Asia/Kolkata"
    document["start_date"] = "2026-05-01T00:00:00+05:30"
    dag = build_dag(document, start_date=START)
    assert dag.timezone.name == "Asia/Kolkata"
    assert dag.start_date.isoformat() == "2026-04-30T18:30:00+00:00"
    with pytest.raises(ValueError, match="timezone"):
        build_dag(document, start_date=datetime(2026, 1, 1))


def test_start_date_is_a_stable_fallback(document):
    dag = build_dag(document, start_date=START)
    assert dag.start_date == START
    fallback = datetime(2026, 1, 1, tzinfo=ZoneInfo("Asia/Kolkata"))
    assert build_dag(document, start_date=fallback).timezone.name == "UTC"


def test_no_etl_engine_dependency():
    import sys

    assert "etl_craft" not in sys.modules


def test_missing_directory_is_refused(tmp_path):
    with pytest.raises(ValueError, match="folder does not exist"):
        load_dags(tmp_path / "missing", start_date=START)


def test_unsupported_remote_demo_rules_were_refused():
    import json

    assert set(json.loads((ROOT / "refused.json").read_text(encoding="utf-8"))) == {
        "CLIENT_ALPHA",
        "SUPPORT_DM",
    }
    assert {p.stem for p in (ROOT / "remote").glob("*.yml")} == {
        "CLIENT_BETA",
        "SUPPORT_EXPORT",
        "SUPPORT_BACKFILL",
    }
