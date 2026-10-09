"""Load versioned etl-craft exports as real Airflow DAGs, without importing the ETL engine."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from graphlib import TopologicalSorter
from importlib.resources import files
from pathlib import Path
from typing import Any

import airflow
import pendulum
import yaml
from jsonschema import Draft202012Validator, FormatChecker
from packaging.version import Version

version = Version(airflow.__version__)
if (version.major, version.minor) == (2, 11):
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    from airflow.sensors.external_task import ExternalTaskSensor
elif (version.major, version.minor) == (3, 3):
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
    from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
    from airflow.sdk import DAG
else:
    raise ImportError(f"etl-craft-airflow supports Airflow 2.11.x and 3.3.x; found {version}")


def build_dag(document: dict[str, Any], *, start_date: datetime) -> DAG:
    """Validate one export and preserve its commands, trigger rules, sensors and graph.

    ``start_date`` is the fallback when metadata does not provide one. Exports without an
    explicit timezone use UTC. Pipeline SLA enforcement stays with etl-craft.
    """
    schema = json.loads(files(__package__).joinpath("dag-yaml-v1.json").read_text())
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(document)
    if start_date.tzinfo is None or start_date.utcoffset() is None:
        raise ValueError("start_date must include a timezone")
    steps = document.get("tasks", document.get("pipelines", {}))
    graph = {name: step["depends_on"] for name, step in steps.items()}
    missing = {name for upstream in graph.values() for name in upstream} - graph.keys()
    if missing:
        raise ValueError(f"DAG {document['dag_id']}: missing dependencies {sorted(missing)}")
    list(TopologicalSorter(graph).static_order())
    defaults = dict(document.get("default_args", {}))
    delay = defaults.pop("retry_delay_minutes", None)
    if delay is not None:
        defaults["retry_delay"] = timedelta(minutes=delay)
    zone = document.get("timezone", "UTC")
    beginning = (
        pendulum.parse(document["start_date"], tz=zone).in_timezone(zone)
        if "start_date" in document
        else pendulum.instance(start_date).in_timezone(zone)
    )
    dag = DAG(
        dag_id=document["dag_id"],
        description=document.get("description"),
        schedule=document.get("schedule"),
        start_date=beginning,
        catchup=document.get("catchup", False),
        max_active_runs=1,
        tags=document.get("tags", []),
        default_args=defaults,
    )
    operators = {}
    for name, step in steps.items():
        kwargs = {"task_id": name, "dag": dag, "trigger_rule": step["trigger_rule"]}
        if "sensor" in step:
            operators[name] = ExternalTaskSensor(**kwargs, **step["sensor"], mode="reschedule")
        elif "trigger_dag_id" in step:
            operators[name] = TriggerDagRunOperator(
                **kwargs,
                trigger_dag_id=step["trigger_dag_id"],
                trigger_run_id="{{ run_id }}",
                logical_date="{{ logical_date }}",
                reset_dag_run=True,
                wait_for_completion=True,
            )
        else:
            operators[name] = BashOperator(
                **kwargs,
                bash_command=step["bash_command"],
                env=step.get("env"),
                append_env=step.get("append_env", False),
                skip_on_exit_code=None,
            )
    for name, upstream in graph.items():
        for dependency in upstream:
            operators[name].set_upstream(operators[dependency])
    return dag


def load_dags(folder: str | Path, *, start_date: datetime) -> dict[str, DAG]:
    """Read each .yml or .yaml file, refusing unknown versions and duplicate DAG ids.

    Register the returned mapping in the Airflow DAG module with ``globals().update(...)``.
    Files are trusted executable workflow definitions; YAML loading itself is safe.
    """
    directory = Path(folder)
    if not directory.is_dir():
        raise ValueError(f"YAML folder does not exist: {directory}")
    dags = {}
    for path in sorted({*directory.glob("*.yml"), *directory.glob("*.yaml")}):
        try:
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
            dag = build_dag(document, start_date=start_date)
        except Exception as error:
            raise ValueError(f"cannot load {path.name}: {error}") from error
        if dag.dag_id in dags:
            raise ValueError(f"duplicate DAG id {dag.dag_id!r} in {path.name}")
        dags[dag.dag_id] = dag
    return dags
