"""Tasks: resolving a code, what running one needs, and its parameters."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.engine import Connection

from etl_craft.core.definitions import Definition, current
from etl_craft.core.enums import Handler, ScriptKind, YesNo
from etl_craft.core.errors import MetadataError
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import with_suggestions


def resolve_task_id(conn: Connection, pipeline_id: int, task_code: str) -> int:
    """Return the id of active task ``task_code`` in ``pipeline_id``; ``MetadataError`` if none."""
    definition = current.get()
    if definition is None:
        task_id = conn.execute(
            statement(conn, "task_id_by_code"), {"pipeline_id": pipeline_id, "task_code": task_code}
        ).scalar_one_or_none()
    else:
        task_id = next(
            (key for key, code in fetch_task_codes(conn, pipeline_id).items() if code == task_code),
            None,
        )
    if task_id is None:
        known = list(fetch_task_codes(conn, pipeline_id).values())
        raise MetadataError(
            with_suggestions(
                f"no active task with TASK_CODE={task_code!r} in pipeline_id={pipeline_id}",
                task_code,
                known,
            )
        )
    return int(task_id)


def fetch_task_codes(conn: Connection, pipeline_id: int) -> dict[int, str]:
    """Map the id of every active task in ``pipeline_id`` to its code."""
    definition = current.get()
    if definition is not None:
        codes = {}
        for row in sorted(definition.rows["CFG_TASKS"], key=lambda row: str(row["task_code"])):
            if row["pipeline_id"] != pipeline_id or row["active_flag"] != YesNo.YES:
                continue
            key, code = row["task_id"], row["task_code"]
            assert isinstance(key, int) and isinstance(code, str)
            codes[key] = code
        return codes
    rows = conn.execute(statement(conn, "task_codes"), {"pipeline_id": pipeline_id})
    return {row.task_id: row.task_code for row in rows}


@dataclass(frozen=True)
class TaskExecutionDetail:
    """What running a task needs beyond its parameters."""

    handler: str
    task_code: str
    pipeline_id: int
    pipeline_code: str
    refresh_type: str
    pool: str = "default"
    pool_active: str | None = YesNo.YES


def fetch_task_execution_detail(conn: Connection, task_id: int) -> TaskExecutionDetail:
    """Return what running ``task_id`` needs; the task must exist."""
    row = conn.execute(statement(conn, "task_execution_detail"), {"task_id": task_id}).one()
    return TaskExecutionDetail(**row._mapping)


def fetch_task_parameters(conn: Connection, task_id: int) -> dict[str, str]:
    """Map each active parameter name of ``task_id`` to its value."""
    definition = current.get()
    if definition is not None:
        return admitted_parameters(definition, task_id)
    rows = conn.execute(statement(conn, "task_parameters"), {"task_id": task_id})
    params = {}
    for row in rows:
        _check_target(task_id, row.parameter_value, row.target_active_flag)
        params[row.parameter_name] = row.parameter_value
    return params


def fetch_all_task_parameters(
    conn: Connection, handler: Handler | None = None
) -> dict[int, dict[str, str]]:
    """Read parameters and target references of active tasks/pipelines in one statement."""
    params: dict[int, dict[str, str]] = {}
    for row in conn.execute(statement(conn, "catalog_task_parameters"), {"handler": handler}):
        _check_target(row.task_id, row.parameter_value, row.target_active_flag)
        params.setdefault(row.task_id, {})[row.parameter_name] = row.parameter_value
    return params


def _check_target(task_id: int, value: str, active: str) -> None:
    if active != YesNo.YES:
        raise MetadataError(
            f"TASK_ID={task_id} target {value}: table contract has "
            "ACTIVE_FLAG=N; reactivate it or disable the task's target reference"
        )


@dataclass(frozen=True)
class TargetTask:
    """Another active SQL task in the same pipeline with the same ``TARGET_OBJECT``."""

    task_id: int
    task_code: str
    sql_action: str


def fetch_target_tasks(conn: Connection, pipeline_id: int, task_id: int) -> list[TargetTask]:
    """Return the other active SQL tasks sharing the stored target of ``task_id``."""
    return list(fetch_target_task_states(conn, pipeline_id, task_id, None)[0])


def fetch_target_task_states(
    conn: Connection, pipeline_id: int, task_id: int, pipeline_run_id: int | None
) -> tuple[tuple[TargetTask, ...], dict[str, str | None]]:
    """Read peers and their exact run statuses together, without one read per peer."""
    rows = conn.execute(
        statement(conn, "target_tasks"),
        {"pipeline_id": pipeline_id, "task_id": task_id, "pipeline_run_id": pipeline_run_id},
    ).all()
    return (
        tuple(TargetTask(row.task_id, row.task_code, row.sql_action) for row in rows),
        {str(row.task_id): row.status for row in rows},
    )


def conflicting_merge_action(conn: Connection, task_id: int, sql_action: str) -> str | None:
    """Find an active opposite merge writer sharing this task's target reference."""
    found = conn.execute(
        statement(conn, "conflicting_merge_action"),
        {
            "task_id": task_id,
            "sql_action": sql_action,
        },
    ).scalar_one_or_none()
    return str(found) if found is not None else None


@dataclass(frozen=True)
class FailureWatchMessage:
    """A task watched through a FAILURE dependency, with its latest error message."""

    depends_on_task_code: str
    error_message: str | None


def fetch_failure_watch_messages(conn: Connection, task_id: int) -> list[FailureWatchMessage]:
    """Return each task ``task_id`` watches through an active FAILURE dependency.

    The message is from the watched task's latest run under any pipeline run, since it may be in
    another pipeline with no run in common.
    """
    rows = conn.execute(statement(conn, "failure_watch_messages"), {"task_id": task_id})
    return [FailureWatchMessage(**row._mapping) for row in rows]


def admitted_parameters(definition: Definition, task_id: int) -> dict[str, str]:
    """Read parameter and normalized-target values from the one admitted image."""
    params = {}
    for row in definition.rows["CFG_TASK_PARAMETERS"]:
        if row["task_id"] == task_id and row["active_flag"] == YesNo.YES:
            value = row["parameter_value"]
            if not isinstance(value, str):
                raise MetadataError(
                    f"task {task_id} parameter {row['parameter_name']}: value={value!r}; "
                    "expected text; correct the parameter in config and start a new run"
                )
            params[str(row["parameter_name"])] = value
    task = definition.row("CFG_TASKS", "task_id", task_id)
    # Untagged images used the ingestion-only contract, even for custom SCRIPT_KIND parameters.
    if task["handler"] == Handler.PYTHON and definition.format_version == 1:
        params["SCRIPT_KIND"] = ScriptKind.INGESTION
    if task.get("target_active_flag") == YesNo.YES and task.get("target_table_id") is not None:
        target = definition.row("CFG_TABLES", "table_id", task["target_table_id"])
        name = ".".join(
            str(target[c])
            for c in ("catalog_name", "schema_name", "table_name")
            if target.get(c) is not None
        )
        _check_target(task_id, name, str(target["active_flag"]))
        params["TARGET_OBJECT"] = name
    return params


def fetch_admitted_parameters(conn: Connection, task_id: int, run_id: int) -> dict[str, str]:
    """Read a trigger's admitted parameters without pinning child admission or permissions."""
    from etl_craft.engine.run_definitions import for_run

    with for_run(conn, run_id):
        return fetch_task_parameters(conn, task_id)
