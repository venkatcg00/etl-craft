"""``validate``: check every active pipeline's metadata, reporting every problem at once.

A run finds a mistake in a task's definition only when it reaches that task, and some mistakes
not even then: an alert that runs before the tasks it reports on, or a ``HAS_DATA`` dependency
on a task that never reports rows. ``validate`` reads the Engine DB and the project's files, and
checks each active pipeline and task with the same code a run uses. It connects to nothing else
and runs nothing: SQL is read and checked, ingestion scripts are parsed, not imported.

Each finding is a ``FAIL`` (a run would fail, hang or silently do something else) or a ``WARN``
(it works, but look at this, such as a parameter no handler reads).
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

from sqlalchemy.engine import Connection, Engine

from etl_craft.config import ConnectorConfig
from etl_craft.config.project import ingestion_script
from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import DependencyType, Handler, SqlAction
from etl_craft.core.errors import EtlCraftError
from etl_craft.core.graph import build_graph
from etl_craft.core.text import suggest
from etl_craft.engine.repository.business_rules import fetch_business_rules_for_task
from etl_craft.engine.repository.dependencies import fetch_pipeline_graph
from etl_craft.engine.repository.pipelines import (
    PIPELINE_PARAMETER_KINDS,
    pipeline_parameter_problems,
    resolve_pipeline_id,
)
from etl_craft.engine.repository.tasks import fetch_target_tasks, fetch_task_parameters
from etl_craft.engine.repository.validation import (
    ActiveTask,
    DependencyEdge,
    PipelineEdge,
    fetch_active_tasks,
    fetch_pipeline_edges,
    fetch_pipeline_parameters,
    fetch_task_dependency_edges,
)
from etl_craft.execution.limits import task_timeout_seconds
from etl_craft.handlers import email_alert, python_scripts
from etl_craft.handlers.business_rules import check_rule
from etl_craft.handlers.registry import COMMON_PARAMETERS, TaskContext
from etl_craft.handlers.sql import task_dialect
from etl_craft.handlers.sql.actions import writer_action
from etl_craft.handlers.sql.spec import PARAMETERS as SQL_PARAMETERS
from etl_craft.handlers.sql.spec import WRITERS, read_sql_task
from etl_craft.services.doctor import Status
from etl_craft.services.lineage import table_name

CODE = re.compile(r"^[A-Za-z0-9_-]+$")
"""What a pipeline or task code may hold: it appears in commands, DAG ids and file names."""

ROWS_REPORTED = frozenset(WRITERS)
"""The SQL actions that report the rows in their target, which a ``HAS_DATA`` edge needs."""

FULL_REPLACE = frozenset({SqlAction.CREATE_TABLE, SqlAction.OVERWRITE_TABLE})
"""The SQL actions that rebuild every row of their target, and so every ``ROW_ID``."""

KNOWN_PARAMETERS: dict[str, frozenset[str]] = {
    Handler.SQL: SQL_PARAMETERS,
    Handler.BUSINESS_RULES: frozenset(),
    Handler.EMAIL_ALERT: email_alert.PARAMETERS,
}
"""The parameters each handler reads. A PYTHON task's script may read any, so none is unknown."""


@dataclass(frozen=True)
class Finding:
    """One problem: where it is (a pipeline, or ``PIPELINE.TASK``), how bad, and what it is."""

    where: str
    status: Status
    message: str


@dataclass
class Report:
    """What ``validate`` checked and found."""

    pipelines: int = 0
    tasks: int = 0
    findings: list[Finding] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        """Whether any finding is a ``FAIL``."""
        return any(f.status is Status.FAIL for f in self.findings)

    def fail(self, where: str, message: str) -> None:
        """Record a problem that fails, hangs or silently changes a run."""
        self.findings.append(Finding(where, Status.FAIL, message))

    def warn(self, where: str, message: str) -> None:
        """Record something that works but deserves a look."""
        self.findings.append(Finding(where, Status.WARN, message))


@dataclass(frozen=True)
class _Metadata:
    """Every active task with its parameters, and every active dependency."""

    tasks: list[ActiveTask]
    params: dict[int, dict[str, str]]
    edges: list[DependencyEdge]
    pipeline_edges: list[PipelineEdge]
    pipeline_parameters: list[tuple[str, object]]
    pipeline_codes: list[str]


def validate(engine: Engine, config: ConnectorConfig, pipeline_code: str | None = None) -> Report:
    """Check every active pipeline, or only ``pipeline_code``; return every finding."""
    with engine.connect() as conn:
        if pipeline_code is not None:
            resolve_pipeline_id(conn, pipeline_code)
        tasks = fetch_active_tasks(conn)
        pipeline_parameters = fetch_pipeline_parameters(conn)
        data = _Metadata(
            tasks=tasks,
            params={task.task_id: fetch_task_parameters(conn, task.task_id) for task in tasks},
            edges=fetch_task_dependency_edges(conn),
            pipeline_edges=fetch_pipeline_edges(conn),
            pipeline_parameters=pipeline_parameters,
            pipeline_codes=[code for code, _ in pipeline_parameters],
        )
        report = Report()
        _pipelines(conn, data, report)
        _dependencies(data, report)
        for task in data.tasks:
            _task(conn, config, data, task, report)

    def mine(where: str) -> bool:
        return pipeline_code is None or where.split(".", 1)[0] == pipeline_code

    report.findings = [f for f in report.findings if mine(f.where)]
    report.pipelines = sum(1 for code in data.pipeline_codes if mine(code))
    report.tasks = sum(1 for task in data.tasks if mine(task.label))
    return report


def _pipelines(conn: Connection, data: _Metadata, report: Report) -> None:
    for code, stored in data.pipeline_parameters:
        if not CODE.match(code):
            report.fail(code, _code_problem("PIPELINE_CODE", code))
        problems, unknown = pipeline_parameter_problems(stored)
        for problem in problems:
            report.fail(code, problem)
        for name in unknown:
            report.warn(
                code,
                _unknown("PIPELINE_PARAMETERS", name, list(PIPELINE_PARAMETER_KINDS)),
            )
        try:
            graph = fetch_pipeline_graph(conn, resolve_pipeline_id(conn, code))
            build_graph(graph.tasks, graph.same_pipeline_edges)
        except EtlCraftError as error:
            report.fail(code, str(error))
    for edge in data.pipeline_edges:
        if not edge.depends_on_pipeline_active:
            report.fail(
                edge.pipeline_code,
                f"depends on pipeline {edge.depends_on_pipeline_code}, which is inactive and "
                "no longer runs, so once its last run is consumed the dependency is never "
                "satisfied again; deactivate the dependency too, or reactivate the pipeline",
            )
    for cycle in _cycles(
        {
            (edge.pipeline_code, edge.depends_on_pipeline_code)
            for edge in data.pipeline_edges
            if edge.depends_on_pipeline_active
        }
    ):
        report.fail(
            cycle[0],
            f"pipeline dependencies form a cycle ({' -> '.join([*cycle, cycle[0]])}), so none "
            "of these pipelines can start",
        )


def _dependencies(data: _Metadata, report: Report) -> None:
    by_id = {task.task_id: task for task in data.tasks}
    for edge in data.edges:
        cross = edge.depends_on_pipeline_code != edge.pipeline_code
        if edge.written_pipeline_id is not None and (
            edge.written_pipeline_id != edge.depends_on_pipeline_id
        ):
            report.fail(
                edge.label,
                f"the dependency on {edge.depends_on_label} has DEPENDS_ON_PIPELINE_ID="
                f"{edge.written_pipeline_id}, but that task belongs to pipeline "
                f"{edge.depends_on_pipeline_code} (PIPELINE_ID={edge.depends_on_pipeline_id})",
            )
        if cross and not (edge.depends_on_task_active and edge.depends_on_pipeline_active):
            what = "task" if not edge.depends_on_task_active else "pipeline"
            report.fail(
                edge.label,
                f"depends on {edge.depends_on_label}, whose {what} is inactive and no longer "
                "runs, so once its last run is consumed the dependency is never satisfied "
                "again; deactivate the dependency too, or reactivate it",
            )
        upstream = by_id.get(edge.depends_on_task_id)
        if edge.dependency_type == DependencyType.HAS_DATA and upstream is not None:
            reason = _no_rows_reason(upstream, data.params[upstream.task_id])
            if reason:
                report.fail(
                    edge.label,
                    f"has a HAS_DATA dependency on {edge.depends_on_label}, which {reason}, so "
                    "the dependency is never satisfied and the task is always skipped; use "
                    "SUCCESS",
                )
    _alert_ordering(data, report)


def _no_rows_reason(task: ActiveTask, params: Mapping[str, str]) -> str | None:
    if task.handler == Handler.SQL:
        action = (params.get("SQL_ACTION") or "").strip().upper()
        if action and action not in ROWS_REPORTED:
            return f"is a {action} task and reports no target rows"
        return None
    if task.handler in (Handler.BUSINESS_RULES, Handler.EMAIL_ALERT):
        return f"is a {task.handler} task and reports no target rows"
    return None


def _alert_ordering(data: _Metadata, report: Report) -> None:
    """Check each alert waits for every task it reports on, and hears about their failures."""
    for code in data.pipeline_codes:
        tasks = [task for task in data.tasks if task.pipeline_code == code]
        alerts = {t.task_code: t for t in tasks if t.handler == Handler.EMAIL_ALERT}
        if not alerts:
            continue
        edges = [
            e for e in data.edges if e.pipeline_code == code and e.depends_on_pipeline_code == code
        ]
        depended_on = {e.depends_on_task_code for e in edges if e.task_code not in alerts}
        leaves = {t.task_code for t in tasks if t.task_code not in alerts} - depended_on
        for alert_code, alert in sorted(alerts.items()):
            waits = {e.depends_on_task_code: e for e in edges if e.task_code == alert_code}
            missing = sorted(leaves - set(waits))
            if missing:
                report.fail(
                    alert.label,
                    f"reports on the whole run but does not depend on {', '.join(missing)}, so "
                    "it can run before they finish and report on a run still in progress; add "
                    "an ALWAYS dependency on each",
                )
            outcomes = _alert_outcomes(data.params[alert.task_id])
            if outcomes & {"FAILED", "COMPLETED_WITH_ERRORS"}:
                strict = sorted(
                    upstream
                    for upstream, edge in waits.items()
                    if edge.dependency_type in (DependencyType.SUCCESS, DependencyType.HAS_DATA)
                )
                if strict:
                    report.warn(
                        alert.label,
                        f"depends on {', '.join(strict)} through SUCCESS or HAS_DATA, so when "
                        "one of them fails the alert is skipped and sends nothing about it; use "
                        "ALWAYS to hear about failures",
                    )


def _alert_outcomes(params: Mapping[str, str]) -> set[str]:
    written = params.get("EMAIL_ON_STATUS") or ""
    wanted = {part.strip().upper() for part in written.split("|") if part.strip()}
    return wanted or set(email_alert.OUTCOMES)


def _task(
    conn: Connection, config: ConnectorConfig, data: _Metadata, task: ActiveTask, report: Report
) -> None:
    params = data.params[task.task_id]
    where = task.label
    if not CODE.match(task.task_code):
        report.fail(where, _code_problem("TASK_CODE", task.task_code))
    try:
        task_timeout_seconds(params, config)
    except EtlCraftError as error:
        report.fail(where, str(error))
    known = KNOWN_PARAMETERS.get(task.handler)
    if known is not None:
        for name in sorted(set(params) - known - COMMON_PARAMETERS):
            report.warn(where, _unknown(f"{task.handler} task parameter", name, sorted(known)))
    context = TaskContext(
        config=config,
        pipeline_id=task.pipeline_id,
        pipeline_code=task.pipeline_code,
        task_id=task.task_id,
        task_code=task.task_code,
        pipeline_run_id=0,
        task_run_id=0,
        attempt=0,
        handler=task.handler,
        refresh_type=task.refresh_type,
        task_params=params,
    )
    check = CHECKS.get(task.handler)
    if check is None:
        return
    try:
        for problem in check(conn, context, data):
            report.fail(where, problem)
    except EtlCraftError as error:
        report.fail(where, str(error))


def _sql(conn: Connection, context: TaskContext, data: _Metadata) -> list[str]:
    spec = read_sql_task(context)
    if context.config.warehouse is None:
        return ["a SQL task needs a Warehouse section in craft-connector.yml"]
    dialect = task_dialect(context)
    problem = dialect.unsupported_storage_problem(
        context.task_params
    ) or dialect.task_storage_problem(context.task_params)
    if problem is not None:
        return [problem]
    if spec.action == SqlAction.SETUP_TABLE:
        others = fetch_target_tasks(conn, context.pipeline_id, context.task_id, spec.target_object)
        writer_action(spec.setup_for, others, spec.target_object)
    return []


def _business_rules(conn: Connection, context: TaskContext, data: _Metadata) -> list[str]:
    config = context.config
    if config.warehouse is None:
        return ["a BUSINESS_RULES task needs a Warehouse section in craft-connector.yml"]
    rules = fetch_business_rules_for_task(conn, context.task_id)
    if not rules:
        return [
            f"task {context.task_code} has HANDLER=BUSINESS_RULES but no active row in "
            "CFG_BUSINESS_RULES"
        ]
    catalog = active_catalog(config)
    rebuilt = {
        table_name(params.get("TARGET_OBJECT", "").split("."), catalog): (task.label, action)
        for task in data.tasks
        if task.handler == Handler.SQL
        for params in [data.params[task.task_id]]
        if (action := (params.get("SQL_ACTION") or "").strip().upper()) in FULL_REPLACE
    }
    problems: list[str] = []
    for rule in rules:
        try:
            check_rule(rule, catalog)
        except EtlCraftError as error:
            problems.append(str(error))
            continue
        writer = rebuilt.get(table_name(rule.target_table.split("."), catalog))
        if rule.business_rule_key_column.upper() == "ROW_ID" and writer is not None:
            problems.append(
                f"business rule {rule.business_rule_name!r}: BUSINESS_RULE_KEY_COLUMN is "
                f"ROW_ID, but {writer[0]} rebuilds {rule.target_table} with {writer[1]}, which "
                "gives every row a new ROW_ID; flags recorded against the old ones are never "
                "cleared. Key the rule on a column that identifies the row across runs"
            )
    return problems


def _python(conn: Connection, context: TaskContext, data: _Metadata) -> list[str]:
    params = context.task_params
    name = (params.get("SCRIPT_NAME") or "").strip()
    if not name:
        return ["SCRIPT_NAME is required for HANDLER=PYTHON: a .py file under ingestion_scripts/"]
    path = ingestion_script(context.config, name)
    python_scripts.parse_input_params(params.get("INPUT_PARAMS"))
    problem = python_scripts.script_definition_problem(path, name)
    return [problem] if problem else []


def _email_alert(conn: Connection, context: TaskContext, data: _Metadata) -> list[str]:
    problems = email_alert.alert_parameter_problems(context.task_params)
    if context.config.email is None:
        problems.insert(0, "an EMAIL_ALERT task needs an Email section in craft-connector.yml")
    return problems


CHECKS: dict[str, Callable[[Connection, TaskContext, _Metadata], list[str]]] = {
    Handler.SQL: _sql,
    Handler.BUSINESS_RULES: _business_rules,
    Handler.PYTHON: _python,
    Handler.EMAIL_ALERT: _email_alert,
}
"""Each handler's checks; each raises or returns the problems it finds."""


def _code_problem(column: str, code: str) -> str:
    return (
        f"{column}={code!r} may hold only letters, digits, '_' and '-': it appears in commands, "
        "DAG ids and file names"
    )


def _unknown(what: str, name: str, known: Sequence[str]) -> str:
    hints = suggest(name, list(known))
    hint = f"; did you mean {', '.join(hints)}" if hints else ""
    return f"{what} {name} is not read by etl-craft, so it has no effect{hint}"


def _cycles(edges: set[tuple[str, str]]) -> list[list[str]]:
    """Return each cycle in the ``(dependent, upstream)`` graph once, from its smallest code."""
    graph: dict[str, list[str]] = {}
    for dependent, upstream in sorted(edges):
        graph.setdefault(dependent, []).append(upstream)
    found: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()

    def walk(node: str, path: list[str]) -> None:
        for upstream in graph.get(node, []):
            if upstream in path:
                cycle = path[path.index(upstream) :]
                start = cycle.index(min(cycle))
                key = tuple(cycle[start:] + cycle[:start])
                if key not in seen:
                    seen.add(key)
                    found.append(list(key))
            elif upstream > path[0]:
                walk(upstream, [*path, upstream])

    for node in sorted(graph):
        walk(node, [node])
    return found
