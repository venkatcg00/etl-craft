"""The documentation catalog: every pipeline, task, table, rule and script, and how they connect.

``build_catalog`` reads the Engine DB and works out the lineage of every active SQL task (see
``services.lineage``), then joins them into assets:

- a **table** for each SQL task's ``TARGET_OBJECT``, each table a SELECT reads, each business
  rule's referenced target, and each ingestion script's ``TARGET_OBJECT``; with its columns, the
  tasks that write and read it, the rules on it, and its writers' latest run;
- a **task** for each active task, with its parameters, documentation, lineage and latest run;
- a **pipeline**, a **business rule** and an **ingestion script** page each.

Tables are joined by name as lineage names them: lower case, without the active warehouse
database. Two kinds of edge connect them: column edges, one per target column and source
column, and table edges, one per source table and target table of a task. A SQL task whose
columns cannot be traced keeps its table edges, from the tables its SELECT reads, and its reason
is listed. An ingestion script appears at table level: a ``PYTHON`` task may name the table it
writes in ``TARGET_OBJECT`` and where it reads from in ``SOURCE_OBJECT``, shown as an external
source.

With ``with_warehouse``, each table that exists in the warehouse gets its column types, and
comments where the warehouse's information schema has them.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConnectorConfig
from etl_craft.config.project import ingestion_script, sql_file
from etl_craft.config.targets import active_catalog, parse_warehouse_url
from etl_craft.core.enums import GapState, Handler, RunStatus, SlaStatus, SqlAction
from etl_craft.core.errors import EtlCraftError, MetadataError
from etl_craft.core.text import is_safe_identifier
from etl_craft.core.time import as_utc
from etl_craft.engine.repository.catalog import (
    Consumption,
    LastRun,
    PipelineRow,
    RuleRow,
    RunSummary,
    TaskRow,
    TaskRunSummary,
    fetch_catalog_interventions,
    fetch_catalog_pipelines,
    fetch_catalog_rules,
    fetch_catalog_tables,
    fetch_catalog_tasks,
    fetch_consumption,
    fetch_documentation_versions,
    fetch_pipeline_runs,
    fetch_task_runs,
)
from etl_craft.engine.repository.intervention_rows import Intervention
from etl_craft.engine.repository.lineage import (
    LineageVersion,
    fetch_lineage_versions,
    fetch_version_edges,
)
from etl_craft.engine.repository.normalization import ObjectIdentity
from etl_craft.engine.repository.pauses import Pause, fetch_open_pauses
from etl_craft.engine.repository.source_columns import fetch_source_columns
from etl_craft.engine.repository.tasks import TargetTask, fetch_all_task_parameters
from etl_craft.engine.repository.triggers import TriggerLink, fetch_trigger_links
from etl_craft.engine.repository.validation import (
    fetch_pipeline_edges,
    fetch_task_dependency_edges,
)
from etl_craft.handlers.sql.actions import writer_action
from etl_craft.handlers.sql.spec import read_action, read_setup_for
from etl_craft.handlers.sql.tables import AUDIT_COLUMNS
from etl_craft.services.catalog.gaps import infers_output_columns, table_records
from etl_craft.services.lineage import Edge, TaskLineage, collect, stored_edge, table_name
from etl_craft.services.source_columns import nullable, warehouse_kind
from etl_craft.warehouse.connection import open_warehouse, task_warehouse_dialect, warehouse_dialect

logger = logging.getLogger(__name__)

EXTERNAL_PREFIX = "external:"
"""How the name of a script's external source is told apart from a warehouse table."""


@dataclass
class Column:
    """A column of a table: its name, and its type and comment when the warehouse gave them."""

    name: str
    data_type: str | None = None
    comment: str | None = None
    declared_type: str | None = None
    warehouse_type: str | None = None
    declared_nullable: str | None = None
    warehouse_nullable: str | None = None
    declared: bool = True
    warehouse_present: bool = False
    engine_managed: bool = False


@dataclass
class TableAsset:
    """A table, or a script's external source, and everything that touches it."""

    name: str
    columns: dict[str, Column] = field(default_factory=dict)
    writers: list[str] = field(default_factory=list)
    readers: list[str] = field(default_factory=list)
    rules: list[int] = field(default_factory=list)
    in_warehouse: bool = False
    warehouse_checked: bool = False
    warehouse_findings: list[str] = field(default_factory=list)
    declared_kind: str | None = None
    warehouse_kind: str | None = None
    lineage_history: list[tuple[LineageVersion, list[Edge], list[Edge]]] = field(
        default_factory=list
    )
    declared_identity: ObjectIdentity | None = None

    @property
    def external(self) -> bool:
        """Whether this is an ingestion script's source rather than a warehouse table."""
        return self.name.startswith(EXTERNAL_PREFIX)

    @property
    def label(self) -> str:
        """The name shown: an external source without its prefix."""
        return self.name.removeprefix(EXTERNAL_PREFIX)

    def add_column(self, name: str) -> None:
        """Record a column, keeping the order columns are first seen in."""
        self.columns.setdefault(name, Column(name))


@dataclass(frozen=True)
class TableEdge:
    """A task reads ``source`` and writes ``target``."""

    source: str
    target: str
    task: str


@dataclass
class TaskAsset:
    """An active task: its definition, lineage, dependencies and latest run.

    ``upstream`` and ``downstream`` are the tasks it waits for and that wait for it, each with
    the dependency type, as ``PIPELINE_CODE.TASK_CODE``; ``rules`` are its business rules.
    """

    row: TaskRow
    params: dict[str, str]
    documentation: str | None
    documentation_version: int | None
    target: str | None = None
    sources: tuple[str, ...] = ()
    lineage_error: str | None = None
    column_edges: list[Edge] = field(default_factory=list)
    upstream: list[tuple[str, str]] = field(default_factory=list)
    downstream: list[tuple[str, str]] = field(default_factory=list)
    rules: list[int] = field(default_factory=list)
    runs: list[TaskRunSummary] = field(default_factory=list)

    @property
    def label(self) -> str:
        """``PIPELINE_CODE.TASK_CODE``."""
        return f"{self.row.pipeline_code}.{self.row.task_code}"

    @property
    def script(self) -> str | None:
        """The ingestion script a ``PYTHON`` task runs."""
        if self.row.handler != Handler.PYTHON:
            return None
        return (self.params.get("SCRIPT_NAME") or "").strip() or None


@dataclass
class PipelineAsset:
    """An active pipeline, its tasks, the pipelines it depends on, and its recent runs.

    ``runs`` are its latest runs, newest first; ``run_changes`` what operators changed in each
    of them, by run; ``paused`` is its open pause.
    """

    row: PipelineRow
    tasks: list[str] = field(default_factory=list)
    depends_on: list[tuple[str, str]] = field(default_factory=list)
    depended_on_by: list[str] = field(default_factory=list)
    run_changes: dict[int, list[Intervention]] = field(default_factory=dict)
    paused: Pause | None = None
    runs: list[RunSummary] = field(default_factory=list)

    @property
    def interventions(self) -> list[Intervention]:
        """What operators changed in its last run."""
        if self.row.last_run_id is None:
            return []
        return self.run_changes.get(self.row.last_run_id, [])


@dataclass
class RuleAsset:
    """An active business rule and the table it checks."""

    row: RuleRow
    table: str

    @property
    def task(self) -> str:
        """The rule's task, ``PIPELINE_CODE.TASK_CODE``."""
        return f"{self.row.pipeline_code}.{self.row.task_code}"


@dataclass
class ScriptAsset:
    """An ingestion script, the tasks that run it, and whether the file is there."""

    name: str
    tasks: list[str] = field(default_factory=list)
    exists: bool = False


@dataclass
class Catalog:
    """Every asset, and every edge between tables and between columns.

    ``built_from`` maps a run to the upstream runs it consumed; ``consumed_by`` maps an upstream
    run to the downstream runs that consumed it.
    """

    generated_at: datetime
    pipelines: dict[str, PipelineAsset]
    tasks: dict[str, TaskAsset]
    tables: dict[str, TableAsset]
    rules: dict[int, RuleAsset]
    scripts: dict[str, ScriptAsset]
    column_edges: list[Edge]
    table_edges: list[TableEdge]
    database: str | None = None
    built_from: dict[int, list[Consumption]] = field(default_factory=dict)
    consumed_by: dict[int, list[Consumption]] = field(default_factory=dict)
    compared_at: datetime | None = None
    warehouse_identity: str | None = None
    contract_version: str = ""
    sql_dialect: str | None = None
    trigger_links: list[TriggerLink] = field(default_factory=list)

    def where(self, table: str) -> tuple[str, str, str]:
        """Return a table's ``(database, schema, table)``; the active database when unnamed."""
        if table in self.tables and (identity := self.tables[table].declared_identity):
            return identity[0] or self.database or "", identity[1], identity[2]
        parts = table.split(".")
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        if len(parts) == 2:
            return self.database or "", parts[0], parts[1]
        return self.database or "", "", table

    @property
    def untraced(self) -> list[TaskAsset]:
        """The SQL tasks whose column lineage could not be worked out."""
        return [task for task in self.tasks.values() if task.lineage_error is not None]


def _pipeline_history(conn: Connection) -> tuple[dict[str, list[RunSummary]], list[TriggerLink]]:
    """Bound trigger links to the same recent runs as catalog history."""
    runs = fetch_pipeline_runs(conn)
    links = fetch_trigger_links(
        conn, [run.pipeline_run_id for rows in runs.values() for run in rows]
    )
    return runs, links


def collect_catalog(
    engine: Engine,
    config: ConnectorConfig,
    *,
    with_warehouse: bool = False,
    record_lineage: bool = True,
) -> Catalog:
    """Read the Engine DB and return the catalog; lineage is stored as ``lineage`` stores it."""
    catalog_name = active_catalog(config) if config.warehouse is not None else None
    with engine.connect() as conn:
        definitions = fetch_source_columns(conn)
        objects = fetch_catalog_tables(conn)
        pipeline_rows = fetch_catalog_pipelines(conn)
        task_rows = fetch_catalog_tasks(conn)
        rule_rows = fetch_catalog_rules(conn)
        versions = fetch_documentation_versions(conn)
        params = fetch_all_task_parameters(conn)
        pipeline_edges = fetch_pipeline_edges(conn)
        task_edges = fetch_task_dependency_edges(conn)
        paused = fetch_open_pauses(conn)
        pipeline_runs, trigger_links = _pipeline_history(conn)
        task_runs = fetch_task_runs(conn)
        consumption = fetch_consumption(conn)
        interventions = fetch_catalog_interventions(conn)
        changes = {
            code: {
                run.pipeline_run_id: interventions[run.pipeline_run_id]
                for run in runs
                if run.pipeline_run_id in interventions
            }
            for code, runs in pipeline_runs.items()
        }
    lineages = {lineage.task: lineage for lineage in collect(engine, config, record=record_lineage)}

    def name_of(object_ref: str) -> str:
        return table_name(object_ref.strip().split("."), catalog_name)

    catalog = Catalog(
        generated_at=datetime.now(UTC),
        trigger_links=trigger_links,
        pipelines={
            row.pipeline_code: PipelineAsset(
                row,
                run_changes=changes.get(row.pipeline_code, {}),
                paused=paused.get(row.pipeline_code),
                runs=pipeline_runs.get(row.pipeline_code, []),
            )
            for row in pipeline_rows
        },
        tasks={},
        tables={},
        rules={},
        scripts={},
        column_edges=[],
        table_edges=[],
        database=catalog_name,
    )
    for used in consumption:
        catalog.built_from.setdefault(used.pipeline_run_id, []).append(used)
        catalog.consumed_by.setdefault(used.upstream_run_id, []).append(used)
    for row in task_rows:
        task_params = params.get(row.task_id, {})
        task = TaskAsset(
            row,
            task_params,
            (task_params.get("DOCUMENTATION") or "").strip() or None,
            versions.get(row.task_id),
            runs=task_runs.get(row.task_id, []),
        )
        catalog.tasks[task.label] = task
        catalog.pipelines[row.pipeline_code].tasks.append(task.label)
        _connect_task(catalog, task, lineages.get(task.label), name_of, config)
    for definition in definitions:
        table = _table(catalog, name_of(definition.table_name))
        if any(infers_output_columns(catalog.tasks[writer]) for writer in table.writers):
            continue
        column = definition.column_name.lower()
        table.columns.setdefault(column, Column(column))
        table.columns[column].data_type = definition.data_type
        table.columns[column].declared_type = definition.data_type
        table.columns[column].declared_nullable = definition.is_nullable
        table.declared_kind = definition.object_kind
    _declared_objects(catalog, objects)
    _writers_order_first(catalog)
    for edge in pipeline_edges:
        if edge.pipeline_code in catalog.pipelines:
            catalog.pipelines[edge.pipeline_code].depends_on.append(
                (edge.depends_on_pipeline_code, edge.dependency_type)
            )
        if edge.depends_on_pipeline_code in catalog.pipelines:
            catalog.pipelines[edge.depends_on_pipeline_code].depended_on_by.append(
                edge.pipeline_code
            )
    for dependency in task_edges:
        if not (dependency.depends_on_task_active and dependency.depends_on_pipeline_active):
            continue
        waiting = catalog.tasks.get(dependency.label)
        upstream = catalog.tasks.get(dependency.depends_on_label)
        if waiting is None or upstream is None:
            continue
        waiting.upstream.append((upstream.label, dependency.dependency_type))
        upstream.downstream.append((waiting.label, dependency.dependency_type))
    for rule_row in rule_rows:
        rule = RuleAsset(rule_row, name_of(rule_row.target_table))
        catalog.rules[rule_row.business_rule_id] = rule
        if rule.task in catalog.tasks:
            catalog.tasks[rule.task].rules.append(rule_row.business_rule_id)
        _table(catalog, rule.table).rules.append(rule_row.business_rule_id)
        _table(catalog, rule.table).add_column(rule_row.key_column.lower())
    _compare_catalog(catalog, engine, config, with_warehouse)
    return catalog


def _compare_catalog(
    catalog: Catalog, engine: Engine, config: ConnectorConfig, requested: bool
) -> None:
    _comparison_identity(catalog, config, requested)
    if requested and config.warehouse is not None and catalog.compared_at is not None:
        try:
            _add_warehouse_columns(catalog, engine, config)
        except (SQLAlchemyError, EtlCraftError) as error:
            for table in catalog.tables.values():
                if not table.external:
                    table.warehouse_findings.append(f"comparison unavailable: {error}")
    for table in catalog.tables.values():
        for record in table_records(catalog, table):
            if record.state in (
                GapState.DEPLOYED_ONLY,
                GapState.CONFIG_ONLY,
                GapState.TYPE_MISMATCH,
                GapState.NULLABILITY_MISMATCH,
                GapState.KIND_MISMATCH,
            ):
                table.warehouse_findings.append(
                    f"schema drift: {record.column or table.name} {record.attribute}: "
                    f"{record.reason} "
                    f"declared={record.declared}; deployed={record.deployed}"
                )


def _table(catalog: Catalog, name: str) -> TableAsset:
    return catalog.tables.setdefault(name, TableAsset(name))


def _declared_objects(catalog: Catalog, objects: list[tuple[ObjectIdentity, str | None]]) -> None:
    for identity, kind in objects:
        name = table_name((part for part in identity if part), catalog.database)
        table = _table(catalog, name)
        resolved = identity[0] or catalog.database, identity[1], identity[2]
        if table.declared_identity and table.declared_identity != resolved:
            raise MetadataError(
                f"catalog object {name!r} conflates {table.declared_identity!r} and {resolved!r}; "
                "the current unquoted catalog cannot distinguish them; use distinct plain "
                "identifiers before generating documentation"
            )
        table.declared_identity = resolved
        table.declared_kind = kind


def _connect_task(
    catalog: Catalog,
    task: TaskAsset,
    lineage: TaskLineage | None,
    name_of: Callable[[str], str],
    config: ConnectorConfig,
) -> None:
    """Record what a task reads and writes, at table level and, for SQL, at column level."""
    params = task.params
    handler = task.row.handler
    if handler == Handler.SQL:
        written = (params.get("TARGET_OBJECT") or "").strip()
        task.target = name_of(written) if written else None
        action = (params.get("SQL_ACTION") or "").strip().upper()
        if lineage is not None:
            task.sources = lineage.sources
            task.lineage_error = lineage.error
            if action != SqlAction.DELETE_ROWS:
                task.column_edges = list(lineage.edges)
    elif handler == Handler.PYTHON:
        written = (params.get("TARGET_OBJECT") or "").strip()
        task.target = name_of(written) if written else None
        source = (params.get("SOURCE_OBJECT") or "").strip()
        task.sources = (f"{EXTERNAL_PREFIX}{source.lower()}",) if source else ()
        if task.script is not None:
            script = catalog.scripts.setdefault(task.script, ScriptAsset(task.script))
            script.tasks.append(task.label)
            try:
                script.exists = ingestion_script(config, task.script).is_file()
            except EtlCraftError:
                script.exists = False
    if task.target is not None:
        _table(catalog, task.target).writers.append(task.label)
    for source in task.sources:
        _table(catalog, source).readers.append(task.label)
        if task.target is not None and source != task.target:
            catalog.table_edges.append(TableEdge(source, task.target, task.label))
    for edge in task.column_edges:
        _table(catalog, edge.target_object).add_column(edge.target_column)
        if edge.source_object is not None and edge.source_column is not None:
            _table(catalog, edge.source_object).add_column(edge.source_column)
        catalog.column_edges.append(edge)


def _writers_order_first(catalog: Catalog) -> None:
    """Order each table's columns as the SELECTs that write it return them, then the rest."""
    written: dict[str, list[str]] = {}
    for task in catalog.tasks.values():
        for edge in task.column_edges:
            order = written.setdefault(edge.target_object, [])
            if edge.target_column not in order:
                order.append(edge.target_column)
    for name, order in written.items():
        table = catalog.tables[name]
        rest = {k: v for k, v in table.columns.items() if k not in order}
        table.columns = {column: table.columns[column] for column in order} | rest


def _add_warehouse_columns(catalog: Catalog, engine: Engine, config: ConnectorConfig) -> None:
    """Give each table the warehouse has its columns' types and comments, in warehouse order."""
    dialect = warehouse_dialect(config)
    default_catalog = active_catalog(config)
    with open_warehouse(config, engine) as warehouse, warehouse.connect() as conn:
        for table in catalog.tables.values():
            if table.external:
                continue
            parts = (
                [table.declared_identity[0] or default_catalog, *table.declared_identity[1:]]
                if table.declared_identity
                else table.name.split(".")
            )
            if len(parts) == 2:
                parts = [default_catalog, *parts]
            if len(parts) != 3:
                table.warehouse_findings.append(
                    f"comparison unavailable: {table.name!r} needs schema.table or "
                    "catalog.schema.table; qualify the source SQL or table contract"
                )
                continue
            database, schema, name = parts
            if not all(is_safe_identifier(part) for part in parts):
                table.warehouse_findings.append(
                    f"comparison unavailable: object components {tuple(parts)!r} require quoted "
                    "identifiers, unsupported by the current catalog; use plain identifiers "
                    "or retain an offline snapshot"
                )
                continue
            known = table.columns
            try:
                dialect.load_table_metadata(conn, schema, name)
                rows = conn.execute(
                    text(
                        "SELECT * FROM information_schema.columns WHERE lower(table_name) = "
                        "lower(:t) AND lower(table_schema) = lower(:s) AND "
                        "lower(table_catalog) = lower(:c) ORDER BY ordinal_position"
                    ),
                    {"t": name, "s": schema, "c": database},
                ).mappings()
                found = [{str(k).lower(): v for k, v in row.items()} for row in rows]
                full_types = dialect.full_column_types(conn, ".".join(parts)) if found else {}
                kind = warehouse_kind(conn, config, table.name) if found else None
            except (SQLAlchemyError, EtlCraftError) as error:
                logger.warning("could not read the columns of %s: %s", table.name, error)
                table.warehouse_findings.append(f"comparison unavailable: {error}")
                continue
            table.warehouse_checked = True
            if not found:
                table.warehouse_findings.append(
                    "table is not deployed; metadata columns are retained"
                )
                continue
            table.in_warehouse = True
            table.columns = {}
            for row in found:
                column = str(row["column_name"]).lower()
                table.columns[column] = Column(
                    column,
                    full_types.get(column) or str(row.get("data_type") or "") or None,
                    _comment(row),
                    known[column].declared_type if column in known else None,
                    full_types.get(column) or str(row.get("data_type") or "") or None,
                    known[column].declared_nullable if column in known else None,
                    nullable(row.get("is_nullable")),
                    column in known,
                    True,
                    known[column].engine_managed if column in known else False,
                )
            for column, value in known.items():
                table.columns.setdefault(column, value)
            table.warehouse_kind = kind


def _comment(row: dict[str, object]) -> str | None:
    for key in ("comment", "column_comment", "remarks"):
        value = row.get(key)
        if value:
            return str(value)
    return None


def _engine_columns(catalog: Catalog, table: TableAsset, config: ConnectorConfig) -> None:
    for writer in table.writers:
        task = catalog.tasks[writer]
        if task.row.handler != Handler.SQL:
            continue
        try:
            action = read_action(task.params)
            dialect = task_warehouse_dialect(config, task.params) if config.warehouse else None
            if action == SqlAction.SETUP_TABLE:
                action = _setup_writer(catalog, table, task)
        except EtlCraftError as error:
            table.warehouse_findings.append(
                f"comparison unavailable: {task.label}: {error}; correct the task parameters"
            )
            continue
        if action not in AUDIT_COLUMNS:
            continue
        for name in ("PIPELINE_RUN_ID", "ROW_ID", *AUDIT_COLUMNS[action]):
            column = table.columns.setdefault(name.lower(), Column(name.lower()))
            column.engine_managed = True
            column.declared_type = (
                "BIGINT"
                if name in ("ROW_ID", "PIPELINE_RUN_ID")
                else dialect.audit_column_contract(name)
                if dialect
                else None
            )


def _setup_writer(catalog: Catalog, table: TableAsset, task: TaskAsset) -> SqlAction:
    specified = read_setup_for(task.params.get("SETUP_FOR"), SqlAction.SETUP_TABLE)
    others = [
        TargetTask(other.row.task_id, other.row.task_code, read_action(other.params))
        for name in table.writers
        if name != task.label
        and (other := catalog.tasks[name]).row.pipeline_code == task.row.pipeline_code
        and infers_output_columns(other)
    ]
    return SqlAction(writer_action(specified, others, table.name))


def _comparison_identity(catalog: Catalog, config: ConnectorConfig, requested: bool) -> None:
    """Identify the inputs without exporting SQL, credentials or connection strings."""
    inputs: list[object] = []
    for table in catalog.tables.values():
        _engine_columns(catalog, table, config)
    for task in catalog.tasks.values():
        params = task.params
        content = params.get("SOURCE_SQL", "")
        if params.get("SOURCE_SQL_FILE"):
            try:
                content = sql_file(config, params["SOURCE_SQL_FILE"]).read_text(encoding="utf-8")
            except (EtlCraftError, OSError) as error:
                content = f"unavailable: {type(error).__name__}"
        inputs.append((task.label, task.target, params.get("SQL_ACTION"), content))
    inputs.extend(
        (
            table.name,
            table.declared_kind,
            [
                (c.name, c.declared_type, c.declared_nullable)
                for c in table.columns.values()
                if c.declared
            ],
        )
        for table in catalog.tables.values()
    )
    catalog.contract_version = hashlib.sha256(
        json.dumps(inputs, sort_keys=True).encode()
    ).hexdigest()
    if config.warehouse:
        location = parse_warehouse_url(config.warehouse.jdbc_url)
        catalog.warehouse_identity = (
            f"{config.warehouse.name}: {location.dialect} / "
            f"{location.host or 'local'}:{location.port or 'default'} / "
            f"{location.path or location.database} / catalog {catalog.database or 'absent'}"
        )
        catalog.sql_dialect = warehouse_dialect(config).sqlglot_dialect
    if requested and any(not table.external for table in catalog.tables.values()):
        catalog.compared_at = datetime.now(UTC)
        if config.warehouse is None:
            for table in catalog.tables.values():
                table.warehouse_findings.append(
                    "comparison unavailable: no warehouse connection configured"
                )


@dataclass(frozen=True)
class RunKpis:
    """What a pipeline's or task's recent runs add up to."""

    runs: int
    finished: int
    succeeded: int
    failed: int
    sla_breached: int
    average_seconds: float | None
    longest_seconds: float | None
    average_rows: float | None = None

    @property
    def success_rate(self) -> float | None:
        """The share of finished runs that succeeded, or ``None`` before any finished."""
        return None if not self.finished else self.succeeded / self.finished


def run_kpis(runs: Sequence[RunSummary | TaskRunSummary]) -> RunKpis:
    """Return the KPIs of ``runs``: successes, failures, SLA misses, durations and rows.

    A run counts as finished once it is ``SUCCESS``, ``FAILED`` or ``CANCELLED``; ``SKIPPED``
    runs did no work and ``IN-PROGRESS`` ones have not ended, so neither counts either way.
    """
    finished = [
        r for r in runs if r.status in (RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.CANCELLED)
    ]
    seconds = [s for r in finished if (s := r.seconds) is not None]
    rows = [
        r.target_count
        for r in runs
        if isinstance(r, TaskRunSummary)
        and r.status == RunStatus.SUCCESS
        and r.target_count is not None
    ]
    return RunKpis(
        runs=len(runs),
        finished=len(finished),
        succeeded=sum(1 for r in finished if r.status == RunStatus.SUCCESS),
        failed=sum(1 for r in finished if r.status != RunStatus.SUCCESS),
        sla_breached=sum(
            1 for r in runs if isinstance(r, RunSummary) and r.sla_status == SlaStatus.BREACHED
        ),
        average_seconds=sum(seconds) / len(seconds) if seconds else None,
        longest_seconds=max(seconds) if seconds else None,
        average_rows=sum(rows) / len(rows) if rows else None,
    )


def latest_run(runs: Iterable[LastRun | None]) -> LastRun | None:
    """Return the run that ended last, or ``None`` when none has."""
    finished = [run for run in runs if run is not None and run.end is not None]
    return max(finished, key=lambda run: as_utc(run.end), default=None)


def _lineage_history(catalog: Catalog, engine: Engine) -> None:
    """Attach table versions and edge additions/removals using one history read."""
    with engine.connect() as conn:
        versions = fetch_lineage_versions(conn)
        edges = fetch_version_edges(conn, None)
    by_version: dict[int, set[Edge]] = {}
    for edge in edges:
        by_version.setdefault(edge.lineage_version_id, set()).add(
            stored_edge(edge, edge.task, catalog.database)
        )
    previous: dict[str, set[Edge]] = {}
    for version in versions:
        name = table_name(
            (version.catalog_name or "", version.schema_name, version.table_name), catalog.database
        )
        current = by_version.get(version.lineage_version_id, set())
        before = previous.get(name, set())
        _table(catalog, name).lineage_history.append(
            (
                version,
                sorted(
                    current - before,
                    key=lambda e: (
                        e.target_column,
                        e.source_object or "",
                        e.source_column or "",
                        e.task,
                        e.transformation,
                    ),
                ),
                sorted(
                    before - current,
                    key=lambda e: (
                        e.target_column,
                        e.source_object or "",
                        e.source_column or "",
                        e.task,
                        e.transformation,
                    ),
                ),
            )
        )
        previous[name] = current


def build_catalog(
    engine: Engine, config: ConnectorConfig, *, with_warehouse: bool = False
) -> Catalog:
    """Build current assets and attach their recorded lineage histories."""
    catalog = collect_catalog(engine, config, with_warehouse=with_warehouse)
    _lineage_history(catalog, engine)
    return catalog
