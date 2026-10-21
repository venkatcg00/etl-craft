"""Column lineage: which columns each SQL task's target is made from, traced across tasks.

Every active SQL task's SELECT (inline or from its file, with the pipeline-id tokens replaced) is
parsed in the warehouse's SQL dialect. Each column of the SELECT is traced back through CTEs,
subqueries and joins to the source columns it is made from, and recorded against the task's
``TARGET_OBJECT``:

- a column copied as it is: ``copy``;
- a column computed from others, such as ``o.amount * c.rate``: the expression, one edge per
  source column;
- a column made from none, such as ``COUNT(*)`` or a constant: the expression, with no source.

Tables are named as the SQL names them, lower case, with the active warehouse database left off,
so a task's target and a later task's source meet. Because one task's target is the next one's
source, the edges join into one graph, which ``LineageGraph`` walks upstream to the first sources
and downstream to the last consumers, across pipelines.

Lineage is stored per task in ``AUD_COLUMN_LINEAGE`` with a hash of the SELECT, target and
dialect and source schemas, and worked out again only when they change. SQL that cannot be
traced, such as
``SELECT *`` over a table whose columns are not known, is reported with the reason.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from contextlib import nullcontext
from dataclasses import asdict, dataclass, field
from datetime import datetime
from importlib.metadata import version

import sqlglot
from sqlalchemy.engine import Engine
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.lineage import lineage as sqlglot_lineage
from sqlglot.optimizer.qualify import qualify as qualify_columns
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.schema import MappingSchema

from etl_craft.config import ConnectorConfig
from etl_craft.config.project import sql_script
from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import Handler, RefreshType, SqlAction
from etl_craft.core.errors import EtlCraftError, LockTimeoutError, MetadataError
from etl_craft.core.text import sha256_hex, split_statements
from etl_craft.engine.locks import LINEAGE
from etl_craft.engine.repository.lineage import (
    SqlTaskRef,
    StoredEdge,
    fetch_lineage_cache,
    fetch_lineage_versions,
    fetch_sql_tasks,
    fetch_version_edges,
    record_lineage_versions,
    store_task_lineage,
)
from etl_craft.engine.repository.source_columns import fetch_source_columns
from etl_craft.engine.repository.tasks import fetch_all_task_parameters
from etl_craft.engine.repository.validation import fetch_active_tasks, fetch_task_dependency_edges
from etl_craft.handlers.sql.spec import resolve_select
from etl_craft.services.source_columns import definition_problem
from etl_craft.warehouse.connection import warehouse_dialect

COPY = "copy"
"""The transformation of a column copied from its source as it is."""

SQLGLOT_VERSION = version("sqlglot")


@dataclass(frozen=True)
class Edge:
    """A target column and one column it is made from, written by one task."""

    target_object: str
    target_column: str
    source_object: str | None
    source_column: str | None
    transformation: str
    task: str = ""


@dataclass(frozen=True)
class TaskLineage:
    """One SQL task's lineage, or why it has none; ``cached`` when it was stored already.

    ``sources`` are the tables its SELECT reads, joins and filters included, known whenever the
    SELECT parses, even when its columns cannot be traced.
    """

    task: str
    target_object: str | None
    edges: list[Edge]
    error: str | None = None
    cached: bool = False
    sources: tuple[str, ...] = ()
    targets: tuple[str, ...] = ()


def table_name(parts: Iterable[str], catalog: str | None) -> str:
    """Return a table's name as lineage compares it: lower case, without the active database."""
    names = [part.lower() for part in parts if part]
    if len(names) == 3 and catalog and names[0] == catalog.lower():
        names = names[1:]
    return ".".join(names)


def extract(
    select_sql: str,
    target_object: str,
    *,
    dialect: str | None,
    catalog: str | None = None,
    schemas: Mapping[str, Mapping[str, str]] | None = None,
) -> list[Edge]:
    """Trace each column of ``select_sql`` to its sources; ``MetadataError`` when it cannot."""
    try:
        tree = sqlglot.parse_one(select_sql, read=dialect)
    except SqlglotError as error:
        raise MetadataError(f"the SELECT could not be parsed: {error}") from error
    if not isinstance(tree, exp.Query):
        raise MetadataError("the statement is not a SELECT")
    target = table_name(target_object.split("."), catalog)
    originals = {
        projection.alias_or_name.lower(): projection.unalias().copy() for projection in tree.selects
    }
    schema = MappingSchema(dialect=dialect)
    if schemas:
        for name, columns in schemas.items():
            parts = name.split(".")
            parts = [catalog or "__etl_craft", *parts] if len(parts) == 2 else parts
            parts = (
                [catalog or "__etl_craft", "__unqualified", *parts] if len(parts) == 1 else parts
            )
            schema.add_table(".".join(parts), dict(columns))
    if schemas:
        try:
            tree = qualify_columns(
                tree,
                dialect=dialect,
                schema=schema,
                catalog=catalog or "__etl_craft",
                db="__unqualified",
                quote_identifiers=False,
                identify=False,
            )
        except (SqlglotError, KeyError, ValueError) as error:
            tables = ", ".join(referenced_tables(select_sql, dialect=dialect, catalog=catalog))
            raise MetadataError(
                f"columns from {tables} could not be resolved: {error}; define missing source "
                f"columns in columns.csv"
            ) from error
    source_names = ", ".join(referenced_tables(select_sql, dialect=dialect, catalog=catalog))
    edges: list[Edge] = []
    for projection in tree.selects:
        column = projection.alias_or_name
        if not column or isinstance(projection, exp.Star) or column == "*":
            raise MetadataError(
                "SELECT * cannot be traced without knowing the table's columns "
                f"({source_names}); define them in columns.csv or list them"
            )
        inner = originals.get(column.lower(), projection.unalias())
        transformation = COPY if isinstance(inner, exp.Column) else inner.sql(dialect=dialect)
        try:
            node = sqlglot_lineage(column, tree, schema, dialect=dialect)
        except (SqlglotError, KeyError, ValueError) as error:
            raise MetadataError(f"column {column!r} could not be traced: {error}") from error
        if any(
            not leaf.downstream and isinstance(leaf.expression, exp.Placeholder)
            for leaf in node.walk()
        ):
            raise MetadataError(
                f"column {column!r} from "
                f"{referenced_tables(select_sql, dialect=dialect, catalog=catalog)} could not "
                f"be resolved; define its source columns in columns.csv"
            )
        sources = sorted(
            {
                (
                    table_name(
                        (
                            ""
                            if leaf.source.catalog.lower() == "__etl_craft"
                            else leaf.source.catalog,
                            "" if leaf.source.db.lower() == "__unqualified" else leaf.source.db,
                            leaf.source.name,
                        ),
                        catalog,
                    ),
                    leaf.name.rsplit(".", 1)[-1].lower(),
                )
                for leaf in node.walk()
                if not leaf.downstream and isinstance(leaf.source, exp.Table)
            }
        )
        if not sources:
            edges.append(Edge(target, column.lower(), None, None, transformation))
        edges.extend(
            Edge(target, column.lower(), source, source_column, transformation)
            for source, source_column in sources
        )
    return edges


def output_columns(select_sql: str, *, dialect: str | None) -> list[str] | None:
    """Return the names of the columns ``select_sql`` returns, or ``None`` when they are unknown.

    They are unknown when the SELECT does not parse, or returns ``*`` or a column with no name.
    """
    try:
        tree = sqlglot.parse_one(select_sql, read=dialect)
    except SqlglotError:
        return None
    if not isinstance(tree, exp.Query):
        return None
    names: list[str] = []
    for projection in tree.selects:
        name = projection.alias_or_name
        if not name or name == "*" or isinstance(projection, exp.Star):
            return None
        names.append(name)
    return names


def referenced_tables(
    select_sql: str, *, dialect: str | None, catalog: str | None = None
) -> tuple[str, ...]:
    """Return every table ``select_sql`` reads, CTEs left out; empty when it does not parse."""
    try:
        tree = sqlglot.parse_one(select_sql, read=dialect)
    except SqlglotError:
        return ()
    ctes = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    found = {
        table_name((t.catalog, t.db, t.name), catalog)
        for t in tree.find_all(exp.Table)
        if t.name and not (not t.db and t.name.lower() in ctes)
    }
    return tuple(sorted(found))


def lineage_key(
    select_sql: str,
    target_object: str,
    dialect: str | None,
    catalog: str | None = None,
    schemas: Mapping[str, Mapping[str, str]] | None = None,
) -> str:
    """Hash what a task's lineage is worked out from; any change means working it out again."""
    return sha256_hex(
        "\0".join(
            (
                select_sql,
                target_object,
                dialect or "",
                catalog or "",
                SQLGLOT_VERSION,
                json.dumps(
                    {name: list(columns.items()) for name, columns in (schemas or {}).items()},
                    sort_keys=True,
                ),
            )
        ).encode()
    )


def _ordered_tasks(
    engine: Engine, config: ConnectorConfig, tasks: list[tuple[SqlTaskRef, dict[str, str]]]
) -> tuple[list[tuple[SqlTaskRef, dict[str, str]]], set[int], set[str]]:
    """Order SQL writers by metadata and references, retaining unresolved cycles."""
    dialect, catalog = dialect_and_catalog(config)
    with engine.connect() as conn:
        dependencies = fetch_task_dependency_edges(conn)
    writers: dict[str, set[int]] = {}
    reads: dict[int, tuple[str, ...]] = {}
    for ref, params in tasks:
        action = (params.get("SQL_ACTION") or "").upper()
        target = (params.get("TARGET_OBJECT") or "").strip()
        if target and action not in (SqlAction.DROP_TABLE, SqlAction.DELETE_ROWS):
            writers.setdefault(table_name(target.split("."), catalog), set()).add(ref.task_id)
        try:
            sql, _ = resolve_select(
                config, params, pipeline_run_id=0, refresh_type=RefreshType.FULL
            )
            reads[ref.task_id] = referenced_tables(sql, dialect=dialect, catalog=catalog)
        except EtlCraftError:
            reads[ref.task_id] = ()
    labels = {f"{ref.pipeline_code}.{ref.task_code}": ref.task_id for ref, _ in tasks}
    targets = {
        ref.task_id: table_name((params.get("TARGET_OBJECT") or "").strip().split("."), catalog)
        for ref, params in tasks
    }
    creators = {
        ref.task_id
        for ref, params in tasks
        if (params.get("SQL_ACTION") or "").upper()
        in (SqlAction.CREATE_TABLE, SqlAction.SETUP_TABLE, SqlAction.CREATE_VIEW)
    }
    before = {
        ref.task_id: {
            writer
            for source in reads[ref.task_id]
            for writer in writers.get(source, ())
            if writer != ref.task_id and (source != targets[ref.task_id] or writer in creators)
        }
        for ref, _ in tasks
    }
    for dependency in dependencies:
        task_id = labels.get(dependency.label)
        if (
            task_id is not None
            and dependency.depends_on_task_id in before
            and dependency.depends_on_task_id != task_id
        ):
            before[task_id].add(dependency.depends_on_task_id)
    ordered = []
    remaining = list(tasks)
    processed: set[int] = set()
    while remaining:
        ready = [(ref, params) for ref, params in remaining if before[ref.task_id] <= processed]
        if not ready:
            break
        ordered.extend(ready)
        processed.update(ref.task_id for ref, _ in ready)
        remaining = [(ref, params) for ref, params in remaining if ref.task_id not in processed]
    ordered.extend(remaining)
    return ordered, processed, set(writers)


def _collect(
    engine: Engine,
    config: ConnectorConfig,
    keys: dict[int, str | None],
    *,
    refresh: bool = False,
    record: bool = True,
) -> list[TaskLineage]:
    """Return the lineage of every active SQL task, working out and storing what changed.

    ``refresh`` works everything out again. Without ``record`` nothing is stored.
    """
    dialect, catalog = dialect_and_catalog(config)
    with engine.connect() as conn:
        params_by_task = fetch_all_task_parameters(conn, Handler.SQL)
        tasks = [(ref, params_by_task.get(ref.task_id, {})) for ref in fetch_sql_tasks(conn)]
        defined = fetch_source_columns(conn)
        cache = {} if refresh else fetch_lineage_cache(conn)
    schemas: dict[str, dict[str, str]] = {}
    invalid: dict[str, str] = {}
    for column in defined:
        name = table_name(column.table_name.split("."), catalog)
        problem = definition_problem(column, dialect)
        if problem:
            invalid[name] = problem
            continue
        schemas.setdefault(table_name(column.table_name.split("."), catalog), {})[
            column.column_name.lower()
        ] = column.data_type
    ordered, processed, writers = _ordered_tasks(engine, config, tasks)
    # A managed target is inferred from its writers, never from a stale source definition.
    for target in writers:
        schemas.pop(target, None)
    results: list[TaskLineage] = []
    for ref, params in ordered:
        name = f"{ref.pipeline_code}.{ref.task_code}"
        if (params.get("SQL_ACTION") or "").upper() == SqlAction.DROP_TABLE:
            continue
        keys[ref.task_id] = None
        target = (params.get("TARGET_OBJECT") or "").strip()
        sources: tuple[str, ...] = ()
        try:
            select_sql, _ = resolve_select(
                config, params, pipeline_run_id=0, refresh_type=RefreshType.FULL
            )
            sources = referenced_tables(select_sql, dialect=dialect, catalog=catalog)
            if not target:
                raise MetadataError("the task has no TARGET_OBJECT")
            if ref.task_id not in processed:
                raise MetadataError(
                    f"{target}: lineage dependency cycle between SQL writers; correct task "
                    f"dependencies and source/target references"
                )
            for source in sources:
                if source in invalid:
                    raise MetadataError(f"source {source}: {invalid[source]}")
            used_schemas = {source: schemas[source] for source in sources if source in schemas}
            contract = [
                asdict(column)
                for column in defined
                if table_name(column.table_name.split("."), catalog) in used_schemas
            ]
            key = sha256_hex(
                (
                    lineage_key(select_sql, target, dialect, catalog, used_schemas)
                    + json.dumps(contract, sort_keys=True)
                ).encode()
            )
            stored = cache.get((ref.task_id, key), [])
            edges = (
                [stored_edge(e, name, catalog) for e in stored]
                if stored
                else [
                    Edge(
                        e.target_object,
                        e.target_column,
                        e.source_object,
                        e.source_column,
                        e.transformation,
                        name,
                    )
                    for e in extract(
                        select_sql, target, dialect=dialect, catalog=catalog, schemas=used_schemas
                    )
                ]
            )
        except EtlCraftError as error:
            reason = f"history not updated: {error}" if record else str(error)
            results.append(TaskLineage(name, target or None, [], error=reason, sources=sources))
            continue
        if (params.get("SQL_ACTION") or "").upper() != SqlAction.DELETE_ROWS:
            schemas[table_name(target.split("."), catalog)] = dict.fromkeys(
                (e.target_column for e in edges), "UNKNOWN"
            )
        if record and not stored:
            with engine.begin() as conn:
                store_task_lineage(conn, ref.task_id, key, [cache_edge(e, catalog) for e in edges])
        keys[ref.task_id] = key
        results.append(TaskLineage(name, target, edges, cached=bool(stored), sources=sources))
    return sorted(results, key=lambda result: result.task)


def dialect_and_catalog(config: ConnectorConfig) -> tuple[str | None, str | None]:
    """Return the sqlglot dialect and the catalog of the active warehouse; ``None`` without one."""
    if config.warehouse is None:
        return None, None
    return warehouse_dialect(config).sqlglot_dialect, active_catalog(config)


def collect(
    engine: Engine, config: ConnectorConfig, *, refresh: bool = False, record: bool = True
) -> list[TaskLineage]:
    """Compute task lineage and publish table versions once under the recording lock."""
    keys: dict[int, str | None] = {}
    try:
        with LINEAGE.hold(engine, wait_seconds=30) if record else nullcontext():
            results = _collect(engine, config, keys, refresh=refresh, record=record)
            if record:
                _, catalog = dialect_and_catalog(config)
                with engine.begin() as conn:
                    record_lineage_versions(conn, catalog.lower() if catalog else None, keys)
            return results + _script_lineages(engine, config)
    except LockTimeoutError as error:
        raise LockTimeoutError(
            f"lineage recording lock unavailable: {error}; check the current lineage or docs "
            "recorder, let it finish or restart it if stuck, then retry"
        ) from error


def stored_edge(stored: StoredEdge, task: str, catalog: str | None) -> Edge:
    """Render stored endpoint components using the graph's display convention."""
    target = table_name(
        (stored.target_catalog_name or "", stored.target_schema_name, stored.target_table_name),
        catalog,
    )
    source = (
        None
        if stored.source_table_name is None
        else table_name(
            (
                stored.source_catalog_name or "",
                stored.source_schema_name or "",
                stored.source_table_name,
            ),
            catalog,
        )
    )
    return Edge(
        target, stored.target_column, source, stored.source_column, stored.transformation, task
    )


def cache_edge(edge: Edge, catalog: str | None) -> StoredEdge:
    """Store the existing tracer's plain identifiers as explicit components."""
    target = edge.target_object.split(".")
    source = edge.source_object.split(".") if edge.source_object else []
    return StoredEdge(
        target[0] if len(target) == 3 else catalog.lower() if catalog else None,
        target[-2],
        target[-1],
        edge.target_column,
        source[0]
        if len(source) == 3
        else catalog.lower()
        if catalog and len(source) == 2
        else None,
        source[-2] if len(source) >= 2 else None,
        source[-1] if source else None,
        edge.source_column,
        edge.transformation,
    )


def historical_lineage(
    engine: Engine, config: ConnectorConfig, moment: datetime
) -> list[TaskLineage]:
    """Read the versions valid at a UTC instant; never parse SQL or update the cache."""
    _, catalog = dialect_and_catalog(config)
    with engine.connect() as conn:
        versions = fetch_lineage_versions(conn)
        earliest = min((v.valid_from for v in versions), default=None)
        if earliest is None or moment < earliest:
            start = earliest.isoformat() if earliest else "no versions recorded yet"
            raise MetadataError(
                f"cannot read lineage as of {moment.isoformat()}: earliest recorded time is "
                f"{start}; choose a time at or after it, or compute lineage first"
            )
        grouped: dict[str, list[Edge]] = {}
        for v in versions:
            if v.valid_from <= moment < v.valid_to:
                for e in fetch_version_edges(conn, v.lineage_version_id):
                    grouped.setdefault(e.task, []).append(stored_edge(e, e.task, catalog))
    return [
        TaskLineage(
            task,
            edges[0].target_object,
            edges,
            sources=tuple(sorted({e.source_object for e in edges if e.source_object})),
        )
        for task, edges in sorted(grouped.items())
    ]


@dataclass
class LineageGraph:
    """Every task's edges, joined where one task's target is another's source."""

    edges: list[Edge]
    _into: dict[tuple[str, str], list[Edge]] = field(default_factory=dict)
    _out_of: dict[tuple[str, str], list[Edge]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Index the edges by the column they lead into and out of."""
        for e in self.edges:
            self._into.setdefault((e.target_object, e.target_column), []).append(e)
            if e.source_object is not None and e.source_column is not None:
                self._out_of.setdefault((e.source_object, e.source_column), []).append(e)

    @classmethod
    def of(cls, lineages: Iterable[TaskLineage]) -> LineageGraph:
        """Join the edges of every task's lineage."""
        return cls([e for lineage in lineages for e in lineage.edges])

    def columns(self, table: str) -> list[str]:
        """Return the columns of ``table`` any edge mentions, by name."""
        found = {c for (t, c) in self._into if t == table} | {
            c for (t, c) in self._out_of if t == table
        }
        return sorted(found)

    def upstream(self, table: str, column: str, depth: int | None = None) -> list[tuple[int, Edge]]:
        """Return the edges into ``table.column`` and, recursively, into their sources."""
        return self._walk((table, column), self._into, upstream=True, depth=depth)

    def downstream(
        self, table: str, column: str, depth: int | None = None
    ) -> list[tuple[int, Edge]]:
        """Return the edges out of ``table.column`` and, recursively, out of their targets."""
        return self._walk((table, column), self._out_of, upstream=False, depth=depth)

    def _walk(
        self,
        start: tuple[str, str],
        index: Mapping[tuple[str, str], list[Edge]],
        *,
        upstream: bool,
        depth: int | None,
    ) -> list[tuple[int, Edge]]:
        """Depth-first, each column visited once, with the level of each edge."""
        found: list[tuple[int, Edge]] = []
        seen = {start}

        def visit(node: tuple[str, str], level: int) -> None:
            if depth is not None and level > depth:
                return
            for e in sorted(index.get(node, []), key=_sort_key):
                found.append((level, e))
                nxt = (
                    (e.source_object, e.source_column)
                    if upstream
                    else (e.target_object, e.target_column)
                )
                if nxt[0] is None or nxt[1] is None or nxt in seen:
                    continue
                seen.add(nxt)  # type: ignore[arg-type]
                visit(nxt, level + 1)  # type: ignore[arg-type]

        visit(start, 1)
        return found

    def upstream_tables(self, table: str) -> list[tuple[int, str, str]]:
        """Return ``(level, table, task)`` for every table ``table`` is made from, recursively."""
        return self._tables(table, upstream=True)

    def downstream_tables(self, table: str) -> list[tuple[int, str, str]]:
        """Return ``(level, table, task)`` for every table made from ``table``, recursively."""
        return self._tables(table, upstream=False)

    def _tables(self, table: str, *, upstream: bool) -> list[tuple[int, str, str]]:
        links: dict[str, set[tuple[str, str]]] = {}
        for e in self.edges:
            if e.source_object is None:
                continue
            key, other = (
                (e.target_object, e.source_object)
                if upstream
                else (e.source_object, e.target_object)
            )
            links.setdefault(key, set()).add((other, e.task))
        found: list[tuple[int, str, str]] = []
        seen = {table}

        def visit(name: str, level: int) -> None:
            for other, task in sorted(links.get(name, set())):
                found.append((level, other, task))
                if other not in seen:
                    seen.add(other)
                    visit(other, level + 1)

        visit(table, 1)
        return found


def _sort_key(edge: Edge) -> tuple[str, str, str, str]:
    return (
        edge.source_object or "",
        edge.source_column or "",
        edge.target_object,
        edge.target_column,
    )


def script_lineage(sql: str, name: str, config: ConnectorConfig) -> TaskLineage:
    """Best-effort unmanaged lineage; procedure effects cannot be inferred."""
    dialect, catalog = dialect_and_catalog(config)
    targets: set[str] = set()
    sources: set[str] = set()
    edges: list[Edge] = []
    unparsed: list[str] = []
    for index, statement_sql in enumerate(split_statements(sql), 1):
        try:
            tree = sqlglot.parse_one(statement_sql, read=dialect, error_message_context=0)
            if config.warehouse is not None:
                tree = qualify_tables(tree, db=config.warehouse.schema or None, catalog=catalog)
        except SqlglotError:
            unparsed.append(str(index))
            continue
        target = (
            tree.this
            if isinstance(tree, (exp.DDL, exp.DML, exp.Drop, exp.Alter, exp.Copy))
            else None
        )
        target = target.this if isinstance(target, exp.Schema) else target
        if isinstance(target, exp.Table):
            written = table_name((part.name for part in target.parts), catalog)
            targets.add(written)
            try:
                edges.extend(_script_columns(tree.expression, written, name, config))
            except EtlCraftError:
                unparsed.append(str(index))
        elif isinstance(tree, (exp.TruncateTable, exp.Drop)):
            for table in tree.expressions or tree.args.get("tables") or []:
                if isinstance(table, exp.Table):
                    targets.add(table_name((part.name for part in table.parts), catalog))
        elif isinstance(tree, exp.Command):
            unparsed.append(str(index))
        ctes = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
        for table in tree.find_all(exp.Table):
            if table is not target and (table.db or table.name.lower() not in ctes):
                sources.add(table_name((part.name for part in table.parts), catalog))
    reason = "unmanaged"
    if unparsed:
        reason += "; incomplete lineage for statement(s) " + ", ".join(unparsed)
    ordered = tuple(sorted(targets))
    return TaskLineage(
        name,
        ordered[0] if ordered else None,
        edges,
        error=reason,
        sources=tuple(sorted(sources)),
        targets=ordered,
    )


def _script_lineages(engine: Engine, config: ConnectorConfig) -> list[TaskLineage]:
    with engine.connect() as conn:
        tasks = fetch_active_tasks(conn)
        params = fetch_all_task_parameters(conn, Handler.SQL_SCRIPT)
    result = []
    for task in tasks:
        if task.handler != Handler.SQL_SCRIPT:
            continue
        try:
            name = params.get(task.task_id, {}).get("SCRIPT_FILE", "")
            sql = sql_script(config, name, setting="SCRIPT_FILE").read_text(encoding="utf-8")
            result.append(script_lineage(sql, task.label, config))
        except (EtlCraftError, OSError, UnicodeError) as error:
            result.append(TaskLineage(task.label, None, [], error=f"unmanaged: {error}"))
    return result


def _script_columns(
    query: exp.Expr | None, target: str, name: str, config: ConnectorConfig
) -> list[Edge]:
    """Trace the SELECT portion of an unmanaged write where it is understood."""
    if not isinstance(query, exp.Query):
        return []
    dialect, catalog = dialect_and_catalog(config)
    return [
        Edge(
            e.target_object,
            e.target_column,
            e.source_object,
            e.source_column,
            e.transformation,
            name,
        )
        for e in extract(query.sql(dialect=dialect), target, dialect=dialect, catalog=catalog)
    ]
