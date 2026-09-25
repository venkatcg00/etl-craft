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
dialect, and worked out again only when one of them changes. SQL that cannot be traced, such as
``SELECT *`` over a table whose columns are not known, is reported with the reason.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

import sqlglot
from sqlalchemy.engine import Engine
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.lineage import lineage as sqlglot_lineage

from etl_craft.config import ConnectorConfig
from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import RefreshType, SqlAction
from etl_craft.core.errors import EtlCraftError, MetadataError
from etl_craft.core.text import sha256_hex
from etl_craft.engine.repository.lineage import (
    StoredEdge,
    fetch_sql_tasks,
    fetch_task_lineage,
    store_task_lineage,
)
from etl_craft.engine.repository.tasks import fetch_task_parameters
from etl_craft.handlers.sql.spec import resolve_select
from etl_craft.warehouse.connection import warehouse_dialect

COPY = "copy"
"""The transformation of a column copied from its source as it is."""

SQLGLOT_DIALECTS = {
    "postgres": "postgres",
    "duckdb": "duckdb",
    "duckdb_iceberg": "duckdb",
    "trino_iceberg": "trino",
    "databricks": "databricks",
    "databricks_iceberg": "databricks",
    "snowflake": "snowflake",
    "snowflake_iceberg": "snowflake",
}
"""The sqlglot dialect each warehouse dialect's SQL is parsed in."""


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


def table_name(parts: Iterable[str], catalog: str | None) -> str:
    """Return a table's name as lineage compares it: lower case, without the active database."""
    names = [part.lower() for part in parts if part]
    if len(names) == 3 and catalog and names[0] == catalog.lower():
        names = names[1:]
    return ".".join(names)


def extract(
    select_sql: str, target_object: str, *, dialect: str | None, catalog: str | None = None
) -> list[Edge]:
    """Trace each column of ``select_sql`` to its sources; ``MetadataError`` when it cannot."""
    try:
        tree = sqlglot.parse_one(select_sql, read=dialect)
    except SqlglotError as error:
        raise MetadataError(f"the SELECT could not be parsed: {error}") from error
    if not isinstance(tree, exp.Query):
        raise MetadataError("the statement is not a SELECT")
    target = table_name(target_object.split("."), catalog)
    edges: list[Edge] = []
    for projection in tree.selects:
        column = projection.alias_or_name
        if not column or isinstance(projection, exp.Star) or column == "*":
            raise MetadataError(
                "SELECT * cannot be traced without knowing the table's columns; list them"
            )
        inner = projection.unalias() if isinstance(projection, exp.Alias) else projection
        transformation = COPY if isinstance(inner, exp.Column) else inner.sql(dialect=dialect)
        try:
            node = sqlglot_lineage(column, tree, dialect=dialect)
        except (SqlglotError, KeyError, ValueError) as error:
            raise MetadataError(f"column {column!r} could not be traced: {error}") from error
        sources = sorted(
            {
                (
                    table_name((leaf.source.catalog, leaf.source.db, leaf.source.name), catalog),
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


def lineage_key(select_sql: str, target_object: str, dialect: str | None) -> str:
    """Hash what a task's lineage is worked out from; any change means working it out again."""
    return sha256_hex("\0".join((select_sql, target_object, dialect or "")).encode())


def collect(
    engine: Engine, config: ConnectorConfig, *, refresh: bool = False, record: bool = True
) -> list[TaskLineage]:
    """Return the lineage of every active SQL task, working out and storing what changed.

    ``refresh`` works everything out again. Without ``record`` nothing is stored.
    """
    dialect, catalog = _dialect_and_catalog(config)
    with engine.connect() as conn:
        tasks = [(ref, fetch_task_parameters(conn, ref.task_id)) for ref in fetch_sql_tasks(conn)]
    results: list[TaskLineage] = []
    for ref, params in tasks:
        name = f"{ref.pipeline_code}.{ref.task_code}"
        if (params.get("SQL_ACTION") or "").upper() == SqlAction.DROP_TABLE:
            continue
        target = (params.get("TARGET_OBJECT") or "").strip()
        sources: tuple[str, ...] = ()
        try:
            select_sql, _ = resolve_select(
                config, params, pipeline_run_id=0, refresh_type=RefreshType.FULL
            )
            sources = referenced_tables(select_sql, dialect=dialect, catalog=catalog)
            if not target:
                raise MetadataError("the task has no TARGET_OBJECT")
            key = lineage_key(select_sql, target, dialect)
            with engine.connect() as conn:
                stored = [] if refresh else fetch_task_lineage(conn, ref.task_id, key)
            if stored:
                edges = [_edge(e, name) for e in stored]
                results.append(TaskLineage(name, target, edges, cached=True, sources=sources))
                continue
            edges = [
                Edge(
                    e.target_object,
                    e.target_column,
                    e.source_object,
                    e.source_column,
                    e.transformation,
                    name,
                )
                for e in extract(select_sql, target, dialect=dialect, catalog=catalog)
            ]
        except EtlCraftError as error:
            results.append(TaskLineage(name, target or None, [], error=str(error), sources=sources))
            continue
        if record:
            with engine.begin() as conn:
                store_task_lineage(conn, ref.task_id, key, [_stored(e) for e in edges])
        results.append(TaskLineage(name, target, edges, sources=sources))
    return results


def _dialect_and_catalog(config: ConnectorConfig) -> tuple[str | None, str | None]:
    if config.warehouse is None:
        return None, None
    return SQLGLOT_DIALECTS.get(warehouse_dialect(config).key), active_catalog(config)


def _edge(stored: StoredEdge, task: str) -> Edge:
    return Edge(
        stored.target_object,
        stored.target_column,
        stored.source_object,
        stored.source_column,
        stored.transformation,
        task,
    )


def _stored(edge: Edge) -> StoredEdge:
    return StoredEdge(
        edge.target_object,
        edge.target_column,
        edge.source_object,
        edge.source_column,
        edge.transformation,
    )


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
