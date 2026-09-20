"""Column-level lineage, parsed from each SQL task's own SOURCE_SQL.

[DEVIATION, 2026-09-20] CLAUDE.md's Non-goals ruled out a SQL parser
dependency, and E2-07/E2-25 deliberately settled for lint-grade string checks
instead. Reversed by explicit instruction: "sql parser to implement column
level lineage. dont re-invent the wheel use metadata and any existing package
that can do this", and on how it should ship, "if it is easy implement our own,
else go with hard dependency, our product should ship with it".

Implementing our own is not easy. Resolving which upstream column feeds a
target column means handling aliasing, CTEs, subqueries, joins, set
operations, star expansion and per-dialect quoting — sqlglot is tens of
thousands of lines across twenty-odd dialects, and a hand-rolled parser would
be subtly wrong in exactly the cases lineage is consulted for. So sqlglot is a
hard dependency. It has no dependencies of its own, so this adds one package
and nothing transitive.

[CHOICE] Both computed and cached, per explicit instruction. The cache key is
an MD5 of the SOURCE_SQL the lineage was derived from, not a timestamp: a
cached row is reused only when it came from byte-for-byte the SQL that is in
CFG_TASK_PARAMETERS right now, so editing SOURCE_SQL invalidates it with
nothing to remember. Same reasoning as HASH_KEY for SCD change detection.

[CHOICE] Parse failures are reported, never raised. A task whose SOURCE_SQL
sqlglot cannot parse (an unusual dialect extension, say) should cost you that
one task's column lineage, not the whole `lineage` command or a whole
documentation build — everything else still has perfectly good lineage.
"""

from __future__ import annotations

import contextlib
import hashlib
from dataclasses import dataclass

import sqlglot
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.qualify import qualify

# sqlglot's own dialect name for the warehouse we parse against. Kept
# deliberately loose: SOURCE_SQL is author-written ANSI-ish SQL, and parsing it
# as generic SQL is more forgiving than guessing a dialect wrong.
DEFAULT_DIALECT: str | None = None


@dataclass(frozen=True)
class ColumnEdge:
    """One target column and the upstream column (if any) that feeds it."""

    target_object: str
    target_column: str
    source_object: str | None
    source_column: str | None
    # The expression, when the column is not a plain pass-through. `None` for
    # `SELECT a` and `SELECT a AS b`; set for `SELECT UPPER(a) AS b`.
    transformation: str | None


@dataclass(frozen=True)
class LineageResult:
    """Everything parsing one task's SOURCE_SQL produced, including a failure."""

    edges: list[ColumnEdge]
    error: str | None = None

    @property
    def ok(self) -> bool:
        """Report whether the SQL parsed."""
        return self.error is None


def source_sql_hash(source_sql: str) -> str:
    """Hash a SOURCE_SQL string for cache-key purposes."""
    return hashlib.md5(source_sql.encode(), usedforsecurity=False).hexdigest()


def _table_name(table: exp.Table | None) -> str | None:
    if table is None:
        return None
    parts = [table.text("db"), table.text("this")]
    return ".".join(p for p in parts if p) or None


def _resolve_column_source(
    expression: exp.Expr, scope_tables: dict[str, str]
) -> tuple[str | None, str | None]:
    """Find the single upstream column an expression reads, if it reads exactly one."""
    columns = list(expression.find_all(exp.Column))
    if len(columns) != 1:
        # Zero (a literal or constant) or several (a CASE over two columns,
        # a concatenation). Neither maps to one upstream column, and inventing
        # one would be worse than saying so — the transformation text is kept
        # instead, which is the honest answer.
        return None, None
    column = columns[0]
    qualifier = column.text("table")
    return scope_tables.get(qualifier, qualifier or None), column.text("this")


def extract_column_lineage(
    source_sql: str, target_object: str, *, dialect: str | None = DEFAULT_DIALECT
) -> LineageResult:
    """Parse `source_sql` and map each projected column back to what feeds it."""
    try:
        statement = sqlglot.parse_one(source_sql, read=dialect)
        if statement is None:
            return LineageResult(edges=[], error="SOURCE_SQL parsed to nothing")
        # qualify() resolves aliases and star expansions where it can. It
        # needs no schema to do the alias half, which is the part that matters
        # for attributing a column to the right table.
        # Qualification is best-effort: an unqualified column in a multi-table
        # join simply stays unattributed below rather than failing the parse.
        with contextlib.suppress(SqlglotError):
            statement = qualify(statement, dialect=dialect, validate_qualify_columns=False)
        select = statement.find(exp.Select)
        if select is None:
            return LineageResult(edges=[], error="SOURCE_SQL is not a SELECT")
    except SqlglotError as exc:
        return LineageResult(edges=[], error=f"could not parse SOURCE_SQL: {exc}")

    cte_sources = _cte_source_map(statement)
    scope_tables = _scope_table_map(select, cte_sources)
    distinct_sources = set(scope_tables.values())
    sole_table = next(iter(distinct_sources)) if len(distinct_sources) == 1 else None

    edges: list[ColumnEdge] = []
    for projection in select.expressions:
        target_column = projection.alias_or_name
        if not target_column or target_column == "*":
            # An unexpanded star: sqlglot could not resolve it without a
            # schema, and inventing column names would be fabrication.
            continue
        inner = projection.unalias() if isinstance(projection, exp.Alias) else projection
        source_object, source_column = _resolve_column_source(inner, scope_tables)
        if source_object is None and source_column is not None:
            # An unqualified column in a single-table query: unambiguous.
            source_object = sole_table
        edges.append(
            ColumnEdge(
                target_object=target_object,
                target_column=target_column,
                source_object=source_object,
                source_column=source_column,
                transformation=(
                    None if isinstance(inner, exp.Column) else inner.sql(dialect=dialect)
                ),
            )
        )
    return LineageResult(edges=edges)


def _cte_source_map(statement: exp.Expr) -> dict[str, str]:
    """Map each CTE name to the single real table it reads, where there is one.

    Lineage that stops at a CTE name is much less useful than lineage that
    reaches the real table, and CTEs are everywhere in ETL SQL. Resolved to a
    fixpoint so a CTE reading another CTE still lands on a real table.

    Only a CTE with exactly one distinct source is resolved. One reading three
    tables has no single answer, and naming one of them would be a guess.
    """
    direct: dict[str, set[str]] = {}
    for cte in statement.find_all(exp.CTE):
        names = {
            name
            for table in cte.this.find_all(exp.Table)
            if (name := _table_name(table)) is not None
        }
        direct[cte.alias_or_name] = names

    resolved: dict[str, str] = {}
    for _ in range(len(direct) + 1):
        changed = False
        for cte_name, sources in direct.items():
            if cte_name in resolved:
                continue
            mapped = {resolved.get(s, s) for s in sources if s != cte_name}
            # Still pointing at an unresolved CTE: try again next pass.
            if any(s in direct and s not in resolved for s in sources):
                continue
            if len(mapped) == 1:
                resolved[cte_name] = next(iter(mapped))
                changed = True
        if not changed:
            break
    return resolved


def _scope_table_map(select: exp.Select, cte_sources: dict[str, str]) -> dict[str, str]:
    """Map every alias and table name in scope to the real object it refers to."""
    scope: dict[str, str] = {}
    for table in select.find_all(exp.Table):
        name = _table_name(table)
        if name is None:
            continue
        real = cte_sources.get(name, name)
        scope[table.alias_or_name] = real
        scope.setdefault(table.text("this"), real)
    return scope


def cached_lineage(conn: Connection, task_id: int, sql_hash: str) -> list[ColumnEdge] | None:
    """Return this task's cached lineage if it was derived from this exact SQL."""
    rows = conn.execute(
        text(
            "SELECT TARGET_OBJECT AS target_object, TARGET_COLUMN AS target_column, "
            "SOURCE_OBJECT AS source_object, SOURCE_COLUMN AS source_column, "
            "TRANSFORMATION AS transformation FROM AUD_COLUMN_LINEAGE "
            "WHERE TASK_ID = :task_id AND SOURCE_SQL_HASH = :sql_hash "
            "ORDER BY TARGET_COLUMN, SOURCE_OBJECT, SOURCE_COLUMN"
        ),
        {"task_id": task_id, "sql_hash": sql_hash},
    ).all()
    if not rows:
        return None
    return [
        ColumnEdge(
            target_object=row.target_object,
            target_column=row.target_column,
            source_object=row.source_object,
            source_column=row.source_column,
            transformation=row.transformation,
        )
        for row in rows
    ]


def store_lineage(conn: Connection, task_id: int, sql_hash: str, edges: list[ColumnEdge]) -> None:
    """Replace this task's cached lineage with `edges`, derived from `sql_hash`."""
    # Every row for the task goes, not just this hash's: a stale hash's rows
    # would never be read again and would grow without bound.
    conn.execute(
        text("DELETE FROM AUD_COLUMN_LINEAGE WHERE TASK_ID = :task_id"), {"task_id": task_id}
    )
    for edge in edges:
        conn.execute(
            text(
                "INSERT INTO AUD_COLUMN_LINEAGE (TASK_ID, SOURCE_SQL_HASH, TARGET_OBJECT, "
                "TARGET_COLUMN, SOURCE_OBJECT, SOURCE_COLUMN, TRANSFORMATION) "
                "VALUES (:task_id, :sql_hash, :target_object, :target_column, "
                ":source_object, :source_column, :transformation)"
            ),
            {
                "task_id": task_id,
                "sql_hash": sql_hash,
                "target_object": edge.target_object,
                "target_column": edge.target_column,
                "source_object": edge.source_object,
                "source_column": edge.source_column,
                "transformation": edge.transformation,
            },
        )


@dataclass(frozen=True)
class TaskLineage:
    """One task's column lineage, and whether it came from the cache."""

    pipeline_code: str
    task_code: str
    task_id: int
    edges: list[ColumnEdge]
    cached: bool
    error: str | None = None


def lineage_for_tasks(conn: Connection, *, refresh: bool = False) -> list[TaskLineage]:
    """Resolve column lineage for every active SQL task, using the cache where valid.

    Per explicit instruction lineage is both computed and cached. A cached row
    is reused only when its SOURCE_SQL_HASH matches the SQL currently in
    CFG_TASK_PARAMETERS, so an edit invalidates it with nothing to remember.
    `refresh=True` re-parses regardless, for when sqlglot itself has been
    upgraded and might resolve something it previously could not.
    """
    rows = conn.execute(
        text(
            "SELECT t.TASK_ID AS task_id, p.PIPELINE_CODE AS pipeline_code, "
            "t.TASK_CODE AS task_code, "
            "MAX(CASE WHEN par.PARAMETER_NAME = 'SOURCE_SQL' THEN par.PARAMETER_VALUE END) "
            "AS source_sql, "
            "MAX(CASE WHEN par.PARAMETER_NAME = 'TARGET_OBJECT' THEN par.PARAMETER_VALUE END) "
            "AS target_object "
            "FROM CFG_TASKS t "
            "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID "
            "JOIN CFG_TASK_PARAMETERS par ON par.TASK_ID = t.TASK_ID AND par.ACTIVE_FLAG = 'Y' "
            "WHERE t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y' AND t.HANDLER = 'SQL' "
            "GROUP BY t.TASK_ID, p.PIPELINE_CODE, t.TASK_CODE "
            "ORDER BY p.PIPELINE_CODE, t.TASK_CODE"
        )
    ).all()

    results: list[TaskLineage] = []
    for row in rows:
        if not row.source_sql or not row.target_object:
            continue
        sql_hash = source_sql_hash(row.source_sql)
        if not refresh:
            cached = cached_lineage(conn, row.task_id, sql_hash)
            if cached is not None:
                results.append(
                    TaskLineage(
                        pipeline_code=row.pipeline_code,
                        task_code=row.task_code,
                        task_id=row.task_id,
                        edges=cached,
                        cached=True,
                    )
                )
                continue
        parsed = extract_column_lineage(row.source_sql, row.target_object)
        if parsed.ok:
            store_lineage(conn, row.task_id, sql_hash, parsed.edges)
        results.append(
            TaskLineage(
                pipeline_code=row.pipeline_code,
                task_code=row.task_code,
                task_id=row.task_id,
                edges=parsed.edges,
                cached=False,
                error=parsed.error,
            )
        )
    return results


def column_lineage_for(
    conn: Connection, column_ref: str, *, refresh: bool = False
) -> tuple[list[TaskLineage], list[TaskLineage]]:
    """Find where `schema.table.column` comes from, and what it feeds.

    Returns (produced_by, feeds) — the tasks writing that column, and the
    tasks reading it. Each TaskLineage is narrowed to only the matching edges.
    """
    parts = column_ref.rsplit(".", 1)
    if len(parts) != 2:
        raise ValueError(
            f"{column_ref!r} must be 'schema.table.column' — a table reference plus one column"
        )
    table_ref, column_name = parts
    wanted_table = table_ref.lower()
    wanted_column = column_name.lower()

    produced_by: list[TaskLineage] = []
    feeds: list[TaskLineage] = []
    for task in lineage_for_tasks(conn, refresh=refresh):
        writes = [
            e
            for e in task.edges
            if e.target_object.lower() == wanted_table and e.target_column.lower() == wanted_column
        ]
        reads = [
            e
            for e in task.edges
            if (e.source_object or "").lower() == wanted_table
            and (e.source_column or "").lower() == wanted_column
        ]
        if writes:
            produced_by.append(_narrowed(task, writes))
        if reads:
            feeds.append(_narrowed(task, reads))
    return produced_by, feeds


def _narrowed(task: TaskLineage, edges: list[ColumnEdge]) -> TaskLineage:
    return TaskLineage(
        pipeline_code=task.pipeline_code,
        task_code=task.task_code,
        task_id=task.task_id,
        edges=edges,
        cached=task.cached,
        error=task.error,
    )
