"""Whether SQL is one read-only query, read with sqlglot in the warehouse's dialect.

The parse tree tells a column named ``copy`` from a ``COPY`` statement, and finds a write
inside a ``WITH``. When sqlglot cannot parse the SQL, the word-level check in ``core.text``
decides instead, since the warehouse may accept syntax sqlglot does not know; ``validate``
reports the parse error as a warning, with its line and column.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp, parse
from sqlglot.errors import ParseError, SqlglotError

from etl_craft.core import text

WRITES: tuple[type[exp.Expression], ...] = (
    exp.Insert,
    exp.Into,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.TruncateTable,
    exp.Copy,
    exp.Grant,
    exp.Revoke,
    exp.Command,
    exp.Set,
    exp.Use,
    exp.Transaction,
    exp.Commit,
    exp.Rollback,
    exp.Cache,
    exp.Uncache,
    exp.Refresh,
    exp.LoadData,
    exp.Analyze,
    exp.Pragma,
    exp.Kill,
)
"""Statements that write, or change the session, anywhere in a read-only query's tree."""


@dataclass(frozen=True)
class Reading:
    """What sqlglot made of some SQL: why it is not one read-only query, or why it did not parse.

    Both are ``None`` for one read-only query.
    """

    problem: str | None = None
    unparsed: str | None = None
    root: exp.Expr | None = field(default=None, compare=False, repr=False)
    """The parsed query, for callers that inspect it further; not part of equality."""


def _statement(node: exp.Expr) -> str:
    if isinstance(node, exp.Command):
        return str(node.this).upper()
    return node.key.upper()


def read_query(sql: str, dialect: str | None) -> Reading:
    """Read ``sql`` in sqlglot ``dialect`` (its own default when ``None``)."""
    try:
        statements = [statement for statement in parse(sql, read=dialect) if statement]
    except ParseError as error:
        first = error.errors[0] if error.errors else {}
        where = f"line {first['line']}, column {first['col']}: " if first.get("line") else ""
        return Reading(unparsed=f"{where}{first.get('description') or error}")
    except SqlglotError as error:
        return Reading(unparsed=str(error))
    if len(statements) != 1:
        return Reading(problem=f"holds {len(statements)} statements")
    root = statements[0]
    if not isinstance(root, (exp.Query, exp.Values)):
        return Reading(problem=f"is not a query but {_statement(root)}")
    writes = sorted({_statement(node) for node in root.walk() if isinstance(node, WRITES)})
    if writes:
        return Reading(problem=f"contains {', '.join(writes)}, which a read-only query cannot")
    return Reading(root=root)


def event_query_problem(sql: str, dialect: str | None, key_column: str | None) -> str | None:
    """Refuse unconditional aggregates and an explicitly absent signal key projection."""
    reading = read_query(sql, dialect)
    root = reading.root.unnest() if reading.root is not None else None
    if (
        isinstance(root, exp.Select)
        and not root.args.get("group")
        and not root.args.get("having")
        and any(
            agg.find_ancestor(exp.Select, exp.Window) is root for agg in root.find_all(exp.AggFunc)
        )
    ):
        return (
            "has a top-level aggregate without GROUP BY or HAVING and always returns a row; "
            "use SELECT 1 AS ready ... WHERE ..., or add HAVING count(*) > 0"
        )
    columns = output_columns(sql, dialect=dialect)
    if (
        key_column
        and columns is not None
        and key_column.lower() not in {name.lower() for name in columns}
    ):
        return (
            f"does not return EVENT_KEY_COLUMN={key_column!r} (returns {', '.join(columns)}); "
            "select that column in EVENT_SQL or correct EVENT_KEY_COLUMN"
        )
    return None


def read_only_problem(sql: str, dialect: str | None) -> str | None:
    """Return why ``sql`` is not one read-only query, or ``None`` when it is.

    The parse tree decides; SQL sqlglot cannot parse falls back to ``core.text``'s check.
    """
    reading = read_query(sql, dialect)
    if reading.unparsed is not None:
        return text.read_only_problem(sql)
    return reading.problem


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
