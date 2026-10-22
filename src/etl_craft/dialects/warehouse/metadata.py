"""DDL invalidates task metadata, including statements issued directly by a dialect."""

from collections.abc import Callable
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlglot import exp, parse_one
from sqlglot.errors import SqlglotError

from etl_craft.core.enums import TableFormat

ColumnMetadata = tuple[str, str, int | None, int | None, str]


@dataclass(frozen=True)
class TableMetadata:
    """Complete column types, with generator and storage evidence available in the same read."""

    columns: tuple[ColumnMetadata, ...]
    generated: bool | None = None
    table_format: TableFormat | None = None


def projection_names(sql: str, dialect: str | None, case: str | None) -> list[str] | None:
    """Return explicit projection identifiers; stars and driver-named expressions need a probe."""
    try:
        root = parse_one(sql, read=dialect)
    except SqlglotError:
        return None
    while isinstance(root, (exp.Union, exp.Intersect, exp.Except, exp.Subquery)):
        root = root.this
    if not isinstance(root, exp.Select):
        return None
    names = []
    fold = {"lower": str.lower, "upper": str.upper}.get(case or "", str)
    for selected in root.expressions:
        identifier = (
            selected.args.get("alias") if isinstance(selected, exp.Alias) else selected.this
        )
        if not isinstance(identifier, exp.Identifier) or selected.is_star or identifier.quoted:
            return None
        names.append(identifier.name if identifier.quoted else fold(identifier.name))
    return names if len({name.lower() for name in names}) == len(names) else None


def ddl_tables(sql: str, dialect: str | None) -> set[str] | None:
    """Return changed table names; unknown DDL invalidates every cached shape."""
    verb = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
    if verb.startswith(("--", "/*")):
        return None
    if verb not in {"CREATE", "ALTER", "DROP", "RENAME", "COMMENT"}:
        return set()
    try:
        root = parse_one(sql, read=dialect)
    except SqlglotError:
        return None
    if not isinstance(root, (exp.Create, exp.Alter, exp.Drop)):
        return None
    if isinstance(root, (exp.Create, exp.Drop)) and root.args.get("kind") == "INDEX":
        return set()
    if not isinstance(root.this, exp.Table):
        return None
    names = {root.this.name.lower()}
    for rename in root.find_all(exp.AlterRename):
        if isinstance(rename.this, exp.Table):
            names.add(rename.this.name.lower())
    return names


def describe_result(
    conn: Connection, sql: str, kind: Callable[[object], str | None]
) -> list[tuple[str, str]] | None:
    """Read exact driver types; unknown types require materialized catalog metadata."""
    with conn.execute(text(sql)) as result:
        columns = result.cursor.description
        types = [kind(column) for column in columns]
        if any(value is None for value in types):
            return None
        return [
            (str(column[0]), value)
            for column, value in zip(columns, types, strict=True)
            if value is not None
        ]


def declarable_source(sql: str, dialect: str | None) -> bool:
    """Protocol types omit collations inherited from source columns or explicit COLLATE."""
    try:
        root = parse_one(sql, read=dialect)
    except SqlglotError:
        return False
    return root.find(exp.Table, exp.Column, exp.Collate) is None
