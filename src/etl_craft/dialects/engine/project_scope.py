"""Which tables a project migration statement writes or changes, read with sqlglot.

A project migration may write its own project's rows in the package's project config tables,
and may write and change only the team tables its project owns and its own temporary tables.
A statement sqlglot can't classify is refused rather than guessed at.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from etl_craft.core.errors import MigrationError
from etl_craft.dialects.engine.guards import PACKAGE_TABLES, PROJECT_CONFIG_TABLES

SQLGLOT_DIALECTS = {"sqlite": "sqlite", "postgresql": "postgres"}
CREATED = frozenset({"TABLE", "VIEW"})
DML = (exp.Insert, exp.Update, exp.Delete, exp.Merge)
RESERVED = frozenset({"SCHEMA_MIGRATIONS", "ETL_CRAFT_PROJECT"})


@dataclass(frozen=True)
class Touch:
    """One table a statement writes (``ddl`` False) or creates, changes or drops."""

    table: str
    ddl: bool
    temporary: bool = False
    creates: bool = False
    index: bool = False


def _name(node: exp.Expr | None) -> tuple[str, bool]:
    """Return a table's upper-case name and whether it is qualified as temporary."""
    table = node.this if isinstance(node, exp.Schema) else node
    if not isinstance(table, exp.Table):
        raise MigrationError(f"no table in {node.sql() if node is not None else 'the statement'}")
    return table.name.upper(), table.db.lower() in {"temp", "pg_temp"}


def _create(tree: exp.Create) -> list[Touch]:
    kind = str(tree.args.get("kind") or "").upper()
    temporary = tree.find(exp.TemporaryProperty) is not None
    if kind in CREATED:
        name, qualified = _name(tree.this)
        return [Touch(name, True, temporary or qualified, creates=True)]
    if kind == "INDEX" and isinstance(tree.this, exp.Index):
        name, qualified = _name(tree.this.args.get("table"))
        return [Touch(name, True, qualified)]
    raise MigrationError(f"CREATE {kind or 'statement'}")


def _alter(tree: exp.Alter) -> list[Touch]:
    if str(tree.args.get("kind") or "").upper() != "TABLE":
        raise MigrationError(f"ALTER {tree.args.get('kind')}")
    name, temporary = _name(tree.this)
    touches = [Touch(name, True, temporary)]
    for action in tree.args.get("actions") or []:
        if isinstance(action, exp.AlterRename):
            renamed, _ = _name(action.this)
            touches.append(Touch(renamed, True, temporary, creates=True))
    return touches


def _drop(tree: exp.Drop) -> list[Touch]:
    kind = str(tree.args.get("kind") or "").upper()
    if kind not in CREATED | {"INDEX"}:
        raise MigrationError(f"DROP {kind or 'statement'}")
    touches = []
    for table in tree.args.get("tables") or []:
        name, temporary = _name(table)
        touches.append(Touch(name, True, temporary, index=kind == "INDEX"))
    return touches


def statement_touches(sql: str, dialect_name: str) -> list[Touch]:
    """Return every table ``sql`` writes or changes; raise ``MigrationError`` when unsure."""
    try:
        trees = sqlglot.parse(sql, read=SQLGLOT_DIALECTS.get(dialect_name, dialect_name))
    except SqlglotError as error:
        raise MigrationError(f"sqlglot could not parse it ({error})") from error
    if len(trees) != 1 or trees[0] is None:
        raise MigrationError("expected exactly one statement")
    tree = trees[0]
    if isinstance(tree, exp.Create):
        touches = _create(tree)
    elif isinstance(tree, exp.Alter):
        touches = _alter(tree)
    elif isinstance(tree, exp.Drop):
        touches = _drop(tree)
    elif isinstance(tree, exp.TruncateTable):
        touches = [Touch(*_name(table)[:1], True, _name(table)[1]) for table in tree.expressions]
    elif isinstance(tree, (exp.Query, *DML)):
        touches = []
    else:
        raise MigrationError(f"{type(tree).__name__} statements")
    for node in tree.find_all(*DML):
        name, temporary = _name(node.this)
        touches.append(Touch(name, False, temporary))
    return touches


def package_table(name: str) -> bool:
    """Whether ``name`` is an etl-craft table or name a project migration may never change."""
    return name in PACKAGE_TABLES | RESERVED or name.startswith("AUD_")


def project_rows(touch: Touch) -> bool:
    """Whether a write lands in project config rows, which the Engine DB scopes per row."""
    return not touch.ddl and touch.table in PROJECT_CONFIG_TABLES
