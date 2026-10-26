"""Business-rule query contracts shared by validation, execution and upgrades."""

import re

from sqlglot import Dialect, exp
from sqlglot.optimizer.scope import build_scope

from etl_craft.core.errors import SqlGuardError
from etl_craft.core.text import TOKEN, as_subquery, is_safe_identifier, split_statements, sql_parts
from etl_craft.dialects.query_analysis import output_columns, read_only_problem, read_query

ROWS_IN_SCOPE = "$$rows_in_scope"
SCOPE_TABLE = "etl_craft_rows_in_scope"


def expand_scope(sql: str, replacement: str = SCOPE_TABLE) -> str:
    """Expand only the scope token outside strings, comments and quoted identifiers."""
    return "".join(
        part
        if protected
        else TOKEN.sub(
            lambda match: replacement if match.group(0) == ROWS_IN_SCOPE else match.group(0), part
        )
        for part, protected in sql_parts(sql, placeholders=True)
    )


def rule_namespace(sql: str) -> str:
    """Keep generated CTE names from shadowing tables or CTEs in the team's query."""
    used = set(re.findall(r"etl_craft_(\d+)_", sql, flags=re.IGNORECASE))
    number = 0
    while str(number) in used:
        number += 1
    return f"etl_craft_{number}_"


def scope_cte(table: str, scope: str, name: str, dialect: str | None) -> str:
    """Name engine-selected target rows for a rule or its zero-row output probe."""
    return f"{name} AS (SELECT t.* FROM {table_identifier(table, dialect)} t WHERE {scope})"


def key_identifier(key: str, dialect: str | None) -> str:
    """Quote a metadata identifier using its warehouse's unquoted-name normalization."""
    if not is_safe_identifier(key):
        raise SqlGuardError(
            f"BUSINESS_RULE_KEY_COLUMN={key!r}: expected a plain column name; correct the rule"
        )
    language = Dialect.get_or_raise(dialect)
    identifier = language.normalize_identifier(exp.to_identifier(key))
    identifier.set("quoted", True)
    return identifier.sql(dialect=dialect)


def table_identifier(table: str, dialect: str | None) -> str:
    """Quote the checked components of an engine-resolved target identity."""
    return ".".join(key_identifier(part, dialect) for part in table.split("."))


def converted_rule(sql: str, key: str, dialect: str | None) -> str:
    """Convert a legacy predicate without rewriting its read-only SQL body."""
    statements = split_statements(sql)
    problem = (
        f"holds {len(statements)} statements"
        if len(statements) != 1
        else read_only_problem(statements[0], dialect)
    )
    if problem:
        raise SqlGuardError(f"BUSINESS_RULE_SQL: expected one read-only SELECT, found {problem}")
    return (
        f"SELECT t.{key_identifier(key, dialect)} FROM {ROWS_IN_SCOPE} t "
        f"WHERE EXISTS {as_subquery(sql)}"
    )


def quote_output_key(name: str, dialect: str | None) -> str:
    """Quote an actual SELECT output label without changing its case."""
    return exp.to_identifier(name, quoted=True).sql(dialect=dialect)


def output_key(sql: str, key: str, dialect: str | None) -> str | None:
    """Quote the native output label, preserving explicitly quoted alias case."""
    statements = split_statements(expand_scope(sql))
    root = read_query(statements[0], dialect).root if len(statements) == 1 else None
    if not isinstance(root, exp.Query):
        return None
    for projection in root.selects:
        if projection.alias_or_name.lower() != key.lower():
            continue
        identifier = projection.args.get("alias")
        if identifier is None and isinstance(projection, exp.Column):
            identifier = projection.this
        if isinstance(identifier, exp.Identifier):
            identifier = Dialect.get_or_raise(dialect).normalize_identifier(identifier.copy())
            return quote_output_key(identifier.name, dialect)
    return None


def rule_problem(sql: str, key: str, dialect: str | None) -> str | None:
    """Check a complete rule, including unresolved legacy outer-row references."""
    statements = split_statements(expand_scope(sql))
    if len(statements) != 1:
        return (
            f"must be one read-only SELECT; it holds {len(statements)} statements; rewrite the rule"
        )
    expanded = statements[0]
    if problem := read_only_problem(expanded, dialect):
        return f"must be a read-only SELECT; it {problem}; rewrite the rule"
    reading = read_query(expanded, dialect)
    root = reading.root
    if root is not None and not isinstance(root, exp.Query):
        return f"is {root.key.upper()}; expected a read-only SELECT; select target row keys instead"
    scope = build_scope(root) if root is not None else None
    if scope is not None and any(column.table.lower() == "t" for column in scope.external_columns):
        return (
            "uses the old correlated fragment form; replace BUSINESS_RULE_SQL with: "
            + converted_rule(sql, key, dialect)
        )
    columns = output_columns(expanded, dialect=dialect)
    if columns is not None and key.lower() not in {name.lower() for name in columns}:
        return (
            f"returns {columns!r}; expected BUSINESS_RULE_KEY_COLUMN={key!r}; "
            "select that target row key in BUSINESS_RULE_SQL"
        )
    return None
