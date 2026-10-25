"""Bounded, parameterized inserts shared by full mirrors and immutable archives."""

from sqlalchemy import TextClause, text

MAX_BOUND_VALUES = 250


def insert_rows(
    table: str,
    names: list[str],
    rows: list[tuple[object, ...]],
) -> tuple[TextClause, dict[str, object]]:
    """Bind every cell while keeping SQL identifiers separate from data."""
    groups = ", ".join(
        "(" + ", ".join(f":r{r}c{c}" for c in range(len(names))) + ")" for r in range(len(rows))
    )
    params = {f"r{r}c{c}": value for r, row in enumerate(rows) for c, value in enumerate(row)}
    return text(f"INSERT INTO {table} ({', '.join(names)}) VALUES {groups}"), params
