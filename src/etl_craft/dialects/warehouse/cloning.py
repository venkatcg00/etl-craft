"""Portable idempotent mirror publication using engine-built primary-key predicates."""

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.dialects.cloning import key_predicate
from etl_craft.dialects.warehouse.copying import MAX_BOUND_VALUES, insert_rows


def replace_keys(
    conn: Connection, mirror: str, keys: list[str], removed: list[list[object]]
) -> None:
    """Delete exactly the changed keys before reinsertion, including purge tombstones."""
    where, params = key_predicate(keys, removed)
    conn.execute(text(f"DELETE FROM {mirror} WHERE {where}"), params)


def write_rows(
    conn: Connection, mirror: str, names: list[str], rows: list[tuple[object, ...]]
) -> None:
    """Keep mirror inserts below the existing warehouse bind-value budget."""
    size = max(1, MAX_BOUND_VALUES // len(names))
    for start in range(0, len(rows), size):
        conn.execute(*insert_rows(mirror, names, rows[start : start + size]))
