"""The published hash contract of each warehouse target, and the statement behind each view.

Both are keyed by the target's resolved catalog, schema and table, compared in lowercase.
"""

from datetime import UTC, datetime

from sqlalchemy.engine import Connection

from etl_craft.core.errors import HandlerError
from etl_craft.core.text import split_object_ref
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import current_project_id


def _key(target: str) -> dict[str, str | int]:
    """Split a resolved ``catalog.schema.table`` into its lowercased components."""
    catalog, schema, table = split_object_ref(target.lower())
    if catalog is None:
        raise HandlerError(f"{target!r} names no catalog; resolve it to catalog.schema.table")
    return {
        "project_id": current_project_id(),
        "catalog": catalog,
        "schema": schema,
        "table": table,
    }


def fetch_hash_version(conn: Connection, target: str) -> int | None:
    """Return the target's recorded version; an unrecorded target has unknown hashes."""
    value = conn.execute(statement(conn, "target_hash_version"), _key(target)).scalar_one_or_none()
    return None if value is None else int(value)


def save_hash_version(conn: Connection, target: str, version: int) -> None:
    """Publish a contract only after its warehouse transaction committed."""
    conn.execute(
        statement(conn, "save_target_hash_version"),
        {**_key(target), "version": version, "now": datetime.now(UTC)},
    )


def clear_hash_version(conn: Connection, target: str) -> None:
    """Forget a target whose physical table was dropped or replaced."""
    conn.execute(statement(conn, "clear_target_hash_version"), _key(target))


def fetch_view_statement(conn: Connection, target: str) -> str | None:
    """Return the SHA-256 of the statement that last made view ``target``, if any."""
    value = conn.execute(
        statement(conn, "target_view_statement"), _key(target)
    ).scalar_one_or_none()
    return None if value is None else str(value)


def save_view_statement(conn: Connection, target: str, sha256: str) -> None:
    """Publish a view's statement only after its warehouse transaction committed."""
    conn.execute(
        statement(conn, "save_target_view_statement"),
        {**_key(target), "sha256": sha256, "now": datetime.now(UTC)},
    )


def fetch_target_contract(conn: Connection, target: str) -> tuple[int | None, str | None]:
    """Read both published contracts in one round trip under the target lock."""
    row = conn.execute(statement(conn, "target_contract"), _key(target)).one()
    return row.hash_version, row.statement_sha256
