"""The published hash contract of each warehouse target."""

from datetime import UTC, datetime

from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement


def fetch_hash_version(conn: Connection, target: str) -> int | None:
    """Return the target's recorded version; an unrecorded target has unknown hashes."""
    value = conn.execute(
        statement(conn, "target_hash_version"), {"target": target.lower()}
    ).scalar_one_or_none()
    return None if value is None else int(value)


def save_hash_version(conn: Connection, target: str, version: int) -> None:
    """Publish a contract only after its warehouse transaction committed."""
    conn.execute(
        statement(conn, "save_target_hash_version"),
        {"target": target.lower(), "version": version, "now": datetime.now(UTC)},
    )


def clear_hash_version(conn: Connection, target: str) -> None:
    """Forget a target whose physical table was dropped or replaced."""
    conn.execute(statement(conn, "clear_target_hash_version"), {"target": target.lower()})
