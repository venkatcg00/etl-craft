"""Current account identity and available-administrator reads."""

from sqlalchemy.engine import Connection, Row

from etl_craft.engine.queries import statement


def find_account(conn: Connection, name: str) -> Row[tuple[object, ...]] | None:
    """Read the current account schema after normal authentication admission."""
    return conn.execute(statement(conn, "account_find"), {"name": name}).one_or_none()


def unlocked_admins(conn: Connection) -> int:
    """Count unlocked active admins in the current account schema."""
    return int(conn.execute(statement(conn, "account_admin_unlocked")).scalar_one())
