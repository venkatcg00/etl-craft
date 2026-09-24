"""Running Engine DB queries from the dialect's query catalog."""

from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import TextClause, text
from sqlalchemy.engine import Connection, Engine

from etl_craft.dialects.engine import for_engine


def statement(bind: Connection | Engine, name: str) -> TextClause:
    """Return catalog query ``name`` for the Engine DB ``bind`` is connected to."""
    return text(for_engine(bind).query(name))


def run_script(conn: Connection, statements: Iterable[str]) -> None:
    """Run each statement exactly as written, with no bind parameters.

    Colons are left alone. A driver whose placeholders use ``%`` (psycopg) still reads one in
    the text as a placeholder, so each ``%`` is doubled for it.
    """
    escape = conn.dialect.paramstyle in {"format", "pyformat"}
    for sql in statements:
        conn.exec_driver_sql(sql.replace("%", "%%") if escape else sql)
