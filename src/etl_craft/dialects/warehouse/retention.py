"""Bound values and registered auxiliary objects, never target business-row writes."""

from datetime import datetime

from sqlalchemy import TextClause, bindparam, text
from sqlalchemy.engine import Connection

from etl_craft.dialects.warehouse.base import WarehouseDialect


def rejects_statement(
    table: str, cutoff: datetime, pipelines: list[int], *, delete: bool
) -> TextClause:
    """Restrict old reject rows to the selected project's stable pipeline identities."""
    verb = "DELETE" if delete else "SELECT COUNT(*) AS count"
    return text(
        f"{verb} FROM {table} WHERE CREATE_DATE<:cutoff AND PIPELINE_ID IN :pipelines"
    ).bindparams(bindparam("pipelines", expanding=True), cutoff=cutoff, pipelines=pipelines)


def relation_kind(dialect: WarehouseDialect, conn: Connection, table: str) -> str | None:
    """Read an existing relation kind through its dialect's catalog query."""
    query, params = dialect.relation_type_query(table)
    return conn.execute(text(query), params).scalar_one_or_none()
