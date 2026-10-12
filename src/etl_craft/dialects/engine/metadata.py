"""The required shape of engine and project configuration tables."""

from sqlalchemy import DateTime, String, inspect
from sqlalchemy.engine import Connection

from etl_craft.core.errors import MetadataError

BASE_COLUMNS = ("created_by", "create_date", "updated_by", "updated_date", "active_flag")


def config_base_enabled(conn: Connection) -> bool:
    """Enable enforcement after the core audit columns have been made mandatory."""
    table = "cfg_pipelines" if conn.dialect.name == "postgresql" else "CFG_PIPELINES"
    columns = {c["name"].lower(): c for c in inspect(conn).get_columns(table)}
    return all(name in columns and not columns[name]["nullable"] for name in BASE_COLUMNS)


def require_config_base(conn: Connection, table: str) -> None:
    """Refuse an incomplete config table before installing its change-history triggers."""
    inspector = inspect(conn)
    columns = {c["name"].lower(): c for c in inspector.get_columns(table)}
    problems = []
    for name in BASE_COLUMNS:
        column = columns.get(name)
        if column is None or column["nullable"]:
            problems.append(f"{name.upper()} must be present and NOT NULL")
            continue
        expected = DateTime if name.endswith("date") else String
        if not isinstance(column["type"], expected):
            problems.append(f"{name.upper()} has incompatible type {column['type']}")
        elif (
            isinstance(column["type"], DateTime)
            and conn.dialect.name == "postgresql"
            and not column["type"].timezone
        ):
            problems.append(f"{name.upper()} must use TIMESTAMPTZ")
    keys = inspector.get_pk_constraint(table)["constrained_columns"]
    if len(keys) != 1:
        problems.append("one stable primary-key column is required")
    elif columns[keys[0].lower()]["nullable"]:
        rowid_key = conn.dialect.name == "sqlite" and not any(
            row.origin == "pk"
            for row in conn.exec_driver_sql(
                f"PRAGMA index_list({conn.dialect.identifier_preparer.quote(table)})"
            )
        )
        if not rowid_key:
            problems.append(f"{keys[0].upper()} primary key must be NOT NULL")
    if problems:
        raise MetadataError(
            f"{table}: {'; '.join(problems)}; define the common config fields and key "
            "in the table's CREATE TABLE statement before applying the project migration"
        )
