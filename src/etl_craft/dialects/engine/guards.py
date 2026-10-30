"""Package row classification and completeness checks for Engine DB guards."""

import re
import sqlite3
from functools import cache

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

from etl_craft.core.actor import migration_project, scoped_actor
from etl_craft.core.errors import MigrationError
from etl_craft.dialects.engine.tables import AUDIT_TABLES, CONFIG_TABLES, retired_history

ADMIN_TABLES = frozenset(
    {
        "CFG_PROJECTS",
        "CFG_POOLS",
        "CFG_ACCOUNTS",
        "CFG_GROUPS",
        "CFG_GROUP_MEMBERS",
        "CFG_PIPELINE_GRANTS",
        "CFG_API_TOKENS",
    }
)
PROJECT_CONFIG_TABLES = CONFIG_TABLES - ADMIN_TABLES
PACKAGE_TABLES = PROJECT_CONFIG_TABLES | ADMIN_TABLES | AUDIT_TABLES


def missing_guards(conn: Connection) -> list[str]:
    """Check every physical package table, including its typed history."""
    tables = {name.upper() for name in inspect(conn).get_table_names()}
    protected = tables & PACKAGE_TABLES
    if conn.dialect.name == "sqlite":
        found = set(
            conn.execute(
                text("SELECT name AS name FROM sqlite_schema WHERE type='trigger'")
            ).scalars()
        )
        expected = {
            f"trg_actor_guard_{table.lower()}_{op}"
            for table in protected
            for op in ("insert", "update", "delete")
        }
    else:
        found = set(
            conn.execute(
                text(
                    "SELECT t.tgname AS name FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
                    "JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname=current_schema() AND NOT t.tgisinternal "
                    "AND t.tgenabled IN ('O','A')"
                )
            ).scalars()
        )
        expected = {
            prefix + table.lower()
            for table in protected
            for prefix in ("trg_actor_guard_", "trg_truncate_")
        }
    from etl_craft.dialects.engine import for_engine

    dialect = for_engine(conn)
    expected |= {
        trigger
        for trigger, table, _ in declared_triggers(dialect.name, dialect.schema_path().read_text())
        if table.upper() in protected
    }
    for table in protected & CONFIG_TABLES:
        if conn.dialect.name == "sqlite":
            expected.update(
                f"trg_audit_{table.lower()}_{op}" for op in ("insert", "update", "delete")
            )
        else:
            expected.update({f"trg_changes_{table.lower()}", f"trg_audit_{table.lower()}"})
    archived = {table for table in tables if retired_history(table)}
    events = ("insert", "update", "delete") if conn.dialect.name == "sqlite" else ("insert",)
    expected.update(f"trg_frozen_{table.lower()}_{event}" for table in archived for event in events)
    return sorted(expected - found)


def require_guards(conn: Connection) -> None:
    """Refuse startup when the package's write protection is incomplete."""
    if missing := missing_guards(conn):
        raise MigrationError(
            f"Engine DB missing guard triggers {missing}; expected every package guard; "
            "re-run `etl-craft setup` or restore the Engine DB"
        )


def sqlite_authorizer(
    action: int,
    first: str | None,
    second: str | None,
    database: str | None,
    source: str | None,
) -> int:
    """Refuse unattributed schema changes on package objects in initialized connections.

    While a project migration's statement runs, its top-level SQL may also neither change package
    tables nor write admin or audit rows.
    """
    ddl = {
        sqlite3.SQLITE_ALTER_TABLE,
        sqlite3.SQLITE_DROP_TABLE,
        sqlite3.SQLITE_DROP_TRIGGER,
        sqlite3.SQLITE_CREATE_TRIGGER,
        sqlite3.SQLITE_DROP_INDEX,
        sqlite3.SQLITE_CREATE_INDEX,
    }
    table = (
        second
        if action
        in {
            sqlite3.SQLITE_ALTER_TABLE,
            sqlite3.SQLITE_CREATE_TRIGGER,
            sqlite3.SQLITE_DROP_TRIGGER,
            sqlite3.SQLITE_CREATE_INDEX,
            sqlite3.SQLITE_DROP_INDEX,
        }
        else first
    )
    name = (table or "").upper()
    if action in ddl and name in PACKAGE_TABLES and scoped_actor() is None:
        return sqlite3.SQLITE_DENY
    if migration_project.get() is not None and source is None and name in PACKAGE_TABLES:
        # A project migration's own statements reach only project config rows; the package's
        # triggers still write audit and history rows on its behalf.
        writes = {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}
        if action in ddl or (action in writes and name not in PROJECT_CONFIG_TABLES):
            return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


@cache
def declared_triggers(dialect_name: str, schema: str) -> tuple[tuple[str, str, str], ...]:
    """Track the schema's surviving static guards, including immutable-row guards."""
    from etl_craft.dialects.engine import for_name

    dialect = for_name(dialect_name)
    definitions: dict[str, tuple[str, str]] = {}
    for sql in dialect.split_statements(schema):
        plain = re.sub(r"(?m)^--[^\n]*", "", sql).strip()
        create = re.match(r"CREATE TRIGGER (\w+).*?\bON\s+(\w+)", plain, re.I | re.S)
        drop = re.match(r"DROP TRIGGER (?:IF EXISTS )?(\w+)", plain, re.I)
        table = re.match(r"DROP TABLE (?:IF EXISTS )?(\w+)", plain, re.I)
        if create:
            definitions[create.group(1)] = (create.group(2), sql)
        elif drop:
            definitions.pop(drop.group(1), None)
        elif table:
            definitions = {
                k: v for k, v in definitions.items() if v[0].lower() != table.group(1).lower()
            }
    return tuple((name, table, sql) for name, (table, sql) in definitions.items())


def restore_guards(conn: Connection) -> None:
    """Restore surviving packaged trigger declarations during explicit setup."""
    from etl_craft.dialects.engine import for_engine

    dialect = for_engine(conn)
    tables = {name.upper() for name in inspect(conn).get_table_names()}
    missing = set(missing_guards(conn))
    if conn.dialect.name == "postgresql":
        rows = conn.execute(
            text(
                "SELECT t.tgname AS name,c.relname AS table_name FROM pg_trigger t "
                "JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=current_schema() AND NOT t.tgisinternal "
                "AND t.tgenabled NOT IN ('O','A')"
            )
        )
        quote = conn.dialect.identifier_preparer.quote
        for row in rows:
            if row.name in missing:
                conn.exec_driver_sql(
                    f"ALTER TABLE {quote(row.table_name)} ENABLE TRIGGER {quote(row.name)}"
                )
                missing.remove(row.name)
    for name, table, sql in declared_triggers(dialect.name, dialect.schema_path().read_text()):
        if name in missing and table.upper() in tables:
            conn.exec_driver_sql(sql)
    require_guards(conn)
