"""Transactional dirty keys for each registered mirror, independent of commit ordering."""

from typing import TYPE_CHECKING

from sqlalchemy import TextClause, inspect, text
from sqlalchemy.engine import Connection

from etl_craft.dialects.cloning import key_predicate
from etl_craft.dialects.engine.tables import AUDIT_TABLES

if TYPE_CHECKING:
    from etl_craft.dialects.engine.base import EngineDialect

INTERNAL = frozenset({"AUD_CLONE_STATE", "AUD_CLONE_CHANGES"})


def source_keys(conn: Connection, table: str) -> list[str]:
    """Use the declared primary key or the gate wait's existing unique identity."""
    keys = list(inspect(conn).get_pk_constraint(table)["constrained_columns"])
    if not keys and table.upper() == "AUD_GATE_WAITS":
        return ["pipeline_run_id", "task_id", "wait_kind"]
    return keys


def sources(conn: Connection) -> list[tuple[str, list[str], list[str]]]:
    """Return installed audit tables, their keys and columns after schema upgrades."""
    inspector = inspect(conn)
    if not inspector.has_table("aud_clone_state"):
        return []
    return [
        (
            name,
            source_keys(conn, name),
            [str(c["name"]) for c in inspector.get_columns(name)],
        )
        for name in sorted(inspector.get_table_names())
        if name.upper() in AUDIT_TABLES - INTERNAL
    ]


def refresh_postgres(conn: Connection) -> None:
    """Capture changes in the writer's transaction; stamps run after the audit guard."""
    quote = conn.dialect.identifier_preparer.quote
    for table, keys, columns in sources(conn):
        if not keys:
            continue
        name = quote(table)
        key = ", ".join(f"r.{quote(k)}" for k in keys)
        function = quote(f"etl_clone_{table}")
        conn.exec_driver_sql(
            f"CREATE OR REPLACE FUNCTION {function}() RETURNS trigger AS $$ "
            "DECLARE r RECORD; BEGIN "
            "IF TG_OP='DELETE' THEN r:=OLD; ELSE r:=NEW; END IF; "
            "INSERT INTO AUD_CLONE_CHANGES (MIRROR_ID,TABLE_NAME,ROW_KEY,VERSION) "
            f"SELECT MIRROR_ID,TABLE_NAME,json_build_array({key})::text, "
            f"gen_random_uuid()::text FROM AUD_CLONE_STATE WHERE TABLE_NAME='{table.upper()}' "
            "ORDER BY MIRROR_ID ON CONFLICT (MIRROR_ID,TABLE_NAME,ROW_KEY) DO UPDATE "
            "SET VERSION=excluded.VERSION; RETURN r; END; $$ LANGUAGE plpgsql"
        )
        conn.exec_driver_sql(f"DROP TRIGGER IF EXISTS trg_clone ON {name}")
        conn.exec_driver_sql(
            f"CREATE TRIGGER trg_clone AFTER INSERT OR UPDATE OR DELETE ON {name} "
            f"FOR EACH ROW EXECUTE FUNCTION {function}()"
        )
        if "updated_at" in columns:
            conn.exec_driver_sql(
                "CREATE OR REPLACE FUNCTION etl_clone_stamp() RETURNS trigger AS $$ "
                "BEGIN NEW.UPDATED_AT:=clock_timestamp(); RETURN NEW; END; $$ LANGUAGE plpgsql"
            )
            conn.exec_driver_sql(f"DROP TRIGGER IF EXISTS zzz_clone_stamp ON {name}")
            conn.exec_driver_sql(
                f"CREATE TRIGGER zzz_clone_stamp BEFORE INSERT OR UPDATE ON {name} "
                "FOR EACH ROW EXECUTE FUNCTION etl_clone_stamp()"
            )


def _sqlite_stamp(conn: Connection, table: str, keys: list[str], columns: list[str]) -> None:
    quote = conn.dialect.identifier_preparer.quote
    regular = [c for c in columns if c.upper() != "UPDATED_AT"]
    same = " AND ".join(f"NEW.{quote(c)} IS OLD.{quote(c)}" for c in regular)
    stamp = f"etl_craft_actor() IS NOT NULL AND NEW.UPDATED_AT IS NOT OLD.UPDATED_AT AND {same}"
    guards = conn.execute(
        text(
            "SELECT name AS name,sql AS sql FROM sqlite_schema WHERE type='trigger' "
            "AND lower(tbl_name)=lower(:table)"
        ),
        {"table": table},
    ).all()
    for guard in guards:
        header, separator, body = guard.sql.partition("BEGIN")
        if "BEFORE UPDATE" not in header.upper():
            continue
        stamp_start = header.find("NOT (etl_craft_actor() IS NOT NULL AND NEW.UPDATED_AT")
        if stamp_start >= 0:
            header = header[:stamp_start] + f"NOT ({stamp}) "
        else:
            prefix, when, condition = header.partition("WHEN")
            header = prefix + " WHEN "
            if when:
                header += f"({condition.strip()}) AND "
            header += f"NOT ({stamp}) "
        conn.exec_driver_sql(f"DROP TRIGGER {quote(guard.name)}")
        conn.exec_driver_sql(header + separator + body)
    where = " AND ".join(f"{quote(k)} IS NEW.{quote(k)}" for k in keys)
    for operation in ("INSERT", "UPDATE OF " + ", ".join(quote(c) for c in regular)):
        trigger = quote(f"trg_stamp_{table}_{operation.split()[0]}")
        conn.exec_driver_sql(f"DROP TRIGGER IF EXISTS {trigger}")
        conn.exec_driver_sql(
            f"CREATE TRIGGER {trigger} AFTER {operation} ON {quote(table)} BEGIN "
            f"UPDATE {quote(table)} SET UPDATED_AT="
            "(strftime('%Y-%m-%d %H:%M:%f','now') || '000+00:00') "
            f"WHERE {where} AND UPDATED_AT IS NOT "
            "(strftime('%Y-%m-%d %H:%M:%f','now') || '000+00:00'); END"
        )


def refresh_sqlite(conn: Connection) -> None:
    """Capture exact dirty keys, including deletes, in the SQLite writer transaction."""
    quote = conn.dialect.identifier_preparer.quote
    for table, keys, columns in sources(conn):
        if not keys:
            continue
        if any(c.upper() == "UPDATED_AT" for c in columns):
            _sqlite_stamp(conn, table, keys, columns)
        for operation in ("INSERT", "UPDATE", "DELETE"):
            source = "OLD" if operation == "DELETE" else "NEW"
            key = ", ".join(f"{source}.{quote(k)}" for k in keys)
            trigger = quote(f"trg_clone_{table}_{operation}")
            conn.exec_driver_sql(f"DROP TRIGGER IF EXISTS {trigger}")
            conn.exec_driver_sql(
                f"CREATE TRIGGER {trigger} AFTER {operation} ON {quote(table)} BEGIN "
                "INSERT INTO AUD_CLONE_CHANGES (MIRROR_ID,TABLE_NAME,ROW_KEY,VERSION) "
                f"SELECT MIRROR_ID,TABLE_NAME,json_array({key}),lower(hex(randomblob(16))) "
                f"FROM AUD_CLONE_STATE WHERE TABLE_NAME='{table.upper()}' "
                "ON CONFLICT (MIRROR_ID,TABLE_NAME,ROW_KEY) DO UPDATE "
                "SET VERSION=excluded.VERSION; END"
            )


def changed_statement(
    dialect: "EngineDialect",
    table: str,
    names: list[str],
    keys: list[str],
    values: list[list[object]],
) -> TextClause:
    """Read only dirty keys using the same project-ownership scope as a baseline."""
    where, params = key_predicate(keys, values)
    scoped = str(dialect.clone_statement(table, names))
    return text(f"SELECT * FROM ({scoped}) clone_source WHERE {where}").bindparams(**params)
