"""Protect project-created metadata and audit tables in the Engine DB schema."""

from sqlalchemy import text
from sqlalchemy.engine import Connection


def refresh_metadata_triggers(conn: Connection) -> None:
    """Install guards and metadata capture on project tables using reserved prefixes."""
    if conn.execute(text("SELECT to_regclass('aud_metadata_changes') AS name")).scalar() is None:
        return
    rows = conn.execute(
        text(
            "SELECT c.relname AS name, c.oid AS id FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() "
            "AND c.relkind='r' AND (upper(c.relname) LIKE 'CFG\\_%' ESCAPE '\\' "
            "OR upper(c.relname) LIKE 'AUD\\_%' ESCAPE '\\')"
        )
    ).all()
    quote = conn.dialect.identifier_preparer.quote
    for row in rows:
        table = row.name
        triggers = set(
            conn.execute(
                text(
                    "SELECT tgname AS name FROM pg_trigger WHERE tgrelid=:id AND NOT tgisinternal"
                ),
                {"id": row.id},
            ).scalars()
        )
        for prefix, event, level, function in (
            ("trg_actor_guard_", "INSERT OR UPDATE OR DELETE", "ROW", "etl_craft_guard()"),
            ("trg_truncate_", "TRUNCATE", "STATEMENT", "etl_craft_guard()"),
        ):
            trigger = prefix + table.lower()
            if trigger not in triggers:
                conn.exec_driver_sql(
                    f"CREATE TRIGGER {quote(trigger)} BEFORE {event} ON {quote(table)} "
                    f"FOR EACH {level} EXECUTE FUNCTION {function}"
                )
        if not table.upper().startswith("CFG_") or table.upper() == "CFG_API_TOKENS":
            continue
        columns = conn.execute(
            text(
                "SELECT attname AS name, attnum AS position FROM pg_attribute "
                "WHERE attrelid=:id AND attnum>0 AND NOT attisdropped ORDER BY attnum"
            ),
            {"id": row.id},
        ).all()
        primary = (
            conn.execute(
                text(
                    "SELECT a.attname AS name FROM pg_index i JOIN pg_attribute a "
                    "ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
                    "WHERE i.indrelid=:id AND i.indisprimary ORDER BY a.attnum LIMIT 1"
                ),
                {"id": row.id},
            ).scalar()
            or columns[0].name
        )
        trigger = "trg_changes_" + table.lower()
        if trigger not in triggers:
            literal = primary.replace("'", "''")
            conn.exec_driver_sql(
                f"CREATE TRIGGER {quote(trigger)} AFTER INSERT OR UPDATE OR DELETE "
                f"ON {quote(table)} FOR EACH ROW EXECUTE FUNCTION "
                f"etl_craft_metadata_change('{literal}')"
            )
        trigger = "trg_audit_" + table.lower()
        if trigger not in triggers and {
            "created_by",
            "create_date",
            "updated_by",
            "updated_date",
        } <= {column.name.lower() for column in columns}:
            conn.exec_driver_sql(
                f"CREATE TRIGGER {quote(trigger)} BEFORE INSERT OR UPDATE ON {quote(table)} "
                "FOR EACH ROW EXECUTE FUNCTION trg_set_audit_columns()"
            )
