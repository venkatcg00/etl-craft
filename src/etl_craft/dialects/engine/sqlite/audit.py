"""Keep metadata audit triggers complete when a project migration adds columns."""

from __future__ import annotations

from sqlalchemy.engine import Connection


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def refresh_metadata_triggers(conn: Connection) -> None:
    """Rebuild metadata triggers using every current column, including project columns."""
    if not conn.exec_driver_sql(
        "SELECT 1 FROM sqlite_schema WHERE upper(name)='AUD_METADATA_CHANGES' AND type='table'"
    ).first():
        return
    tables = conn.exec_driver_sql(
        "SELECT name AS name FROM sqlite_schema WHERE type='table' "
        "AND (upper(name) LIKE 'CFG\\_%' ESCAPE '\\' "
        "OR upper(name) LIKE 'AUD\\_%' ESCAPE '\\')"
    ).all()
    now = "(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00')"
    for row in tables:
        table = row.name
        if table.upper().startswith("AUD_"):
            literal = table.replace("'", "''")
            for operation in ("INSERT", "UPDATE", "DELETE"):
                guard = f"trg_actor_guard_{table.lower()}_{operation.lower()}"
                conn.exec_driver_sql(
                    f"CREATE TRIGGER IF NOT EXISTS {_quote(guard)} BEFORE {operation} "
                    f"ON {_quote(table)} BEGIN SELECT CASE WHEN etl_craft_actor() IS NULL "
                    "THEN RAISE(ABORT, 'audit rows are written only by etl-craft; "
                    "use etl-craft run, mark or cancel') END; END"
                )
            continue
        columns = conn.exec_driver_sql(f"PRAGMA table_info({_quote(table)})").all()
        names = [column.name for column in columns]
        primary = next((column.name for column in columns if column.pk), names[0])
        stamp_names = {"CREATED_BY", "CREATE_DATE", "UPDATED_BY", "UPDATED_DATE"}
        regular = [name for name in names if name.upper() not in stamp_names]

        def object_json(prefix: str, names: list[str] = names) -> str:
            return (
                "json_object("
                + ", ".join(
                    "'" + name.lower().replace("'", "''") + "', " + prefix + "." + _quote(name)
                    for name in names
                )
                + ")"
            )

        for operation in ("INSERT", "UPDATE", "DELETE"):
            trigger = f"trg_audit_{table.lower()}_{operation.lower()}"
            conn.exec_driver_sql(f"DROP TRIGGER IF EXISTS {_quote(trigger)}")
            changed = " OR ".join(
                f"NEW.{_quote(name)} IS NOT OLD.{_quote(name)}" for name in regular
            )
            literal = table.replace("'", "''")
            not_inserting = f"NOT etl_craft_inserting('{literal.upper()}')"
            body = ""
            if operation != "DELETE" and stamp_names <= {name.upper() for name in names}:
                created = "etl_craft_actor()" if operation == "INSERT" else "OLD.CREATED_BY"
                create_date = now if operation == "INSERT" else "OLD.CREATE_DATE"
                guard = f" AND {not_inserting}" if operation == "UPDATE" else ""
                dependency = (
                    "DEPENDS_ON_PIPELINE_ID=COALESCE(NEW.DEPENDS_ON_PIPELINE_ID, "
                    "(SELECT PIPELINE_ID FROM CFG_TASKS WHERE TASK_ID=NEW.DEPENDS_ON_TASK_ID)), "
                    if table.upper() == "CFG_TASK_DEPENDENCY" and operation == "INSERT"
                    else ""
                )
                body += (
                    f"UPDATE {_quote(table)} SET {dependency}CREATED_BY={created}, "
                    f"CREATE_DATE={create_date}, "
                    f"UPDATED_BY=etl_craft_actor(), UPDATED_DATE={now} "
                    f"WHERE {_quote(primary)}=NEW.{_quote(primary)}{guard};"
                )
            before = "NULL" if operation == "INSERT" else object_json("OLD")
            after = (
                "NULL"
                if operation == "DELETE"
                else (
                    f"(SELECT {object_json('row')} FROM {_quote(table)} row "
                    f"WHERE row.{_quote(primary)}=NEW.{_quote(primary)})"
                )
            )
            key = f"{'OLD' if operation == 'DELETE' else 'NEW'}.{_quote(primary)}"
            literal = table.replace("'", "''")
            predicate = f" WHERE ({changed}) AND {not_inserting}" if operation == "UPDATE" else ""
            body += (
                "INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, "
                "OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) SELECT "
                f"etl_craft_actor(), etl_craft_actor_kind(), '{literal.upper()}', "
                f"CAST({key} AS TEXT), '{operation}', {before}, {after}, "
                f"NULLIF(etl_craft_migration(), ''){predicate};"
            )
            if operation == "INSERT":
                body += f"SELECT etl_craft_exit_insert('{literal.upper()}');"
            conn.exec_driver_sql(
                f"CREATE TRIGGER {_quote(trigger)} AFTER {operation} ON {_quote(table)}"
                f" BEGIN {body} END"
            )
            guard_name = f"trg_actor_guard_{table.lower()}_{operation.lower()}"
            enter = (
                f"SELECT etl_craft_enter_insert('{literal.upper()}');"
                if operation == "INSERT"
                else ""
            )
            conn.exec_driver_sql(
                f"CREATE TRIGGER IF NOT EXISTS {_quote(guard_name)} "
                f"BEFORE {operation} ON {_quote(table)} BEGIN SELECT CASE WHEN "
                "etl_craft_actor() IS NULL THEN RAISE(ABORT, 'metadata is written only by "
                f"etl-craft; use a project migration (etl-craft migrate)') END; {enter} END"
            )
