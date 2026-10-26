"""Keep the metadata change triggers complete when a migration adds tables or columns.

Every change to a package-owned ``CFG_`` row is kept as a version in its typed history table,
``AUD_<table>_HISTORY``: the row's values after an insert or update, or when deleted, with who
changed it and under which project migration. A version is valid from ``VALID_FROM`` until
``VALID_TO``; the current one has ``CURRENT_FLAG`` ``'Y'`` and ``VALID_TO`` 9999-12-31, and the
next change closes it. Nothing else in a version ever changes. ``AUD_METADATA_CHANGES`` is a
view over those tables giving each change's before and after values as JSON.

An Engine DB from before the history tables kept changes as JSON rows in an
``AUD_METADATA_CHANGES`` table instead. Until migration 0021 creates the history tables, the
triggers write there; the first refresh after it moves those rows into the history tables and
replaces the table with the view.
"""

from __future__ import annotations

from collections.abc import Callable

from sqlalchemy.engine import Connection

from etl_craft.core.text import quote_identifier as quote
from etl_craft.dialects.engine.metadata import (
    CREDENTIAL_COLUMNS,
    config_base_enabled,
    legacy_team_tables,
    require_config_base,
)
from etl_craft.dialects.engine.tables import (
    CONFIG_TABLES,
    OWNED_CONFIG_TABLES,
    retired_history,
    team_config,
    validate_audit_table,
)

NOW = "(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00')"
STAMPS = {"CREATED_BY", "CREATE_DATE", "UPDATED_BY", "UPDATED_DATE"}
CHANGE_COLUMNS = (
    "CHANGE_ID",
    "VALID_FROM",
    "VALID_TO",
    "CURRENT_FLAG",
    "OPERATION",
    "ACTOR",
    "ACTOR_KIND",
    "MIGRATION",
)
OPEN = "'9999-12-31 23:59:59.000000+00:00'"
"""``VALID_TO`` of a current version."""


def history_table(table: str) -> str:
    """Return the history table of ``CFG_`` table ``table``."""
    return f"AUD_{table.upper()}_HISTORY"


def _exists(conn: Connection, name: str, kind: str = "table") -> bool:
    return (
        conn.exec_driver_sql(
            "SELECT 1 FROM sqlite_schema WHERE upper(name)=? AND type=?", (name.upper(), kind)
        ).first()
        is not None
    )


def _columns(conn: Connection, table: str) -> list[tuple[str, str, bool]]:
    """Return (name, declared type, is primary key) for each column of ``table``."""
    rows = conn.exec_driver_sql(f"PRAGMA table_info({quote(table)})").all()
    return [
        (row.name, row.type or "TEXT", bool(row.pk))
        for row in rows
        if row.name.upper() not in CREDENTIAL_COLUMNS.get(table.upper(), ())
    ]


def refresh_metadata_triggers(conn: Connection) -> None:
    """Rebuild the change triggers from every current column, including project columns."""
    history = _exists(conn, history_table("CFG_PIPELINES"))
    log = _exists(conn, "AUD_METADATA_CHANGES")
    if not history and not log:
        return
    tables = [
        row.name
        for row in conn.exec_driver_sql(
            "SELECT name AS name FROM sqlite_schema WHERE type='table' "
            "AND (upper(name) LIKE 'CFG\\_%' ESCAPE '\\' "
            "OR upper(name) LIKE 'AUD\\_%' ESCAPE '\\')"
        )
    ]
    archived = _release_team_tables(conn, tables)
    for table in tables:
        name = table.upper()
        history_of_config = name.startswith("AUD_CFG_") and name.endswith("_HISTORY")
        if (name.startswith("AUD_") and not history_of_config) or name == "CFG_API_TOKENS":
            _guard_writes(conn, table)
    # Sorted, so a migrated Engine DB gets the same triggers and view as a new one.
    owned = CONFIG_TABLES if _exists(conn, "CFG_TABLES") else OWNED_CONFIG_TABLES
    configs = sorted(
        table
        for table in tables
        if table.upper() in owned and (history or table.upper() != "CFG_API_TOKENS")
    )
    if config_base_enabled(conn):
        for table in configs:
            require_config_base(conn, table)
    if not history:
        for table in configs:
            _change_triggers(conn, table, _log_capture(table))
        return
    for table in configs:
        _ensure_history(conn, table)
        _history_guards(conn, history_table(table))
    if log:
        archived.extend(_copy_legacy_team_log(conn, tables, configs))
    histories = configs + (
        ["CFG_SOURCE_COLUMNS"]
        if _exists(conn, history_table("CFG_SOURCE_COLUMNS"))
        and "CFG_SOURCE_COLUMNS" not in {table.upper() for table in configs}
        else []
    )
    for table in histories:
        _history_guards(conn, history_table(table))
    next_id = (
        "(SELECT COALESCE(MAX(m), 0) + 1 FROM ("
        + " UNION ALL ".join(
            f"SELECT MAX(CHANGE_ID) AS m FROM {quote(table)}"
            for table in [history_table(name) for name in histories] + archived
        )
        + "))"
    )
    for table in configs:
        _change_triggers(conn, table, _history_capture(table, next_id))
    if log:
        conn.exec_driver_sql("DROP TABLE AUD_METADATA_CHANGES")
    _change_view(conn, histories)
    _freeze_histories(conn, archived)


def _guard_writes(conn: Connection, table: str) -> None:
    """Refuse writes to an audit table, or the API tokens, without etl-craft's actor."""
    kind = "API tokens" if table.upper() == "CFG_API_TOKENS" else "audit rows"
    remedy = (
        "use etl-craft token"
        if table.upper() == "CFG_API_TOKENS"
        else "use etl-craft run, mark or cancel"
    )
    for operation in ("INSERT", "UPDATE", "DELETE"):
        guard = f"trg_actor_guard_{table.lower()}_{operation.lower()}"
        immutable = (
            "SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, "
            "'source conversion baseline is immutable') END;"
            if table.upper() == "AUD_SOURCE_COLUMNS_BASELINE"
            else ""
        )
        conn.exec_driver_sql(
            f"CREATE TRIGGER IF NOT EXISTS {quote(guard)} BEFORE {operation} "
            f"ON {quote(table)} BEGIN SELECT CASE WHEN etl_craft_actor() IS NULL "
            f"THEN RAISE(ABORT, '{kind} are written only by etl-craft; "
            f"{remedy}') END; {immutable} END"
        )


def _ensure_history(conn: Connection, table: str) -> None:
    """Create ``table``'s history table, or add the columns ``table`` has gained since."""
    history = history_table(table)
    columns = [
        (name, "BIGINT" if kind.upper() == "INTEGER" else kind)
        for name, kind, _ in _columns(conn, table)
    ]
    present = {name.upper() for name, _, _ in _columns(conn, history)}
    if not present:
        listed = ", ".join(f"{quote(name)} {kind}" for name, kind in columns)
        conn.exec_driver_sql(
            f"CREATE TABLE {quote(history)} (CHANGE_ID BIGINT PRIMARY KEY, VALID_FROM TIMESTAMP "
            "NOT NULL, VALID_TO TIMESTAMP NOT NULL, CURRENT_FLAG VARCHAR(1) NOT NULL, OPERATION "
            "VARCHAR NOT NULL, ACTOR VARCHAR NOT NULL, ACTOR_KIND VARCHAR NOT NULL, MIGRATION "
            f"VARCHAR, {listed})"
        )
        return
    for name, kind in columns:
        if name.upper() not in present:
            conn.exec_driver_sql(f"ALTER TABLE {quote(history)} ADD COLUMN {quote(name)} {kind}")


def _history_guards(conn: Connection, history: str) -> None:
    """Let a version change only when the next one closes it; refuse anything else.

    Rebuilt on every refresh, so a column added to the history table is covered.
    """
    names = [name for name, _, _ in _columns(conn, history)]
    unchanged = " AND ".join(
        f"NEW.{quote(name)} IS OLD.{quote(name)}"
        for name in names
        if name.upper() not in {"VALID_TO", "CURRENT_FLAG"}
    )
    actor = (
        "SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, "
        f"'{history} is written only by etl-craft') END;"
    )
    kept = (
        "SELECT CASE WHEN etl_craft_purpose() <> 'retention' {} THEN "
        f"RAISE(ABORT, '{history} is append-only: a version changes only when the next one "
        "closes it; use etl-craft retention') END;"
    )
    closing = f"AND NOT (OLD.CURRENT_FLAG = 'Y' AND NEW.CURRENT_FLAG = 'N' AND {unchanged})"
    for operation, body in (
        ("INSERT", actor),
        ("UPDATE", actor + kept.format(closing)),
        ("DELETE", actor + kept.format("")),
    ):
        trigger = quote(f"trg_actor_guard_{history.lower()}_{operation.lower()}")
        conn.exec_driver_sql(f"DROP TRIGGER IF EXISTS {trigger}")
        conn.exec_driver_sql(
            f"CREATE TRIGGER {trigger} BEFORE {operation} ON {quote(history)} BEGIN {body} END"
        )


def _move_log(conn: Connection, configs: list[str]) -> None:
    """Copy the JSON change rows of each ``CFG_`` table into its history table."""
    for table in configs:
        names = [name for name, _, _ in _columns(conn, table)]
        values = ", ".join(
            "(SELECT value FROM json_each(COALESCE(c.AFTER_JSON, c.BEFORE_JSON)) WHERE key=?)"
            for name in names
        )
        conn.exec_driver_sql(
            f"INSERT INTO {quote(history_table(table))} ({', '.join(CHANGE_COLUMNS)}, "
            f"{', '.join(quote(name) for name in names)}) SELECT c.CHANGE_ID, c.CHANGED_AT, "
            "CASE WHEN c.OPERATION = 'DELETE' THEN c.CHANGED_AT "
            f"ELSE COALESCE(LEAD(c.CHANGED_AT) OVER w, {OPEN}) END, "
            "CASE WHEN c.OPERATION <> 'DELETE' AND LEAD(c.CHANGE_ID) OVER w IS NULL "
            "THEN 'Y' ELSE 'N' END, "
            f"c.OPERATION, c.ACTOR, c.ACTOR_KIND, c.MIGRATION, {values} "
            "FROM AUD_METADATA_CHANGES c WHERE c.TABLE_NAME=? "
            "WINDOW w AS (PARTITION BY c.ROW_KEY ORDER BY c.CHANGE_ID) ORDER BY c.CHANGE_ID",
            (*[name.lower() for name in names], table.upper()),
        )


Capture = Callable[[str, list[str], str, str], str]
"""Render the statement recording one change: (operation, columns, key, changed predicate)."""


def _history_capture(table: str, next_id: str) -> Capture:
    def capture(operation: str, names: list[str], primary: str, changed: str) -> str:
        history = quote(history_table(table))
        source = "OLD" if operation == "DELETE" else "row"
        record = "OLD" if operation == "DELETE" else "NEW"
        values = ", ".join(f"{source}.{quote(name)}" for name in names)
        when = (
            f" AND ({changed}) AND NOT etl_craft_inserting('{table.upper()}')"
            if operation == "UPDATE"
            else ""
        )
        key = f"{quote(primary)}=NEW.{quote(primary)}"
        where = (
            f" WHERE 1 = 1{when}"
            if operation == "DELETE"
            else f" FROM {quote(table)} row WHERE row.{key}{when}"
        )
        valid_to, current = (NOW, "'N'") if operation == "DELETE" else (OPEN, "'Y'")
        return (
            f"UPDATE {history} SET VALID_TO={NOW}, CURRENT_FLAG='N' WHERE CURRENT_FLAG='Y' "
            f"AND {quote(primary)}={record}.{quote(primary)}{when};"
            f"INSERT INTO {history} ({', '.join(CHANGE_COLUMNS)}, "
            f"{', '.join(quote(name) for name in names)}) SELECT {next_id}, {NOW}, {valid_to}, "
            f"{current}, '{operation}', etl_craft_actor(), etl_craft_actor_kind(), "
            f"NULLIF(etl_craft_migration(), ''), {values}{where};"
        )

    return capture


def _log_capture(table: str) -> Capture:
    def capture(operation: str, names: list[str], primary: str, changed: str) -> str:
        def object_json(prefix: str) -> str:
            return (
                "json_object("
                + ", ".join(f"'{name.lower()}', {prefix}.{quote(name)}" for name in names)
                + ")"
            )

        before = "NULL" if operation == "INSERT" else object_json("OLD")
        after = (
            "NULL"
            if operation == "DELETE"
            else (
                f"(SELECT {object_json('row')} FROM {quote(table)} row "
                f"WHERE row.{quote(primary)}=NEW.{quote(primary)})"
            )
        )
        key = f"{'OLD' if operation == 'DELETE' else 'NEW'}.{quote(primary)}"
        predicate = (
            f" WHERE ({changed}) AND NOT etl_craft_inserting('{table.upper()}')"
            if operation == "UPDATE"
            else ""
        )
        return (
            "INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, "
            "OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) SELECT "
            f"etl_craft_actor(), etl_craft_actor_kind(), '{table.upper()}', "
            f"CAST({key} AS TEXT), '{operation}', {before}, {after}, "
            f"NULLIF(etl_craft_migration(), ''){predicate};"
        )

    return capture


def _change_triggers(conn: Connection, table: str, capture: Capture) -> None:
    """Stamp ``table``'s rows with their actor and time, and record each change."""
    columns = _columns(conn, table)
    names = [name for name, _, _ in columns]
    primary = next((name for name, _, pk in columns if pk), names[0])
    regular = [name for name in names if name.upper() not in STAMPS]
    regular.extend(CREDENTIAL_COLUMNS.get(table.upper(), ()))
    changed = " OR ".join(f"NEW.{quote(name)} IS NOT OLD.{quote(name)}" for name in regular)
    literal = table.upper().replace("'", "''")
    not_inserting = f"NOT etl_craft_inserting('{literal}')"
    for operation in ("INSERT", "UPDATE", "DELETE"):
        trigger = f"trg_audit_{table.lower()}_{operation.lower()}"
        conn.exec_driver_sql(f"DROP TRIGGER IF EXISTS {quote(trigger)}")
        body = ""
        if operation != "DELETE" and {name.upper() for name in names} >= STAMPS:
            created = "etl_craft_actor()" if operation == "INSERT" else "OLD.CREATED_BY"
            create_date = NOW if operation == "INSERT" else "OLD.CREATE_DATE"
            guard = f" AND {not_inserting}" if operation == "UPDATE" else ""
            dependency = (
                "DEPENDS_ON_PIPELINE_ID=COALESCE(NEW.DEPENDS_ON_PIPELINE_ID, "
                "(SELECT PIPELINE_ID FROM CFG_TASKS WHERE TASK_ID=NEW.DEPENDS_ON_TASK_ID)), "
                if table.upper() == "CFG_TASK_DEPENDENCY" and operation == "INSERT"
                else ""
            )
            body += (
                f"UPDATE {quote(table)} SET {dependency}CREATED_BY={created}, "
                f"CREATE_DATE={create_date}, "
                f"UPDATED_BY=etl_craft_actor(), UPDATED_DATE={NOW} "
                f"WHERE {quote(primary)}=NEW.{quote(primary)}{guard};"
            )
        body += capture(operation, names, primary, changed)
        if operation == "INSERT":
            body += f"SELECT etl_craft_exit_insert('{literal}');"
        conn.exec_driver_sql(
            f"CREATE TRIGGER {quote(trigger)} AFTER {operation} ON {quote(table)} BEGIN {body} END"
        )
        guard_name = f"trg_actor_guard_{table.lower()}_{operation.lower()}"
        enter = f"SELECT etl_craft_enter_insert('{literal}');" if operation == "INSERT" else ""
        flag = (
            "SELECT CASE WHEN NEW.ACTIVE_FLAG IS NULL OR NEW.ACTIVE_FLAG NOT IN ('Y','N') "
            f"THEN RAISE(ABORT, '{literal}.ACTIVE_FLAG expects Y or N') END;"
            if operation != "DELETE" and "ACTIVE_FLAG" in {name.upper() for name in names}
            else ""
        )
        conn.exec_driver_sql(f"DROP TRIGGER IF EXISTS {quote(guard_name)}")
        conn.exec_driver_sql(
            f"CREATE TRIGGER {quote(guard_name)} "
            f"BEFORE {operation} ON {quote(table)} BEGIN SELECT CASE WHEN "
            "etl_craft_actor() IS NULL THEN RAISE(ABORT, 'metadata is written only by "
            f"etl-craft; use a project migration (etl-craft migrate)') END; {flag} {enter} END"
        )


def _change_view(conn: Connection, configs: list[str]) -> None:
    """Make ``AUD_METADATA_CHANGES`` the changes of every history table, as JSON."""
    conn.exec_driver_sql("DROP VIEW IF EXISTS AUD_METADATA_CHANGES")
    parts = []
    for table in configs:
        history = history_table(table)
        columns = [] if table.upper() == "CFG_SOURCE_COLUMNS" else _columns(conn, table)
        if not columns and table.upper() == "CFG_SOURCE_COLUMNS":
            columns = [
                (name, kind, name.upper() == "SOURCE_COLUMN_ID")
                for name, kind, _ in _columns(conn, history)
                if name.upper() not in (*CHANGE_COLUMNS, "UPDATED_AT")
            ]
        primary = next((name for name, _, pk in columns if pk), columns[0][0])
        if table.upper() == "CFG_BUSINESS_RULES":
            columns = [
                (name, kind, pk)
                for name, kind, pk in _columns(conn, history)
                if name.upper() not in (*CHANGE_COLUMNS, "UPDATED_AT")
            ]
        values = (
            "json_object("
            + ", ".join(f"'{name.lower()}', {quote(name)}" for name, _, _ in columns)
            + ")"
        )
        key = quote(primary)
        parts.append(
            "SELECT CHANGE_ID, VALID_FROM AS CHANGED_AT, ACTOR, ACTOR_KIND, "
            f"'{table.upper()}' AS TABLE_NAME, json_quote({key}) AS ROW_KEY, OPERATION, "
            f"CASE OPERATION WHEN 'INSERT' THEN NULL WHEN 'DELETE' THEN {values} "
            f"ELSE LAG({values}) OVER (PARTITION BY {key} ORDER BY CHANGE_ID) END AS BEFORE_JSON, "
            f"CASE OPERATION WHEN 'DELETE' THEN NULL ELSE {values} END AS AFTER_JSON, MIGRATION "
            f"FROM {quote(history)}"
        )
    conn.exec_driver_sql(f"CREATE VIEW AUD_METADATA_CHANGES AS {' UNION ALL '.join(parts)}")


def _release_team(conn: Connection, table: str, triggers: set[str]) -> None:
    """Remove only trigger names installed by etl-craft, leaving the team's triggers alone."""
    for event in ("insert", "update", "delete"):
        for prefix in ("trg_actor_guard_", "trg_audit_"):
            trigger = f"{prefix}{table.lower()}_{event}"
            if trigger in triggers:
                conn.exec_driver_sql(f"DROP TRIGGER {quote(trigger)}")


def _freeze_history(conn: Connection, table: str) -> None:
    """Retain former team history as read-only evidence, including for engine actors."""
    literal = table.replace("'", "''")
    for event in ("INSERT", "UPDATE", "DELETE"):
        trigger = quote(f"trg_frozen_{table.lower()}_{event.lower()}")
        conn.exec_driver_sql(
            f"CREATE TRIGGER IF NOT EXISTS {trigger} BEFORE {event} ON {quote(table)} "
            f"BEGIN SELECT RAISE(ABORT, '{literal}: retired team history is read-only; "
            "query preserved records and write the team CFG_ table instead'); END"
        )


def _release_team_tables(conn: Connection, tables: list[str]) -> list[str]:
    """Release team triggers and validate the reserved audit namespace before installing guards."""
    archived = []
    for table in tables:
        triggers = {
            row.name
            for row in conn.exec_driver_sql(
                "SELECT name AS name FROM sqlite_schema WHERE type='trigger' AND tbl_name=? "
                "AND (sql LIKE '%etl_craft_%' OR name LIKE 'trg_frozen_%')",
                (table,),
            )
        }
        validate_audit_table(table, triggers)
        if team_config(table, legacy=not _exists(conn, "CFG_TABLES")):
            _release_team(conn, table, triggers)
        if retired_history(table):
            archived.append(table)
    return archived


def _freeze_histories(conn: Connection, tables: list[str]) -> None:
    """Keep every retired history table read-only after refresh."""
    for table in tables:
        _freeze_history(conn, table)


def _copy_legacy_team_log(conn: Connection, tables: list[str], configs: list[str]) -> list[str]:
    """Convert recorded team changes before retiring the legacy log table."""
    team_logs = legacy_team_tables(conn, set(tables))
    for table in team_logs:
        _ensure_history(conn, table)
    _move_log(conn, configs + team_logs)
    return [history_table(table) for table in team_logs]
