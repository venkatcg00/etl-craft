"""Keep the metadata change triggers complete when a migration adds tables or columns.

Every change to a ``CFG_`` row is kept as a version in its typed history table,
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

from etl_craft.dialects.engine.metadata import config_base_enabled, require_config_base

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


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


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
    rows = conn.exec_driver_sql(f"PRAGMA table_info({_quote(table)})").all()
    return [
        (row.name, row.type or "TEXT", bool(row.pk))
        for row in rows
        if not (table.upper() == "CFG_API_TOKENS" and row.name.upper() == "TOKEN_SHA256")
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
    for table in tables:
        name = table.upper()
        history_of_config = name.startswith("AUD_CFG_") and name.endswith("_HISTORY")
        if (name.startswith("AUD_") and not history_of_config) or name == "CFG_API_TOKENS":
            _guard_writes(conn, table)
    # Sorted, so a migrated Engine DB gets the same triggers and view as a new one.
    configs = sorted(
        table
        for table in tables
        if table.upper().startswith("CFG_") and (history or table.upper() != "CFG_API_TOKENS")
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
        _move_log(conn, configs)
    next_id = (
        "(SELECT COALESCE(MAX(m), 0) + 1 FROM ("
        + " UNION ALL ".join(
            f"SELECT MAX(CHANGE_ID) AS m FROM {_quote(history_table(table))}" for table in configs
        )
        + "))"
    )
    for table in configs:
        _change_triggers(conn, table, _history_capture(table, next_id))
    if log:
        conn.exec_driver_sql("DROP TABLE AUD_METADATA_CHANGES")
    _change_view(conn, configs)


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
        conn.exec_driver_sql(
            f"CREATE TRIGGER IF NOT EXISTS {_quote(guard)} BEFORE {operation} "
            f"ON {_quote(table)} BEGIN SELECT CASE WHEN etl_craft_actor() IS NULL "
            f"THEN RAISE(ABORT, '{kind} are written only by etl-craft; "
            f"{remedy}') END; END"
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
        listed = ", ".join(f"{_quote(name)} {kind}" for name, kind in columns)
        conn.exec_driver_sql(
            f"CREATE TABLE {_quote(history)} (CHANGE_ID BIGINT PRIMARY KEY, VALID_FROM TIMESTAMP "
            "NOT NULL, VALID_TO TIMESTAMP NOT NULL, CURRENT_FLAG VARCHAR(1) NOT NULL, OPERATION "
            "VARCHAR NOT NULL, ACTOR VARCHAR NOT NULL, ACTOR_KIND VARCHAR NOT NULL, MIGRATION "
            f"VARCHAR, {listed})"
        )
        return
    for name, kind in columns:
        if name.upper() not in present:
            conn.exec_driver_sql(f"ALTER TABLE {_quote(history)} ADD COLUMN {_quote(name)} {kind}")


def _history_guards(conn: Connection, history: str) -> None:
    """Let a version change only when the next one closes it; refuse anything else.

    Rebuilt on every refresh, so a column added to the history table is covered.
    """
    names = [name for name, _, _ in _columns(conn, history)]
    unchanged = " AND ".join(
        f"NEW.{_quote(name)} IS OLD.{_quote(name)}"
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
        trigger = _quote(f"trg_actor_guard_{history.lower()}_{operation.lower()}")
        conn.exec_driver_sql(f"DROP TRIGGER IF EXISTS {trigger}")
        conn.exec_driver_sql(
            f"CREATE TRIGGER {trigger} BEFORE {operation} ON {_quote(history)} BEGIN {body} END"
        )


def _move_log(conn: Connection, configs: list[str]) -> None:
    """Copy the JSON change rows of each ``CFG_`` table into its history table."""
    for table in configs:
        names = [name for name, _, _ in _columns(conn, table)]
        values = ", ".join(
            f"json_extract(COALESCE(c.AFTER_JSON, c.BEFORE_JSON), '$.\"{name.lower()}\"')"
            for name in names
        )
        conn.exec_driver_sql(
            f"INSERT INTO {_quote(history_table(table))} ({', '.join(CHANGE_COLUMNS)}, "
            f"{', '.join(_quote(name) for name in names)}) SELECT c.CHANGE_ID, c.CHANGED_AT, "
            "CASE WHEN c.OPERATION = 'DELETE' THEN c.CHANGED_AT "
            f"ELSE COALESCE(LEAD(c.CHANGED_AT) OVER w, {OPEN}) END, "
            "CASE WHEN c.OPERATION <> 'DELETE' AND LEAD(c.CHANGE_ID) OVER w IS NULL "
            "THEN 'Y' ELSE 'N' END, "
            f"c.OPERATION, c.ACTOR, c.ACTOR_KIND, c.MIGRATION, {values} "
            "FROM AUD_METADATA_CHANGES c WHERE c.TABLE_NAME=? "
            "WINDOW w AS (PARTITION BY c.ROW_KEY ORDER BY c.CHANGE_ID) ORDER BY c.CHANGE_ID",
            (table.upper(),),
        )


Capture = Callable[[str, list[str], str, str], str]
"""Render the statement recording one change: (operation, columns, key, changed predicate)."""


def _history_capture(table: str, next_id: str) -> Capture:
    def capture(operation: str, names: list[str], primary: str, changed: str) -> str:
        history = _quote(history_table(table))
        source = "OLD" if operation == "DELETE" else "row"
        record = "OLD" if operation == "DELETE" else "NEW"
        values = ", ".join(f"{source}.{_quote(name)}" for name in names)
        when = (
            f" AND ({changed}) AND NOT etl_craft_inserting('{table.upper()}')"
            if operation == "UPDATE"
            else ""
        )
        key = f"{_quote(primary)}=NEW.{_quote(primary)}"
        where = (
            f" WHERE 1 = 1{when}"
            if operation == "DELETE"
            else f" FROM {_quote(table)} row WHERE row.{key}{when}"
        )
        valid_to, current = (NOW, "'N'") if operation == "DELETE" else (OPEN, "'Y'")
        return (
            f"UPDATE {history} SET VALID_TO={NOW}, CURRENT_FLAG='N' WHERE CURRENT_FLAG='Y' "
            f"AND {_quote(primary)}={record}.{_quote(primary)}{when};"
            f"INSERT INTO {history} ({', '.join(CHANGE_COLUMNS)}, "
            f"{', '.join(_quote(name) for name in names)}) SELECT {next_id}, {NOW}, {valid_to}, "
            f"{current}, '{operation}', etl_craft_actor(), etl_craft_actor_kind(), "
            f"NULLIF(etl_craft_migration(), ''), {values}{where};"
        )

    return capture


def _log_capture(table: str) -> Capture:
    def capture(operation: str, names: list[str], primary: str, changed: str) -> str:
        def object_json(prefix: str) -> str:
            return (
                "json_object("
                + ", ".join(f"'{name.lower()}', {prefix}.{_quote(name)}" for name in names)
                + ")"
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
    if table.upper() == "CFG_API_TOKENS":
        regular.append("TOKEN_SHA256")
    changed = " OR ".join(f"NEW.{_quote(name)} IS NOT OLD.{_quote(name)}" for name in regular)
    literal = table.upper().replace("'", "''")
    not_inserting = f"NOT etl_craft_inserting('{literal}')"
    for operation in ("INSERT", "UPDATE", "DELETE"):
        trigger = f"trg_audit_{table.lower()}_{operation.lower()}"
        conn.exec_driver_sql(f"DROP TRIGGER IF EXISTS {_quote(trigger)}")
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
                f"UPDATE {_quote(table)} SET {dependency}CREATED_BY={created}, "
                f"CREATE_DATE={create_date}, "
                f"UPDATED_BY=etl_craft_actor(), UPDATED_DATE={NOW} "
                f"WHERE {_quote(primary)}=NEW.{_quote(primary)}{guard};"
            )
        body += capture(operation, names, primary, changed)
        if operation == "INSERT":
            body += f"SELECT etl_craft_exit_insert('{literal}');"
        conn.exec_driver_sql(
            f"CREATE TRIGGER {_quote(trigger)} AFTER {operation} ON {_quote(table)}"
            f" BEGIN {body} END"
        )
        guard_name = f"trg_actor_guard_{table.lower()}_{operation.lower()}"
        enter = f"SELECT etl_craft_enter_insert('{literal}');" if operation == "INSERT" else ""
        flag = (
            "SELECT CASE WHEN NEW.ACTIVE_FLAG IS NULL OR NEW.ACTIVE_FLAG NOT IN ('Y','N') "
            f"THEN RAISE(ABORT, '{literal}.ACTIVE_FLAG expects Y or N') END;"
            if operation != "DELETE" and "ACTIVE_FLAG" in {name.upper() for name in names}
            else ""
        )
        conn.exec_driver_sql(f"DROP TRIGGER IF EXISTS {_quote(guard_name)}")
        conn.exec_driver_sql(
            f"CREATE TRIGGER {_quote(guard_name)} "
            f"BEFORE {operation} ON {_quote(table)} BEGIN SELECT CASE WHEN "
            "etl_craft_actor() IS NULL THEN RAISE(ABORT, 'metadata is written only by "
            f"etl-craft; use a project migration (etl-craft migrate)') END; {flag} {enter} END"
        )


def _change_view(conn: Connection, configs: list[str]) -> None:
    """Make ``AUD_METADATA_CHANGES`` the changes of every history table, as JSON."""
    conn.exec_driver_sql("DROP VIEW IF EXISTS AUD_METADATA_CHANGES")
    parts = []
    for table in configs:
        history = history_table(table)
        columns = _columns(conn, table)
        primary = next((name for name, _, pk in columns if pk), columns[0][0])
        values = (
            "json_object("
            + ", ".join(f"'{name.lower()}', {_quote(name)}" for name, _, _ in columns)
            + ")"
        )
        key = _quote(primary)
        parts.append(
            "SELECT CHANGE_ID, VALID_FROM AS CHANGED_AT, ACTOR, ACTOR_KIND, "
            f"'{table.upper()}' AS TABLE_NAME, CAST({key} AS TEXT) AS ROW_KEY, OPERATION, "
            f"CASE OPERATION WHEN 'INSERT' THEN NULL WHEN 'DELETE' THEN {values} "
            f"ELSE LAG({values}) OVER (PARTITION BY {key} ORDER BY CHANGE_ID) END AS BEFORE_JSON, "
            f"CASE OPERATION WHEN 'DELETE' THEN NULL ELSE {values} END AS AFTER_JSON, MIGRATION "
            f"FROM {_quote(history)}"
        )
    conn.exec_driver_sql(f"CREATE VIEW AUD_METADATA_CHANGES AS {' UNION ALL '.join(parts)}")
