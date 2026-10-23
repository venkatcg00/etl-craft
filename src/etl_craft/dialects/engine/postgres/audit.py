"""Protect package-owned metadata and audit tables in the Engine DB schema.

``etl_craft_metadata_change`` keeps each package-owned config change as a version in its typed
history table, ``AUD_<table>_HISTORY``, valid from ``VALID_FROM`` until ``VALID_TO``; the next
change closes the current one, and nothing else in a version changes. ``AUD_METADATA_CHANGES``
is a view over those tables giving each change's before and after values as JSON.

An Engine DB from before the history tables kept changes as JSON rows in an
``AUD_METADATA_CHANGES`` table instead; the first refresh after migration 0021 moves those rows
into the history tables and replaces the table with the view.
"""

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.dialects.engine.metadata import (
    CREDENTIAL_COLUMNS,
    config_base_enabled,
    legacy_team_tables,
    require_config_base,
)
from etl_craft.dialects.engine.tables import (
    OWNED_CONFIG_TABLES,
    retired_history,
    team_config,
    validate_audit_table,
)

CHANGE_COLUMNS = (
    "change_id",
    "valid_from",
    "valid_to",
    "current_flag",
    "operation",
    "actor",
    "actor_kind",
    "migration",
)


def history_table(table: str) -> str:
    """Return the history table of ``CFG_`` table ``table``."""
    return f"aud_{table.lower()}_history"


def _kind(conn: Connection, name: str) -> str | None:
    """Return the relation kind of ``name`` in the current schema: ``r`` table, ``v`` view."""
    value = conn.execute(
        text(
            "SELECT c.relkind AS kind FROM pg_class c JOIN pg_namespace n "
            "ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname=:name"
        ),
        {"name": name},
    ).scalar()
    return None if value is None else str(value)


def refresh_metadata_triggers(conn: Connection) -> None:
    """Install guards and metadata capture on project tables using reserved prefixes."""
    history = _kind(conn, history_table("CFG_PIPELINES")) == "r"
    log = _kind(conn, "aud_metadata_changes") == "r"
    if not history and not log:
        return
    configs: list[str] = []
    rows = conn.execute(
        text(
            "SELECT c.relname AS name, c.oid AS id FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() "
            "AND c.relkind='r' AND (upper(c.relname) LIKE 'CFG\\_%' ESCAPE '\\' "
            "OR upper(c.relname) LIKE 'AUD\\_%' ESCAPE '\\')"
        )
    ).all()
    quote = conn.dialect.identifier_preparer.quote
    enforce_base = config_base_enabled(conn)
    for row in rows:
        if _configure_table(conn, row.name, row.id, history, enforce_base):
            configs.append(row.name)
    if not history:
        return
    # Sorted, so a migrated Engine DB gets the same view as a new one.
    configs.sort()
    for table in configs:
        _ensure_history(conn, table)
    if log:
        team_logs = legacy_team_tables(conn, {row.name for row in rows})
        for table in team_logs:
            _ensure_history(conn, table)
        for table in configs + team_logs:
            name = quote(history_table(table))
            literal = table.upper().replace("'", "''")
            conn.exec_driver_sql(
                f"INSERT INTO {name} SELECT r.* FROM (SELECT c.*, CASE WHEN c.operation = "
                "'DELETE' THEN c.changed_at ELSE COALESCE(LEAD(c.changed_at) OVER w, "
                "'9999-12-31 23:59:59+00'::timestamptz) END AS valid_to, CASE WHEN c.operation "
                "<> 'DELETE' AND LEAD(c.change_id) OVER w IS NULL THEN 'Y' ELSE 'N' END "
                f"AS current_flag FROM aud_metadata_changes c WHERE c.table_name = '{literal}' "
                "WINDOW w AS (PARTITION BY c.row_key ORDER BY c.change_id)) c, "
                f"jsonb_populate_record(NULL::{name}, COALESCE(c.after_json, c.before_json) "
                "|| jsonb_build_object('change_id', c.change_id, 'valid_from', c.changed_at, "
                "'valid_to', c.valid_to, 'current_flag', c.current_flag, 'operation', "
                "c.operation, 'actor', c.actor, 'actor_kind', c.actor_kind, 'migration', "
                "c.migration)) r ORDER BY c.change_id"
            )
        conn.execute(
            text(
                "SELECT setval('etl_craft_change_id', COALESCE((SELECT MAX(change_id) "
                "FROM aud_metadata_changes), 0) + 1, false)"
            )
        )
        conn.exec_driver_sql("DROP TABLE aud_metadata_changes")
    histories = configs + (
        ["cfg_source_columns"]
        if _kind(conn, history_table("cfg_source_columns")) == "r"
        and "cfg_source_columns" not in configs
        else []
    )
    _change_view(conn, histories)
    _freeze_archived(conn)


def _columns(conn: Connection, table: str) -> list[tuple[str, str]]:
    """Return (name, complete type) for each column of ``table``, in order."""
    rows = conn.execute(
        text(
            "SELECT attname AS name, format_type(atttypid, atttypmod) AS type FROM pg_attribute "
            "WHERE attrelid=CAST(:name AS regclass) AND attnum>0 AND NOT attisdropped "
            "ORDER BY attnum"
        ),
        {"name": table},
    ).all()
    return [
        (str(row.name), str(row.type))
        for row in rows
        if row.name.upper() not in CREDENTIAL_COLUMNS.get(table.upper(), ())
    ]


def _ensure_history(conn: Connection, table: str) -> None:
    """Create ``table``'s history table, or add the columns ``table`` has gained since."""
    quote = conn.dialect.identifier_preparer.quote
    history = history_table(table)
    columns = _columns(conn, table)
    if _kind(conn, history) is None:
        listed = ", ".join(f"{quote(name)} {kind}" for name, kind in columns)
        conn.exec_driver_sql(
            f"CREATE TABLE {quote(history)} (CHANGE_ID BIGINT PRIMARY KEY, VALID_FROM "
            "TIMESTAMPTZ NOT NULL, VALID_TO TIMESTAMPTZ NOT NULL, CURRENT_FLAG VARCHAR(1) "
            "NOT NULL, OPERATION VARCHAR NOT NULL, ACTOR VARCHAR NOT NULL, ACTOR_KIND VARCHAR "
            f"NOT NULL, MIGRATION VARCHAR, {listed})"
        )
        for prefix, event, level in (
            ("trg_actor_guard_", "INSERT OR UPDATE OR DELETE", "ROW"),
            ("trg_truncate_", "TRUNCATE", "STATEMENT"),
        ):
            conn.exec_driver_sql(
                f"CREATE TRIGGER {quote(prefix + history)} BEFORE {event} ON {quote(history)} "
                f"FOR EACH {level} EXECUTE FUNCTION etl_craft_guard()"
            )
        return
    present = {name for name, _ in _columns(conn, history)}
    for name, kind in columns:
        if name not in present:
            conn.exec_driver_sql(f"ALTER TABLE {quote(history)} ADD COLUMN {quote(name)} {kind}")


def _change_view(conn: Connection, configs: list[str]) -> None:
    """Make ``AUD_METADATA_CHANGES`` the changes of every history table, as JSON."""
    quote = conn.dialect.identifier_preparer.quote
    conn.exec_driver_sql("DROP VIEW IF EXISTS aud_metadata_changes")
    dropped = ", ".join(f"'{name}'" for name in CHANGE_COLUMNS)
    parts = []
    for table in configs:
        key = quote(
            "source_column_id"
            if table == "cfg_source_columns"
            else (_primary(conn, table) or _columns(conn, table)[0][0])
        )
        parts.append(
            "SELECT change_id, valid_from AS changed_at, actor, actor_kind, "
            f"CAST('{table.upper()}' AS VARCHAR) AS table_name, "
            f"CAST(to_json({key}) AS VARCHAR) AS row_key, "
            "operation, CASE operation WHEN 'INSERT' THEN NULL WHEN 'DELETE' THEN v "
            f"ELSE LAG(v) OVER (PARTITION BY {key} ORDER BY change_id) END AS before_json, "
            "CASE WHEN operation = 'DELETE' THEN NULL ELSE v END AS after_json, migration "
            f"FROM (SELECT h.*, to_jsonb(h) - ARRAY[{dropped}] AS v "
            f"FROM {quote(history_table(table))} h) h"
        )
    conn.exec_driver_sql(f"CREATE VIEW aud_metadata_changes AS {' UNION ALL '.join(parts)}")


def _primary(conn: Connection, table: str) -> str | None:
    value = conn.execute(
        text(
            "SELECT a.attname AS name FROM pg_index i JOIN pg_attribute a "
            "ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
            "WHERE i.indrelid=CAST(:name AS regclass) AND i.indisprimary "
            "ORDER BY a.attnum LIMIT 1"
        ),
        {"name": table},
    ).scalar()
    return None if value is None else str(value)


def _release_team(conn: Connection, table: str, triggers: set[str]) -> None:
    """Remove only etl-craft's named guards, stamps and history capture."""
    quote = conn.dialect.identifier_preparer.quote
    for prefix in ("trg_actor_guard_", "trg_truncate_", "trg_changes_", "trg_audit_"):
        trigger = prefix + table.lower()
        if trigger in triggers:
            conn.exec_driver_sql(f"DROP TRIGGER {quote(trigger)} ON {quote(table)}")


def _freeze_history(conn: Connection, table: str) -> None:
    """Retain former team history as read-only evidence, including for engine actors."""
    quote = conn.dialect.identifier_preparer.quote
    conn.exec_driver_sql(
        "CREATE OR REPLACE FUNCTION etl_craft_retired_history_guard() RETURNS "
        "trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING MESSAGE = "
        "upper(TG_TABLE_NAME) || ': retired team history is read-only; "
        "query preserved records and write the team CFG_ table instead'; END; $$"
    )
    trigger = quote(f"trg_frozen_{table.lower()}_insert")
    conn.exec_driver_sql(f"DROP TRIGGER IF EXISTS {trigger} ON {quote(table)}")
    conn.exec_driver_sql(
        f"CREATE TRIGGER {trigger} BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON "
        f"{quote(table)} FOR EACH STATEMENT EXECUTE FUNCTION "
        f"etl_craft_retired_history_guard()"
    )


def _configure_table(
    conn: Connection, table: str, table_id: int, history: bool, enforce_base: bool
) -> bool:
    """Install package guards and return whether this table supplies current config history."""
    quote = conn.dialect.identifier_preparer.quote
    triggers = set(
        conn.execute(
            text(
                "SELECT t.tgname AS name FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid "
                "WHERE t.tgrelid=:id AND NOT t.tgisinternal AND p.proname IN "
                "('etl_craft_guard','etl_craft_metadata_change','trg_set_audit_columns','etl_craft_retired_history_guard')"
            ),
            {"id": table_id},
        ).scalars()
    )
    validate_audit_table(table, triggers)
    if team_config(table, legacy=_kind(conn, "cfg_tables") is None):
        _release_team(conn, table, triggers)
        return False
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
    if table.upper() not in OWNED_CONFIG_TABLES or (
        not history and table.upper() == "CFG_API_TOKENS"
    ):
        return False
    if enforce_base:
        require_config_base(conn, table)
    columns = conn.execute(
        text(
            "SELECT attname AS name, attnum AS position FROM pg_attribute "
            "WHERE attrelid=:id AND attnum>0 AND NOT attisdropped ORDER BY attnum"
        ),
        {"id": table_id},
    ).all()
    primary = (
        conn.execute(
            text(
                "SELECT a.attname AS name FROM pg_index i JOIN pg_attribute a "
                "ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
                "WHERE i.indrelid=:id AND i.indisprimary ORDER BY a.attnum LIMIT 1"
            ),
            {"id": table_id},
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
    return True


def _freeze_archived(conn: Connection) -> None:
    """Freeze retained team history after legacy-log conversion has finished."""
    from sqlalchemy import inspect

    for table in inspect(conn).get_table_names():
        if retired_history(table):
            _freeze_history(conn, table)
