"""The ``CFG_`` tables as they stood at a past moment, read from their history tables.

Each ``AUD_<table>_HISTORY`` holds every version of a ``CFG_`` row, valid from ``VALID_FROM``
until ``VALID_TO``. A row's state at a moment is the version valid then; a row with versions but
none valid then did not exist. A row with no version at all has not changed since the Engine DB
began recording, and stands as it is now. The rows go into an in-memory SQLite database holding
the configuration tables, so they are read exactly as the live ones are.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.pool import StaticPool

from etl_craft.core.errors import MetadataError
from etl_craft.core.time import as_utc
from etl_craft.engine.repository.config_rows import TABLES

Row = dict[str, Any]


def config_as_of(conn: Connection, as_of: datetime) -> Engine:
    """Rebuild the ``CFG_`` tables at ``as_of``; ``MetadataError`` where history cannot."""
    versions = {
        table.table: _rows(conn, f"SELECT * FROM AUD_{table.table}_HISTORY ORDER BY CHANGE_ID")
        for table in TABLES
    }
    legacy_versions = _rows(conn, "SELECT * FROM AUD_CFG_SOURCE_COLUMNS_HISTORY ORDER BY CHANGE_ID")
    _require_history_at(as_of, versions, legacy_versions)
    scratch = create_engine("sqlite://", poolclass=StaticPool)
    try:
        with scratch.begin() as out:
            legacy_rules = _copy_tables(conn, out, versions, as_of)
            _rebuild_legacy_sources(conn, out, legacy_versions, as_of)
            _rebuild_rule_targets(out, legacy_rules, as_of)
            _rebuild_task_targets(out, as_of)
    except Exception:
        scratch.dispose()
        raise
    return scratch


def _require_history_at(
    as_of: datetime, versions: dict[str, list[dict[str, Any]]], legacy: list[dict[str, Any]]
) -> None:
    """Refuse a time before the first recorded config change."""
    starts = [as_utc(rows[0]["valid_from"]) for rows in versions.values() if rows]
    starts += [as_utc(legacy[0]["valid_from"])] if legacy else []
    if not starts or as_of < min(starts):
        since = f"from {min(starts).isoformat()} on" if starts else "none yet"
        raise MetadataError(
            f"cannot rebuild the config as of {as_of.isoformat()}: the Engine DB records config "
            f"changes {since}, and nothing earlier; choose a later time"
        )


def _copy_tables(
    conn: Connection,
    out: Connection,
    versions: dict[str, list[dict[str, Any]]],
    as_of: datetime,
) -> list[dict[str, Any]]:
    """Write each ``CFG_`` table's rows at ``as_of``; return the rules without a target id."""
    legacy_rules: list[dict[str, Any]] = []
    for table in TABLES:
        key = table.id_column.lower()
        current = _rows(conn, f"SELECT * FROM {table.table}")
        columns = list(current[0]) if current else _names(conn, table.table)
        rows = _at(current, versions[table.table], key, table.table, as_of)
        if table.table == "CFG_BUSINESS_RULES":
            legacy_rules = [row for row in rows if row.get("target_table_id") is None]
        if table.table == "CFG_TASKS":
            rows = [
                row | {"target_active_flag": row.get("target_active_flag") or "Y"} for row in rows
            ]
        out.execute(text(f"CREATE TABLE {table.table} ({', '.join(columns)})"))
        if rows:
            out.execute(
                text(
                    f"INSERT INTO {table.table} ({', '.join(columns)}) "
                    f"VALUES ({', '.join(f':{name}' for name in columns)})"
                ),
                [{name: _cell(row.get(name)) for name in columns} for row in rows],
            )
    return legacy_rules


def _rebuild_legacy_sources(
    conn: Connection, out: Connection, legacy_versions: list[dict[str, Any]], as_of: datetime
) -> None:
    """Rebuild source tables and columns from the legacy history before its conversion."""
    from etl_craft.engine.repository.normalization import object_identity

    baseline = _rows(conn, "SELECT * FROM AUD_SOURCE_COLUMNS_BASELINE")
    converted = conn.execute(
        text(
            "SELECT APPLIED_AT AS applied_at FROM SCHEMA_MIGRATIONS "
            "WHERE SOURCE='ENGINE' AND VERSION='0028_source_contracts.sql'"
        )
    ).scalar_one_or_none()
    if not (baseline or legacy_versions) or converted is None or as_of >= as_utc(converted):
        return
    legacy = _at(baseline, legacy_versions, "source_column_id", "CFG_SOURCE_COLUMNS", as_of)
    identities: dict[tuple[str | None, str, str], int] = {}
    out.execute(text("DELETE FROM CFG_COLUMNS"))
    out.execute(text("DELETE FROM CFG_TABLES"))
    for row in legacy:
        # Earlier evidence has no inferred catalog or declared object kind.
        try:
            identity = object_identity(str(row["table_name"]), None, None)
        except ValueError as error:
            raise MetadataError(
                f"cannot rebuild source SOURCE_COLUMN_ID={row['source_column_id']} "
                f"as of {as_of.isoformat()}: {error}; inspect its retained legacy "
                "history or choose a time after its corrected definition"
            ) from error
        table_id = identities.setdefault(identity, len(identities) + 1)
        out.execute(
            text(
                "INSERT INTO CFG_TABLES (TABLE_ID, CATALOG_NAME, SCHEMA_NAME, "
                "TABLE_NAME, "
                "ACTIVE_FLAG) SELECT :id, :catalog, :schema, :table, 'Y' "
                "WHERE NOT EXISTS (SELECT 1 FROM CFG_TABLES WHERE TABLE_ID=:id)"
            ),
            dict(zip(("catalog", "schema", "table"), identity, strict=True)) | {"id": table_id},
        )
        out.execute(
            text(
                "INSERT INTO CFG_COLUMNS (COLUMN_ID, TABLE_ID, COLUMN_NAME, DATA_TYPE, "
                "COLUMN_POSITION, ACTIVE_FLAG) VALUES (:source_column_id, :table_id, "
                ":column_name, :data_type, :column_position, :active_flag)"
            ),
            {name: _cell(value) for name, value in row.items()} | {"table_id": table_id},
        )
    out.execute(
        text(
            "UPDATE CFG_TABLES SET ACTIVE_FLAG=CASE WHEN EXISTS (SELECT 1 FROM"
            " CFG_COLUMNS c "
            "WHERE c.TABLE_ID=CFG_TABLES.TABLE_ID AND c.ACTIVE_FLAG='Y') THEN "
            "'Y' ELSE 'N' END"
        )
    )


def _rebuild_rule_targets(
    out: Connection, legacy_rules: list[dict[str, Any]], as_of: datetime
) -> None:
    """Give each rule recorded with a combined target name its target table id."""
    from etl_craft.engine.repository.normalization import object_identity

    for row in legacy_rules:
        try:
            identity = object_identity(str(row["target_table"]), None, None)
        except ValueError as error:
            raise MetadataError(
                f"cannot rebuild BUSINESS_RULE_ID={row['business_rule_id']} as of "
                f"{as_of.isoformat()}: {error}; inspect its retained target history"
            ) from error
        out.execute(
            text(
                "UPDATE CFG_BUSINESS_RULES SET TARGET_TABLE_ID=:target WHERE BUSINESS_RULE_ID=:id"
            ),
            {"target": _table_id(out, identity), "id": row["business_rule_id"]},
        )


def _rebuild_task_targets(out: Connection, as_of: datetime) -> None:
    """Turn ``TARGET_OBJECT`` task parameters into task target ids, then drop them."""
    from etl_craft.engine.repository.normalization import object_identity

    for row in _rows(out, "SELECT * FROM CFG_TASK_PARAMETERS WHERE PARAMETER_NAME='TARGET_OBJECT'"):
        try:
            identity = object_identity(str(row["parameter_value"]), None, None)
        except ValueError as error:
            raise MetadataError(
                f"cannot rebuild TASK_PARAMETER_ID={row['task_parameter_id']} as of "
                f"{as_of.isoformat()}: {error}; inspect its retained target history"
            ) from error
        out.execute(
            text(
                "UPDATE CFG_TASKS SET "
                "TARGET_TABLE_ID=:target,TARGET_ACTIVE_FLAG=:active WHERE "
                "TASK_ID=:id"
            ),
            {
                "target": _table_id(out, identity),
                "active": row["active_flag"],
                "id": row["task_id"],
            },
        )
    out.execute(text("DELETE FROM CFG_TASK_PARAMETERS WHERE PARAMETER_NAME='TARGET_OBJECT'"))


def _table_id(out: Connection, identity: tuple[str | None, str, str]) -> int:
    """Return the rebuilt ``CFG_TABLES`` id of ``identity``, adding an active row if missing."""
    params = dict(zip(("catalog", "schema", "table"), identity, strict=True))
    table_id = out.execute(
        text(
            "SELECT TABLE_ID AS table_id FROM CFG_TABLES WHERE SCHEMA_NAME=:schema "
            "AND TABLE_NAME=:table AND (CATALOG_NAME=:catalog OR "
            "(CATALOG_NAME IS NULL AND :catalog IS NULL))"
        ),
        params,
    ).scalar_one_or_none()
    if table_id is None:
        table_id = out.execute(
            text("SELECT COALESCE(MAX(TABLE_ID),0)+1 AS table_id FROM CFG_TABLES")
        ).scalar_one()
        out.execute(
            text(
                "INSERT INTO CFG_TABLES "
                "(TABLE_ID,CATALOG_NAME,SCHEMA_NAME,TABLE_NAME,ACTIVE_FLAG) "
                "VALUES (:id,:catalog,:schema,:table,'Y')"
            ),
            params | {"id": table_id},
        )
    return int(table_id)


def _at(
    current: list[Row], versions: list[Row], key: str, table: str, as_of: datetime
) -> list[Row]:
    valid: dict[str, Row] = {}
    first: dict[str, Row] = {}
    for version in versions:
        row_id = str(version[key])
        first.setdefault(row_id, version)
        if as_utc(version["valid_from"]) <= as_of < as_utc(version["valid_to"]):
            valid[row_id] = version
    unknown = [
        row_id
        for row_id, version in first.items()
        if row_id not in valid
        and version["operation"] != "INSERT"
        and not (
            (
                table == "CFG_BUSINESS_RULES"
                and version.get("migration") == "engine/0029_rule_target_contracts.sql"
            )
            or (
                table == "CFG_TASKS"
                and version.get("migration") == "engine/0030_task_target_contracts.sql"
            )
            or (
                table == "CFG_TASK_PARAMETERS"
                and version.get("migration") == "engine/0030_task_target_contracts.sql"
                and version.get("parameter_name") == "TARGET_OBJECT"
                and version["operation"] == "DELETE"
            )
        )
        and as_of < as_utc(version["valid_from"])
    ]
    if unknown:
        raise MetadataError(
            f"cannot rebuild {table} as of {as_of.isoformat()}: row(s) "
            f"{key.upper()} {', '.join(unknown)} existed before the Engine DB "
            "recorded changes and changed later, so their values then are unknown; "
            "choose a time after their first recorded change"
        )
    baselines = []
    for row_id, version in first.items():
        if row_id in valid or as_of >= as_utc(version["valid_from"]):
            continue
        if (
            table == "CFG_BUSINESS_RULES"
            and version.get("migration") == "engine/0029_rule_target_contracts.sql"
        ):
            baselines.append(version | {"target_table_id": None})
        elif (
            table == "CFG_TASKS"
            and version.get("migration") == "engine/0030_task_target_contracts.sql"
        ):
            baselines.append(version | {"target_table_id": None, "target_active_flag": "Y"})
        elif (
            table == "CFG_TASK_PARAMETERS"
            and version.get("migration") == "engine/0030_task_target_contracts.sql"
            and version.get("parameter_name") == "TARGET_OBJECT"
            and version["operation"] == "DELETE"
        ):
            baselines.append(version)
    rows = list(valid.values()) + baselines + [row for row in current if str(row[key]) not in first]
    return rows


def _rows(conn: Connection, sql: str) -> list[Row]:
    """Return every row ``sql`` selects, keyed by lower-case column name."""
    return [
        {str(name).lower(): value for name, value in row.items()}
        for row in conn.execute(text(sql)).mappings()
    ]


def _names(conn: Connection, table: str) -> list[str]:
    result = conn.execute(text(f"SELECT * FROM {table} WHERE 1 = 0"))
    return [str(name).lower() for name in result.keys()]  # noqa: SIM118 - a result, not a dict


def _cell(value: object) -> object:
    """Keep a value as SQLite can hold it: nested JSON, decimals and dates as text."""
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, (Decimal, date)):
        return value.isoformat() if isinstance(value, date) else str(value)
    return value
