"""Typed Engine DB archive snapshots and source revalidation."""

import json
from dataclasses import dataclass, replace

from sqlalchemy import TextClause, column, inspect
from sqlalchemy.engine import Connection
from sqlalchemy.types import NullType, TypeEngine

from etl_craft.core.archive import ArchiveColumn, ArchivePart, Scalar, encoded, kind_of
from etl_craft.core.enums import ArchiveKind
from etl_craft.core.errors import ArchiveError
from etl_craft.dialects.engine import for_engine
from etl_craft.dialects.engine.archive import candidate_query, projected, read_part
from etl_craft.engine.repository.pipelines import current_project_id


@dataclass(frozen=True)
class SourceColumn:
    """The physical source name/type and its portable archive representation."""

    name: str
    sql_type: TypeEngine[object] | NullType
    archive: ArchiveColumn


def shape(conn: Connection, table: str) -> tuple[tuple[SourceColumn, ...], tuple[str, ...]]:
    """Reflect original names so quoted SQLite and PostgreSQL columns read identically."""
    inspector = inspect(conn)
    actual = next(name for name in inspector.get_table_names() if name.upper() == table)
    definitions = []
    for c in inspector.get_columns(actual):
        try:
            kind = kind_of(c["type"].python_type)
        except NotImplementedError as error:
            raise ArchiveError(
                f"{table}.{c['name']}: unsupported source type {c['type']}; "
                "migrate to a supported scalar type before archive"
            ) from error
        name = str(c["name"])
        if name.upper().startswith("_ETL_ARCHIVE_"):
            raise ArchiveError(f"{table}.{name}: reserved archive column; rename it before purge")
        sql_type = NullType() if kind in (ArchiveKind.DECIMAL, ArchiveKind.FLOAT) else c["type"]
        definitions.append(SourceColumn(name, sql_type, ArchiveColumn(name.upper(), kind)))
    keys = tuple(str(k).upper() for k in inspector.get_pk_constraint(actual)["constrained_columns"])
    if table == "AUD_GATE_WAITS":
        keys = ("PIPELINE_RUN_ID", "TASK_ID", "WAIT_KIND")
    if not keys:
        raise ArchiveError(f"{table}: no stable archive key; restore its source key before purge")
    return tuple(definitions), keys


def capture_query(
    conn: Connection,
    table: str,
    query: TextClause,
    cutoff: object = None,
    max_bytes: int | None = None,
) -> ArchivePart:
    """Apply native source processors before freezing the complete row."""
    columns, keys = shape(conn, table)
    typed = projected(query, [c.name for c in columns]).columns(
        *(column(c.name.lower(), c.sql_type) for c in columns)
    )
    values: list[tuple[Scalar, ...]] = []
    size = 0
    conn.execution_options(stream_results=True, yield_per=1)
    result = conn.execute(typed, {"project_id": current_project_id(), "cutoff": cutoff})
    try:
        for row in result:
            value = tuple(encoded(c.archive, v) for c, v in zip(columns, row, strict=True))
            weight = len(json.dumps(value, ensure_ascii=False).encode())
            if max_bytes is not None and values and size + weight > max_bytes:
                break
            values.append(value)
            size += weight
    finally:
        result.close()
        conn.execution_options(stream_results=False, yield_per=None)
    return ArchivePart(table, tuple(c.archive for c in columns), keys, tuple(values))


def capture(conn: Connection, table: str, query_name: str, cutoff: object) -> ArchivePart:
    """Freeze the exact eligibility decision used by ordinary purge."""
    return capture_query(conn, table, candidate_query(query_name), cutoff, 8 * 1024 * 1024)


def unchanged(conn: Connection, part: ArchivePart) -> ArchivePart:
    """Retain source rows whose values or schema changed after preparation."""
    columns, _ = shape(conn, part.table)
    if tuple(c.archive for c in columns) != part.columns:
        return replace(part, rows=())
    current = capture_query(conn, part.table, read_part(part, for_engine(conn).archive_lock_suffix))
    return replace(part, rows=tuple(row for row in part.rows if row in set(current.rows)))


PARENTS = {
    "AUD_TASK_ATTEMPTS": ("AUD_TASK_RUN_LOG", "TASK_RUN_ID"),
    "AUD_TASK_RUN_LOG": ("AUD_PIPELINES_RUN_LOG", "PIPELINE_RUN_ID"),
    "AUD_BUSINESS_RULES_RESULTS": ("AUD_BUSINESS_RULES_RUN_LOG", "BUSINESS_RULE_RUN_ID"),
    "AUD_BUSINESS_RULES_RUN_LOG": ("AUD_TASK_RUN_LOG", "TASK_RUN_ID"),
    "AUD_ATTEMPT_ACKNOWLEDGEMENTS": ("AUD_TASK_ATTEMPTS", "ATTEMPT_ID"),
    "AUD_ATTEMPT_LOG_CHUNKS": ("AUD_TASK_ATTEMPTS", "ATTEMPT_ID"),
    "AUD_TASK_ITEMS": ("AUD_TASK_ATTEMPTS", "ATTEMPT_ID"),
    **dict.fromkeys(
        (
            "AUD_GATE_DECISIONS",
            "AUD_RUN_INTERVENTIONS",
            "AUD_CONTROL_REQUESTS",
            "AUD_DEPENDENCY_CONSUMPTION",
            "AUD_GATE_WAITS",
            "AUD_ACTIONS",
        ),
        ("AUD_PIPELINES_RUN_LOG", "PIPELINE_RUN_ID"),
    ),
}


def with_context(conn: Connection, root: ArchivePart) -> tuple[ArchivePart, ...]:
    """Capture ownership ancestors so revalidation can lock runs before their children."""
    while True:
        parts = [root]
        while parts[-1].table in PARENTS:
            parent, key = PARENTS[parts[-1].table]
            names = [c.name for c in parts[-1].columns]
            ids = {
                row[names.index(key)] for row in parts[-1].rows if row[names.index(key)] is not None
            }
            if not ids:
                return tuple(parts)
            selected = ArchivePart(
                parent,
                (ArchiveColumn(key, ArchiveKind.INTEGER),),
                (key,),
                tuple((value,) for value in sorted(ids, key=str)),
            )
            captured = capture_query(conn, parent, read_part(selected), max_bytes=8 * 1024 * 1024)
            if len(captured.rows) < len(ids):
                if len(root.rows) == 1:
                    raise ArchiveError(
                        f"{parent}: source ancestor missing; expected {len(ids)} rows; "
                        "restore the source relationship before archive"
                    )
                root = replace(root, rows=root.rows[: max(1, len(root.rows) // 2)])
                break
            parts.append(captured)
        else:
            return tuple(parts)
