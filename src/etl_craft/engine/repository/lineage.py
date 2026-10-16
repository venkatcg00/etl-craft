"""Derived task lineage cache and append-only table lineage versions."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from sqlalchemy import BigInteger, String, bindparam
from sqlalchemy.engine import Connection
from sqlalchemy.types import TupleType

from etl_craft.core.enums import YesNo
from etl_craft.core.text import sha256_hex
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class SqlTaskRef:
    """An active SQL task of an active pipeline."""

    task_id: int
    pipeline_code: str
    task_code: str


def fetch_sql_tasks(conn: Connection) -> list[SqlTaskRef]:
    """Return every active SQL task, by pipeline and task code."""
    return [SqlTaskRef(**r._mapping) for r in conn.execute(statement(conn, "sql_tasks"))]


@dataclass(frozen=True)
class StoredEdge:
    """One target column and its source components; constants have no source."""

    target_catalog_name: str | None
    target_schema_name: str
    target_table_name: str
    target_column: str
    source_catalog_name: str | None
    source_schema_name: str | None
    source_table_name: str | None
    source_column: str | None
    transformation: str


@dataclass(frozen=True)
class RecordedEdge(StoredEdge):
    """An edge together with the identity of its writing task."""

    task_id: int


@dataclass(frozen=True)
class HistoricalEdge(RecordedEdge):
    """An edge labelled with its writing task for graph display."""

    task: str
    lineage_version_id: int


@dataclass(frozen=True)
class LineageVersion:
    """One table's edge fingerprint and validity interval."""

    lineage_version_id: int
    catalog_name: str | None
    schema_name: str
    table_name: str
    version_number: int
    edges_hash: str
    valid_from: datetime
    valid_to: datetime
    current_flag: YesNo

    @property
    def identity(self) -> tuple[str | None, str, str]:
        """The recorded target components, independent of current configuration."""
        return self.catalog_name, self.schema_name, self.table_name


def fetch_lineage_cache(conn: Connection) -> dict[tuple[int, str], list[StoredEdge]]:
    """Read active SQL tasks' edges grouped by their complete computation keys."""
    cache: dict[tuple[int, str], list[StoredEdge]] = {}
    for row in conn.execute(statement(conn, "catalog_lineage_cache")):
        values = dict(row._mapping)
        key = values.pop("task_id"), values.pop("source_sql_hash")
        cache.setdefault(key, []).append(StoredEdge(**values))
    return cache


def store_task_lineage(
    conn: Connection, task_id: int, source_sql_hash: str, edges: Sequence[StoredEdge]
) -> None:
    """Replace the lineage stored for ``task_id``."""
    conn.execute(statement(conn, "delete_task_lineage"), {"task_id": task_id})
    if edges:
        now = datetime.now(UTC)
        conn.execute(
            statement(conn, "insert_lineage_edge"),
            [
                asdict(e) | {"task_id": task_id, "source_sql_hash": source_sql_hash, "now": now}
                for e in edges
            ],
        )


def fetch_lineage_versions(conn: Connection, *, current_only: bool = False) -> list[LineageVersion]:
    """Return every table version in recording order, with UTC validity bounds."""
    return [
        LineageVersion(
            **dict(r._mapping)
            | {
                "current_flag": YesNo(r.current_flag),
                "valid_from": as_utc(r.valid_from),
                "valid_to": as_utc(r.valid_to),
            }
        )
        for r in conn.execute(
            statement(conn, "lineage_versions"), {"current_only": int(current_only)}
        )
    ]


def fetch_version_edges(conn: Connection, version_id: int | None) -> list[HistoricalEdge]:
    """Read a version's edges without consulting the task cache or parsing SQL."""
    return [
        HistoricalEdge(**r._mapping)
        for r in conn.execute(statement(conn, "lineage_version_edges"), {"version_id": version_id})
    ]


def record_lineage_versions(
    conn: Connection, catalog: str | None, keys: Mapping[int, str | None]
) -> None:
    """Publish changed combined edges; the caller holds the lineage recording lock."""
    grouped: dict[tuple[str | None, str, str], list[RecordedEdge]] = defaultdict(list)
    query = statement(conn, "lineage_cache_edges").bindparams(
        bindparam("confirmed_keys", expanding=True, type_=TupleType(BigInteger(), String())),
        bindparam("failed_tasks", expanding=True),
    )
    rows = conn.execute(
        query,
        {
            "catalog": catalog,
            "confirmed_keys": [(task, key) for task, key in keys.items() if key is not None],
            "failed_tasks": [task for task, key in keys.items() if key is None],
        },
    )
    for row in rows:
        edge = RecordedEdge(**row._mapping)
        grouped[(edge.target_catalog_name, edge.target_schema_name, edge.target_table_name)].append(
            edge
        )
    versions = fetch_lineage_versions(conn, current_only=True)
    current = {v.identity: v for v in versions if v.current_flag == YesNo.YES}
    now = datetime.now(UTC)
    for identity, current_version in current.items():
        if identity not in grouped:
            conn.execute(
                statement(conn, "lineage_version_close"),
                {"version_id": current_version.lineage_version_id, "now": now},
            )
    for identity, edges in grouped.items():
        fingerprint = sha256_hex(
            json.dumps(
                sorted({json.dumps(asdict(edge), sort_keys=True) for edge in edges})
            ).encode()
        )
        previous = current.get(identity)
        if previous is not None and previous.edges_hash == fingerprint:
            continue
        if previous is not None:
            conn.execute(
                statement(conn, "lineage_version_close"),
                {"version_id": previous.lineage_version_id, "now": now},
            )
        number = conn.execute(
            statement(conn, "lineage_version_next"),
            {"catalog_name": identity[0], "schema_name": identity[1], "table_name": identity[2]},
        ).scalar_one()
        version_id = conn.execute(
            statement(conn, "lineage_version_insert"),
            {
                "catalog_name": identity[0],
                "schema_name": identity[1],
                "table_name": identity[2],
                "version_number": number,
                "edges_hash": fingerprint,
                "now": now,
                "forever": datetime(9999, 12, 31, tzinfo=UTC),
            },
        ).scalar_one()
        conn.execute(
            statement(conn, "lineage_version_edge_insert"),
            [asdict(edge) | {"lineage_version_id": version_id} for edge in edges],
        )
