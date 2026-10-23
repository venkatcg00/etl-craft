"""Event conditions checked by the scheduler without occupying a task agent."""

from __future__ import annotations

import glob
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import HandlerError
from etl_craft.engine.repository.events import consumed_keys
from etl_craft.handlers.resources import LocalLocker
from etl_craft.handlers.sql.analysis import event_query_problem, read_only_problem
from etl_craft.warehouse.connection import open_warehouse, warehouse_dialect

PARAMETERS = frozenset(
    {
        "EVENT_FILE",
        "EVENT_SQL",
        "EVENT_KEY_COLUMN",
        "EVENT_MAX_ROWS",
        "EVENT_SETTLE_SECONDS",
        "EVENT_TIMEOUT_MINUTES",
        "EVENT_ON_TIMEOUT",
        "EVENT_POLL_SECONDS",
    }
)


@dataclass(frozen=True)
class Event:
    """One file glob or warehouse query, with its settling and timeout policy."""

    file: str | None
    sql: str | None
    settle_seconds: float
    timeout_seconds: float | None
    on_timeout: str
    poll_seconds: float = 60
    key_column: str | None = None
    max_rows: int = 10000
    task_id: int = 0
    session_tag: str | None = None


def read_event(config: ConnectorConfig, params: Mapping[str, str]) -> Event:
    """Validate a condition without checking the filesystem or connecting to the warehouse."""
    file = (params.get("EVENT_FILE") or "").strip() or None
    sql = (params.get("EVENT_SQL") or "").strip() or None
    key = (params.get("EVENT_KEY_COLUMN") or "").strip() or None
    if bool(file) == bool(sql):
        raise HandlerError("EVENT_WAIT needs exactly one of EVENT_FILE or EVENT_SQL; set one")
    if file and "\x00" in file:
        raise HandlerError(f"EVENT_FILE={file!r} contains a NUL; use a valid filesystem glob")
    if file and key:
        raise HandlerError(
            f"EVENT_KEY_COLUMN={key!r} cannot name a column on EVENT_FILE={file!r}; "
            "keys require an EVENT_SQL result; remove EVENT_KEY_COLUMN or use EVENT_SQL"
        )
    maximum = params.get("EVENT_MAX_ROWS", "10000")
    try:
        max_rows = int(maximum)
    except ValueError:
        max_rows = 0
    if not maximum.isascii() or not maximum.isdecimal() or max_rows < 1:
        raise HandlerError(
            f"EVENT_MAX_ROWS={maximum!r}; expected a positive whole number; correct EVENT_MAX_ROWS"
        )
    if sql:
        if config.warehouse is None:
            raise HandlerError("EVENT_SQL needs a Warehouse section in craft-connector.yml")
        dialect = warehouse_dialect(config).sqlglot_dialect
        problem = read_only_problem(sql, dialect) or event_query_problem(sql, dialect, key)
        if problem:
            raise HandlerError(
                f"EVENT_SQL must be one read-only SELECT; it {problem}; fix the query"
            )
    timeout = (params.get("EVENT_ON_TIMEOUT") or "FAIL").strip().upper()
    if timeout not in {"FAIL", "SKIP"}:
        raise HandlerError(f"EVENT_ON_TIMEOUT={timeout!r}; expected FAIL or SKIP; set one")
    indefinite = params.get("EVENT_TIMEOUT_MINUTES", "").strip().upper() == "NONE"
    minutes = None if indefinite else _seconds(params, "EVENT_TIMEOUT_MINUTES", 60)
    poll = _seconds(params, "EVENT_POLL_SECONDS", 60)
    if poll < 1:
        raise HandlerError("EVENT_POLL_SECONDS must be >= 1; increase the check interval")
    try:
        datetime.now(UTC) + timedelta(seconds=max((minutes or 0) * 60, poll))
    except OverflowError as error:
        raise HandlerError(
            "EVENT_TIMEOUT_MINUTES or EVENT_POLL_SECONDS exceeds the supported datetime range; "
            "set a smaller finite wait budget or NONE for an indefinite timeout"
        ) from error
    return Event(
        file,
        sql,
        _seconds(params, "EVENT_SETTLE_SECONDS", 30),
        None if minutes is None else minutes * 60,
        timeout,
        poll,
        key,
        max_rows,
    )


def _seconds(params: Mapping[str, str], name: str, default: float) -> float:
    value = params.get(name, str(default))
    try:
        number = float(value)
    except ValueError:
        number = math.nan
    if not math.isfinite(number) or number < 0:
        raise HandlerError(
            f"{name}={value!r}; expected a finite number >= 0; correct the parameter"
        )
    return number


def probe(
    event: Event,
    config: ConnectorConfig,
    engine: Engine | None,
    now: datetime,
    observations: dict[str, Any],
    consumed: set[str],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Return triggering evidence, or new durable file observations for the next check."""
    if event.sql:
        with (
            open_warehouse(
                config,
                LocalLocker(engine, config) if engine is not None else None,
                wait_seconds=0.1,
                session_tag=event.session_tag,
            ) as warehouse,
            warehouse.connect() as conn,
        ):
            sql = warehouse_dialect(config).event_probe_sql(event.sql, event.max_rows + 1)
            with conn.execute(text(sql)) as result:
                columns = list(result.keys())
                rows = [dict(row) for row in result.mappings().fetchmany(event.max_rows + 1)]
            if len(rows) > event.max_rows:
                raise HandlerError(
                    f"EVENT_SQL returned at least {len(rows)} rows, above "
                    f"EVENT_MAX_ROWS={event.max_rows}; expected a bounded window; "
                    "narrow EVENT_SQL's window or increase EVENT_MAX_ROWS"
                )
            if event.key_column:
                selected = _keyed_rows(event.key_column, columns, rows)
                if engine is not None and event.task_id:
                    with engine.connect() as engine_conn:
                        taken = consumed_keys(engine_conn, event.task_id, selected)
                    selected = {k: v for k, v in selected.items() if k not in taken}
                if not selected:
                    return None, {}
                return {"keys": list(selected), "rows": list(selected.values())}, {}
            return ({"row": rows[0]} if rows else None), {}
    assert event.file is not None
    pattern = event.file if Path(event.file).is_absolute() else str(config.project_dir / event.file)
    current: dict[str, Any] = {}
    settled = []
    for written in sorted(glob.glob(pattern, recursive=True)):
        path = Path(written).resolve()
        name = str(path)
        if name in current or name in consumed or not path.is_file():
            continue
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        previous = observations.get(name, {})
        unchanged = (
            previous.get("size") == stat.st_size and previous.get("mtime_ns") == stat.st_mtime_ns
        )
        since = previous["since"] if unchanged else now.timestamp()
        current[name] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "since": since}
        if now.timestamp() - since >= event.settle_seconds:
            settled.append({"path": name, "size": stat.st_size, "mtime_ns": stat.st_mtime_ns})
    return ({"files": settled} if settled else None), current


def _keyed_rows(
    key_column: str, columns: list[str], rows: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """Return each signal's key and payload once, in query order, using the query's casing."""
    names = [name for name in columns if name.lower() == key_column.lower()]
    if len(names) != 1:
        raise HandlerError(
            f"EVENT_SQL returns columns {columns!r}, not one EVENT_KEY_COLUMN={key_column!r}; "
            "select that column exactly once or correct EVENT_KEY_COLUMN"
        )
    selected: dict[str, dict[str, Any]] = {}
    for row in rows:
        value = row.pop(names[0])
        if (
            value is None
            or isinstance(value, (Mapping, list, tuple, set, bytes))
            or str(value) == ""
        ):
            raise HandlerError(
                f"EVENT_KEY_COLUMN={key_column!r} returned {value!r}; expected a nonempty "
                "scalar event id; correct EVENT_SQL to select stable ids"
            )
        key = str(value)
        if key in selected and selected[key] != row:
            raise HandlerError(
                f"EVENT_KEY_COLUMN={key_column!r} returned conflicting rows for {key!r}; "
                "expected one payload per event id; correct EVENT_SQL or the signal table"
            )
        selected[key] = row
    return selected
