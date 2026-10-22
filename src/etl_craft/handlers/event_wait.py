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
from etl_craft.handlers.resources import LocalLocker
from etl_craft.handlers.sql.analysis import read_only_problem
from etl_craft.warehouse.connection import open_warehouse, warehouse_dialect

PARAMETERS = frozenset(
    {
        "EVENT_FILE",
        "EVENT_SQL",
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


def read_event(config: ConnectorConfig, params: Mapping[str, str]) -> Event:
    """Validate a condition without checking the filesystem or connecting to the warehouse."""
    file = (params.get("EVENT_FILE") or "").strip() or None
    sql = (params.get("EVENT_SQL") or "").strip() or None
    if bool(file) == bool(sql):
        raise HandlerError("EVENT_WAIT needs exactly one of EVENT_FILE or EVENT_SQL; set one")
    if file and "\x00" in file:
        raise HandlerError(f"EVENT_FILE={file!r} contains a NUL; use a valid filesystem glob")
    if sql:
        if config.warehouse is None:
            raise HandlerError("EVENT_SQL needs a Warehouse section in craft-connector.yml")
        problem = read_only_problem(sql, warehouse_dialect(config).sqlglot_dialect)
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
    engine: Engine,
    now: datetime,
    observations: dict[str, Any],
    consumed: set[str],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Return triggering evidence, or new durable file observations for the next check."""
    if event.sql:
        with (
            open_warehouse(config, LocalLocker(engine, config), wait_seconds=0.1) as warehouse,
            warehouse.connect() as conn,
        ):
            row = conn.execute(text(event.sql)).first()
            return (None if row is None else {"row": dict(row._mapping)}), {}
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
