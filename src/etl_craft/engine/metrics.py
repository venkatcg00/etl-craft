"""Process telemetry, cursor timing and transaction-bound lifecycle observations."""

import threading
import time
from datetime import UTC, datetime
from weakref import WeakKeyDictionary

from sqlalchemy import event
from sqlalchemy.engine import Connection, Engine, ExceptionContext, ExecutionContext
from sqlalchemy.pool import ConnectionPoolEntry

from etl_craft.core.enums import AttemptStatus
from etl_craft.core.metrics import Labels, Metrics
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement

_states: WeakKeyDictionary[Engine, Metrics] = WeakKeyDictionary()
_lock = threading.Lock()

FAMILIES = {
    "controller_leader": ("This process holds deployment leadership.", "gauge"),
    "controller_heartbeat_age_seconds": (
        "Seconds since this leader's recorded heartbeat.",
        "gauge",
    ),
    "runs": ("Current nonterminal pipeline runs by project and status.", "gauge"),
    "attempts": ("Current nonterminal attempts by project and status.", "gauge"),
    "queue_depth": ("Queued attempts by project, pool and slot kind.", "gauge"),
    "queue_oldest_age_seconds": ("Age of the oldest queued attempt.", "gauge"),
    "active_agents": ("Fresh ACTIVE agents by pool.", "gauge"),
    "agent_slots": ("Advertised, occupied and available agent slots.", "gauge"),
    "agent_running_children": ("RUNNING attempts owned by an agent.", "gauge"),
    "agent_heartbeat_age_seconds": ("Age of each current agent registration's heartbeat.", "gauge"),
    "gate_waits": ("Persisted active gate waits by project.", "gauge"),
    "runs_ended_total": ("Terminal run transitions observed by this process.", "counter"),
    "attempts_ended_total": ("Terminal attempt outcomes observed by this process.", "counter"),
    "lease_expiries_total": ("Attempts reconciled after ownership lease expiry.", "counter"),
    "lost_attempts_total": ("Attempts committed LOST by this process.", "counter"),
    "retries_total": ("Additional attempts queued for an existing task run.", "counter"),
    "admission_latency_seconds": ("Queued run creation to successful admission.", "histogram"),
    "dispatch_latency_seconds": ("Attempt queue time to successful ownership claim.", "histogram"),
    "engine_db_statement_latency_seconds": (
        "Cursor execution duration by catalog query.",
        "histogram",
    ),
}


def state(engine: Engine) -> Metrics | None:
    """Read an installed process registry without enabling telemetry for task subprocesses."""
    with _lock:
        return _states.get(engine)


def install(engine: Engine) -> Metrics:
    """Attach once to the controller's existing engine; retain no statement text."""
    with _lock:
        if engine in _states:
            return _states[engine]
        metrics = Metrics()
        for name, (help, kind) in FAMILIES.items():
            metrics.define(name, help, kind)
        _states[engine] = metrics
    _instrument(engine, metrics)
    return metrics


def _instrument(engine: Engine, metrics: Metrics) -> None:
    """Publish committed observations only after commit has returned successfully."""

    def flush(info: dict[str, object]) -> None:
        committed = info.pop("metrics_committed", [])
        assert isinstance(committed, list)
        for name, labels, value in committed:
            metrics.add(name, labels, value)

    @event.listens_for(engine, "begin")
    def begin(conn: Connection) -> None:
        flush(conn.info)
        conn.info["metrics_pending"] = {}
        conn.info["metrics_savepoints"] = []

    @event.listens_for(engine, "commit")
    def commit(conn: Connection) -> None:
        flush(conn.info)
        pending = conn.info.pop("metrics_pending", {})
        conn.info["metrics_committed"] = [
            sample for samples in pending.values() for sample in samples
        ]

    @event.listens_for(engine, "rollback")
    def rollback(conn: Connection) -> None:
        if not conn.invalidated:
            conn.info.pop("metrics_pending", None)
            conn.info.pop("metrics_committed", None)

    @event.listens_for(engine, "savepoint")
    def savepoint(conn: Connection, name: str | None) -> None:
        conn.info["metrics_savepoints"].append(dict(conn.info.get("metrics_pending", {})))

    @event.listens_for(engine, "rollback_savepoint")
    def undo(conn: Connection, name: str, context: object) -> None:
        conn.info["metrics_pending"] = conn.info["metrics_savepoints"].pop()

    @event.listens_for(engine, "release_savepoint")
    def release(conn: Connection, name: str, context: object) -> None:
        conn.info["metrics_savepoints"].pop()

    @event.listens_for(engine, "checkin")
    def checkin(driver: object, record: ConnectionPoolEntry) -> None:
        info = record.info
        if driver is not None:
            flush(info)
        info.pop("metrics_pending", None)
        info.pop("metrics_committed", None)

    _timing(engine, metrics)


def _timing(engine: Engine, metrics: Metrics) -> None:
    """Measure cursor execution without retaining SQL or bind values."""
    from etl_craft.dialects.engine import for_engine

    names = for_engine(engine).query_names()

    @event.listens_for(engine, "before_cursor_execute")
    def before(
        conn: Connection,
        cursor: object,
        sql: str,
        params: object,
        context: ExecutionContext,
        many: bool,
    ) -> None:
        conn.info["metrics_started"] = time.perf_counter()

    @event.listens_for(engine, "after_cursor_execute")
    def after(
        conn: Connection,
        cursor: object,
        sql: str,
        params: object,
        context: ExecutionContext,
        many: bool,
    ) -> None:
        started = conn.info.pop("metrics_started", None)
        if started is not None:
            query = context.execution_options.get("etl_craft_query", "unknown")
            query = query if query in names else "unknown"
            metrics.add(
                "engine_db_statement_latency_seconds",
                (("query", query),),
                time.perf_counter() - started,
            )

    @event.listens_for(engine, "handle_error")
    def failed(context: ExceptionContext) -> None:
        conn = context.connection
        if conn is not None and not conn.invalidated:
            conn.info.pop("metrics_committed", None)


def lifecycle(conn: Connection, kind: str, row_id: int, status: str = "") -> None:
    """Stage one exact transition; publication follows its enclosing successful commit."""
    metrics = state(conn.engine)
    if metrics is None or not metrics.leader:
        return
    query = "run" if kind in {"run", "admission"} else "attempt"
    row = conn.execute(statement(conn, "metrics_event_" + query), {"row_id": row_id}).one()
    labels: Labels = (("project", str(row.project_id)),)
    samples: list[tuple[str, Labels, float]] = []
    now = datetime.now(UTC)
    if kind == "run":
        samples.append(("runs_ended_total", (*labels, ("status", str(status))), 1))
    elif kind == "admission":
        samples.append(
            (
                "admission_latency_seconds",
                labels,
                max(0, (now - as_utc(row.started_at)).total_seconds()),
            )
        )
    elif kind == "retry":
        if row.attempt_number > 1:
            samples.append(("retries_total", labels, 1))
    elif kind == "dispatch":
        samples.append(
            (
                "dispatch_latency_seconds",
                labels,
                max(0, (now - as_utc(row.queued_at)).total_seconds()),
            )
        )
    else:
        status = status or row.status
        samples.append(("attempts_ended_total", (*labels, ("status", str(status))), 1))
        if status == AttemptStatus.LOST:
            samples.extend(
                (name, labels, 1) for name in ("lease_expiries_total", "lost_attempts_total")
            )
    conn.info.setdefault("metrics_pending", {})[(kind, row_id)] = samples
