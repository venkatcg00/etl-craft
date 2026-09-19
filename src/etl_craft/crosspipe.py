"""Cross-pipeline dependency resolution — the "self-check/poll step" from CLAUDE.md.

Same-pipeline dependencies compile into native Airflow task chaining (or are
checked live against a shared `pipeline_run_id` for a manual/backfill run —
see `resolver.py`). Cross-pipeline edges (`CFG_PIPELINE_DEPENDENCY`, and
`CFG_TASK_DEPENDENCY` rows where `DEPENDS_ON_PIPELINE_ID != PIPELINE_ID`)
have no DAG-native equivalent in any orchestrator, so they're always
resolved here, at runtime, per CLAUDE.md's "Dependency resolution &
polling" section:

  * Poll only while the dependency is genuinely `IN-PROGRESS` (its own
    latest run/task-run row) — if it isn't, go straight to the tracker
    comparison below rather than polling blindly.
  * While polling, use the confirmed poll cadence: a hard 1-hour timeout,
    duration-aware intervals (first poll at 70% of the dependency's average
    run duration, next at 80%, +10% each poll after, capped at 30 polls).
    Resolved directly with the user: this percentage schedule is the only
    mechanism — CLAUDE.md's stray "exponential backoff of 60 minutes"
    mention is superseded, not a second real mechanism.
  * A candidate run satisfies the edge only if it's newer than whatever was
    already recorded *consumed* for that specific edge in
    `AUD_PIPELINE_DEPENDENCY_TRACKER`/`AUD_TASK_DEPENDENCY_TRACKER` (no
    tracker row yet == never consumed == any qualifying run counts) *and*
    matches the edge's own `DEPENDENCY_TYPE`, mirroring resolver.py's own
    same-pipeline edge semantics exactly (SUCCESS/FAILURE/ALWAYS/HAS_DATA).
  * The tracker only updates *after* the gated task/pipeline completes —
    `consume_*_dependency_edges` below, called from the finalize point, not
    the gate-check point, so a downstream run that dies before finishing
    never "burns" a run it didn't actually get to use.

[CHOICE] Pipeline-level `HAS_DATA` (confirmed with the user): schema.sql's
CFG_PIPELINE_DEPENDENCY allows it, but AUD_PIPELINES_RUN_LOG has no
TARGET_COUNT of its own — it means "the run succeeded AND at least one of
its own tasks reported TARGET_COUNT > 0" (via AUD_TASK_RUN_LOG), the
pipeline-grain analogue of resolver.py's task-level rule.

[ADDITION] `DEFAULT_ASSUMED_DURATION_SECONDS`/`MIN_POLL_INTERVAL_SECONDS`
are this module's own engineering choices, not specified anywhere: a new
pipeline/task pairing with no run-duration history yet has nothing to
compute 70%/80%/etc. of, so a seed default keeps the percentage schedule
well-defined; the minimum floor exists purely so an "already overdue"
check (elapsed already past the target fraction) doesn't hot-loop through
all 30 polls near-instantly with an effectively-zero delay between them.

[CHOICE] Every public function here takes an `Engine`, not a `Connection`,
and opens a fresh short-lived connection per query rather than holding one
open across the whole check (including, for the poll path, across real
`time.sleep()` calls that can span up to an hour). An earlier version of
this module took a single `Connection` throughout — functionally correct,
but wrong for a real deployment: a task genuinely polling for close to an
hour would tie up one pooled connection the entire time, and with many
concurrent Airflow-triggered tasks each doing the same thing, that's a real
path to connection-pool exhaustion, not a theoretical one. Found and fixed
during review, before anything using it shipped.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from etl_craft.cfg import (
    PipelineDependencyEdgeId,
    TaskCrossPipelineDependencyId,
    fetch_pipeline_dependency_edge_ids,
    fetch_task_cross_pipeline_dependency_ids,
)

FIRST_POLL_FRACTION = 0.70
POLL_FRACTION_STEP = 0.10
MAX_POLLS = 30
POLL_TIMEOUT_SECONDS = 3600.0
MIN_POLL_INTERVAL_SECONDS = 1.0
DEFAULT_ASSUMED_DURATION_SECONDS = 300.0

SleepFn = Callable[[float], None]
NowFn = Callable[[], datetime]


def _default_now() -> datetime:
    return datetime.now(UTC)


def _next_poll_delay(
    avg_duration_seconds: float, elapsed_seconds: float, fraction: float, remaining_deadline: float
) -> float:
    """Pure poll-interval math — how long to sleep before the next status check."""
    target = avg_duration_seconds * fraction
    delay = max(MIN_POLL_INTERVAL_SECONDS, target - elapsed_seconds)
    return max(0.0, min(delay, remaining_deadline))


@dataclass(frozen=True)
class _LatestRun:
    run_id: int
    status: str
    start_date: datetime


# ==============================================================================
# Pipeline-level (CFG_PIPELINE_DEPENDENCY / AUD_PIPELINES_RUN_LOG)
# ==============================================================================

_PIPELINE_CONDITION_SQL = {
    "SUCCESS": "r.STATUS = 'SUCCESS'",
    "FAILURE": "r.STATUS = 'FAILED'",
    "ALWAYS": "r.STATUS IN ('SUCCESS','FAILED','SKIPPED')",
    "HAS_DATA": (
        "r.STATUS = 'SUCCESS' AND EXISTS (SELECT 1 FROM AUD_TASK_RUN_LOG t "
        "WHERE t.PIPELINE_RUN_ID = r.PIPELINE_RUN_ID AND t.TARGET_COUNT > 0)"
    ),
}


def _latest_pipeline_run(conn: Connection, pipeline_id: int) -> _LatestRun | None:
    row = conn.execute(
        text(
            "SELECT PIPELINE_RUN_ID AS run_id, STATUS AS status, START_DATE AS start_date "
            "FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_ID = :pipeline_id "
            "ORDER BY PIPELINE_RUN_ID DESC LIMIT 1"
        ),
        {"pipeline_id": pipeline_id},
    ).one_or_none()
    return None if row is None else _LatestRun(row.run_id, row.status, row.start_date)


def _average_pipeline_duration_seconds(conn: Connection, pipeline_id: int) -> float | None:
    result = conn.execute(
        text(
            "SELECT AVG(EXTRACT(EPOCH FROM (END_DATE - START_DATE))) FROM AUD_PIPELINES_RUN_LOG "
            "WHERE PIPELINE_ID = :pipeline_id AND STATUS IN ('SUCCESS','FAILED','SKIPPED') "
            "AND END_DATE IS NOT NULL"
        ),
        {"pipeline_id": pipeline_id},
    ).scalar_one_or_none()
    return float(result) if result is not None else None


def _find_qualifying_pipeline_run(
    conn: Connection, depends_on_pipeline_id: int, dependency_type: str, after_run_id: int | None
) -> int | None:
    condition = _PIPELINE_CONDITION_SQL[dependency_type]
    return conn.execute(
        text(
            f"SELECT r.PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG r "
            f"WHERE r.PIPELINE_ID = :depends_on_pipeline_id AND {condition} "
            # Explicit CAST: a bind param compared only via "IS NULL"
            # (never against a typed column directly, since the OR's other
            # branch is what does that) leaves Postgres unable to infer its
            # type on its own — found for real, not theoretical
            # (AmbiguousParameter), the same class of "verify against real
            # Postgres" lesson as this repo's other raw-SQL pitfalls. Uses
            # CAST(...) rather than the `::` shorthand: SQLAlchemy's text()
            # treats a literal "::" right after a bind name as an escaped
            # colon, not a cast, so `:after_run_id::bigint` silently never
            # gets substituted at all (also found for real, not guessed).
            "AND (CAST(:after_run_id AS BIGINT) IS NULL "
            "OR r.PIPELINE_RUN_ID > CAST(:after_run_id AS BIGINT)) "
            "ORDER BY r.PIPELINE_RUN_ID DESC LIMIT 1"
        ),
        {"depends_on_pipeline_id": depends_on_pipeline_id, "after_run_id": after_run_id},
    ).scalar_one_or_none()


def _pipeline_tracker_last_consumed(conn: Connection, pipeline_dependency_id: int) -> int | None:
    return conn.execute(
        text(
            "SELECT LAST_CONSUMED_PIPELINE_RUN_ID FROM AUD_PIPELINE_DEPENDENCY_TRACKER "
            "WHERE PIPELINE_DEPENDENCY_ID = :id"
        ),
        {"id": pipeline_dependency_id},
    ).scalar_one_or_none()


def _wait_for_pipeline_dependency_to_settle(
    engine: Engine, depends_on_pipeline_id: int, *, sleep: SleepFn, now: NowFn
) -> None:
    """Poll while `depends_on_pipeline_id`'s latest run is IN-PROGRESS; return once it isn't.

    Opens a fresh, short-lived connection per status check — never holds
    one open across a `sleep()` call, which can span most of an hour.
    """
    with engine.connect() as conn:
        latest = _latest_pipeline_run(conn, depends_on_pipeline_id)
    if latest is None or latest.status != "IN-PROGRESS":
        return
    with engine.connect() as conn:
        avg_duration = (
            _average_pipeline_duration_seconds(conn, depends_on_pipeline_id)
            or DEFAULT_ASSUMED_DURATION_SECONDS
        )
    deadline = now() + timedelta(seconds=POLL_TIMEOUT_SECONDS)
    fraction = FIRST_POLL_FRACTION
    for _ in range(MAX_POLLS):
        current = now()
        if current >= deadline:
            return
        elapsed = (current - latest.start_date).total_seconds()
        remaining = (deadline - current).total_seconds()
        delay = _next_poll_delay(avg_duration, elapsed, fraction, remaining)
        if delay > 0:
            sleep(delay)
        fraction += POLL_FRACTION_STEP
        with engine.connect() as conn:
            latest = _latest_pipeline_run(conn, depends_on_pipeline_id)
        if latest is None or latest.status != "IN-PROGRESS":
            return
    # Poll cap or deadline hit while still IN-PROGRESS — give up waiting.
    # The tracker comparison right after this call will correctly find
    # nothing new yet, same as if we'd never polled at all.


def _pipeline_dependency_satisfied(
    conn: Connection, edge: PipelineDependencyEdgeId
) -> tuple[bool, int | None]:
    last_consumed = _pipeline_tracker_last_consumed(conn, edge.pipeline_dependency_id)
    candidate = _find_qualifying_pipeline_run(
        conn, edge.depends_on_pipeline_id, edge.dependency_type, last_consumed
    )
    return candidate is not None, candidate


def check_pipeline_dependencies(
    engine: Engine,
    pipeline_id: int,
    *,
    sleep: SleepFn = time.sleep,
    now: NowFn = _default_now,
) -> str | None:
    """Check (and poll) `pipeline_id`'s active cross-pipeline edges; None if satisfied."""
    with engine.connect() as conn:
        edges = fetch_pipeline_dependency_edge_ids(conn, pipeline_id)
    for edge in edges:
        _wait_for_pipeline_dependency_to_settle(
            engine, edge.depends_on_pipeline_id, sleep=sleep, now=now
        )
        with engine.connect() as conn:
            satisfied, _ = _pipeline_dependency_satisfied(conn, edge)
        if not satisfied:
            return (
                f"cross-pipeline dependency on pipeline_id={edge.depends_on_pipeline_id} "
                f"({edge.dependency_type}) not satisfied"
            )
    return None


def consume_pipeline_dependency_edges(engine: Engine, pipeline_id: int) -> None:
    """After `pipeline_id`'s own run finishes, advance each edge's tracker if newly satisfied."""
    with engine.connect() as conn:
        edges = fetch_pipeline_dependency_edge_ids(conn, pipeline_id)
    for edge in edges:
        with engine.begin() as conn:
            satisfied, candidate = _pipeline_dependency_satisfied(conn, edge)
            if satisfied:
                conn.execute(
                    text(
                        "INSERT INTO AUD_PIPELINE_DEPENDENCY_TRACKER (PIPELINE_DEPENDENCY_ID, "
                        "PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, LAST_CONSUMED_PIPELINE_RUN_ID, "
                        "LAST_CONSUMED_END_DATE, LAST_UPDATED_TIMESTAMP) "
                        "SELECT :edge_id, :pipeline_id, :depends_on_pipeline_id, :run_id, "
                        "(SELECT END_DATE FROM AUD_PIPELINES_RUN_LOG "
                        "WHERE PIPELINE_RUN_ID = :run_id), now() "
                        "ON CONFLICT (PIPELINE_DEPENDENCY_ID) DO UPDATE SET "
                        "LAST_CONSUMED_PIPELINE_RUN_ID = EXCLUDED.LAST_CONSUMED_PIPELINE_RUN_ID, "
                        "LAST_CONSUMED_END_DATE = EXCLUDED.LAST_CONSUMED_END_DATE, "
                        "LAST_UPDATED_TIMESTAMP = EXCLUDED.LAST_UPDATED_TIMESTAMP"
                    ),
                    {
                        "edge_id": edge.pipeline_dependency_id,
                        "pipeline_id": pipeline_id,
                        "depends_on_pipeline_id": edge.depends_on_pipeline_id,
                        "run_id": candidate,
                    },
                )


# ==============================================================================
# Task-level (cross-pipeline CFG_TASK_DEPENDENCY rows / AUD_TASK_RUN_LOG)
# ==============================================================================

_TASK_CONDITION_SQL = {
    "SUCCESS": "STATUS = 'SUCCESS'",
    "FAILURE": "STATUS = 'FAILED'",
    "ALWAYS": "STATUS IN ('SUCCESS','FAILED','SKIPPED')",
    "HAS_DATA": "STATUS = 'SUCCESS' AND TARGET_COUNT > 0",
}


def _latest_task_run(conn: Connection, task_id: int) -> _LatestRun | None:
    row = conn.execute(
        text(
            "SELECT TASK_RUN_ID AS run_id, STATUS AS status, START_DATE AS start_date "
            "FROM AUD_TASK_RUN_LOG WHERE TASK_ID = :task_id ORDER BY TASK_RUN_ID DESC LIMIT 1"
        ),
        {"task_id": task_id},
    ).one_or_none()
    return None if row is None else _LatestRun(row.run_id, row.status, row.start_date)


def _average_task_duration_seconds(conn: Connection, task_id: int) -> float | None:
    result = conn.execute(
        text(
            "SELECT AVG(EXTRACT(EPOCH FROM (END_DATE - START_DATE))) FROM AUD_TASK_RUN_LOG "
            "WHERE TASK_ID = :task_id AND STATUS IN ('SUCCESS','FAILED','SKIPPED') "
            "AND END_DATE IS NOT NULL"
        ),
        {"task_id": task_id},
    ).scalar_one_or_none()
    return float(result) if result is not None else None


def _find_qualifying_task_run(
    conn: Connection, depends_on_task_id: int, dependency_type: str, after_run_id: int | None
) -> int | None:
    condition = _TASK_CONDITION_SQL[dependency_type]
    return conn.execute(
        text(
            f"SELECT TASK_RUN_ID FROM AUD_TASK_RUN_LOG "
            f"WHERE TASK_ID = :depends_on_task_id AND {condition} "
            # See _find_qualifying_pipeline_run's own comment on this cast.
            "AND (CAST(:after_run_id AS BIGINT) IS NULL "
            "OR TASK_RUN_ID > CAST(:after_run_id AS BIGINT)) "
            "ORDER BY TASK_RUN_ID DESC LIMIT 1"
        ),
        {"depends_on_task_id": depends_on_task_id, "after_run_id": after_run_id},
    ).scalar_one_or_none()


def _task_tracker_last_consumed(conn: Connection, task_dependency_id: int) -> int | None:
    return conn.execute(
        text(
            "SELECT LAST_CONSUMED_TASK_RUN_ID FROM AUD_TASK_DEPENDENCY_TRACKER "
            "WHERE TASK_DEPENDENCY_ID = :id"
        ),
        {"id": task_dependency_id},
    ).scalar_one_or_none()


def _wait_for_task_dependency_to_settle(
    engine: Engine, depends_on_task_id: int, *, sleep: SleepFn, now: NowFn
) -> None:
    """Poll while `depends_on_task_id`'s latest run is IN-PROGRESS; return once it isn't.

    Opens a fresh, short-lived connection per status check — see
    `_wait_for_pipeline_dependency_to_settle`'s own docstring for why.
    """
    with engine.connect() as conn:
        latest = _latest_task_run(conn, depends_on_task_id)
    if latest is None or latest.status != "IN-PROGRESS":
        return
    with engine.connect() as conn:
        avg_duration = (
            _average_task_duration_seconds(conn, depends_on_task_id)
            or DEFAULT_ASSUMED_DURATION_SECONDS
        )
    deadline = now() + timedelta(seconds=POLL_TIMEOUT_SECONDS)
    fraction = FIRST_POLL_FRACTION
    for _ in range(MAX_POLLS):
        current = now()
        if current >= deadline:
            return
        elapsed = (current - latest.start_date).total_seconds()
        remaining = (deadline - current).total_seconds()
        delay = _next_poll_delay(avg_duration, elapsed, fraction, remaining)
        if delay > 0:
            sleep(delay)
        fraction += POLL_FRACTION_STEP
        with engine.connect() as conn:
            latest = _latest_task_run(conn, depends_on_task_id)
        if latest is None or latest.status != "IN-PROGRESS":
            return


def _task_dependency_satisfied(
    conn: Connection, edge: TaskCrossPipelineDependencyId
) -> tuple[bool, int | None]:
    last_consumed = _task_tracker_last_consumed(conn, edge.task_dependency_id)
    candidate = _find_qualifying_task_run(
        conn, edge.depends_on_task_id, edge.dependency_type, last_consumed
    )
    return candidate is not None, candidate


def check_task_cross_pipeline_dependencies(
    engine: Engine,
    task_id: int,
    *,
    sleep: SleepFn = time.sleep,
    now: NowFn = _default_now,
) -> str | None:
    """Check (and poll) `task_id`'s active cross-pipeline edges; None if satisfied."""
    with engine.connect() as conn:
        edges = fetch_task_cross_pipeline_dependency_ids(conn, task_id)
    for edge in edges:
        _wait_for_task_dependency_to_settle(engine, edge.depends_on_task_id, sleep=sleep, now=now)
        with engine.connect() as conn:
            satisfied, _ = _task_dependency_satisfied(conn, edge)
        if not satisfied:
            return (
                f"cross-pipeline task dependency on task_id={edge.depends_on_task_id} "
                f"({edge.dependency_type}) not satisfied"
            )
    return None


def consume_task_dependency_edges(engine: Engine, task_id: int) -> None:
    """After `task_id`'s own run finishes, advance each edge's tracker if newly satisfied."""
    with engine.connect() as conn:
        edges = fetch_task_cross_pipeline_dependency_ids(conn, task_id)
    for edge in edges:
        with engine.begin() as conn:
            satisfied, candidate = _task_dependency_satisfied(conn, edge)
            if satisfied:
                conn.execute(
                    text(
                        "INSERT INTO AUD_TASK_DEPENDENCY_TRACKER (TASK_DEPENDENCY_ID, TASK_ID, "
                        "PIPELINE_ID, DEPENDS_ON_TASK_ID, DEPENDS_ON_PIPELINE_ID, "
                        "LAST_CONSUMED_TASK_RUN_ID, LAST_CONSUMED_END_DATE, "
                        "LAST_UPDATED_TIMESTAMP) "
                        "SELECT :edge_id, :task_id, :pipeline_id, :depends_on_task_id, "
                        ":depends_on_pipeline_id, :run_id, "
                        "(SELECT END_DATE FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID = :run_id), "
                        "now() "
                        "ON CONFLICT (TASK_DEPENDENCY_ID) DO UPDATE SET "
                        "LAST_CONSUMED_TASK_RUN_ID = EXCLUDED.LAST_CONSUMED_TASK_RUN_ID, "
                        "LAST_CONSUMED_END_DATE = EXCLUDED.LAST_CONSUMED_END_DATE, "
                        "LAST_UPDATED_TIMESTAMP = EXCLUDED.LAST_UPDATED_TIMESTAMP"
                    ),
                    {
                        "edge_id": edge.task_dependency_id,
                        "task_id": task_id,
                        "pipeline_id": edge.pipeline_id,
                        "depends_on_task_id": edge.depends_on_task_id,
                        "depends_on_pipeline_id": edge.depends_on_pipeline_id,
                        "run_id": candidate,
                    },
                )
