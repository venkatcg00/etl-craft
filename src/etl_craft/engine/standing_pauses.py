"""Versioned task pauses and the exact outcomes captured when a run is admitted."""

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import BigInteger, bindparam
from sqlalchemy.engine import Connection

from etl_craft.core.actor import SYSTEM_ACTOR, Actor, acting_as
from etl_craft.core.enums import Handler, InterventionAction, RunStatus, ScriptKind
from etl_craft.core.script_kinds import script_kind
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.intervention_rows import InterventionDetails, record_intervention
from etl_craft.engine.repository.tasks import fetch_task_execution_detail, fetch_task_parameters


@dataclass(frozen=True)
class TaskPause:
    """An immutable pause version; closure and expiry determine whether it still holds."""

    pause_id: int
    pipeline_id: int
    task_id: int
    outcome: RunStatus | None
    expires_at: datetime | None
    paused_at: datetime
    actor: str
    reason: str
    task_code: str = ""

    def __post_init__(self) -> None:
        """Normalize SQLite and PostgreSQL timestamps to the same public UTC values."""
        object.__setattr__(self, "paused_at", as_utc(self.paused_at))
        if self.expires_at is not None:
            object.__setattr__(self, "expires_at", as_utc(self.expires_at))

    def active(self, now: datetime) -> bool:
        """Expired versions stop applying without modifying their history."""
        return self.expires_at is None or as_utc(self.expires_at) > now

    def describe(self) -> str:
        """Show the control's identity and every operator-owned field."""
        return (
            f"{self.task_code}: standing pause {self.pause_id}: actor={self.actor}, "
            f"outcome={self.outcome or 'hold'}, "
            f"expiry={self.expires_at or 'until resume'}, reason={self.reason!r}"
        )


def lock(conn: Connection, pipeline_id: int) -> None:
    """Serialize pause changes with admission using a transaction-scoped database lock."""
    from etl_craft.dialects.engine import for_engine
    from etl_craft.engine.locks import advisory_key

    for_engine(conn).standing_pause_lock(conn, advisory_key(f"standing-pause:{pipeline_id}"))


def current(conn: Connection, pipeline_id: int | None) -> tuple[TaskPause, ...]:
    """Read open versions including expired ones, which a new pause may close."""
    return tuple(
        TaskPause(**row._mapping)
        for row in conn.execute(
            statement(conn, "standing_pause_current").bindparams(
                bindparam("pipeline_id", type_=BigInteger)
            ),
            {"pipeline_id": pipeline_id},
        )
    )


def pin(conn: Connection, pipeline_id: int, run_id: int, actor: Actor) -> None:
    """Record each effective pause version atomically with run admission."""
    now = datetime.now(UTC)
    for pause in current(conn, pipeline_id):
        if pause.active(now):
            record_intervention(
                conn,
                pipeline_id=pipeline_id,
                pipeline_run_id=run_id,
                task_id=pause.task_id,
                action=InterventionAction.PAUSE_ADMISSION,
                to_status=pause.outcome,
                reason=pause.reason,
                requested_by=actor.name,
                details=InterventionDetails(item_key=str(pause.pause_id)),
            )


def at_task(
    conn: Connection, pipeline_id: int, run_id: int, task_id: int, now: datetime
) -> tuple[TaskPause | None, bool]:
    """Return the admitted outcome or a live hold, never inherit a newer pause outcome."""
    lock(conn, pipeline_id)
    return effects(conn, pipeline_id, run_id, now).get(task_id, (None, False))


def effects(
    conn: Connection, pipeline_id: int, run_id: int, now: datetime
) -> dict[int, tuple[TaskPause, bool]]:
    """Read all effective versions in two statements, including earlier admissions' holds."""
    live_pauses = current(conn, pipeline_id)
    if not live_pauses:
        return {}
    admitted = {
        row.task_id: TaskPause(**row._mapping)
        for row in conn.execute(statement(conn, "standing_pause_admission"), {"run_id": run_id})
    }
    result = {}
    for live in live_pauses:
        if not live.active(now):
            continue
        pinned = admitted.get(live.task_id)
        if pinned is None or pinned.outcome is None or not pinned.active(now):
            result[live.task_id] = (live, True)
        else:
            result[live.task_id] = (pinned, False)
    return result


@acting_as(SYSTEM_ACTOR)
def apply(conn: Connection, pause: TaskPause, run_id: int) -> None:
    """Publish an outcome with zero data and no attempt, retry policy or alert execution."""
    from etl_craft.engine import transitions

    previous = applied(conn, run_id, pause.task_id)
    if previous is not None and previous.outcome is not None:
        return
    params = fetch_task_parameters(conn, pause.task_id)
    utility = fetch_task_execution_detail(conn, pause.task_id).handler == Handler.PYTHON and (
        script_kind(params, pause.task_code) == ScriptKind.UTILITY
    )
    count = None if utility else 0
    binding = transitions.find_or_create_task_run(conn, pause.task_id, run_id)
    assert pause.outcome is not None
    transitions.mark_task_run(
        conn,
        binding.task_run_id,
        status=pause.outcome,
        error_message=(
            f"{pause.outcome.lower()} by standing pause {pause.pause_id}: "
            f"actor={pause.actor}, expiry={pause.expires_at or 'until resume'}, "
            f"reason={pause.reason}"
        ),
        target_count=count,
    )
    conn.execute(
        statement(conn, "standing_pause_summary"),
        {"task_run_id": binding.task_run_id, "count": count},
    )
    record_intervention(
        conn,
        pipeline_id=pause.pipeline_id,
        pipeline_run_id=run_id,
        task_id=pause.task_id,
        action=InterventionAction.PAUSE_APPLIED,
        to_status=pause.outcome,
        target_count=count,
        reason=pause.reason,
        requested_by=SYSTEM_ACTOR.name,
        details=InterventionDetails(item_key=str(pause.pause_id)),
    )


def applied(conn: Connection, run_id: int, task_id: int) -> TaskPause | None:
    """Return the historical applied version even after replacement, resume or expiry."""
    row = conn.execute(
        statement(conn, "standing_pause_applied"),
        {
            "run_id": run_id,
            "task_id": task_id,
        },
    ).one_or_none()
    return None if row is None else TaskPause(**row._mapping)


@acting_as(SYSTEM_ACTOR)
def hold(conn: Connection, pause: TaskPause, run_id: int) -> None:
    """Record a reached hold once per version without creating an attempt or task ending."""
    previous = applied(conn, run_id, pause.task_id)
    if previous is not None and previous.pause_id == pause.pause_id:
        return
    record_intervention(
        conn,
        pipeline_id=pause.pipeline_id,
        pipeline_run_id=run_id,
        task_id=pause.task_id,
        action=InterventionAction.PAUSE_APPLIED,
        reason=pause.reason,
        requested_by=SYSTEM_ACTOR.name,
        details=InterventionDetails(item_key=str(pause.pause_id)),
    )
