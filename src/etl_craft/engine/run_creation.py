"""Create a run and pin its executable metadata and optional bundle in one statement."""

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from uuid import uuid4

from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError

from etl_craft.core.actor import Actor, sso_groups
from etl_craft.core.enums import RunStatus, TriggerKind, YesNo
from etl_craft.core.errors import ConfigurationError, RunStateError, StaleTransitionError
from etl_craft.engine import runlog, standing_pauses
from etl_craft.engine.metrics import lifecycle
from etl_craft.engine.queries import statement
from etl_craft.engine.refreshes import admitted


@dataclass(frozen=True)
class RunCreation:
    """The caller's immutable logical run identity and initial state."""

    run_date: date | None = None
    trigger_kind: str = TriggerKind.MANUAL
    run_key: str | None = None
    status: str = RunStatus.IN_PROGRESS


def create(conn: Connection, pipeline_id: int, actor: Actor, options: RunCreation) -> int:
    """Create an active run or a queued/skipped schedule with its exact key."""
    from etl_craft.engine.transitions import actor_parameters

    run_date, trigger_kind, run_key, status = (
        options.run_date,
        options.trigger_kind,
        options.run_key,
        options.status,
    )
    _validate(pipeline_id, status, trigger_kind)
    refresh = (
        admitted(conn, pipeline_id)
        if conn.engine.get_execution_options().get("refresh_required")
        and status != RunStatus.SKIPPED
        else None
    )
    standing_pauses.lock(conn, pipeline_id)
    key = run_key or f"{trigger_kind.lower().replace('_', '-')}:{uuid4()}"
    try:
        with conn.begin_nested():
            result = conn.execute(
                statement(conn, "transition_create_run"),
                {
                    **actor_parameters(actor),
                    "pipeline_id": pipeline_id,
                    "use_refresh": refresh is not None,
                    "refresh_snapshot": None if refresh is None else refresh[0],
                    "refresh_version": None if refresh is None else refresh[1],
                    "refresh_bundle": None if refresh is None else refresh[2],
                    "use_bundles": bool(
                        conn.engine.get_execution_options().get("bundles_enabled", False)
                    ),
                    "sso_group_ids": json.dumps(sso_groups.get()),
                    "run_date": run_date or runlog.today(),
                    "status": status,
                    "end_date": datetime.now(UTC) if status == RunStatus.SKIPPED else None,
                    "ended_by": actor.name if status == RunStatus.SKIPPED else None,
                    "ended_by_kind": actor.kind.value if status == RunStatus.SKIPPED else None,
                    "backfill": YesNo.of(trigger_kind == TriggerKind.BACKFILL),
                    "trigger_kind": trigger_kind,
                    "run_key": key,
                },
            )
            run_id = result.scalar_one_or_none()
            if run_id is None:
                raise ConfigurationError(
                    f"pipeline_id={pipeline_id}: Artifacts.Store is set but no "
                    f"bundle is published; "
                    "expected an admitted project bundle; run `etl-craft bundle publish` first"
                )
            if status != RunStatus.SKIPPED:
                standing_pauses.pin(conn, pipeline_id, int(run_id), actor)
            if status == RunStatus.SKIPPED:
                lifecycle(conn, "run", int(run_id), status)
            elif status == RunStatus.IN_PROGRESS:
                lifecycle(conn, "admission", int(run_id))
            return int(run_id)
    except IntegrityError as error:
        found = (
            conn.execute(
                statement(conn, "run_conflict"), {"pipeline_id": pipeline_id, "run_key": key}
            )
            .mappings()
            .one_or_none()
        )
        state = "invalid parent" if found is None else repr(dict(found))
        raise StaleTransitionError(
            f"pipeline_id={pipeline_id}, run_key={key!r}: expected no active run and a new key, "
            f"owner=None; found {state}. Check run history before retrying."
        ) from error


def _validate(pipeline_id: int, status: str, trigger_kind: str) -> None:
    """Refuse invalid initial identities before taking admission locks."""
    if status not in {RunStatus.IN_PROGRESS, RunStatus.QUEUED, RunStatus.SKIPPED} or (
        status == RunStatus.SKIPPED and trigger_kind != TriggerKind.SCHEDULE
    ):
        raise RunStateError(
            f"pipeline_id={pipeline_id}: initial status={status!r}; "
            "only schedules may start SKIPPED"
        )
    if trigger_kind not in set(TriggerKind):
        raise RunStateError(
            f"pipeline_id={pipeline_id}: invalid trigger_kind={trigger_kind!r}; "
            "use SCHEDULE, MANUAL, BACKFILL, ORCHESTRATOR or STAND_IN"
        )
