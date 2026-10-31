"""Operator marks, definition pauses and dependency overrides for local runs.

etl-craft is the orchestrator, so it is where an operator steps in:

- ``mark`` sets a task of the selected run, or the run itself, to ``SUCCESS``,
  ``FAILED`` or ``SKIPPED``, with a reason. A marked ``SUCCESS`` satisfies a ``HAS_DATA``
  dependency only with a stated row count. Marking a task of a run that has ended reopens the
  run, and the tasks the engine skipped without running are reset, so running the pipeline again
  resumes it from there: mark a failed task ``SUCCESS`` and its dependents run.
- ``mark --new-run`` records a finished stand-in run, with a task in it if named, so a gate on
  this pipeline passes where it cannot really run (an upstream that only exists elsewhere).
- ``run --skip`` records a run ``SKIPPED`` on purpose, as ``mark --new-run`` does, so the
  pipelines that depend on it see a run that did nothing.
- ``pause`` stops a pipeline from running until ``resume``: ``run`` starts nothing of it, and a
  run in progress starts no more tasks and stays ``IN-PROGRESS``, to go on once resumed. Pauses
  are recorded in ``AUD_PIPELINE_PAUSES``.

Every change is recorded in ``AUD_RUN_INTERVENTIONS`` with what the row held before, who asked
and why, and no attempt, log or error is erased.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass

from sqlalchemy.engine import Connection, Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import Actor, current_actor
from etl_craft.core.enums import (
    FINISHED_RUN_STATUSES,
    MARKABLE_STATUSES,
    GatePolicy,
    Handler,
    InterventionAction,
    OffsetType,
    RunStatus,
    ScriptKind,
)
from etl_craft.core.errors import HandlerError, RunRefusedError, RunStateError, UsageError
from etl_craft.core.governance import check_reason
from etl_craft.core.script_kinds import script_kind
from etl_craft.engine import controls, runlog, transitions
from etl_craft.engine.queries import statement
from etl_craft.engine.repository import intervention_rows as record
from etl_craft.engine.repository import pauses
from etl_craft.engine.repository.offsets import StoredOffset, fetch_task_offset, save_task_offset
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import (
    fetch_task_execution_detail,
    fetch_task_parameters,
    resolve_task_id,
)
from etl_craft.engine.stewardship import refuse_bypass
from etl_craft.execution.reconcile import reconcile
from etl_craft.handlers.registry import TaskContext
from etl_craft.handlers.sql.spec import read_sql_task
from etl_craft.scripting import Offset

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Intervened:
    """What an intervention did, in one line, and the run it changed."""

    message: str
    pipeline_run_id: int


def mark_task(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    task_code: str,
    status: str,
    reason: str,
    *,
    rows: int | None = None,
    stale: bool = False,
    requested_by: str | None = None,
    selector: runlog.RunSelector = runlog.ACTIVE_RUN,
) -> Intervened:
    """Mark the selected task; refuse live attempts and reconcile stale ones first."""
    status = _check(config, "mark", reason, status, rows)
    who = requested_by or current_actor().name
    lost = []
    if stale:
        with engine.connect() as conn:
            pipeline_id = resolve_pipeline_id(conn, pipeline_code)
            task_id = resolve_task_id(conn, pipeline_id, task_code)
        lost = reconcile(engine, pipeline_id=pipeline_id, task_id=task_id).lost
    with engine.begin() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        task_id = resolve_task_id(conn, pipeline_id, task_code)
        refuse_bypass(conn, pipeline_id, task_id, status, "mark")
        run_id, run_status = _selected_run(conn, pipeline_id, selector)
        before = runlog.fetch_task_run_status(conn, task_id, run_id)
        handed_off = conn.execute(
            statement(conn, "trigger_task_handoff"), {"task_id": task_id, "run_id": run_id}
        ).scalar_one_or_none()
        if before == RunStatus.IN_PROGRESS and handed_off is None:
            raise RunRefusedError(
                f"{pipeline_code}.{task_code} is running under pipeline_run_id={run_id}; wait "
                "for it to end, or end the run with `etl-craft stop` or `abort`. If the process "
                "running it is gone, give `stale` after its lease expires to record its outcome"
            )
        if before == status and rows is None and not lost:
            raise UsageError(
                f"{pipeline_code}.{task_code} is already {status} under "
                f"pipeline_run_id={run_id}; nothing to mark"
            )
        binding = transitions.find_or_create_task_run(conn, task_id, run_id)
        previous = runlog.fetch_task_run_result(conn, binding.task_run_id).error_message
        transitions.mark_task_run(
            conn,
            binding.task_run_id,
            status=status,
            error_message=(
                f"marked {status} by {who}"
                + (" (its process was gone)" if before == RunStatus.IN_PROGRESS else "")
                + f": {reason}"
            ),
            target_count=rows,
        )
        record.record_intervention(
            conn,
            pipeline_id=pipeline_id,
            pipeline_run_id=run_id,
            task_id=task_id,
            action=InterventionAction.MARK,
            from_status=before,
            to_status=status,
            target_count=rows,
            details=record.InterventionDetails(previous_message=previous),
            reason=reason,
            requested_by=who,
        )
        message = (
            f"{pipeline_code}.{task_code}: marked {status} under pipeline_run_id={run_id} "
            f"(was {before or 'not run'})"
        )
        if run_status in FINISHED_RUN_STATUSES:
            _reopen(conn, pipeline_id, pipeline_code, run_id, run_status, reason, who)
            message += f"; the run was {run_status} and is IN-PROGRESS again"
        reset, kept = _reset_skipped(conn, pipeline_id, run_id, reason, who)
        if reset:
            message += f"; reset to run again: {', '.join(reset)}"
        if kept:
            message += f"; kept SKIPPED: {', '.join(kept)}"
    if run_status != RunStatus.IN_PROGRESS or reset:
        message += f". Run `etl-craft run --pipeline-code {pipeline_code}` to resume it"
    logger.warning("%s (by %s: %s)", message, who, reason)
    return Intervened(message, run_id)


def mark_run(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    status: str,
    reason: str,
    *,
    requested_by: str | None = None,
    selector: runlog.RunSelector = runlog.ACTIVE_RUN,
) -> Intervened:
    """Mark the selected run ``status``, ending it now if it had not ended.

    A downstream pipeline's gate judges the marked status. Its tasks keep theirs, and no
    upstream run is consumed. Raises ``RunRefusedError`` while a task of the run is running.
    """
    status = _check(config, "mark", reason, status, None)
    who = requested_by or current_actor().name
    with engine.begin() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        run_id, run_status = _selected_run(conn, pipeline_id, selector)
        running = [
            row.task_code
            for row in record.fetch_task_rows(conn, run_id)
            if row.status == RunStatus.IN_PROGRESS
        ]
        if running:
            raise RunRefusedError(
                f"{pipeline_code}: tasks of pipeline_run_id={run_id} are running "
                f"({', '.join(running)}); end the run with `etl-craft stop` or `abort`, or wait"
            )
        if run_status == status:
            raise UsageError(
                f"{pipeline_code}: pipeline_run_id={run_id} is already {status}; nothing to mark"
            )
        transitions.mark_run(conn, run_id, status, current_actor())
        record.record_intervention(
            conn,
            pipeline_id=pipeline_id,
            pipeline_run_id=run_id,
            action=InterventionAction.MARK,
            from_status=run_status,
            to_status=status,
            reason=reason,
            requested_by=who,
        )
    message = f"{pipeline_code}: pipeline_run_id={run_id} marked {status} (was {run_status})"
    logger.warning("%s (by %s: %s)", message, who, reason)
    return Intervened(message, run_id)


def record_stand_in_run(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    status: str,
    reason: str,
    *,
    task_code: str | None = None,
    rows: int | None = None,
    requested_by: str | None = None,
) -> Intervened:
    """Record a finished run of ``pipeline_code`` that did not really run: ``mark --new-run``.

    Downstream gates judge it like any run: on the pipeline, or with ``task_code`` on that task,
    which gets ``status`` and ``rows``. Raises ``RunStateError`` while the pipeline has a run in
    progress.
    """
    status = _check(config, "a stand-in run", reason, status, rows)
    if rows is not None and task_code is None:
        raise UsageError("`rows` states a task's row count: name the task with `task_code`")
    who = requested_by or current_actor().name
    with engine.begin() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        task_id = None if task_code is None else resolve_task_id(conn, pipeline_id, task_code)
        active = runlog.fetch_exclusive_run_id(conn, pipeline_id)
        if active is not None:
            raise RunStateError(
                f"{pipeline_code} has a run in progress (pipeline_run_id={active}); mark that "
                "run, or cancel it, before recording a stand-in run"
            )
        created = transitions.create_active_run(
            conn, pipeline_id, run_date=runlog.today(config.timezone), trigger_kind="STAND_IN"
        )
        if created is None:
            raise RunStateError(
                f"{pipeline_code}: another process started a run just now "
                f"(pipeline_run_id={runlog.fetch_exclusive_run_id(conn, pipeline_id)}); "
                "mark that run, or cancel it, before recording a stand-in run"
            )
        run_id = created
        if task_id is not None:
            binding = transitions.find_or_create_task_run(conn, task_id, run_id)
            transitions.mark_task_run(
                conn,
                binding.task_run_id,
                status=status,
                error_message=f"stand-in run recorded {status} by {who}: {reason}",
                target_count=rows,
            )
        transitions.mark_run(conn, run_id, status, current_actor())
        record.record_intervention(
            conn,
            pipeline_id=pipeline_id,
            pipeline_run_id=run_id,
            task_id=task_id,
            action=InterventionAction.NEW_RUN,
            to_status=status,
            target_count=rows,
            reason=reason,
            requested_by=who,
        )
    what = pipeline_code if task_code is None else f"{pipeline_code}.{task_code}"
    message = f"{what}: stand-in run pipeline_run_id={run_id} recorded {status}"
    if rows is not None:
        message += f" with {rows} row(s)"
    logger.warning("%s (by %s: %s)", message, who, reason)
    return Intervened(message, run_id)


def skip_run(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    reason: str,
    *,
    requested_by: str | None = None,
) -> Intervened:
    """Record a run of ``pipeline_code`` ``SKIPPED`` on purpose: ``run --skip``.

    Raises ``RunStateError`` while the pipeline has a run in progress.
    """
    done = record_stand_in_run(
        engine, config, pipeline_code, RunStatus.SKIPPED, reason, requested_by=requested_by
    )
    return Intervened(
        f"{pipeline_code}: pipeline_run_id={done.pipeline_run_id} SKIPPED on purpose: {reason}",
        done.pipeline_run_id,
    )


def pause_pipeline(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    reason: str,
    *,
    requested_by: str | None = None,
) -> str:
    """Pause ``pipeline_code`` until it is resumed; return what was done, in one line.

    Raises ``RunStateError`` when it is already paused.
    """
    _check(config, "pause", reason, None, None)
    who = requested_by or current_actor().name
    with engine.begin() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        existing = pauses.fetch_open_pause(conn, pipeline_id)
        if existing is not None:
            raise RunStateError(f"{pipeline_code} is already {existing.describe()}")
        pauses.record_pause(conn, pipeline_id, who, reason)
        active = runlog.fetch_active_pipeline_run_id(conn, pipeline_id)
    message = f"{pipeline_code}: paused; `etl-craft run` starts nothing of it until resumed"
    if active is not None:
        message += (
            f", and pipeline_run_id={active}, in progress, starts no more tasks and stays "
            "IN-PROGRESS"
        )
    logger.warning("%s (by %s: %s)", message, who, reason)
    return message


def open_pause(
    engine: Engine,
    pipeline_code: str,
    *,
    selector: runlog.RunSelector | None = None,
    pipeline_id: int | None = None,
) -> pauses.Pause | None:
    """Return the open pause of ``pipeline_code``, or ``None`` when it is not paused."""
    with engine.connect() as conn:
        if pipeline_id is None:
            pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        pause = pauses.fetch_open_pause(conn, pipeline_id)
        if (
            pause is not None
            and selector is not None
            and (selector.run_id is not None or selector.run_key is not None)
        ):
            if selector.run_id is None and not any(
                r.run_key == selector.run_key for r in runlog.run_candidates(conn, pipeline_id)
            ):
                return pause
            selected = runlog.select_run(conn, pipeline_id, selector)
            if controls.permits_paused(conn, selected.pipeline_run_id):
                return None
        return pause


def record_change(
    engine: Engine,
    *,
    pipeline_id: int,
    pipeline_run_id: int,
    action: InterventionAction,
    reason: str,
    task_id: int | None = None,
    from_status: str | None = None,
    to_status: str | None = None,
) -> None:
    """Record one change an operator made to a run, from its own transaction."""
    with engine.begin() as conn:
        record.record_intervention(
            conn,
            pipeline_id=pipeline_id,
            pipeline_run_id=pipeline_run_id,
            task_id=task_id,
            action=action,
            from_status=from_status,
            to_status=to_status,
            reason=reason,
            requested_by=current_actor().name,
        )


def check_override(config: ConnectorConfig, option: str, reason: str | None) -> str:
    """Return the reason for ``option``, an override of the engine's checks.

    Raises ``UsageError`` without a reason.
    """
    return check_reason(reason)


def record_gate_bypass(
    engine: Engine,
    pipeline_id: int,
    pipeline_run_id: int,
    policy: GatePolicy,
    bypassed: Sequence[str],
    *,
    task_id: int | None = None,
) -> None:
    """Record that ``Dependency_gates`` let a run, or a task of it, through unsatisfied gates."""
    reason = f"Dependency_gates is {policy}: " + "; ".join(bypassed)
    with engine.begin() as conn:
        record.record_intervention(
            conn,
            pipeline_id=pipeline_id,
            pipeline_run_id=pipeline_run_id,
            task_id=task_id,
            action=InterventionAction.GATE_BYPASS,
            reason=reason,
            requested_by=current_actor().name,
        )
    logger.warning("pipeline_run_id=%d: %s", pipeline_run_id, reason)


def _check(
    config: ConnectorConfig, command: str, reason: str, status: str | None, rows: int | None
) -> RunStatus:
    check_reason(reason)
    if rows is not None and rows < 0:
        raise UsageError(f"`rows` is a row count, so it cannot be negative (got {rows})")
    if status is None:
        return RunStatus.IN_PROGRESS
    if status not in MARKABLE_STATUSES:
        raise UsageError(f"cannot mark {status!r}: choose one of {', '.join(MARKABLE_STATUSES)}")
    if rows is not None and status != RunStatus.SUCCESS:
        raise UsageError(
            "`rows` goes with SUCCESS: it is the row count a HAS_DATA dependency reads"
        )
    return RunStatus(status)


def _selected_run(
    conn: Connection, pipeline_id: int, selector: runlog.RunSelector
) -> tuple[int, str]:
    selected = runlog.select_run(conn, pipeline_id, selector)
    return selected.pipeline_run_id, selected.status


def _reopen(
    conn: Connection,
    pipeline_id: int,
    pipeline_code: str,
    run_id: int,
    run_status: str,
    reason: str,
    who: str,
) -> None:
    transitions.reopen_run(conn, run_id, Actor(who, current_actor().kind), reason=reason)


def reset_engine_skipped(
    engine: Engine,
    pipeline_id: int,
    run_id: int,
    reason: str,
    *,
    only: Collection[int] | None = None,
) -> tuple[list[str], list[str]]:
    """Reset, in a transaction of its own, the tasks the engine skipped without running.

    ``only`` limits it to those task ids. Return the tasks reset and, one line each, those kept
    because a downstream consumed them.
    """
    with engine.begin() as conn:
        return _reset_skipped(conn, pipeline_id, run_id, reason, current_actor().name, only=only)


def _reset_skipped(
    conn: Connection,
    pipeline_id: int,
    run_id: int,
    reason: str,
    who: str,
    *,
    only: Collection[int] | None = None,
) -> tuple[list[str], list[str]]:
    """Reset the tasks the engine skipped without running, so the resumed run decides again.

    A skipped row that another pipeline's run consumed, through an ``ALWAYS`` dependency, is
    part of that run's history, so it is kept as it is. Return the tasks reset, and a line per
    task kept.
    """
    reset = []
    kept = []
    for row in record.fetch_task_rows(conn, run_id):
        if row.status != RunStatus.SKIPPED or row.marked or row.has_rule_runs or row.has_attempts:
            continue
        if only is not None and row.task_id not in only:
            continue
        if row.consumed_by is not None:
            kept.append(f"{row.task_code} (consumed by {row.consumed_by})")
            continue
        record.record_intervention(
            conn,
            pipeline_id=pipeline_id,
            pipeline_run_id=run_id,
            task_id=row.task_id,
            action=InterventionAction.RESET,
            from_status=RunStatus.SKIPPED,
            details=record.InterventionDetails(previous_message=row.error_message),
            reason=reason,
            requested_by=who,
        )
        transitions.delete_skipped_task_run(conn, row.task_run_id)
        reset.append(row.task_code)
    return reset, kept


@dataclass(frozen=True)
class OffsetReset:
    """One deliberate move of a Python task's offset, and why."""

    pipeline_code: str
    task_code: str
    value: str
    reason: str
    watermark: bool = False


def reset_offset(
    engine: Engine,
    config: ConnectorConfig,
    change: OffsetReset,
    *,
    selector: runlog.RunSelector = runlog.ACTIVE_RUN,
) -> Intervened:
    """Reset every offset kind through one serialized, attributed mutation path."""
    label = "watermark" if change.watermark else "offset"
    _check(config, f"a {label} reset", change.reason, None, None)
    with engine.begin() as conn:
        conn.execute(statement(conn, "window_lock"))
        pipeline_id = resolve_pipeline_id(conn, change.pipeline_code)
        task_id = resolve_task_id(conn, pipeline_id, change.task_code)
        selected = runlog.select_run(conn, pipeline_id, selector)
        active = conn.execute(
            statement(conn, "watermark_active_attempts"), {"task_id": task_id}
        ).scalar_one()
        if active:
            raise RunRefusedError(
                f"{change.pipeline_code}.{change.task_code} has {active} active attempt(s); "
                f"wait for them to end or cancel and reconcile before resetting its {label}"
            )
        before = fetch_task_offset(conn, task_id)
        if before is not None and before.pending_task_run_id is not None:
            raise RunRefusedError(
                f"{change.pipeline_code}.{change.task_code}: pending timestamp window belongs "
                f"to task_run_id={before.pending_task_run_id}; resume its owner or cancel and "
                "reconcile all writers before resetting the cursor"
            )
        wanted, shown = _reset_definition(conn, config, change, task_id, before)
        save_task_offset(conn, task_id, StoredOffset(wanted.datatype, wanted.stored()))
        message = (
            f"{change.pipeline_code}.{change.task_code}: {label} {shown} "
            f"-> {wanted.stored()} ({wanted.datatype})"
        )
        record.record_intervention(
            conn,
            pipeline_id=pipeline_id,
            pipeline_run_id=selected.pipeline_run_id,
            task_id=task_id,
            action=InterventionAction.MARK,
            details=record.InterventionDetails(previous_message=message),
            reason=change.reason,
            requested_by=current_actor().name,
        )
    logger.warning("%s (by %s: %s)", message, current_actor().name, change.reason)
    return Intervened(message, selected.pipeline_run_id)


def _reset_definition(
    conn: Connection,
    config: ConnectorConfig,
    change: OffsetReset,
    task_id: int,
    before: StoredOffset | None,
) -> tuple[Offset, str]:
    """Check handler eligibility and typed values inside the common reset transaction."""
    detail = fetch_task_execution_detail(conn, task_id)
    params = fetch_task_parameters(conn, task_id)
    task = f"{change.pipeline_code}.{change.task_code}"
    if change.watermark:
        if detail.handler != Handler.SQL:
            raise UsageError(f"{task} has HANDLER={detail.handler}; `watermark` needs a SQL task")
        context = TaskContext(
            config,
            detail.pipeline_id,
            change.pipeline_code,
            task_id,
            change.task_code,
            0,
            0,
            0,
            Handler.SQL,
            detail.refresh_type,
            params,
        )
        spec = read_sql_task(context, before)
        if spec.watermark is None:
            raise UsageError(
                f"{task} has no WATERMARK_COLUMN; define a SQL watermark before resetting it"
            )
        return Offset(change.value, spec.watermark.start.datatype), spec.watermark.start.stored()
    if detail.handler != Handler.PYTHON:
        raise UsageError(
            f"{task} has HANDLER={detail.handler}; `offset` needs a Python task; "
            "a SQL task's position is reset with `watermark`"
        )
    if script_kind(params, task) == ScriptKind.UTILITY:
        raise UsageError(
            f"{task}: SCRIPT_KIND=UTILITY has no offset to reset; "
            "use SCRIPT_KIND=INGESTION for a source with a position"
        )
    try:
        wanted = _reset_value(task, before, change.value)
    except HandlerError as error:
        raise UsageError(f"{task}: `offset` {change.value!r}: {error}") from error
    shown = "none" if before is None else f"{before.offset_value} ({before.offset_type})"
    return wanted, shown


def _reset_value(task: str, before: StoredOffset | None, value: str) -> Offset:
    """Apply ``value`` to the stored offset: partition changes, or a new scalar value."""
    partitioned = before is None or before.offset_type == OffsetType.PARTITIONS
    if partitioned:
        try:
            changes = json.loads(value)
        except json.JSONDecodeError:
            changes = None
        if not isinstance(changes, dict):
            if before is None:
                raise UsageError(
                    f"{task} has no stored offset; give a PARTITIONS object such as "
                    f"'{{\"0\": 120}}', or let its first successful run store one"
                )
            raise UsageError(
                f"{task} stores a PARTITIONS offset; `offset` must be a JSON object of partition "
                f'positions, with null to remove one, such as \'{{"0": 120, "7": null}}\''
            )
        current: dict[str, object] = {}
        if before is not None and before.offset_value is not None:
            stored = Offset.from_stored(before.offset_type, before.offset_value).value
            assert isinstance(stored, Mapping)
            current = dict(stored)
        for key, position in changes.items():
            if position is None:
                if key not in current:
                    raise UsageError(f"{task} has no stored partition {key!r} to remove")
                del current[key]
            else:
                current[key] = position
        # Through the stored form, so every position is checked as a returned offset is.
        return Offset.from_stored(OffsetType.PARTITIONS, json.dumps(current))
    assert before is not None
    return Offset.from_stored(before.offset_type, value)


def reset_watermark(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    task_code: str,
    value: str,
    reason: str,
    *,
    selector: runlog.RunSelector = runlog.ACTIVE_RUN,
) -> Intervened:
    """Keep the watermark selector while sharing every reset mutation and ownership check."""
    return reset_offset(
        engine,
        config,
        OffsetReset(pipeline_code, task_code, value, reason, watermark=True),
        selector=selector,
    )
