"""``mark`` and ``cancel``: an operator's control over runs, in local mode.

In local mode etl-craft is the orchestrator, so it is where an operator steps in:

- ``mark`` sets a task of the pipeline's latest run, or the run itself, to ``SUCCESS``,
  ``FAILED`` or ``SKIPPED``, with a reason. A marked ``SUCCESS`` satisfies a ``HAS_DATA``
  dependency only with a stated row count. Marking a task of a run that has ended reopens the
  run, and the tasks the engine skipped without running are reset, so running the pipeline again
  resumes it from there: mark a failed task ``SUCCESS`` and its dependents run.
- ``mark --new-run`` records a finished stand-in run, with a task in it if named, so a gate on
  this pipeline passes where it cannot really run (an upstream that only exists elsewhere).
- ``cancel`` ends the pipeline's run ``CANCELLED``, with its running tasks. The process running
  each task watches for this and stops it; the process running the pipeline starts nothing more.

Every change is recorded in ``AUD_RUN_INTERVENTIONS`` with what the row held before, who asked
and why, and no attempt, log or error is erased. In remote mode the orchestrator is the only
source of truth, so these are refused: mark or clear the task in the orchestrator instead.
"""

from __future__ import annotations

import getpass
import logging
import socket
from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import (
    FINISHED_RUN_STATUSES,
    MARKABLE_STATUSES,
    GatePolicy,
    InterventionAction,
    Mode,
    RunStatus,
)
from etl_craft.core.errors import RunRefusedError, RunStateError, UsageError
from etl_craft.engine import runlog
from etl_craft.engine.queries import statement
from etl_craft.engine.repository import interventions as record
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import resolve_task_id

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Intervened:
    """What an intervention did, in one line, and the run it changed."""

    message: str
    pipeline_run_id: int


def current_operator() -> str:
    """Return who is asking, as ``user@host``, for ``REQUESTED_BY``."""
    try:
        user = getpass.getuser()
    except (KeyError, OSError):
        user = "unknown"
    return f"{user}@{socket.gethostname()}"


def mark_task(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    task_code: str,
    status: str,
    reason: str,
    *,
    rows: int | None = None,
    requested_by: str | None = None,
) -> Intervened:
    """Mark ``task_code`` ``status`` under the pipeline's latest run.

    A run that has ended is reopened, and the tasks the engine skipped without running are
    reset, so the next ``run --pipeline_code`` resumes it. Raises ``RunStateError`` when the
    pipeline has no run, and ``RunRefusedError`` in remote mode or while the task is running.
    """
    status = _check(config, "mark", reason, status, rows)
    who = requested_by or current_operator()
    with engine.begin() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        task_id = resolve_task_id(conn, pipeline_id, task_code)
        run_id, run_status = _latest_run(conn, pipeline_id, pipeline_code)
        before = runlog.fetch_task_run_status(conn, task_id, run_id)
        if before == RunStatus.IN_PROGRESS:
            raise RunRefusedError(
                f"{pipeline_code}.{task_code} is running under pipeline_run_id={run_id}; wait "
                "for it to end, or cancel the run with `etl-craft cancel`"
            )
        if before == status and rows is None:
            raise UsageError(
                f"{pipeline_code}.{task_code} is already {status} under "
                f"pipeline_run_id={run_id}; nothing to mark"
            )
        binding = runlog.find_or_create_task_run(conn, task_id, run_id)
        previous = runlog.fetch_task_run_result(conn, binding.task_run_id).error_message
        record.mark_task_run(
            conn,
            binding.task_run_id,
            status=status,
            error_message=f"marked {status} by {who}: {reason}",
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
            previous_message=previous,
            reason=reason,
            requested_by=who,
        )
        message = (
            f"{pipeline_code}.{task_code}: marked {status} under pipeline_run_id={run_id} "
            f"(was {before or 'not run'})"
        )
        if run_status in FINISHED_RUN_STATUSES or run_status == RunStatus.SKIPPED:
            _reopen(conn, pipeline_id, pipeline_code, run_id, run_status, reason, who)
            message += f"; the run was {run_status} and is IN-PROGRESS again"
        reset = _reset_skipped(conn, pipeline_id, run_id, reason, who)
        if reset:
            message += f"; reset to run again: {', '.join(reset)}"
    if run_status != RunStatus.IN_PROGRESS or reset:
        message += f". Run `etl-craft run --pipeline_code {pipeline_code}` to resume it"
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
) -> Intervened:
    """Mark the pipeline's latest run ``status``, ending it now if it had not ended.

    A downstream pipeline's gate judges the marked status. Its tasks keep theirs, and no
    upstream run is consumed. Raises ``RunRefusedError`` while a task of the run is running.
    """
    status = _check(config, "mark", reason, status, None)
    who = requested_by or current_operator()
    with engine.begin() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        run_id, run_status = _latest_run(conn, pipeline_id, pipeline_code)
        running = [
            row.task_code
            for row in record.fetch_task_rows(conn, run_id)
            if row.status == RunStatus.IN_PROGRESS
        ]
        if running:
            raise RunRefusedError(
                f"{pipeline_code}: tasks of pipeline_run_id={run_id} are running "
                f"({', '.join(running)}); cancel the run with `etl-craft cancel`, or wait"
            )
        if run_status == status:
            raise UsageError(
                f"{pipeline_code}: pipeline_run_id={run_id} is already {status}; nothing to mark"
            )
        record.mark_pipeline_run(conn, run_id, status)
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
    status = _check(config, "mark --new-run", reason, status, rows)
    if rows is not None and task_code is None:
        raise UsageError("--rows states a task's row count: name the task with --task_code")
    who = requested_by or current_operator()
    with engine.begin() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        task_id = None if task_code is None else resolve_task_id(conn, pipeline_id, task_code)
        active = runlog.fetch_active_pipeline_run_id(conn, pipeline_id)
        if active is not None:
            raise RunStateError(
                f"{pipeline_code} has a run in progress (pipeline_run_id={active}); mark that "
                "run, or cancel it, before recording a stand-in run"
            )
        run_id = runlog.find_or_create_active_run(conn, pipeline_id)
        if task_id is not None:
            binding = runlog.find_or_create_task_run(conn, task_id, run_id)
            record.mark_task_run(
                conn,
                binding.task_run_id,
                status=status,
                error_message=f"stand-in run recorded {status} by {who}: {reason}",
                target_count=rows,
            )
        record.mark_pipeline_run(conn, run_id, status)
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


def cancel_run(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    reason: str,
    *,
    requested_by: str | None = None,
) -> Intervened:
    """End the pipeline's run in progress ``CANCELLED``, with every task still running.

    The process running each task stops it within a few seconds, and the process running the
    pipeline starts nothing more. Raises ``RunStateError`` when no run is in progress.
    """
    _check(config, "cancel", reason, None, None)
    who = requested_by or current_operator()
    with engine.begin() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        run_id = runlog.fetch_active_pipeline_run_id(conn, pipeline_id)
        if run_id is None:
            raise RunStateError(f"{pipeline_code} has no run in progress to cancel")
        stopped = []
        for row in record.fetch_task_rows(conn, run_id):
            if row.status != RunStatus.IN_PROGRESS:
                continue
            if record.cancel_task_run(conn, row.task_run_id, f"cancelled by {who}: {reason}"):
                stopped.append(row.task_code)
                record.record_intervention(
                    conn,
                    pipeline_id=pipeline_id,
                    pipeline_run_id=run_id,
                    task_id=row.task_id,
                    action=InterventionAction.CANCEL,
                    from_status=RunStatus.IN_PROGRESS,
                    to_status=RunStatus.CANCELLED,
                    previous_message=row.error_message,
                    reason=reason,
                    requested_by=who,
                )
        record.cancel_pipeline_run(conn, run_id)
        record.record_intervention(
            conn,
            pipeline_id=pipeline_id,
            pipeline_run_id=run_id,
            action=InterventionAction.CANCEL,
            from_status=RunStatus.IN_PROGRESS,
            to_status=RunStatus.CANCELLED,
            reason=reason,
            requested_by=who,
        )
    message = f"{pipeline_code}: pipeline_run_id={run_id} CANCELLED"
    if stopped:
        message += (
            f"; stopping {len(stopped)} running task(s): {', '.join(stopped)} (the process "
            "running each stops it within a few seconds)"
        )
    logger.warning("%s (by %s: %s)", message, who, reason)
    return Intervened(message, run_id)


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
            requested_by=current_operator(),
        )


def check_override(config: ConnectorConfig, option: str, reason: str | None) -> str:
    """Return the reason for ``option``, an override of local mode's checks.

    Raises ``RunRefusedError`` in remote mode and ``UsageError`` without a reason.
    """
    if config.mode == Mode.REMOTE:
        raise RunRefusedError(
            f"{option} is only available in local mode: in remote mode run --task_code already "
            "runs the task whenever the orchestrator says (clear it in the orchestrator to run "
            "it again)"
        )
    if reason is None or not reason.strip():
        raise UsageError(f"{option} needs a --reason: it is recorded with the run")
    return reason


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
            requested_by=current_operator(),
        )
    logger.warning("pipeline_run_id=%d: %s", pipeline_run_id, reason)


def _check(
    config: ConnectorConfig, command: str, reason: str, status: str | None, rows: int | None
) -> RunStatus:
    if config.mode == Mode.REMOTE:
        raise RunRefusedError(
            f"{command} is only available in local mode: in remote mode the orchestrator is the "
            "only source of truth for runs, so mark, clear or stop the task in the orchestrator "
            "instead (in Airflow: Mark Success, Mark Failed, Clear)"
        )
    if not reason.strip():
        raise UsageError(f"{command} needs a --reason: it is recorded with the change")
    if rows is not None and rows < 0:
        raise UsageError(f"--rows is a row count, so it cannot be negative (got {rows})")
    if status is None:
        return RunStatus.IN_PROGRESS
    if status not in MARKABLE_STATUSES:
        raise UsageError(f"cannot mark {status!r}: choose one of {', '.join(MARKABLE_STATUSES)}")
    if rows is not None and status != RunStatus.SUCCESS:
        raise UsageError(
            "--rows goes with SUCCESS: it is the row count a HAS_DATA dependency reads"
        )
    return RunStatus(status)


def _latest_run(conn: Connection, pipeline_id: int, pipeline_code: str) -> tuple[int, str]:
    latest = conn.execute(
        statement(conn, "latest_pipeline_run"), {"pipeline_id": pipeline_id}
    ).one_or_none()
    if latest is None:
        raise RunStateError(
            f"{pipeline_code} has no run to mark; `etl-craft mark --new-run` records a stand-in run"
        )
    return int(latest.pipeline_run_id), str(latest.status)


def _reopen(
    conn: Connection,
    pipeline_id: int,
    pipeline_code: str,
    run_id: int,
    run_status: str,
    reason: str,
    who: str,
) -> None:
    try:
        with conn.begin_nested():
            conn.execute(statement(conn, "reopen_pipeline_run"), {"pipeline_run_id": run_id})
    except IntegrityError:
        raise RunStateError(
            f"{pipeline_code}: pipeline_run_id={run_id} cannot be reopened while another run "
            "of the pipeline is in progress"
        ) from None
    record.record_intervention(
        conn,
        pipeline_id=pipeline_id,
        pipeline_run_id=run_id,
        action=InterventionAction.REOPEN,
        from_status=run_status,
        to_status=RunStatus.IN_PROGRESS,
        reason=reason,
        requested_by=who,
    )


def _reset_skipped(
    conn: Connection, pipeline_id: int, run_id: int, reason: str, who: str
) -> list[str]:
    """Reset the tasks the engine skipped without running, so the resumed run decides again."""
    reset = []
    for row in record.fetch_task_rows(conn, run_id):
        if row.status != RunStatus.SKIPPED or row.marked or row.has_rule_runs:
            continue
        record.record_intervention(
            conn,
            pipeline_id=pipeline_id,
            pipeline_run_id=run_id,
            task_id=row.task_id,
            action=InterventionAction.RESET,
            from_status=RunStatus.SKIPPED,
            previous_message=row.error_message,
            reason=reason,
            requested_by=who,
        )
        record.delete_skipped_task_run(conn, row.task_run_id)
        reset.append(row.task_code)
    return reset
