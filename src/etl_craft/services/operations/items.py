"""Exact-run item inspection and audited operator controls."""

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import ClassVar
from uuid import uuid4

from sqlalchemy.engine import Connection, Row

from etl_craft.core.actor import current_actor
from etl_craft.core.enums import Handler, InterventionAction, ItemAction, ItemStatus, RunStatus
from etl_craft.core.errors import ResourceNotFoundError, RunRefusedError, UsageError
from etl_craft.core.items import ItemProgress, ItemRecord, progress, valid_item_key
from etl_craft.engine import controls, items
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.intervention_rows import InterventionDetails, record_intervention
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import Request, require_text


@dataclass(frozen=True, kw_only=True)
class ItemInput(Request):
    """A task's items are always selected by an explicit run identity."""

    pipeline_code: str
    task_code: str
    run_id: int

    def __post_init__(self) -> None:
        """Refuse incomplete scope before any state can change."""
        require_text("pipeline_code", self.pipeline_code)
        require_text("task_code", self.task_code)
        if self.run_id < 1:
            raise UsageError(
                f"run_id={self.run_id}; expected a positive run id; select the exact run"
            )


@dataclass(frozen=True, kw_only=True)
class ItemControl(ItemInput):
    """Control one named item, or the failed items, with a recorded reason."""

    action: ItemAction
    reason: str
    item_key: str | None = None
    all_failed: bool = False
    status: ItemStatus | None = None
    rows: int = 0

    def __post_init__(self) -> None:
        """Keep selection exclusive and marks terminal."""
        super().__post_init__()
        try:
            object.__setattr__(self, "action", ItemAction(self.action))
            object.__setattr__(
                self, "status", None if self.status is None else ItemStatus(self.status)
            )
        except ValueError as error:
            raise UsageError(
                f"item action={self.action!r}, status={self.status!r}; expected an item action "
                "and terminal mark status; choose the documented action/status values"
            ) from error
        require_text("reason", self.reason)
        if self.all_failed == (self.item_key is not None):
            raise UsageError(
                "item selection: expected exactly one of item_key and all_failed; select "
                "the work to change"
            )
        if self.item_key is not None and not valid_item_key(self.item_key):
            raise UsageError(
                f"item_key={self.item_key!r}; expected UTF-8 text of 1-512 characters without NUL; "
                "choose a recorded stable item key"
            )
        if self.action == ItemAction.MARK and self.status not in {
            ItemStatus.SUCCESS,
            ItemStatus.FAILED,
            ItemStatus.SKIPPED,
        }:
            raise UsageError(
                f"item mark status={self.status}; expected SUCCESS, FAILED or "
                f"SKIPPED; choose a terminal mark"
            )
        if self.action != ItemAction.MARK and (self.status is not None or self.rows != 0):
            raise UsageError(
                f"item action={self.action}, status={self.status}, rows={self.rows}; "
                "expected status/rows only for MARK; remove those fields"
            )
        if type(self.rows) is not int or self.rows < 0:
            raise UsageError(f"item rows={self.rows}; expected a nonnegative count; correct rows")


@dataclass(frozen=True)
class ItemView:
    """The same persisted item rows and counts behind every user-facing view."""

    SCHEMA: ClassVar[str] = "etl-craft/items/1"
    task_run_id: int
    records: tuple[ItemRecord, ...]
    progress: ItemProgress


def inspect(ctx: OperationContext, request: ItemInput) -> ItemView:
    """Read items only from the explicitly selected pipeline run and task."""
    with ctx.engine.connect() as conn:
        task = conn.execute(statement(conn, "item_task"), vars(request)).one_or_none()
        if task is None:
            raise ResourceNotFoundError(
                f"{request.pipeline_code}.{request.task_code} run {request.run_id}: "
                f"task run missing; select a run where this task started"
            )
        records = items.latest(conn, task.task_run_id)
        return ItemView(task.task_run_id, records, progress(records))


def control(ctx: OperationContext, request: ItemControl) -> ItemView:
    """Commit a mark or a truthful pending cancellation; reruns retain other successes."""
    with ctx.engine.begin() as conn:
        controls.lock_run(conn, request.run_id)
        task = conn.execute(statement(conn, "item_task"), vars(request)).one_or_none()
        if task is None or task.handler != Handler.PYTHON or task.attempt_id is None:
            raise RunRefusedError(
                f"{request.task_code} run {request.run_id}: no admitted Python "
                f"attempt; expected recorded items; run the Python task first"
            )
        if request.action == ItemAction.RERUN and task.status in {
            RunStatus.QUEUED,
            RunStatus.IN_PROGRESS,
        }:
            raise RunRefusedError(
                f"{request.task_code}: task status={task.status}; expected an ended "
                f"task before rerunning items; wait for the task to finish or cancel "
                f"it"
            )
        records = items.latest(conn, task.task_run_id)
        selected = [
            r
            for r in records
            if (r.status == ItemStatus.FAILED if request.all_failed else r.key == request.item_key)
        ]
        if not selected:
            raise ResourceNotFoundError(
                f"{request.task_code}: item "
                f"selection={request.item_key or 'all failed'} found no items; "
                f"inspect the exact task run and choose a recorded key"
            )
        for old in selected:
            change(conn, task, old, request)
    if request.action == ItemAction.RERUN:
        from etl_craft.services.operations.requests import RunRequest
        from etl_craft.services.operations.runs import execute_run

        execute_run(
            ctx,
            RunRequest(
                pipeline_code=request.pipeline_code,
                task_code=request.task_code,
                run_id=request.run_id,
                rerun=True,
                reason=request.reason,
            ),
        )
    return inspect(
        ctx,
        ItemInput(
            pipeline_code=request.pipeline_code, task_code=request.task_code, run_id=request.run_id
        ),
    )


def change(
    conn: Connection, task: Row[tuple[object, ...]], old: ItemRecord, request: ItemControl
) -> None:
    """Serialize the decision with item starts and endings under the same run lock."""
    if old.started is not None and old.ended is None and old.status == ItemStatus.FAILED:
        raise RunRefusedError(
            f"item {old.key}: its task ended without an item ending; expected known completed "
            "work before an override; recover the task to establish this item's outcome"
        )
    live = old.status in {ItemStatus.RUNNING, ItemStatus.CANCEL_REQUESTED}
    if live and request.action != ItemAction.CANCEL:
        raise RunRefusedError(
            f"item {old.key}: status={old.status}; expected an ended item for "
            f"{request.action}; cancel and wait for acknowledgement first"
        )
    if request.action == ItemAction.CANCEL:
        status = ItemStatus.CANCEL_REQUESTED if live else ItemStatus.CANCELLED
    elif request.action == ItemAction.RERUN:
        status = ItemStatus.QUEUED
    else:
        assert request.status is not None
        status = request.status
    ending = replace(
        old,
        status=status,
        row_count=request.rows if request.action == ItemAction.MARK else old.row_count,
        error=request.reason,
        report_id=str(uuid4()),
        ended=None if live else datetime.now(UTC).isoformat(),
    )
    items.append(conn, task.task_run_id, task.attempt_id, ending)
    record_intervention(
        conn,
        pipeline_id=task.pipeline_id,
        pipeline_run_id=task.run_id,
        task_id=task.task_id,
        action=InterventionAction(request.action),
        reason=request.reason,
        requested_by=current_actor().name,
        from_status=old.status,
        to_status=status,
        target_count=request.rows,
        details=InterventionDetails(item_key=old.key),
    )
