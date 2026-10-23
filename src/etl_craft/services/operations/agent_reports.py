"""Fenced handler metadata, target ownership and atomic attempt endings over HTTP."""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime
from typing import TypeVar

from etl_craft.config.targets import active_catalog
from etl_craft.core.actor import acting_as
from etl_craft.core.counts import NO_COUNTS
from etl_craft.core.enums import (
    ActionName,
    ActionOutcome,
    AgentReport,
    AttemptStatus,
    Handler,
    YesNo,
)
from etl_craft.core.errors import (
    LockTimeoutError,
    PermissionDeniedError,
    StaleTransitionError,
    UsageError,
)
from etl_craft.core.ownership import attempt_holder
from etl_craft.core.text import qualify, sha256_hex
from etl_craft.core.time import as_utc
from etl_craft.engine import (
    attempt_cancellation,
    attempt_starts,
    controls,
    locks,
    resource_locks,
    transitions,
)
from etl_craft.engine.agent_state import fence
from etl_craft.engine.audit import ActionRequest
from etl_craft.engine.notifications import notification_hub
from etl_craft.engine.queries import statement
from etl_craft.engine.task_store import LocalTaskStore, TaskIdentity
from etl_craft.execution.context import build_task_context
from etl_craft.handlers.registry import TaskContext
from etl_craft.handlers.resources import LocalLocker
from etl_craft.services.operations.agent_models import AgentReply, AgentTask
from etl_craft.services.operations.agents import check_agent
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.serialization import encode
from etl_craft.warehouse.connection import WAREHOUSE_LOCK_KEY, is_single_writer

T = TypeVar("T")


def required(value: T | None, name: str) -> T:
    """Refuse incomplete phase data before recording progress."""
    if value is None:
        raise UsageError(f"agent report: `{name}` is required for this operation")
    return value


def _context(ctx: OperationContext, request: AgentTask) -> TaskContext:
    with ctx.engine.begin() as conn:
        task_run = fence(conn, request.attempt_id, str(ctx.credential_token_id))
        work = conn.execute(statement(conn, "agent_work"), {"attempt_id": request.attempt_id}).one()
    context = build_task_context(
        ctx.engine,
        ctx.config,
        task_run,
        force=work.force_flag == YesNo.YES,
        rerun=work.rerun_flag == YesNo.YES,
    )
    return replace(context, attempt_id=request.attempt_id, owner=str(ctx.credential_token_id))


def _store(ctx: OperationContext, task: TaskContext) -> LocalTaskStore:
    return LocalTaskStore(
        ctx.engine,
        ctx.config,
        TaskIdentity(
            task.pipeline_id,
            task.task_id,
            task.pipeline_run_id,
            task.task_run_id,
            task.attempt_id,
            task.owner,
            ctx.credential_digest,
        ),
        LocalLocker(ctx.engine, ctx.config),
    )


def _target(ctx: OperationContext, task: TaskContext, target: str) -> None:
    expected = task.task_params.get("TARGET_OBJECT")
    catalog = active_catalog(ctx.config)
    if expected is None or qualify(target, catalog).lower() != qualify(expected, catalog).lower():
        raise PermissionDeniedError(
            f"attempt {task.attempt_id}: target {target!r} is not its admitted target; "
            "use the target returned to this task"
        )
    lock = locks.target(expected, catalog)
    with ctx.engine.begin() as conn:
        fence(conn, required(task.attempt_id, "attempt_id"), task.owner or "")
        owned = conn.execute(
            statement(conn, "resource_lock_owned"),
            {
                "key": lock.key,
                "holder": attempt_holder(
                    required(task.attempt_id, "attempt_id"),
                    ctx.credential_digest or task.owner or "",
                ),
                "owner": task.owner,
            },
        ).scalar_one_or_none()
        if owned is None:
            raise StaleTransitionError(
                f"attempt {task.attempt_id}: no live lock on {target}; "
                "acquire it before target work"
            )


def _lock(ctx: OperationContext, task: TaskContext, request: AgentTask) -> AgentReply:
    key = required(request.lock_key, "lock_key")
    allowed = {}
    if target := task.task_params.get("TARGET_OBJECT"):
        lock = locks.target(target, active_catalog(ctx.config))
        allowed[lock.key] = lock.name
    if is_single_writer(ctx.config):
        allowed[WAREHOUSE_LOCK_KEY] = "warehouse"
    if key not in allowed:
        raise PermissionDeniedError(
            f"attempt {task.attempt_id}: lock {key} is outside its resources; use its task lock"
        )
    holder = resource_locks.Holder(
        attempt_holder(request.attempt_id, ctx.credential_digest or task.owner or ""),
        task.owner or "",
        ctx.actor,
        request.attempt_id,
    )
    with ctx.engine.begin() as conn:
        fence(conn, request.attempt_id, holder.owner)
        if request.operation == AgentReport.UNLOCK:
            resource_locks.release(conn, key, holder)
            return AgentReply()
        owned = conn.execute(
            statement(conn, "resource_lock_owned"),
            {
                "key": key,
                "holder": holder.holder_id,
                "owner": holder.owner,
            },
        ).scalar_one_or_none()
        attempt = (
            resource_locks.Acquisition(budget=0)
            if owned is not None
            else resource_locks.acquire(conn, key, allowed[key], holder, request.confirmed_holder)
        )
        row = conn.execute(statement(conn, "resource_lock_read"), {"key": key}).one_or_none()
        detail = "holder changed while waiting"
        if row is not None:
            held = (as_utc(row.db_now) - as_utc(row.acquired_at)).total_seconds()
            detail = f"holder={row.holder_id}, actor={row.actor}, held for {held:g}s"
        return AgentReply(
            {
                "acquired": attempt.budget is not None,
                "stale_holder": attempt.stale_holder,
                "resource": allowed[key],
                "holder": detail,
            }
        )


async def _wait_lock(ctx: OperationContext, request: AgentTask) -> AgentReply:
    limit = request.lock_wait_seconds
    if not math.isfinite(limit) or not 0 <= limit <= 30:
        raise UsageError("lock_wait_seconds must be finite and between 0 and 30")
    deadline = time.monotonic() + limit
    hub = notification_hub(ctx.config, ctx.engine)
    with acting_as(ctx.actor):
        while True:
            generation = hub.generation
            task = await asyncio.to_thread(_context, ctx, request)
            answer = await asyncio.to_thread(_lock, ctx, task, request)
            assert isinstance(answer.data, dict)
            # A stale holder goes back to the agent, which stops its statements and retries.
            stale = answer.data["stale_holder"]
            if answer.data["acquired"] or (stale and stale != request.confirmed_holder):
                return answer
            remaining = deadline - time.monotonic()
            if remaining <= 0 or (ctx.shutdown is not None and ctx.shutdown.is_set()):
                raise LockTimeoutError(
                    f"{answer.data['resource']}: lock unavailable after {limit:g}s; "
                    f"{answer.data['holder']}; inspect its holder and retry"
                )
            await hub.wait_async(generation, min(1, remaining), ctx.shutdown)


def report(ctx: OperationContext, request: AgentTask) -> AgentReply | Awaitable[AgentReply]:
    """Apply this token's bounded task operation; no route accepts SQL or another task's ids."""
    check_agent(ctx, request)
    if request.operation in {AgentReport.END, AgentReport.CANCEL}:
        return {AgentReport.END: _end, AgentReport.CANCEL: _cancel}[request.operation](ctx, request)
    if request.operation == AgentReport.LOCK:
        return _wait_lock(ctx, request)
    task = _context(ctx, request)
    store = _store(ctx, task)
    if (
        request.operation in {AgentReport.ITEMS, AgentReport.ITEM_EVENT, AgentReport.ITEM_PREPARE}
        and task.handler != Handler.PYTHON
    ):
        raise PermissionDeniedError(
            f"attempt {task.attempt_id}: handler={task.handler}; expected PYTHON for item reports; "
            "report only this task's admitted work"
        )
    progress: dict[AgentReport, Callable[[], object]] = {
        AgentReport.STATE: lambda: store.state(task.task_params.get("SQL_ACTION")),
        AgentReport.ALERT: lambda: store.alert(task.task_params.get("EMAIL_PIPELINES")),
        AgentReport.ITEMS: store.items,
        AgentReport.ITEM_PREPARE: lambda: store.prepare_items(request.item_keys),
        AgentReport.ITEM_EVENT: lambda: store.item_event(required(request.item, "item")),
    }
    if request.operation in progress:
        return AgentReply(progress[request.operation]())
    match request.operation:
        case AgentReport.LOCK | AgentReport.UNLOCK:
            return _lock(ctx, task, request)
        case AgentReport.START:
            return _start(ctx, task, request)
        case AgentReport.TARGET:
            target = required(request.target, "target")
            _target(ctx, task, target)
            return AgentReply(store.target(target))
        case AgentReport.PUBLISH:
            publication = required(request.publication, "publication")
            _target(ctx, task, publication.target)
            store.publish(publication)
        case AgentReport.WATERMARK:
            store.watermark(
                required(request.watermark_from, "watermark_from"),
                required(request.watermark_to, "watermark_to"),
            )
        case AgentReport.RULE_BEGIN:
            return AgentReply(store.begin_rule(required(request.rule_id, "rule_id")))
        case AgentReport.RULE_KEYS:
            return AgentReply(sorted(store.rule_keys(required(request.rule_id, "rule_id"))))
        case AgentReport.RULE_END:
            store.end_rule(required(request.rule_ending, "rule_ending"))
    return AgentReply()


def _start(ctx: OperationContext, task: TaskContext, request: AgentTask) -> AgentReply:
    with ctx.engine.begin() as conn:
        fence(conn, request.attempt_id, task.owner or "")
        agent = conn.execute(
            statement(conn, "agent_get"), {"agent_id": ctx.credential_token_id}
        ).one()
        attempt_starts.ensure_attempt_started(
            conn,
            request.attempt_id,
            ctx.actor,
            owner=task.owner or "",
            process=attempt_starts.StartedProcess(
                agent.host, required(request.pid, "pid"), request.process_start
            ),
        )
    if task.handler == Handler.RUN_PIPELINE:
        from etl_craft.services.operations.task_process import execute_trigger

        execute_trigger(ctx.engine, task)
        return AgentReply({"trigger": True})
    return AgentReply({"trigger": False})


def _end(ctx: OperationContext, request: AgentTask) -> AgentReply:
    """Commit outcome, summary, checkpoint, consumption and its idempotency receipt together."""
    status = required(request.status, "status")
    if status not in {
        AttemptStatus.SUCCESS,
        AttemptStatus.FAILED,
        AttemptStatus.TIMED_OUT,
        AttemptStatus.CANCELLED,
    }:
        raise UsageError(f"agent ending {status}: expected SUCCESS, FAILED, TIMED_OUT or CANCELLED")
    digest = sha256_hex(json.dumps(encode(request), sort_keys=True).encode())
    owner = str(ctx.credential_token_id)
    with ctx.engine.begin() as conn:
        row = conn.execute(
            statement(conn, "agent_work"), {"attempt_id": request.attempt_id}
        ).one_or_none()
        receipt = conn.execute(
            statement(conn, "agent_ending_receipt"), {"attempt_text": str(request.attempt_id)}
        ).one_or_none()
        if row is not None and row.owner_id == owner and receipt is not None:
            if receipt.request_sha256 == digest:
                return AgentReply()
            raise StaleTransitionError(
                f"attempt {request.attempt_id}: ending differs from its committed report; "
                "keep the original ending"
            )
        fence(conn, request.attempt_id, owner)
        if request.failure is not None:
            transitions.record_attempt_failure(conn, request.attempt_id, request.failure)
        if status == AttemptStatus.CANCELLED and request.queries_ended:
            attempt_cancellation.acknowledge_cancellation(conn, request.attempt_id, owner)
        transitions.finish_attempt(
            conn,
            request.attempt_id,
            status,
            ctx.actor,
            owner=owner,
            counts=request.counts or NO_COUNTS,
            error_message=request.error_message,
            task_log=request.task_log,
            exit_code=request.exit_code,
            offset=request.offset,
        )
        if status == AttemptStatus.CANCELLED:
            controls.observe_exit(conn, request.attempt_id, queries_ended=request.queries_ended)
        ActionRequest(
            ActionName.AGENT_REPORT,
            {
                "attempt_id": str(request.attempt_id),
                "operation": AgentReport.END,
                "request_sha256": digest,
            },
            datetime.now(UTC),
            actor=ctx.actor,
            outcome=ActionOutcome.SUCCESS,
            exit_code=request.exit_code,
        ).write(conn)
    return AgentReply()


def _cancel(ctx: OperationContext, request: AgentTask) -> AgentReply:
    """Accept physical observations only from the pinned attempt's authenticated owner."""
    owner = str(ctx.credential_token_id)
    with ctx.engine.begin() as conn:
        row = conn.execute(
            statement(conn, "agent_control_owner"),
            {
                "attempt_id": request.attempt_id,
                "owner": owner,
            },
        ).one_or_none()
        if row is None:
            raise PermissionDeniedError(
                f"attempt {request.attempt_id}: no pending control owned by agent {owner}; "
                "expected this token's pinned attempt; refresh the control request"
            )
        if row.status in {AttemptStatus.CLAIMED, AttemptStatus.RUNNING}:
            fence(conn, request.attempt_id, owner)
            if request.queries_ended:
                attempt_cancellation.acknowledge_cancellation(conn, request.attempt_id, owner)
        controls.observe_exit(conn, request.attempt_id, queries_ended=request.queries_ended)
    from etl_craft.execution.control_flow import advance

    advance(ctx.engine, row.pipeline_run_id, observed=True, reason=request.error_message)
    return AgentReply()
