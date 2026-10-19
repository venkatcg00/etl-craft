"""Authenticated SCD edits, integrations, provenance and explicit human unlocks."""

from __future__ import annotations

import math
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import ClassVar

from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import ActionName, ActorKind, Handler, ProtectionEvent
from etl_craft.core.errors import PermissionDeniedError, SqlGuardError, UsageError
from etl_craft.core.time import as_utc
from etl_craft.engine import locks
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import (
    conflicting_merge_action,
    fetch_task_execution_detail,
    fetch_task_parameters,
    resolve_task_id,
)
from etl_craft.handlers.registry import TaskContext
from etl_craft.handlers.sql.actions import ACTIONS, ActionContext
from etl_craft.handlers.sql.protection import MERGES, Protection, ProtectionRequest, exists
from etl_craft.handlers.sql.session import Session
from etl_craft.handlers.sql.spec import read_sql_task
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.recording import recorded
from etl_craft.services.operations.requests import Request
from etl_craft.warehouse.connection import open_warehouse, task_warehouse_dialect


@dataclass(frozen=True, kw_only=True)
class ValueKey(Request):
    """One exact business key of a SQL task's target, cast by the warehouse's declared types."""

    pipeline_code: str
    task_code: str
    key: dict[str, object]


@dataclass(frozen=True, kw_only=True)
class ValueEdit(ValueKey):
    """Explicit values for one key; omitted existing values are retained."""

    values: dict[str, object]
    reason: str
    row_lock: bool = False


@dataclass(frozen=True, kw_only=True)
class ValueUnlock(ValueKey):
    """An explicit release; no columns means all active locks on the selected key."""

    reason: str
    columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class ValueResult:
    """Latest value attribution and locks, with bounded immutable operation evidence."""

    SCHEMA: ClassVar[str] = "etl-craft/scd-values/1"
    target: str
    enabled: bool
    request_id: str | None
    records: tuple[dict[str, object], ...]
    history: tuple[dict[str, object], ...]


def _task_context(ctx: OperationContext, pipeline: str, task: str) -> TaskContext:
    """Resolve a definition without creating or selecting an execution run."""
    with ctx.engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline)
        task_id = resolve_task_id(conn, pipeline_id, task)
        detail = fetch_task_execution_detail(conn, task_id)
        params = fetch_task_parameters(conn, task_id)
    if detail.handler != Handler.SQL:
        raise UsageError(
            f"{pipeline}/{task}: HANDLER={detail.handler}; expected SQL SCD1/SCD2; "
            "select a merge task"
        )
    return TaskContext(
        ctx.config,
        pipeline_id,
        pipeline,
        task_id,
        task,
        0,
        0,
        0,
        detail.handler,
        detail.refresh_type,
        params,
    )


@contextmanager
def _selected(
    ctx: OperationContext, pipeline: str, task: str
) -> Iterator[tuple[Protection, TaskContext]]:
    """Share engine ownership, single-writer access and atomic warehouse transactions with ETL."""
    context = _task_context(ctx, pipeline, task)
    spec = read_sql_task(context)
    if spec.action not in MERGES:
        raise UsageError(
            f"{pipeline}/{task}: SQL_ACTION={spec.action}; expected SCD1_MERGE or SCD2_MERGE; "
            "select a merge task"
        )
    with ctx.engine.connect() as conn:
        other = conflicting_merge_action(conn, context.task_id, spec.action)
    if other is not None:
        raise SqlGuardError(
            f"{spec.target_object}: {spec.action} conflicts with an active {other} writer; "
            "give the target one merge action before editing"
        )
    catalog = active_catalog(ctx.config)
    with (
        locks.target(spec.target_object, catalog).hold(ctx.engine),
        open_warehouse(ctx.config, ctx.engine) as warehouse,
        warehouse.begin() as conn,
    ):
        session = Session(
            conn,
            task_warehouse_dialect(ctx.config, context.task_params),
            catalog=catalog,
            action=spec.action,
            target_object=spec.target_object,
            task_run_id=0,
            params=context.task_params,
        )
        guard = Protection(session, spec)
        guard.acquire()
        try:
            yield guard, context
        finally:
            session.sweep()


def _validate_input(guard: Protection, key: Mapping[str, object]) -> None:
    """Require the exact stable business key and bound scalar values."""
    if set(key) != set(guard.task.merge_key) or any(value is None for value in key.values()):
        raise UsageError(
            f"{guard.session.target}: key columns={tuple(key)}; expected non-NULL values for "
            f"{guard.task.merge_key}; supply exactly those business-key columns"
        )
    _validate_scalars(key)


def _validate_scalars(values: Mapping[str, object]) -> None:
    """Refuse structured and non-finite values before they reach a driver."""
    for column, value in values.items():
        if (value is not None and not isinstance(value, (str, int, float, bool))) or (
            isinstance(value, float) and not math.isfinite(value)
        ):
            raise UsageError(
                f"column {column!r}: value type={type(value).__name__}; expected a finite scalar "
                "or NULL; encode dates/times as ISO strings and supply individual column values"
            )


def _key_row(guard: Protection, key: Mapping[str, object]) -> dict[str, object] | None:
    """Read one latest business-key version, including a deleted row awaiting revival."""
    _validate_input(guard, key)
    params = {f"key_{i}": value for i, value in enumerate(key.values())}
    types = guard.session.column_types(guard.session.target)
    predicate = " AND ".join(
        f"t.{column} = CAST(:key_{i} AS {types[column.lower()]})" for i, column in enumerate(key)
    )
    rows = (
        guard.session.run(
            f"SELECT t.* FROM ({guard.sql.current()}) t WHERE {predicate}",
            params,
            step="select the business key",
        )
        .mappings()
        .all()
    )
    return None if not rows else dict(rows[0])


def _edit_stage(guard: Protection, edit: ValueEdit) -> tuple[str, str]:
    """Stage an exact row using declared warehouse types and bound input values."""
    s = guard.session
    previous = _key_row(guard, edit.key)
    unknown = set(edit.values) - set(guard.business)
    if unknown or set(edit.values) & set(guard.task.merge_key):
        raise UsageError(
            f"{s.target}: edit columns={tuple(edit.values)}; expected business columns other "
            f"than {guard.task.merge_key}; remove unknown, managed and key columns"
        )
    _validate_scalars(edit.values)
    values = {**(previous or {}), **edit.key, **edit.values}
    missing = set(guard.business) - set(values)
    if missing or not edit.values:
        raise UsageError(
            f"{s.target}: missing insert columns={sorted(missing)}, "
            f"edit columns={tuple(edit.values)}; "
            "supply at least one value and every business column for a new key"
        )
    stage = s.scratch("edit")
    names = ", ".join(guard.business)
    s.create_scratch(stage, f"SELECT {names} FROM {s.target} WHERE 1 = 0", step="stage an edit")
    types = s.column_types(s.target)
    expressions = ", ".join(
        f"CAST(:value_{i} AS {types[c.lower()]})" for i, c in enumerate(guard.business)
    )
    s.run(
        f"INSERT INTO {stage} ({names}) SELECT {expressions}",
        {f"value_{i}": values[c] for i, c in enumerate(guard.business)},
        step="bind edited values",
    )
    key_hash = str(
        s.run(
            f"SELECT {guard.sql.key_hash} AS key_hash FROM {stage} t", step="identify edited key"
        ).scalar_one()
    )
    return stage, key_hash


def _result(guard: Protection, key_hash: str, request_id: str | None) -> ValueResult:
    """Return current attribution and the last hundred events for exactly this key."""
    records = (
        guard.session.run(
            f"SELECT * FROM ({guard.sql.state()}) p WHERE key_hash = :key ORDER BY column_name",
            {"key": key_hash},
            step="read column provenance",
        )
        .mappings()
        .all()
    )
    history = (
        guard.session.run(
            f"SELECT * FROM {guard.sql.journal} WHERE key_hash = :key "
            "ORDER BY revision DESC, column_name LIMIT 100",
            {"key": key_hash},
            step="read protection history",
        )
        .mappings()
        .all()
    )
    return ValueResult(
        guard.session.target,
        True,
        request_id,
        tuple({**row, "happened_at": as_utc(row["happened_at"])} for row in records),
        tuple({**row, "happened_at": as_utc(row["happened_at"])} for row in history),
    )


def write_values(ctx: OperationContext, edit: ValueEdit) -> ValueResult:
    """Apply an admin's edit or authenticated integration write with accurate attribution.

    The audit row names the columns, not their values, and the protection request id.
    """
    pipeline, task = edit.pipeline_code, edit.task_code
    request = ProtectionRequest.create(ctx.actor, edit.reason)
    arguments = {
        "pipeline_code": pipeline,
        "task_code": task,
        "columns": tuple(edit.values),
        "reason": edit.reason,
        "request_id": request.request_id,
    }
    with recorded(ctx, ActionName.DATA_EDIT, arguments):
        _check_reason(edit.reason)
        if ctx.actor.kind not in {ActorKind.USER, ActorKind.SERVICE}:
            raise PermissionDeniedError(
                f"actor={ctx.actor.name!r}, kind={ctx.actor.kind}; expected a registered USER "
                "or SERVICE admin; sign in with an account credential"
            )
        if edit.row_lock and ctx.actor.kind != ActorKind.USER:
            raise PermissionDeniedError("row_lock=True requires a human admin; omit row_lock")
        with _selected(ctx, pipeline, task) as (guard, context):
            if exists(guard.session):
                guard.check()
            else:
                guard.enable(request)
            stage, key_hash = _edit_stage(guard, edit)
            spec = replace(guard.task, preserve_target=False, watermark=None)
            guard.session.protection = guard
            ACTIONS[spec.action](
                guard.session,
                ActionContext(
                    spec,
                    context,
                    ctx.engine,
                    ctx.actor.name,
                    request.now,
                    stage,
                    respect_protection=ctx.actor.kind != ActorKind.USER,
                ),
            )
            guard.finish(request)
            if edit.row_lock:
                _set_row_lock(guard, key_hash, request)
            return _result(guard, key_hash, request.request_id)


def _check_reason(reason: str) -> None:
    """Keep every successful edit/unlock explainable without accepting unbounded evidence."""
    if not reason.strip() or len(reason) > 4096:
        raise UsageError(
            f"data edit/unlock reason length={len(reason)}; expected 1-4096 nonblank characters; "
            "supply a concise explanation"
        )


def _set_row_lock(guard: Protection, key_hash: str, request: ProtectionRequest) -> None:
    """Explicit whole-row protection can also accompany a human update."""
    guard.session.run(
        guard.sql.insert_event(
            "SELECT :revision, :key, '*', NULL, TRUE, :actor, :actor_kind, :actor, :actor_kind, "
            ":now, :request_id, :reason, :event, NULL"
        ),
        {**guard.parameters(request, ProtectionEvent.LOCK), "key": key_hash},
        step="protect the whole business row",
    )


def _selected_key(guard: Protection, key: Mapping[str, object]) -> str:
    """Resolve a typed key even after deletion, so its evidence remains inspectable."""
    _validate_input(guard, key)
    types = guard.session.column_types(guard.session.target)
    expression = ", ".join(
        f"CAST(:key_{i} AS {types[c.lower()]}) AS {c}" for i, c in enumerate(key)
    )
    return str(
        guard.session.run(
            f"SELECT {guard.sql.key_hash} AS key_hash FROM (SELECT {expression}) t",
            {f"key_{i}": v for i, v in enumerate(key.values())},
            step="identify selected key",
        ).scalar_one()
    )


def unlock_values(ctx: OperationContext, edit: ValueUnlock) -> ValueResult:
    """Release protection explicitly; integrations cannot unlock a person's values."""
    pipeline, task = edit.pipeline_code, edit.task_code
    request = ProtectionRequest.create(ctx.actor, edit.reason)
    with recorded(
        ctx,
        ActionName.DATA_UNLOCK,
        {
            "pipeline_code": pipeline,
            "task_code": task,
            "reason": edit.reason,
            "request_id": request.request_id,
        },
    ):
        _check_reason(edit.reason)
        if ctx.actor.kind != ActorKind.USER:
            raise PermissionDeniedError(
                f"actor={ctx.actor.name!r}, kind={ctx.actor.kind}; expected a human admin; "
                "sign in as a USER admin to release protection"
            )
        with _selected(ctx, pipeline, task) as (guard, _context):
            if not exists(guard.session):
                raise UsageError(
                    f"{guard.session.target}: protection is not enabled; nothing to unlock"
                )
            guard.check()
            unknown = set(edit.columns) - {*guard.business, "*"}
            if unknown:
                raise UsageError(
                    f"{guard.session.target}: unlock columns={sorted(unknown)}; expected business "
                    "columns or '*' for the row lock; correct the column names"
                )
            key_hash = _selected_key(guard, edit.key)
            predicate = "key_hash = :key AND locked"
            if edit.columns:
                predicate += (
                    " AND column_name IN ("
                    + ", ".join(f":column_{i}" for i in range(len(edit.columns)))
                    + ")"
                )
            guard.session.run(
                guard.sql.lock_change(predicate),
                {
                    **guard.parameters(request, ProtectionEvent.UNLOCK),
                    "key": key_hash,
                    "locked": False,
                    **{f"column_{i}": c for i, c in enumerate(edit.columns)},
                },
                step="release selected human locks",
            )
            return _result(guard, key_hash, request.request_id)


def provenance(ctx: OperationContext, request: ValueKey) -> ValueResult:
    """Read known writers and lock evidence; never infer identity from actor-column strings."""
    key = request.key
    with _selected(ctx, request.pipeline_code, request.task_code) as (guard, _context):
        if not exists(guard.session):
            return ValueResult(guard.session.target, False, None, (), ())
        guard.check()
        return _result(guard, _selected_key(guard, key), None)
