"""Expose the control policy and its data effects without executing a control."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from etl_craft.core.enums import ControlAction, Mode
from etl_craft.core.errors import EtlCraftError
from etl_craft.engine import run_scopes, runlog
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import resolve_task_id
from etl_craft.services.operations.context import OperationContext, operation
from etl_craft.services.operations.controls import (
    ControlRequest,
    check_control,
    control_command,
    control_scope,
)
from etl_craft.services.operations.permissions import has_role, required_role
from etl_craft.services.operations.recovery import control_effect


@dataclass(frozen=True)
class Capability:
    """One action and the concrete reason it is available or refused now."""

    action: ControlAction
    available: bool
    reason: str
    preserves_successful_work: bool
    retains_checkpoints: bool = True


@dataclass(frozen=True)
class CapabilitiesView:
    """A current snapshot; actions revalidate under the same locks before acceptance."""

    SCHEMA: ClassVar[str] = "etl-craft/capabilities/1"
    pipeline_code: str
    pipeline_run_id: int
    task_code: str | None
    actions: tuple[Capability, ...]


def control_capabilities(
    ctx: OperationContext,
    pipeline_code: str,
    *,
    selector: runlog.RunSelector = runlog.ACTIVE_RUN,
    task_code: str | None = None,
) -> CapabilitiesView:
    """Inspect exact-scope controls through the same policy used when accepting them."""
    with operation(ctx, "status", {"pipeline_code": pipeline_code}):
        with ctx.engine.connect() as conn:
            pipeline_id = resolve_pipeline_id(conn, pipeline_code)
            selected = runlog.select_run(conn, pipeline_id, selector)
            task_id = None if task_code is None else resolve_task_id(conn, pipeline_id, task_code)
            if task_id is not None:
                run_scopes.check_task(conn, selected.pipeline_run_id, task_id)
        actions = []
        for action in ControlAction:
            request = ControlRequest(
                pipeline_code,
                action,
                "capability inspection",
                runlog.RunSelector(run_id=selected.pipeline_run_id),
                task_code,
            )
            reason = _reason(ctx, request)
            if reason is None:
                try:
                    with control_scope(ctx, request), ctx.engine.connect() as conn:
                        current = runlog.select_run(conn, pipeline_id, request.selector)
                        check_control(conn, ctx, request, current, task_id)
                except EtlCraftError as error:
                    reason = str(error)
            actions.append(
                Capability(
                    action,
                    reason is None,
                    reason or control_effect(action),
                    action != ControlAction.RESTART,
                )
            )
        return CapabilitiesView(pipeline_code, selected.pipeline_run_id, task_code, tuple(actions))


def _reason(ctx: OperationContext, request: ControlRequest) -> str | None:
    """Describe role/adapter restrictions without recording a refused mutation on a read."""
    command = control_command(request.action)
    required = required_role(command, {"pipeline_code": request.pipeline_code})
    if not has_role(ctx.role, required):
        return (
            f"account {ctx.actor.name!r}: role={ctx.role or 'unproven'}; {command!r} needs "
            f"the {required} role; ask an administrator to grant it"
        )
    if ctx.config.mode != Mode.LOCAL:
        return "legacy orchestrator has no native agent adapter; control its exact task there"
    return None
