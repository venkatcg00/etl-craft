"""Expose the control policy and its data effects without executing a control."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from etl_craft.core.enums import DEFINITION_CONTROLS, ActionName, ControlAction
from etl_craft.core.errors import EtlCraftError
from etl_craft.engine import run_scopes, runlog
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import resolve_task_id
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.controls import check_control, control_scope
from etl_craft.services.operations.recovery import control_effect
from etl_craft.services.operations.requests import CapabilitiesInput, ControlRequest


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


def control_capabilities(ctx: OperationContext, request: CapabilitiesInput) -> CapabilitiesView:
    """Inspect exact-scope controls through the same policy used when accepting them."""
    pipeline_code, task_code = request.pipeline_code, request.task_code
    with ctx.engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        selected = runlog.select_run(conn, pipeline_id, request.selector)
        task_id = None if task_code is None else resolve_task_id(conn, pipeline_id, task_code)
        if task_id is not None:
            run_scopes.check_task(conn, selected.pipeline_run_id, task_id)
    actions = []
    for action in ControlAction:
        if action in DEFINITION_CONTROLS:
            continue
        control = ControlRequest(
            pipeline_code=pipeline_code,
            action=action,
            reason="capability inspection",
            run_id=selected.pipeline_run_id,
            task_code=task_code,
        )
        reason = _role_refusal(ctx)
        if reason is None:
            try:
                with control_scope(ctx, control), ctx.engine.connect() as conn:
                    current = runlog.select_run(conn, pipeline_id, control.selector)
                    check_control(conn, ctx, control, current, task_id)
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


def _role_refusal(ctx: OperationContext) -> str | None:
    """Describe a role restriction without recording a refused change on a read."""
    from etl_craft.services.operations.actions import ACTIONS, has_role

    required = ACTIONS[ActionName.CONTROL].role
    if not has_role(ctx.role, required):
        return (
            f"account {ctx.actor.name!r}: role={ctx.role or 'unproven'}; controls need the "
            f"{required} role; ask an administrator to grant it"
        )
    return None
