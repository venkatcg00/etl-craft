"""Record one action's request in the audit, under the actor it acts as."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from etl_craft.core.actor import acting_as
from etl_craft.core.enums import ActionName, ActionOutcome
from etl_craft.core.errors import EtlCraftError
from etl_craft.core.text import mask_arguments
from etl_craft.engine.audit import ActionRequest, command_request, register_engine
from etl_craft.services.operations.context import OperationContext


@contextmanager
def recorded(
    ctx: OperationContext, name: ActionName, arguments: Mapping[str, Any]
) -> Iterator[ActionRequest]:
    """Record the request once, before the action changes anything, whatever its outcome."""
    with (
        acting_as(ctx.actor),
        command_request(
            name, _http_arguments(ctx, arguments), project_id=ctx.project_id
        ) as request,
    ):
        register_engine(ctx.engine)
        yield request


def record_refusal(
    ctx: OperationContext, name: ActionName, arguments: Mapping[str, Any], error: EtlCraftError
) -> None:
    """Record one ``REFUSED`` row for a request the role policy turned away."""
    with acting_as(ctx.actor):
        ActionRequest(
            name,
            mask_arguments(_http_arguments(ctx, arguments)),
            datetime.now(UTC),
            actor=ctx.actor,
            engine=ctx.engine,
            outcome=ActionOutcome.REFUSED,
            exit_code=error.exit_code,
            project_id=ctx.project_id,
        ).record()


def _http_arguments(ctx: OperationContext, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
    if ctx.client_address is None:
        return arguments
    return {
        **arguments,
        "_http": {"client_address": ctx.client_address, "scheme": ctx.client_scheme},
    }
