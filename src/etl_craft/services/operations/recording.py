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
    ctx: OperationContext,
    name: ActionName,
    arguments: Mapping[str, Any],
    *,
    on_completion: bool = False,
) -> Iterator[ActionRequest]:
    """Record once under the actor, immediately or in the action outcome transaction."""
    with (
        acting_as(ctx.actor),
        command_request(
            name, _http_arguments(ctx, arguments), project_id=ctx.project_id
        ) as request,
    ):
        if on_completion:
            request.engine = ctx.engine
        else:
            register_engine(ctx.engine)
        yield request


def record_refusal(
    ctx: OperationContext, name: ActionName, arguments: Mapping[str, Any], error: EtlCraftError
) -> None:
    """Record one ``REFUSED`` row for a request the role policy turned away."""
    with acting_as(ctx.actor):
        request = ActionRequest(
            name,
            mask_arguments(_http_arguments(ctx, arguments)),
            datetime.now(UTC),
            actor=ctx.actor,
            engine=ctx.engine,
            outcome=ActionOutcome.REFUSED,
            exit_code=error.exit_code,
            project_id=ctx.project_id,
        )
        request.record()
        if name == ActionName.UPLOAD:
            from etl_craft.core.uploads import UploadStatus
            from etl_craft.engine.queries import statement

            with ctx.engine.begin() as conn:
                conn.execute(
                    statement(conn, "upload_insert"),
                    {
                        "upload_id": request.action_id,
                        "pipeline_id": None,
                        "account": ctx.actor.name,
                        "file_name": "(no file)",
                        "size_bytes": 0,
                        "sha256": None,
                        "status": UploadStatus.REFUSED,
                        "reason": str(mask_arguments({"reason": str(error)})["reason"]),
                        "now": request.requested_at,
                        "ended_at": datetime.now(UTC),
                    },
                )


def _http_arguments(ctx: OperationContext, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
    if ctx.client_address is None:
        return arguments
    return {
        **arguments,
        "_http": {"client_address": ctx.client_address, "scheme": ctx.client_scheme},
    }
