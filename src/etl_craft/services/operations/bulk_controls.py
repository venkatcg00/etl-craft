"""Apply an explicit selection with independent, durable per-object results."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar

from etl_craft.core.enums import ControlAction
from etl_craft.core.errors import EtlCraftError, UsageError
from etl_craft.engine.controls import ControlView
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.controls import ControlRequest, request_control


@dataclass(frozen=True)
class BulkOutcome:
    """Keep the requested identity even when this object refuses its action."""

    pipeline_code: str
    task_code: str | None
    request_key: str
    action: ControlAction
    control: ControlView | None
    error: str | None


@dataclass(frozen=True)
class BulkControlView:
    """Inspect each object independently; a partial failure never rolls back other objects."""

    SCHEMA: ClassVar[str] = "etl-craft/bulk-controls/1"
    outcomes: tuple[BulkOutcome, ...]
    message: str


def request_bulk_controls(
    ctx: OperationContext, requests: Sequence[ControlRequest]
) -> BulkControlView:
    """Validate the explicit selection, then invoke the ordinary audited action per object."""
    if not requests or len(requests) > 100:
        raise UsageError(
            f"bulk selection has {len(requests)} objects; supply 1 to 100 explicit objects"
        )
    for request in requests:
        if request.request_key is None or (
            request.selector.run_id is None and request.selector.run_key is None
        ):
            raise UsageError(
                "every bulk object needs an explicit run id/key and request_key; implicit "
                "active-run selection is unsafe across a batch"
            )
    if len({r.request_key for r in requests}) != len(requests):
        raise UsageError("bulk request_key values repeat; give each selected object a unique key")
    outcomes = []
    for request in requests:
        control, error = None, None
        try:
            control = request_control(ctx, request)
        except EtlCraftError as refused:
            error = str(refused)
        outcomes.append(
            BulkOutcome(
                request.pipeline_code,
                request.task_code,
                request.request_key or "",
                request.action,
                control,
                error,
            )
        )
    failures = sum(o.error is not None for o in outcomes)
    return BulkControlView(
        tuple(outcomes),
        f"{len(outcomes) - failures} accepted; {failures} refused; "
        "inspect each durable control for completion",
    )
