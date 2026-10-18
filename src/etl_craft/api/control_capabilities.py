"""Thin capability and explicit-selection control transports."""

from typing import Annotated, Any

from fastapi import Depends, FastAPI, Query
from pydantic import BaseModel, ConfigDict, Field

from etl_craft.api.selected import Authorize, ScopedControl
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations import OperationContext, to_json
from etl_craft.services.operations.bulk_controls import request_bulk_controls
from etl_craft.services.operations.capabilities import control_capabilities
from etl_craft.services.operations.controls import ControlRequest


class SelectedControl(ScopedControl):
    """One selected pipeline and its exact scoped action."""

    pipeline_code: str = Field(min_length=1)


class BulkControls(BaseModel):
    """A bounded explicit selection with independent outcomes."""

    model_config = ConfigDict(extra="forbid")
    requests: list[SelectedControl] = Field(min_length=1, max_length=100)


def add_control_capability_routes(app: FastAPI, authorize: Authorize) -> None:
    """Expose the same service decisions and bulk operation as the CLI."""

    @app.get("/api/v1/pipelines/{code}/capabilities")
    def capabilities(
        code: str,
        caller: Annotated[OperationContext, Depends(authorize("status"))],
        run_id: Annotated[int | None, Query(ge=1)] = None,
        run_key: str | None = None,
        task_code: str | None = None,
    ) -> dict[str, Any]:
        return to_json(
            control_capabilities(
                caller, code, selector=RunSelector(run_id, run_key), task_code=task_code
            )
        )

    @app.post("/api/v1/controls/bulk")
    def bulk(
        body: BulkControls,
        caller: Annotated[OperationContext, Depends(authorize("run"))],
    ) -> dict[str, Any]:
        return to_json(
            request_bulk_controls(
                caller,
                tuple(
                    ControlRequest(
                        r.pipeline_code, r.action, r.reason, r.selector, r.task_code, r.request_key
                    )
                    for r in body.requests
                ),
            )
        )
