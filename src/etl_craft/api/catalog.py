"""Authenticated, read-only schema and lineage comparison routes."""

from collections.abc import Callable
from typing import Annotated, Any

from fastapi import Depends, FastAPI

from etl_craft.services.operations import OperationContext, inspect, to_json
from etl_craft.services.operations.catalog import get_gaps


def add_catalog_routes(
    app: FastAPI, authorize: Callable[[str], Callable[..., OperationContext]]
) -> None:
    """Use the server's existing viewer authorization for metadata comparisons."""

    @app.get("/api/v1/pipelines")
    def list_pipelines(
        caller: Annotated[OperationContext, Depends(authorize("list"))],
    ) -> dict[str, Any]:
        return to_json(inspect.list_pipelines(caller))

    @app.get("/api/v1/catalog/gaps")
    def catalog_gaps(
        caller: Annotated[OperationContext, Depends(authorize("catalog gaps"))],
        compare: bool = False,
    ) -> dict[str, Any]:
        return to_json(get_gaps(caller, compare=compare))
