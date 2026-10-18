"""SCD value writes and protection through the shared authenticated service."""

from collections.abc import Callable
from typing import Annotated, Any

from fastapi import Depends, FastAPI
from pydantic import BaseModel, ConfigDict, Field

from etl_craft.services.operations import OperationContext, to_json, values


class Key(BaseModel):
    """An exact business key, with values cast by the warehouse's declared types."""

    model_config = ConfigDict(extra="forbid")
    key: dict[str, Any]


class Edit(Key):
    """A reasoned edit or insert; human inserts protect their whole row."""

    values: dict[str, Any]
    reason: str = Field(min_length=1, max_length=4096, pattern=r"\S")
    row_lock: bool = False


class Unlock(Key):
    """An explicit release of selected columns, or every lock for the key."""

    reason: str = Field(min_length=1, max_length=4096, pattern=r"\S")
    columns: tuple[str, ...] = ()


def add_value_routes(
    app: FastAPI, authorize: Callable[[str], Callable[..., OperationContext]]
) -> None:
    """Keep authorization, validation and writes in the same service as Python callers."""
    path = "/api/v1/pipelines/{code}/tasks/{task}/values"

    @app.post(path)
    def write(
        code: str,
        task: str,
        body: Edit,
        caller: Annotated[OperationContext, Depends(authorize("data edit"))],
    ) -> dict[str, Any]:
        return to_json(
            values.write_values(
                caller,
                code,
                task,
                values.ValueEdit(body.key, body.values, body.reason, body.row_lock),
            )
        )

    @app.post(path + "/unlock")
    def unlock(
        code: str,
        task: str,
        body: Unlock,
        caller: Annotated[OperationContext, Depends(authorize("data unlock"))],
    ) -> dict[str, Any]:
        return to_json(
            values.unlock_values(
                caller, code, task, values.ValueUnlock(body.key, body.reason, body.columns)
            )
        )

    @app.post(path + "/provenance")
    def provenance(
        code: str,
        task: str,
        body: Key,
        caller: Annotated[OperationContext, Depends(authorize("data provenance"))],
    ) -> dict[str, Any]:
        return to_json(values.provenance(caller, code, task, body.key))
