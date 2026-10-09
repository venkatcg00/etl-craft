"""Version-one HTTP routes sharing the CLI's operations and JSON documents."""

import logging
from collections.abc import Callable
from dataclasses import replace
from datetime import date
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.core.errors import EtlCraftError, MetadataError, ResourceNotFoundError, UsageError
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations import (
    OperationContext,
    api_reads,
    backfills,
    inspect,
    pipelines,
    runs,
    tasks,
    to_json,
    tokens,
)
from etl_craft.services.operations.status import explain_task

logger = logging.getLogger(__name__)


class Trigger(BaseModel):
    """A pipeline trigger with optional logical date and audit reason."""

    model_config = ConfigDict(extra="forbid")
    run_date: date | None = None
    reason: str | None = None


class Reason(BaseModel):
    """A required, nonblank explanation for a human intervention."""

    model_config = ConfigDict(extra="forbid")
    reason: str = Field(min_length=1, max_length=4096, pattern=r"\S")


class Backfill(Reason):
    """The inclusive date range to execute."""

    first: date
    last: date


class Mark(Reason):
    """One task's requested state and optional written row count."""

    status: str
    rows: int | None = Field(default=None, ge=0)
    stale: bool = False


class Rerun(Reason):
    """Whether to rerun downstream tasks as well."""

    with_downstream: bool = False


class TokenInput(BaseModel):
    """The name, role and optional day lifetime of a new credential."""

    model_config = ConfigDict(extra="forbid")
    name: str
    role: str
    expires: str | None = None


def create_app(ctx: OperationContext) -> FastAPI:
    """Build routes without opening connections or starting the overseer."""
    app = FastAPI(
        title="etl-craft",
        version="1",
        docs_url="/api/v1/docs",
        redoc_url=None,
        openapi_url="/api/v1/openapi.json",
    )
    bearer = HTTPBearer(auto_error=False)

    def authorize(role: str) -> Callable[..., OperationContext]:
        def authorized(
            credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
        ) -> OperationContext:
            identity = (
                None if credentials is None else tokens.authenticate(ctx, credentials.credentials)
            )
            if identity is None:
                raise HTTPException(
                    401, "a valid bearer token is required", headers={"WWW-Authenticate": "Bearer"}
                )
            if tokens.ROLES.index(identity.role) < tokens.ROLES.index(role):
                raise HTTPException(403, f"{role} role is required")
            return replace(ctx, actor=identity.actor)

        return authorized

    read = authorize("viewer")
    write = authorize("operator")
    admin = authorize("admin")

    @app.exception_handler(EtlCraftError)
    async def domain_error(request: Request, error: EtlCraftError) -> JSONResponse:
        status = (
            400
            if isinstance(error, UsageError)
            else 404
            if isinstance(error, (MetadataError, ResourceNotFoundError))
            else 409
        )
        return JSONResponse(
            {
                "error": type(error).__name__,
                "message": str(error),
                "exit_code": int(error.exit_code),
            },
            status_code=status,
        )

    @app.exception_handler(SQLAlchemyError)
    async def storage_error(request: Request, error: SQLAlchemyError) -> JSONResponse:
        logger.error("Engine DB request failed: %s", type(error).__name__)
        return JSONResponse(
            {"error": "EngineDbError", "message": "Engine DB request failed; check the server log"},
            status_code=503,
        )

    @app.get("/api/v1/health")
    def health(caller: Annotated[OperationContext, Depends(read)]) -> dict[str, str]:
        return {"schema": "etl-craft/health/1", "status": "ok"}

    @app.get("/api/v1/pipelines")
    def list_pipelines(caller: Annotated[OperationContext, Depends(read)]) -> dict[str, Any]:
        return to_json(inspect.list_pipelines(caller))

    @app.get("/api/v1/pipelines/{code}")
    def pipeline(code: str, caller: Annotated[OperationContext, Depends(read)]) -> dict[str, Any]:
        return to_json(api_reads.get_pipeline(caller, code))

    @app.get("/api/v1/pipelines/{code}/runs")
    def history(
        code: str,
        caller: Annotated[OperationContext, Depends(read)],
        limit: Annotated[int, Query(ge=1, le=100)] = 20,
        before: Annotated[int | None, Query(ge=1)] = None,
    ) -> dict[str, Any]:
        return to_json(api_reads.get_runs(caller, code, limit, before))

    @app.post("/api/v1/pipelines/{code}/runs")
    def trigger(
        code: str, body: Trigger, caller: Annotated[OperationContext, Depends(write)]
    ) -> dict[str, Any]:
        return to_json(
            runs.trigger_run(
                caller, code, run_date=body.run_date, reason=body.reason, init_only=True
            )
        )

    @app.post("/api/v1/pipelines/{code}/backfills")
    def backfill(
        code: str, body: Backfill, caller: Annotated[OperationContext, Depends(write)]
    ) -> dict[str, Any]:
        return to_json(backfills.run_backfill(caller, code, body.first, body.last, body.reason))

    @app.post("/api/v1/pipelines/{code}/pause")
    def pause(
        code: str, body: Reason, caller: Annotated[OperationContext, Depends(write)]
    ) -> dict[str, Any]:
        return to_json(pipelines.set_pause(caller, code, body.reason, verb="pause"))

    @app.post("/api/v1/pipelines/{code}/resume")
    def resume(
        code: str, body: Reason, caller: Annotated[OperationContext, Depends(write)]
    ) -> dict[str, Any]:
        return to_json(pipelines.set_pause(caller, code, body.reason, verb="resume"))

    @app.get("/api/v1/runs/{run_id}")
    def run(run_id: int, caller: Annotated[OperationContext, Depends(read)]) -> dict[str, Any]:
        return to_json(api_reads.get_run_status(caller, run_id).run)

    @app.get("/api/v1/runs/{run_id}/tasks")
    def run_tasks(
        run_id: int, caller: Annotated[OperationContext, Depends(read)]
    ) -> dict[str, Any]:
        return to_json(
            inspect.pipeline_steps(
                caller, api_reads.run_code(caller, run_id), selector=RunSelector(run_id=run_id)
            )
        )

    @app.get("/api/v1/runs/{run_id}/tasks/{task}/explain")
    def explain(
        run_id: int, task: str, caller: Annotated[OperationContext, Depends(read)]
    ) -> dict[str, Any]:
        return to_json(
            explain_task(
                caller,
                api_reads.run_code(caller, run_id),
                task,
                selector=RunSelector(run_id=run_id),
            )
        )

    @app.post("/api/v1/runs/{run_id}/cancel")
    def cancel(
        run_id: int, body: Reason, caller: Annotated[OperationContext, Depends(write)]
    ) -> dict[str, Any]:
        return to_json(
            runs.cancel_run(
                caller,
                api_reads.run_code(caller, run_id),
                body.reason,
                selector=RunSelector(run_id=run_id),
            )
        )

    @app.post("/api/v1/runs/{run_id}/tasks/{task}/mark")
    def mark(
        run_id: int, task: str, body: Mark, caller: Annotated[OperationContext, Depends(write)]
    ) -> dict[str, Any]:
        return to_json(
            tasks.mark_task(
                caller,
                api_reads.run_code(caller, run_id),
                task,
                body.status,
                body.reason,
                rows=body.rows,
                stale=body.stale,
                selector=RunSelector(run_id=run_id),
            )
        )

    @app.post("/api/v1/runs/{run_id}/tasks/{task}/rerun")
    def rerun(
        run_id: int, task: str, body: Rerun, caller: Annotated[OperationContext, Depends(write)]
    ) -> dict[str, Any]:
        return to_json(
            tasks.rerun_task(
                caller,
                api_reads.run_code(caller, run_id),
                task,
                body.reason,
                with_downstream=body.with_downstream,
                selector=RunSelector(run_id=run_id),
            )
        )

    @app.get("/api/v1/attempts/{attempt_id}")
    def attempt(
        attempt_id: int, caller: Annotated[OperationContext, Depends(read)]
    ) -> dict[str, Any]:
        return to_json(api_reads.get_attempt(caller, attempt_id))

    @app.get("/api/v1/attempts/{attempt_id}/log")
    def log(
        attempt_id: int,
        caller: Annotated[OperationContext, Depends(read)],
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> StreamingResponse:
        return StreamingResponse(
            api_reads.attempt_log(caller, attempt_id, offset), media_type="text/plain"
        )

    @app.get("/api/v1/tokens")
    def list_tokens(caller: Annotated[OperationContext, Depends(admin)]) -> dict[str, Any]:
        return to_json(tokens.list_tokens(caller))

    @app.post("/api/v1/tokens")
    def token_create(
        body: TokenInput, caller: Annotated[OperationContext, Depends(admin)]
    ) -> dict[str, Any]:
        return to_json(tokens.create_token(caller, body.name, body.role, body.expires))

    @app.post("/api/v1/tokens/{token_id}/revoke")
    def token_revoke(
        token_id: int, caller: Annotated[OperationContext, Depends(admin)]
    ) -> dict[str, Any]:
        return to_json(tokens.revoke_token(caller, token_id))

    return app
