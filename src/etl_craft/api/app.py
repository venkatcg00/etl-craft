"""The version-one HTTP API: one generated route per action, sharing the CLI's documents."""

import asyncio
import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.api.gateway import Gateway, GatewayMiddleware
from etl_craft.api.login import SIGN_IN, add_login_routes
from etl_craft.api.routes import add_action_routes
from etl_craft.core.errors import (
    EtlCraftError,
    MetadataError,
    PermissionDeniedError,
    ResourceNotFoundError,
    UsageError,
)
from etl_craft.services.operations import OperationContext, to_json
from etl_craft.services.operations.actions import ACTIONS
from etl_craft.services.operations.processes import probe, readiness

logger = logging.getLogger(__name__)


def create_app(ctx: OperationContext, *, gateway: Gateway | None = None) -> FastAPI:
    """Build routes without opening connections or starting the controller."""
    app = FastAPI(
        title="etl-craft",
        version="1",
        docs_url="/api/v1/docs",
        redoc_url=None,
        openapi_url="/api/v1/openapi.json",
    )

    app.state.gateway = gateway
    app.add_middleware(GatewayMiddleware, ctx=ctx, gateway=gateway)

    @app.get("/health")
    async def health() -> object:
        if gateway is None:
            return to_json(probe(ctx))
        try:
            return to_json(await asyncio.to_thread(readiness, ctx))
        except (EtlCraftError, SQLAlchemyError) as error:
            return JSONResponse({"message": f"Engine DB is not ready: {error}"}, 503)

    _add_error_handlers(app)
    add_login_routes(app, ctx)
    routed = frozenset(name for name, action in ACTIONS.items() if action.path is not None)
    add_action_routes(app, ctx, routed - SIGN_IN)
    return app


def _add_error_handlers(app: FastAPI) -> None:
    """Answer domain errors with their exit code, and Engine DB errors with 503."""

    @app.exception_handler(EtlCraftError)
    async def domain_error(request: Request, error: EtlCraftError) -> JSONResponse:
        status = (
            403
            if isinstance(error, PermissionDeniedError)
            else 400
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
