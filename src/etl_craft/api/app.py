"""The version-one HTTP API: one generated route per action, sharing the CLI's documents."""

import asyncio
import logging
from dataclasses import replace

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException

from etl_craft.api.browser_sessions import html_request
from etl_craft.api.gateway import Gateway, GatewayMiddleware
from etl_craft.api.login import SIGN_IN, add_login_routes
from etl_craft.api.routes import add_action_routes
from etl_craft.api.ui_errors import browser_error, http_error
from etl_craft.api.ui_headers import BrowserHeaders, protect
from etl_craft.api.ui_live import add_live_route
from etl_craft.api.ui_monitor import add_monitor_pages
from etl_craft.api.ui_pages import add_pages
from etl_craft.core.errors import (
    EtlCraftError,
    MetadataError,
    MigrationError,
    PermissionDeniedError,
    ResourceNotFoundError,
    UsageError,
)
from etl_craft.services.operations import OperationContext, to_json
from etl_craft.services.operations.actions import ACTIONS
from etl_craft.services.operations.metrics import scrape
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

    @app.get("/metrics")
    def metrics() -> Response:
        result = scrape(ctx)
        return Response(result.body, result.status, headers={"Content-Type": result.content_type})

    ctx = replace(ctx, shutdown=None if gateway is None else gateway.stop)
    app.state.gateway = gateway
    app.add_middleware(GatewayMiddleware, ctx=ctx, gateway=gateway)

    @app.get("/health")
    async def health() -> object:
        if gateway is None:
            return to_json(probe(ctx))
        try:
            return to_json(await asyncio.to_thread(readiness, ctx))
        except (EtlCraftError, SQLAlchemyError) as error:
            logger.error("UI readiness check failed: %s", type(error).__name__)
            cause = (
                "package version does not match applied migrations; "
                "install the matching package and run `etl-craft migrate`"
                if isinstance(error, MigrationError)
                else "database or configuration check failed; "
                "check Engine DB connectivity, database logs and server configuration"
            )
            return JSONResponse({"message": f"Engine DB is not ready: {cause}"}, 503)

    _add_error_handlers(app)
    app.add_middleware(BrowserHeaders)
    add_pages(app, ctx)
    add_monitor_pages(app, ctx)
    add_live_route(app, ctx)
    add_login_routes(app, ctx)
    routed = frozenset(name for name, action in ACTIONS.items() if action.path is not None)
    add_action_routes(app, ctx, routed - SIGN_IN)
    return app


def _add_error_handlers(app: FastAPI) -> None:
    """Answer domain errors with their exit code, and Engine DB errors with 503."""

    @app.exception_handler(EtlCraftError)
    async def domain_error(request: Request, error: EtlCraftError) -> Response:
        status = (
            403
            if isinstance(error, PermissionDeniedError)
            else 400
            if isinstance(error, UsageError)
            else 404
            if isinstance(error, (MetadataError, ResourceNotFoundError))
            else 409
        )
        if html_request(request):
            return browser_error(request, status, str(error))
        return JSONResponse(
            {
                "error": type(error).__name__,
                "message": str(error),
                "exit_code": int(error.exit_code),
            },
            status_code=status,
        )

    @app.exception_handler(SQLAlchemyError)
    async def storage_error(request: Request, error: SQLAlchemyError) -> Response:
        logger.error("Engine DB request failed: %s", type(error).__name__)
        if html_request(request):
            return browser_error(request, 503)
        return JSONResponse(
            {"error": "EngineDbError", "message": "Engine DB request failed; check the server log"},
            status_code=503,
        )

    @app.exception_handler(HTTPException)
    async def http_failure(request: Request, error: HTTPException) -> Response:
        return http_error(request, error)

    @app.exception_handler(Exception)
    async def unexpected(request: Request, error: Exception) -> Response:
        # The HTTP process boundary records unexpected failures without exposing their details.
        logger.exception("HTTP request %s failed", getattr(request.state, "request_id", "unknown"))
        if html_request(request):
            response = browser_error(request, 500)
            protect(response.headers, getattr(request.state, "request_id", "unknown"))
            return response
        response = JSONResponse(
            {
                "error": "ServerError",
                "message": "Request failed; check the server log",
                "request_id": getattr(request.state, "request_id", "unknown"),
            },
            500,
        )

        protect(response.headers, getattr(request.state, "request_id", "unknown"))
        return response
