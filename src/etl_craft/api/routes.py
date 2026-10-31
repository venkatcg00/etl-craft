"""One HTTP route per action, generated from the action table.

A route only reads the request (query and path for a GET, JSON body for a POST), calls
``invoke`` as the bearer token's account, and answers with the result's JSON document.
"""

import json
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import replace
from functools import cache
from inspect import isawaitable
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import TypeAdapter, ValidationError
from starlette.background import BackgroundTask
from starlette.concurrency import run_in_threadpool

from etl_craft.api import browser_sessions as browser
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, HttpMethod, HttpScheme, Transport
from etl_craft.core.errors import UsageError
from etl_craft.core.text import sha256_hex
from etl_craft.services.operations import OperationContext, to_json
from etl_craft.services.operations.action_types import Action
from etl_craft.services.operations.actions import ACTIONS, action_path, invoke
from etl_craft.services.operations.catalog_pages import CatalogPage
from etl_craft.services.operations.identity import authenticated_context
from etl_craft.services.operations.run_zip import RunZip
from etl_craft.services.operations.tokens import authenticate

BEARER = HTTPBearer(auto_error=False)

Endpoint = Callable[..., Awaitable[Response]]


@cache
def _adapter(action: ActionName) -> TypeAdapter[object]:
    return TypeAdapter(ACTIONS[action].request)


def add_action_routes(app: FastAPI, ctx: OperationContext, names: frozenset[ActionName]) -> None:
    """Add the route of every action in ``names``, as its table row describes it."""
    for name in sorted(names):
        action = ACTIONS[name]
        assert action.method is not None and action.path is not None
        app.add_api_route(
            action_path(action),
            _endpoint(ctx, action),
            methods=[action.method],
            name=name,
            operation_id=name.replace(" ", "_").replace("-", "_"),
            summary=_summary(action),
            openapi_extra=_openapi(action),
            response_model=None,
        )
        if name in {ActionName.UPLOAD, ActionName.UPLOADS, ActionName.UPLOAD_PIPELINES}:
            app.add_api_route(
                action.path,
                _endpoint(ctx, action),
                methods=[action.method],
                name=f"{name}-default",
                include_in_schema=False,
                response_model=None,
            )


def _summary(action: Action) -> str:
    """Return the first line of the operation's docstring, else the action's name."""
    lines = (getattr(action.call, "__doc__", None) or "").strip().splitlines()
    return lines[0] if lines else action.name


def _endpoint(ctx: OperationContext, action: Action) -> Endpoint:
    if action.role is None:

        async def open_route(request: Request) -> Response:
            return await _answer(
                replace(
                    ctx,
                    actor=SYSTEM_ACTOR,
                    role=None,
                    transport=Transport.HTTP,
                    browser_session=browser.browser_request(request),
                ),
                action,
                request,
            )

        return open_route

    async def signed_in(
        request: Request,
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(BEARER)],
    ) -> Response:
        caller = await run_in_threadpool(_caller, ctx, credentials, request)
        return await _answer(caller, action, request)

    return signed_in


def _caller(
    ctx: OperationContext, credentials: HTTPAuthorizationCredentials | None, request: Request
) -> OperationContext:
    """Prove the bearer token's account; the action table decides what it may do."""
    if credentials is None and browser.browser_request(request):
        return browser.cookie_context(ctx, request)
    identity = None if credentials is None else authenticate(ctx, credentials.credentials)
    if identity is None:
        if request.method == "POST" and request.url.path.endswith("/uploads"):
            from etl_craft.core.errors import PermissionDeniedError
            from etl_craft.services.operations.recording import record_refusal

            pipeline = request.query_params.get("pipeline_code", "(missing)")
            record_refusal(
                replace(ctx, actor=SYSTEM_ACTOR, role=None),
                ActionName.UPLOAD,
                {"pipeline_code": pipeline},
                PermissionDeniedError(
                    f"pipeline {pipeline}: signed-in identity is missing or invalid; "
                    "expected a valid account token with CAN_UPLOAD; sign in and upload one file"
                ),
            )
        detail = "a valid bearer token is required"
        if request.url.path.startswith("/api/v1/agent/uploads/"):
            upload = request.path_params.get("upload_id")
            attempt = request.query_params.get("attempt_id")
            detail = (
                f"upload {upload}, attempt {attempt}, run unavailable: credential or live "
                "attempt lease is invalid; expected the owning attempt token with a live lease; "
                "claim an eligible attempt and use its issued credential"
            )
        raise HTTPException(401, detail, headers={"WWW-Authenticate": "Bearer"})
    assert credentials is not None
    return replace(
        authenticated_context(ctx, identity),
        transport=Transport.HTTP,
        credential_digest=sha256_hex(credentials.credentials.encode()),
    )


async def _upload_answer(caller: OperationContext, action: Action, request: Request) -> Response:
    from etl_craft.api.uploads import receive

    if browser.browser_request(request):
        browser.csrf_check(caller, action.name, request, request.headers.get("X-CSRF-Token", ""))
    return await receive(caller, request)


async def _answer(caller: OperationContext, action: Action, request: Request) -> Response:
    caller = replace(
        caller,
        client_address=request.client.host if request.client else None,
        client_scheme=HttpScheme(request.scope["scheme"]),
    )
    given: dict[str, object] = dict(request.path_params)
    project = given.pop("project", None)
    if project is not None:
        caller = replace(caller, project=str(project), project_id=None)
    if action.name == ActionName.UPLOAD:
        return await _upload_answer(caller, action, request)
    destination = "/ui"
    if action.method == HttpMethod.GET:
        properties = _adapter(action.name).json_schema().get("properties", {})
        given.update(
            {
                key: request.query_params.getlist(key)
                if properties.get(key, {}).get("type") == "array"
                else request.query_params[key]
                for key in request.query_params
            }
        )
    else:
        body = await read_body(request, getattr(action.request, "MAX_BODY_BYTES", None))
        if not isinstance(body, dict):
            raise UsageError("the request body must be a JSON object")
        if browser.browser_request(request):
            await run_in_threadpool(
                browser.csrf_check,
                caller,
                action.name,
                request,
                request.headers.get("X-CSRF-Token", str(body.pop("_csrf", ""))),
            )
            destination = browser.return_path(str(body.pop("_return_to", "/ui")))
            _browser_fields(body, action.name, request)
        given.update(body)
    parsed = parse_request(action, given)
    result = await run_in_threadpool(invoke, caller, action.name, parsed)
    if isawaitable(result):
        result = await result
    if action.method == HttpMethod.POST and browser.browser_request(request):
        response = browser.browser_answer(caller, action.name, request, result, destination)
        if response is not None:
            return response
    if action.streams:
        return _stream_answer(action, result)
    if action.result is None:
        return Response(status_code=204)
    return JSONResponse(to_json(result))


def _browser_fields(body: dict[str, object], action: ActionName, request: Request) -> None:
    """Consume browser-only proof fields before the action request is parsed."""
    confirmation = body.pop("_confirm_password", None)
    if confirmation is not None and confirmation != body.get("password"):
        raise UsageError("password confirmation differs; enter the same new password twice")
    if action == ActionName.LOGOUT:
        body["token"] = request.cookies.get(browser.SESSION, "")


async def read_body(request: Request, maximum: int | None) -> object:
    """Bound upload bytes before JSON parsing, including chunked requests without a length."""
    maximum = maximum or (65536 if browser.browser_request(request) else None)
    content = bytearray()
    async for chunk in request.stream():
        found = len(content) + len(chunk)
        if maximum is not None and found > maximum:
            raise UsageError(
                f"{request.url.path}: request body has at least {found} bytes; expected "
                f"at most {maximum}; send a smaller request with shorter paths or content"
            )
        content.extend(chunk)
    try:
        if request.headers.get("content-type", "").startswith("application/x-www-form-urlencoded"):
            from urllib.parse import parse_qsl

            return dict(
                parse_qsl(content.decode("utf-8"), keep_blank_values=True, max_num_fields=32)
            )
        return json.loads(content) if content else {}
    except (ValueError, UnicodeError) as error:
        raise UsageError(
            "request body is malformed; expected a JSON object or browser form; correct the input"
        ) from error


def parse_request(action: Action, given: dict[str, object]) -> object:
    """Build the request; an unknown or malformed field is refused by its name."""
    try:
        return _adapter(action.name).validate_python(given)
    except ValidationError as error:
        problems = "; ".join(
            f"`{'.'.join(str(p) for p in e['loc'])}` {e['msg'].lower()}" for e in error.errors()
        )
        raise UsageError(f"{action.name}: {problems}") from None


def _openapi(action: Action) -> dict[str, object]:
    """Describe the request in the generated contract, as a body or as parameters."""
    schema = _adapter(action.name).json_schema()
    path = action_path(action)
    in_path = {part[1:-1] for part in path.split("/") if part.startswith("{")}
    in_path.discard("project")
    required = set(schema.get("required", ()))
    parameters = [
        {
            "name": field,
            "in": "path" if field in in_path else "query",
            "required": field in in_path or field in required,
            "schema": spec,
        }
        for field, spec in schema.get("properties", {}).items()
        if field in in_path or action.method == HttpMethod.GET or action.name == ActionName.UPLOAD
    ]
    if "{project}" in path:
        parameters.insert(
            0, {"name": "project", "in": "path", "required": True, "schema": {"type": "string"}}
        )
    extra: dict[str, object] = {"parameters": parameters}
    if action.method == HttpMethod.POST and set(schema.get("properties", {})) - in_path:
        extra["requestBody"] = {
            "required": bool(required - in_path),
            "content": {"application/json": {"schema": schema}},
        }
    if action.name == ActionName.UPLOAD:
        extra["requestBody"] = {
            "required": True,
            "content": {
                "multipart/form-data": {
                    "schema": {
                        "type": "object",
                        "required": ["file"],
                        "properties": {"file": {"type": "string", "format": "binary"}},
                    }
                }
            },
        }
    return extra


def _stream_answer(action: Action, result: object) -> Response:
    """Render a service-owned iterator without buffering it."""
    if isinstance(result, CatalogPage):
        return StreamingResponse(
            result,
            media_type=result.content_type,
            headers={"X-Content-Type-Options": "nosniff"},
            background=BackgroundTask(result.close),
        )
    if isinstance(result, RunZip):
        return StreamingResponse(
            result,
            media_type=RunZip.CONTENT_TYPE,
            headers={"Content-Disposition": f'attachment; filename="{result.filename}"'},
        )
    assert isinstance(result, Iterator)
    return StreamingResponse(
        result,
        media_type="application/octet-stream"
        if action.name == ActionName.AGENT_UPLOAD_CONTENT
        else "text/plain",
    )
