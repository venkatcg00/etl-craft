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
from starlette.concurrency import run_in_threadpool

from etl_craft.core.enums import ActionName, HttpMethod, HttpScheme, Transport
from etl_craft.core.errors import UsageError
from etl_craft.core.text import sha256_hex
from etl_craft.services.operations import OperationContext, to_json
from etl_craft.services.operations.actions import ACTIONS, Action, action_path, invoke
from etl_craft.services.operations.identity import authenticated_context
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


def _summary(action: Action) -> str:
    """Return the first line of the operation's docstring, else the action's name."""
    lines = (getattr(action.call, "__doc__", None) or "").strip().splitlines()
    return lines[0] if lines else action.name


def _endpoint(ctx: OperationContext, action: Action) -> Endpoint:
    if action.role is None:

        async def open_route(request: Request) -> Response:
            return await _answer(replace(ctx, transport=Transport.HTTP), action, request)

        return open_route

    async def signed_in(
        request: Request,
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(BEARER)],
    ) -> Response:
        caller = await run_in_threadpool(_caller, ctx, credentials)
        return await _answer(caller, action, request)

    return signed_in


def _caller(
    ctx: OperationContext, credentials: HTTPAuthorizationCredentials | None
) -> OperationContext:
    """Prove the bearer token's account; the action table decides what it may do."""
    identity = None if credentials is None else authenticate(ctx, credentials.credentials)
    if identity is None:
        raise HTTPException(
            401, "a valid bearer token is required", headers={"WWW-Authenticate": "Bearer"}
        )
    assert credentials is not None
    return replace(
        authenticated_context(ctx, identity),
        transport=Transport.HTTP,
        credential_digest=sha256_hex(credentials.credentials.encode()),
    )


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
        given.update(body)
    parsed = _parse(action, given)
    result = await run_in_threadpool(invoke, caller, action.name, parsed)
    if isawaitable(result):
        result = await result
    if action.streams:
        assert isinstance(result, Iterator)
        return StreamingResponse(result, media_type="text/plain")
    if action.result is None:
        return Response(status_code=204)
    return JSONResponse(to_json(result))


async def read_body(request: Request, maximum: int | None) -> object:
    """Bound upload bytes before JSON parsing, including chunked requests without a length."""
    content = bytearray()
    async for chunk in request.stream():
        found = len(content) + len(chunk)
        if maximum is not None and found > maximum:
            raise UsageError(
                f"{request.url.path}: request body has at least {found} bytes; expected "
                f"at most {maximum}; send a smaller request with shorter paths or content"
            )
        content.extend(chunk)
    return json.loads(content) if content else {}


def _parse(action: Action, given: dict[str, object]) -> object:
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
        if field in in_path or action.method == HttpMethod.GET
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
    return extra
