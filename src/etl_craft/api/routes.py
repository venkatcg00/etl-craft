"""One HTTP route per action, generated from the action table.

A route only reads the request (query and path for a GET, JSON body for a POST), calls
``invoke`` as the bearer token's account, and answers with the result's JSON document.
"""

from collections.abc import Awaitable, Callable, Iterator
from dataclasses import replace
from functools import cache
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import TypeAdapter, ValidationError
from starlette.concurrency import run_in_threadpool

from etl_craft.core.enums import ActionName, ApiRole, HttpMethod, Transport
from etl_craft.core.errors import UsageError
from etl_craft.services.operations import OperationContext, to_json
from etl_craft.services.operations.actions import ACTIONS, Action, invoke
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
            action.path,
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
    return replace(ctx, actor=identity.actor, role=ApiRole(identity.role), transport=Transport.HTTP)


async def _answer(caller: OperationContext, action: Action, request: Request) -> Response:
    given: dict[str, object] = dict(request.path_params)
    if action.method == HttpMethod.GET:
        given.update(request.query_params)
    else:
        body = await request.json() if await request.body() else {}
        if not isinstance(body, dict):
            raise UsageError("the request body must be a JSON object")
        given.update(body)
    parsed = _parse(action, given)
    result = await run_in_threadpool(invoke, caller, action.name, parsed)
    if action.streams:
        assert isinstance(result, Iterator)
        return StreamingResponse(result, media_type="text/plain")
    if action.result is None:
        return Response(status_code=204)
    return JSONResponse(to_json(result))


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
    assert action.path is not None
    in_path = {part[1:-1] for part in action.path.split("/") if part.startswith("{")}
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
    extra: dict[str, object] = {"parameters": parameters}
    if action.method == HttpMethod.POST and set(schema.get("properties", {})) - in_path:
        extra["requestBody"] = {
            "required": bool(required - in_path),
            "content": {"application/json": {"schema": schema}},
        }
    return extra
