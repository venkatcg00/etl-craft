"""Every action row, through every interface: the same request reaches the same operation."""

import re
import types
import typing
from contextlib import contextmanager, nullcontext
from dataclasses import fields, replace
from urllib.parse import urlsplit

import pytest
from fastapi.testclient import TestClient

from etl_craft.api import routes
from etl_craft.api.app import create_app
from etl_craft.cli.client import ApiClient
from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import ActionScope, ApiRole, Audit, HttpMethod, Permission, TokenKind
from etl_craft.core.errors import PermissionDeniedError, ResourceNotFoundError, UsageError
from etl_craft.services.operations import OperationContext, actions
from etl_craft.services.operations.actions import ACTIONS, action_path, invoke
from etl_craft.services.operations.permissions import PipelineAccess
from etl_craft.services.operations.serialization import encode
from fixtures.actions import build

pytestmark = pytest.mark.unit

ROUTED = [name for name, action in ACTIONS.items() if action.path is not None]
CALLED = [name for name, action in ACTIONS.items() if action.call is not None]
AUDITED = [name for name in CALLED if ACTIONS[name].audit == Audit.REQUEST]
ADMIN = Actor("owner", ActorKind.SERVICE)


def context(role=ApiRole.ADMIN):
    return OperationContext(
        types.SimpleNamespace(connect=lambda: nullcontext(None)),
        types.SimpleNamespace(docs_site=None, artifacts=types.SimpleNamespace(store=None)),
        ADMIN,
        role=role,
        project_id=1,
        project="default",
    )


class CapturedError(Exception):
    """Raised by the stand-in operation, carrying what reached it."""


@pytest.fixture
def server(monkeypatch):
    """The generated app, with a proven admin token and operations that only record calls."""
    calls = []

    def capture(ctx, name, request):
        calls.append((name, request))
        raise CapturedError

    monkeypatch.setattr(routes, "invoke", capture)
    monkeypatch.setattr(
        routes,
        "authenticate",
        lambda ctx, token: types.SimpleNamespace(
            actor=ADMIN,
            role=ApiRole.ADMIN.value,
            token_id=1,
            token_kind=TokenKind.SERVICE,
            attempt_id=None,
        ),
    )
    return TestClient(create_app(context())), calls


def test_the_app_has_exactly_one_route_per_routed_action():
    app = create_app(context())
    generated = {
        (method, route.path)
        for route in app.routes
        if route.path.startswith("/api/v1/")
        for method in getattr(route, "methods", ()) or ()
        if method in set(HttpMethod)
    }
    table = {(ACTIONS[name].method, action_path(ACTIONS[name])) for name in ROUTED}
    assert len(table) == len(ROUTED)
    # FastAPI serves its own contract and its interactive page beside the actions.
    assert generated - {("GET", "/api/v1/openapi.json"), ("GET", "/api/v1/docs")} == table
    contract = app.openapi()["paths"]
    for method, path in table:
        if "{project}" in path:
            parameter = next(
                p for p in contract[path][method.lower()]["parameters"] if p["name"] == "project"
            )
            assert parameter["in"] == "path" and parameter["required"] is True


@pytest.mark.parametrize("name", ROUTED)
def test_a_route_names_its_method_and_carries_only_request_fields_in_its_path(name):
    action = ACTIONS[name]
    assert action.method in set(HttpMethod)
    assert re.fullmatch(r"/api/v1(/[a-z-]+|/\{[a-z_]+\})+", action_path(action)), action_path(
        action
    )
    in_path = set(re.findall(r"{([a-z_]+)}", action_path(action)))
    assert in_path <= {f.name for f in fields(action.request)} | {"project"}
    build(action.request)


@pytest.mark.parametrize("name", ROUTED)
def test_the_client_and_the_route_hand_the_operation_the_same_request(server, monkeypatch, name):
    client, calls = server
    monkeypatch.setenv("ETL_CRAFT_TOKEN", "service-token")

    class Opener:
        def open(self, request, timeout):
            url = urlsplit(request.full_url)
            client.request(
                request.get_method(),
                url.path + (f"?{url.query}" if url.query else ""),
                content=request.data,
                headers=dict(request.header_items()),
            )

    monkeypatch.setattr("etl_craft.core.http_client.build_opener", lambda *handlers: Opener())
    request = build(ACTIONS[name].request)
    with pytest.raises(CapturedError):
        result = ApiClient("http://127.0.0.1:8000", "default").invoke(name, request)
        list(result)  # a streamed action sends its request when read
    assert calls == [(name, request)]


@pytest.mark.parametrize("name", ROUTED)
def test_an_unknown_field_is_refused_by_name_before_the_operation_runs(server, name):
    client, calls = server
    action = ACTIONS[name]
    given = encode(build(action.request))
    given["project"] = "default"
    path = action_path(action).format(
        **{key: given.pop(key) for key in re.findall(r"{([a-z_]+)}", action_path(action))}
    )
    given = {key: value for key, value in given.items() if value is not None} | {"unexpected": 1}
    headers = {"Authorization": "Bearer token"}
    if action.method == HttpMethod.GET:
        response = client.get(path, params=given, headers=headers)
    else:
        response = client.post(path, json=given, headers=headers)
    assert response.status_code == 400, response.text
    assert "`unexpected`" in response.json()["message"]
    assert calls == []


INT_FIELDS = [
    (name, f.name)
    for name in ROUTED
    for f in fields(ACTIONS[name].request)
    if typing.get_type_hints(ACTIONS[name].request)[f.name] in (int, int | None)
]


@pytest.mark.parametrize("name,field", INT_FIELDS)
def test_a_malformed_value_is_refused_by_name_before_the_operation_runs(server, name, field):
    client, calls = server
    action = ACTIONS[name]
    given = {k: v for k, v in encode(build(action.request)).items() if v is not None}
    given["project"] = "default"
    given[field] = "not-a-number"
    path_fields = re.findall(r"{([a-z_]+)}", action_path(action))
    path = action_path(action).format(**{key: given.pop(key) for key in path_fields})
    headers = {"Authorization": "Bearer token"}
    if action.method == HttpMethod.GET:
        response = client.get(path, params=given, headers=headers)
    else:
        response = client.post(path, json=given, headers=headers)
    assert response.status_code == 400, response.text
    assert f"`{field}`" in response.json()["message"]
    assert calls == []


@pytest.fixture
def operations(monkeypatch):
    """Every action's operation replaced by one that records it ran."""
    ran = []
    # Transport tests isolate identity lookup; grant resolution is exercised on both Engine DBs.
    monkeypatch.setattr(actions, "_request_context", lambda ctx, name, request: ctx)

    def project_check(conn, ctx, needed=Permission.CAN_VIEW):
        if ctx.role is None:
            raise ResourceNotFoundError("project 'default' is missing or not permitted")
        if needed == Permission.CAN_MANAGE and ctx.role not in (ApiRole.DEVELOPER, ApiRole.ADMIN):
            raise PermissionDeniedError("project requires CAN_MANAGE")

    monkeypatch.setattr(actions, "require_project", project_check)
    monkeypatch.setattr(actions, "_request_codes", lambda conn, action, request: ["x"])
    monkeypatch.setattr(
        actions, "pipeline_access", lambda conn, ctx: PipelineAccess({"x": ctx.role})
    )
    for name in CALLED:
        monkeypatch.setitem(
            ACTIONS, name, replace(ACTIONS[name], call=lambda ctx, request, n=name: ran.append(n))
        )
    return ran


@pytest.mark.parametrize("name", CALLED)
def test_a_request_of_the_wrong_type_never_reaches_the_operation(operations, name):
    with pytest.raises(UsageError, match=f"{re.escape(name)}: request is object"):
        invoke(context(), name, object())
    assert operations == []


@contextmanager
def no_record(*args):
    pytest.fail(f"recorded {args[1]}")
    yield


@pytest.mark.parametrize(
    "name", [n for n in CALLED if ACTIONS[n].audit != Audit.REQUEST and ACTIONS[n].escalate is None]
)
def test_an_unaudited_action_records_no_request(operations, monkeypatch, name):
    monkeypatch.setattr(actions, "recorded", no_record)
    role = ApiRole.ADMIN if name in actions.PROJECT_MANAGE_ACTIONS else ACTIONS[name].role
    invoke(context(role), name, build(ACTIONS[name].request))
    assert operations == [name]


@pytest.mark.parametrize("name", [n for n in AUDITED if ACTIONS[n].escalate is None])
def test_an_audited_change_is_recorded_once_and_a_refusal_never_runs(operations, monkeypatch, name):
    action = ACTIONS[name]
    entered, refused = [], []

    @contextmanager
    def recorded(ctx, recorded_name, given):
        entered.append(recorded_name)
        yield

    monkeypatch.setattr(actions, "recorded", recorded)
    monkeypatch.setattr(actions, "record_refusal", lambda ctx, n, given, error: refused.append(n))
    invoke(context(action.role), name, build(action.request))
    assert (entered, operations) == ([name], [name])
    below = tuple(ApiRole)[: tuple(ApiRole).index(action.role)]
    for role in (None, *below):
        pattern = (
            rf"{action.role}( role is required| role)|CAN_MANAGE|missing or not permitted"
            if action.scope != ActionScope.DEPLOYMENT
            else rf"{action.role}( role is required| role)|missing or not permitted"
        )
        with pytest.raises((PermissionDeniedError, ResourceNotFoundError), match=pattern):
            invoke(context(role), name, build(action.request))
    assert operations == [name]
    assert refused == [name] * (1 + len(below))
    assert entered == [name]
