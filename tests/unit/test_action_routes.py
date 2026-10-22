"""Every action row, through every interface: the same request reaches the same operation."""

import enum
import re
import types
import typing
from contextlib import contextmanager, nullcontext
from dataclasses import MISSING, fields, is_dataclass, replace
from urllib.parse import urlsplit

import pytest
from fastapi.testclient import TestClient

from etl_craft.api import routes
from etl_craft.api.app import create_app
from etl_craft.cli.client import ApiClient
from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import ApiRole, Audit, HttpMethod, RunStatus
from etl_craft.core.errors import PermissionDeniedError, UsageError
from etl_craft.services.operations import OperationContext, actions
from etl_craft.services.operations.actions import ACTIONS, invoke
from etl_craft.services.operations.permissions import PipelineAccess
from etl_craft.services.operations.serialization import encode
from fixtures.agents import resources

pytestmark = pytest.mark.unit

ROUTED = [name for name, action in ACTIONS.items() if action.path is not None]
CALLED = [name for name, action in ACTIONS.items() if action.call is not None]
AUDITED = [name for name in CALLED if ACTIONS[name].audit == Audit.REQUEST]
ADMIN = Actor("owner", ActorKind.SERVICE)


def sample(hint):
    """Return a value of ``hint`` that its request type accepts."""
    origin = typing.get_origin(hint)
    if origin in (typing.Union, types.UnionType):
        return sample(next(arg for arg in typing.get_args(hint) if arg is not type(None)))
    if origin is tuple:
        return (sample(typing.get_args(hint)[0]),)
    if origin is dict:
        return {"id": 1}
    if isinstance(hint, type) and issubclass(hint, enum.Enum):
        return next(iter(hint))
    if is_dataclass(hint):
        return build(hint)
    return {str: "x", int: 1, bool: False}[hint]


# Requests whose checks need more than one value of each required field's type.
GIVEN = {
    "MarkRequest": {"status": RunStatus.SUCCESS},
    "ControlRequest": {"run_id": 1, "request_key": "k"},
    "AccountAdd": {"password": "secret", "role": ApiRole.ADMIN},
    "AccountRole": {"role": ApiRole.ADMIN},
    "AgentRegister": {"resources": resources()},
    "AgentRenew": {"resources": resources()},
}


def build(cls):
    hints = typing.get_type_hints(cls)
    required = {
        f.name: sample(hints[f.name])
        for f in fields(cls)
        if f.default is MISSING
        and f.default_factory is MISSING
        and f.name not in GIVEN.get(cls.__name__, {})
    }
    return cls(**{**required, **GIVEN.get(cls.__name__, {})})


def context(role=ApiRole.ADMIN):
    return OperationContext(
        types.SimpleNamespace(connect=lambda: nullcontext(None)),
        types.SimpleNamespace(docs_site=None),
        ADMIN,
        role=role,
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
        lambda ctx, token: types.SimpleNamespace(actor=ADMIN, role=ApiRole.ADMIN.value, token_id=1),
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
    table = {(ACTIONS[name].method, ACTIONS[name].path) for name in ROUTED}
    assert len(table) == len(ROUTED)
    # FastAPI serves its own contract and its interactive page beside the actions.
    assert generated - {("GET", "/api/v1/openapi.json"), ("GET", "/api/v1/docs")} == table


@pytest.mark.parametrize("name", ROUTED)
def test_a_route_names_its_method_and_carries_only_request_fields_in_its_path(name):
    action = ACTIONS[name]
    assert action.method in set(HttpMethod)
    assert re.fullmatch(r"/api/v1(/[a-z-]+|/\{[a-z_]+\})+", action.path), action.path
    in_path = set(re.findall(r"{([a-z_]+)}", action.path))
    assert in_path <= {f.name for f in fields(action.request)}
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
        result = ApiClient("http://127.0.0.1:8000").invoke(name, request)
        list(result)  # a streamed action sends its request when read
    assert calls == [(name, request)]


@pytest.mark.parametrize("name", ROUTED)
def test_an_unknown_field_is_refused_by_name_before_the_operation_runs(server, name):
    client, calls = server
    action = ACTIONS[name]
    given = encode(build(action.request))
    path = action.path.format(
        **{key: given.pop(key) for key in re.findall(r"{([a-z_]+)}", action.path)}
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
    given[field] = "not-a-number"
    path_fields = re.findall(r"{([a-z_]+)}", action.path)
    path = action.path.format(**{key: given.pop(key) for key in path_fields})
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
    invoke(context(ACTIONS[name].role), name, build(ACTIONS[name].request))
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
        with pytest.raises(PermissionDeniedError, match=rf"{action.role}( role is required| role)"):
            invoke(context(role), name, build(action.request))
    assert operations == [name]
    assert refused == [name] * (1 + len(below))
    assert entered == [name]
