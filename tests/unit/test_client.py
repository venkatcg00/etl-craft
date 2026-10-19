"""Server addresses, server errors and operation documents rebuilt from their JSON."""

import json
from datetime import UTC, date, datetime

import pytest

from etl_craft.cli.client import _error, server_url
from etl_craft.core.enums import AccountKind, RunStatus, TokenKind
from etl_craft.core.errors import PermissionDeniedError, ServerError, UsageError
from etl_craft.core.graph import CycleError
from etl_craft.services.operations import from_json, to_json
from etl_craft.services.operations.models import BackfillView, OperationResult, RunView
from etl_craft.services.operations.sessions import Session
from etl_craft.services.operations.tokens import TokenView

pytestmark = pytest.mark.unit

STARTED = datetime(2026, 1, 5, 1, 2, 3, 400000, tzinfo=UTC)
RUN = RunView(
    1, "P", 7, "manual:k", "MANUAL", date(2026, 1, 5), "SUCCESS",
    STARTED, None, None, True, "alice", "USER", None, None,
)  # fmt: skip
DONE = OperationResult(RunStatus.SUCCESS, "P: done", 1, "P", run=RUN, waiting=True)


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("https://etl.example.com/", "https://etl.example.com"),
        ("https://etl.example.com:8443", "https://etl.example.com:8443"),
        ("http://localhost:8000", "http://localhost:8000"),
        ("http://127.0.0.1:9", "http://127.0.0.1:9"),
        ("http://[::1]:9", "http://[::1]:9"),
    ],
)
def test_servers_are_https_or_this_machine(given, expected):
    assert server_url(given) == expected


@pytest.mark.parametrize(
    ("given", "message"),
    [
        ("http://etl.example.com", "plain HTTP is refused"),
        ("ftp://etl.example.com", "nothing after it"),
        ("https://user:secret@etl.example.com", "nothing after it"),
        ("https://etl.example.com/api", "nothing after it"),
        ("https://etl.example.com/?token=x", "nothing after it"),
        ("https://etl.example.com:99999", "malformed"),
    ],
)
def test_other_server_addresses_are_refused(given, message):
    with pytest.raises(UsageError, match=message):
        server_url(given)


def test_documents_rebuild_nested_dates_enums_unions_and_optional_values():
    backfill = BackfillView(
        1, "P", date(2026, 1, 1), date(2026, 1, 2), RunStatus.FAILED, "b", (DONE, DONE), DONE
    )
    assert from_json(OperationResult, to_json(DONE)) == DONE
    assert from_json(BackfillView, to_json(backfill)) == backfill
    created = datetime(2026, 1, 5, tzinfo=UTC)
    token = TokenView(
        3, "alice", AccountKind.USER, TokenKind.ACCESS, "viewer", None, "alice", created, None, None
    )
    session = Session("access", token, None, created)
    assert from_json(Session, json.loads(json.dumps(to_json(session)))) == session


def test_a_newer_server_may_add_fields_but_not_drop_or_change_them():
    document = {**to_json(DONE), "added_later": 1}
    assert from_json(OperationResult, document) == DONE
    missing = to_json(DONE)
    del missing["message"]
    with pytest.raises(ServerError, match=r"OperationResult\.message: missing"):
        from_json(OperationResult, missing)
    with pytest.raises(ServerError, match="schema"):
        from_json(OperationResult, {**to_json(DONE), "schema": "etl-craft/other/1"})
    with pytest.raises(ServerError, match=r"status='LATER'; expected one of"):
        from_json(OperationResult, {**to_json(DONE), "status": "LATER"})
    with pytest.raises(ServerError, match="same etl-craft version"):
        from_json(OperationResult, {**to_json(DONE), "pipeline_id": "one"})


def test_server_errors_keep_their_class_and_exit_code():
    def answer(error, code):
        body = {"error": error.__name__, "message": "refused", "exit_code": int(code)}
        return _error(400, json.dumps(body).encode(), "https://etl")

    assert type(answer(PermissionDeniedError, PermissionDeniedError.exit_code)) is (
        PermissionDeniedError
    )
    assert type(answer(CycleError, CycleError.exit_code)) is CycleError
    assert type(answer(ServerError, 999)) is ServerError
    detail = _error(422, b'{"detail": [{"loc": ["body", "reason"]}]}', "https://etl")
    assert isinstance(detail, UsageError) and "HTTP 422" in str(detail)
    assert isinstance(_error(502, b"<html>bad gateway</html>", "https://etl"), ServerError)
