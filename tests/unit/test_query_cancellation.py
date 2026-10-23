"""Query-control responses must prove termination, not merely accept a cancel request."""

import io
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.error import URLError

import pytest
from sqlalchemy.engine import URL

from etl_craft.core.errors import ConfigurationError, LockTimeoutError, SqlGuardError
from etl_craft.dialects.warehouse import databricks_queries as queries
from etl_craft.dialects.warehouse import for_key
from etl_craft.execution.query_cancellation import confirm
from etl_craft.warehouse import fencing

pytestmark = pytest.mark.unit
TAG = "etl-craft/attempt:7:0123456789abcdef01234567"
QUERY = "00000000-0000-0000-0000-000000000001"


def tagged(*, ended=None, tag=TAG, identity=QUERY):
    return {
        "query_id": identity,
        "query_end_time_ms": ended,
        "status": "FINISHED" if ended else "RUNNING",
        "query_tags": [{"key": "etl_craft_holder", "value": tag}],
    }


@pytest.mark.parametrize("ends", [None, 0, 123])
def test_databricks_tag_lookup_reads_every_page_and_excludes_other_holders(monkeypatch, ends):
    request = Mock(
        side_effect=[
            {"res": [tagged(tag=TAG + "other")], "has_next_page": True, "next_page_token": "+page"},
            {"res": [tagged(ended=ends)], "has_next_page": False},
        ]
    )
    monkeypatch.setattr(queries, "request", request)
    assert queries.active(Mock(), TAG) == ([QUERY] if not ends else [])
    assert "page_token=%2Bpage" in request.call_args.args[1]
    assert request.call_count == 2


@pytest.mark.parametrize(
    "data",
    [
        {},
        {"res": None},
        {"res": [1]},
        {"res": [tagged(identity="../other")]},
        {"res": [], "has_next_page": True},
    ],
)
def test_databricks_incomplete_history_never_means_stopped(monkeypatch, data):
    monkeypatch.setattr(queries, "request", Mock(return_value=data))
    with pytest.raises(
        SqlGuardError, match=r"Databricks holder.*expected|Databricks holder.*invalid"
    ):
        queries.active(Mock(), TAG)


def test_databricks_cancel_receipt_is_not_a_query_ending(monkeypatch):
    responses = []

    def request(conn, path, *, method="GET"):
        responses.append((path, method))
        return {} if method == "POST" else {"res": [tagged()]}

    monkeypatch.setattr(queries, "request", request)
    dialect = for_key("databricks")
    conn = Mock()
    dialect.stop_tagged(conn, TAG)
    assert (f"statements/{QUERY}/cancel", "POST") in responses
    assert dialect.running_tagged(conn, TAG) == 1
    assert dialect.session_tag_args(TAG) == {"query_tags": {"etl_craft_holder": TAG}}


def test_databricks_control_reuses_session_authentication_and_exact_host(monkeypatch):
    conn = Mock()
    conn.engine.url = URL.create("databricks", host="workspace.example")
    conn.connection.dbapi_connection.session.auth_provider.add_headers.side_effect = lambda h: (
        h.update(Authorization="Bearer test-token")
    )
    send = Mock(return_value=io.BytesIO(b"{}"))
    monkeypatch.setattr(queries, "open_request", send)
    assert queries.request(conn, f"statements/{QUERY}/cancel", method="POST") == {}
    sent = send.call_args.args[0]
    assert sent.full_url == f"https://workspace.example/api/2.0/sql/statements/{QUERY}/cancel"
    assert sent.get_header("Authorization") == "Bearer test-token"
    assert sent.method == "POST"
    assert send.call_args.kwargs["timeout"] == 5


@pytest.mark.parametrize("failure", [URLError("private detail"), ValueError("private detail")])
def test_databricks_control_failures_do_not_echo_authentication_or_response_body(
    monkeypatch, failure
):
    conn = Mock()
    conn.engine.url = URL.create("databricks", host="workspace.example")
    monkeypatch.setattr(queries, "open_request", Mock(side_effect=failure))
    with pytest.raises(SqlGuardError) as caught:
        queries.request(conn, f"statements/{QUERY}/cancel", method="POST")
    for expected in ("Databricks", "expected an authenticated", "permissions", "retry"):
        assert expected in str(caught.value)
    assert "private detail" not in str(caught.value)


def test_databricks_missing_authentication_provider_fails_before_request(monkeypatch):
    conn = SimpleNamespace(connection=SimpleNamespace(dbapi_connection=None))
    send = Mock()
    monkeypatch.setattr(queries, "open_request", send)
    with pytest.raises(ConfigurationError, match=r"no authenticated session provider.*upgrade"):
        queries.request(conn, "history/queries")
    send.assert_not_called()


def test_failed_confirmation_returns_the_pending_reason(monkeypatch):
    config = SimpleNamespace(warehouse=object())
    dialect = SimpleNamespace(session_fencing=True, process_local_queries=False)
    monkeypatch.setattr(
        "etl_craft.execution.query_cancellation.warehouse_dialect", lambda _: dialect
    )
    failure = LockTimeoutError(
        f"{TAG}: 1 statement still running; stop it in the warehouse, then retry"
    )
    monkeypatch.setattr(
        "etl_craft.execution.query_cancellation.confirm_stopped", Mock(side_effect=failure)
    )
    reason = confirm(config, TAG)
    assert TAG in reason and "pending" in reason and "still running" in reason


@pytest.mark.parametrize("embedded", [True, False])
def test_sigterm_cancels_only_the_bound_holder(monkeypatch, embedded):
    config = SimpleNamespace(warehouse=object())
    dialect = SimpleNamespace(session_fencing=True, process_local_queries=embedded)
    monkeypatch.setattr(fencing, "warehouse_dialect", lambda _: dialect)
    stop = Mock()
    monkeypatch.setattr(fencing, "confirm_stopped", stop)
    with pytest.raises(KeyboardInterrupt), fencing.cancellation_scope(config, TAG):
        fencing.stop_current_queries()
    fencing.stop_current_queries()
    assert stop.call_args_list == ([] if embedded else [((config, TAG),)])


def test_event_holder_tag_fits_postgresql_without_truncation():
    from etl_craft.engine.resource_locks import holder_tag

    tag = holder_tag(f"action:e:{2**63 - 1:x}:{2**63 - 1:x}:ffffffff")
    assert len(tag.encode()) <= 63
    assert tag.endswith(":ffffffff")


@pytest.mark.parametrize("state", ["UNKNOWN", None])
def test_unknown_databricks_query_state_cannot_acknowledge_cancellation(monkeypatch, state):
    row = tagged()
    row["status"] = state
    monkeypatch.setattr(queries, "request", Mock(return_value={"res": [row]}))
    with pytest.raises(SqlGuardError, match=r"query state.*expected.*inspect"):
        queries.active(Mock(), TAG)


def test_databricks_explicit_empty_final_page_confirms_no_running_queries(monkeypatch):
    monkeypatch.setattr(queries, "request", Mock(return_value={"has_next_page": False}))
    assert queries.active(Mock(), TAG) == []
