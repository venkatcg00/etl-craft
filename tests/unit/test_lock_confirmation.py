"""A stale lock passes only once its holder is confirmed stopped, where a warehouse can do it."""

from types import SimpleNamespace

import pytest

from etl_craft.agent.task_store import RemoteTaskStore
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import AgentReport
from etl_craft.core.errors import LockTimeoutError
from etl_craft.dialects.warehouse import registry
from etl_craft.engine.resource_locks import attempt_scope, session_tag
from etl_craft.warehouse import fencing

pytestmark = pytest.mark.unit


class Client:
    """An API that reports a stale holder until the agent confirms it."""

    def __init__(self):
        self.requests = []
        self.token = "agent-a"

    def call(self, action, request, timeout=None):
        self.requests.append(request)
        if request.operation == AgentReport.UNLOCK:
            return SimpleNamespace(data=None)
        if request.confirmed_holder == "attempt:7":
            return SimpleNamespace(data={"acquired": True, "stale_holder": None})
        return SimpleNamespace(data={"acquired": False, "stale_holder": "attempt:7"})


def locks(client):
    return [r.confirmed_holder for r in client.requests if r.operation == AgentReport.LOCK]


def test_an_agent_confirms_a_stale_holder_before_taking_its_lock():
    client, confirmed = Client(), []
    with RemoteTaskStore(client, 9, confirmed.append).hold(5, "s.target", 30):
        assert confirmed == ["attempt:7"]
    assert locks(client) == [None, "attempt:7"]


def test_an_unconfirmed_stop_keeps_the_agent_out_of_the_lock():
    client = Client()

    def refuse(holder_id):
        raise LockTimeoutError("2 warehouse statement(s) still run")

    store = RemoteTaskStore(client, 9, refuse)
    with (
        pytest.raises(LockTimeoutError, match=r"s\.target: the lock stays with attempt:7"),
        store.hold(5, "s.target", 30),
    ):
        pytest.fail("the agent wrote while the stale holder's statements still ran")
    assert locks(client) == [None]


@pytest.mark.parametrize("dialect", registry.all_dialects(), ids=lambda d: d.spec.key)
def test_cluster_execution_needs_a_warehouse_that_can_stop_old_statements(dialect, monkeypatch):
    monkeypatch.setattr(fencing, "warehouse_dialect", lambda config: dialect)
    problem = fencing.cluster_problem(SimpleNamespace(warehouse=object()))
    if dialect.session_fencing:
        assert problem is None
    else:
        assert problem is not None
        assert dialect.spec.display_name in problem
        assert "use local execution" in problem


def test_postgres_session_tag_limit_counts_bytes_and_accepts_the_boundary():
    from etl_craft.core.errors import SqlGuardError
    from etl_craft.dialects.warehouse.postgres import PostgresWarehouse

    dialect = PostgresWarehouse()
    tag = "é" * 31 + "x"
    assert len(tag.encode()) == 63
    assert dialect.session_tag_args(tag) == {"application_name": tag}
    with pytest.raises(SqlGuardError, match="63-byte limit"):
        dialect.session_tag_args("é" * 32)


def test_trino_refuses_a_query_id_containing_sql_before_calling_kill_query():
    from etl_craft.core.errors import SqlGuardError
    from etl_craft.dialects.warehouse.trino_iceberg import TrinoIcebergWarehouse

    calls = []

    def execute(statement, parameters=None):
        calls.append(str(statement))
        return SimpleNamespace(all=lambda: [SimpleNamespace(query_id="q'; DROP TABLE target;--")])

    with pytest.raises(SqlGuardError, match="unexpected query id"):
        TrinoIcebergWarehouse().stop_tagged(SimpleNamespace(execute=execute), "etl-craft/attempt:1")
    assert len(calls) == 1
    assert "kill_query" not in calls[0]


@pytest.mark.parametrize("confirmable", [True, False])
def test_failed_agent_warehouse_work_confirms_before_unlocking(confirmable):
    client, confirmed = Client(), []

    def confirm(holder):
        assert not any(r.operation == AgentReport.UNLOCK for r in client.requests)
        confirmed.append(holder)
        if holder != "attempt:7" and not confirmable:
            raise LockTimeoutError("warehouse still running")

    store = RemoteTaskStore(client, 9, confirm)
    expected = ConnectionError if confirmable else LockTimeoutError
    with pytest.raises(expected), store.hold(5, "s.target", 30):
        raise ConnectionError("warehouse connection dropped")
    assert len(confirmed) == 2
    assert confirmed[0] == "attempt:7"
    assert confirmed[1].startswith("attempt:9:")
    assert any(r.operation == AgentReport.UNLOCK for r in client.requests) == confirmable


def test_local_attempt_tags_are_distinct_between_deployments():
    tags = []
    with acting_as(SYSTEM_ACTOR):
        for owner in ("controller-one", "controller-two"):
            with attempt_scope(7, owner):
                tags.append(session_tag("unused"))
    assert tags[0] != tags[1]


def test_agent_attempt_tags_are_distinct_between_deployments():
    tags = [
        RemoteTaskStore(SimpleNamespace(token=token), 7, lambda holder: None).session_tag
        for token in ("agent-one", "agent-two")
    ]
    assert tags[0] != tags[1]
    assert all(len(tag.encode()) <= 63 for tag in tags)


def test_largest_attempt_id_keeps_its_tag_within_postgres_limit():
    from etl_craft.core.ownership import attempt_holder
    from etl_craft.dialects.warehouse.postgres import PostgresWarehouse

    tag = "etl-craft/" + attempt_holder(2**63 - 1, "agent-token-fingerprint")
    assert len(tag.encode()) <= 63
    assert "agent-token-fingerprint" not in tag
    assert PostgresWarehouse().session_tag_args(tag)["application_name"] == tag


def test_agent_checks_its_warehouse_fencing_before_registering(monkeypatch):
    import threading

    from etl_craft.agent import loop
    from etl_craft.core.errors import ConfigurationError
    from etl_craft.dialects.warehouse.databricks import DatabricksWarehouse
    from etl_craft.warehouse import fencing

    monkeypatch.setattr(fencing, "warehouse_dialect", lambda config: DatabricksWarehouse())

    def refuse_client(url):
        pytest.fail("an unfenceable agent reached registration")

    monkeypatch.setattr(loop, "AgentClient", refuse_client)
    with pytest.raises(ConfigurationError, match="cannot stop"):
        loop.AgentLoop(
            SimpleNamespace(warehouse=object()),
            SimpleNamespace(api="http://localhost"),
            threading.Event(),
        )
