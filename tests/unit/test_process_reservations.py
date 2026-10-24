"""Database-independent reservation lifecycle, reconnect and shared-slot behavior."""

import threading
from contextlib import ExitStack
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from sqlalchemy.exc import OperationalError

from etl_craft.core.actor import SYSTEM_ACTOR, current_actor
from etl_craft.core.enums import ConnectionUse, ProcessRole
from etl_craft.core.errors import ConfigurationError
from etl_craft.dialects.engine import for_engine
from etl_craft.engine import process_reservations
from etl_craft.engine.connection_budget import reservations
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.processes import connection_process
from fixtures.engine_db import apply_schema, sqlite_engine_db

pytestmark = pytest.mark.unit


@pytest.fixture
def lifecycle(tmp_path, monkeypatch, engine_actor):
    db = sqlite_engine_db(tmp_path)
    apply_schema(db.engine)
    monkeypatch.setattr(for_engine(db.engine), "supports_api_replicas", True)
    config = replace(db.config, limits=replace(db.config.limits, max_parallel_tasks=1))
    try:
        yield OperationContext(db.engine, config, SYSTEM_ACTOR)
    finally:
        db.engine.dispose()


def test_busy_tasks_leave_their_reserved_readiness_and_gate_probe_slots_available(lifecycle):
    with connection_process(lifecycle, ProcessRole.UI):
        with process_reservations.connection_slot(lifecycle.engine) as task:
            assert task
            with ExitStack() as checks:
                assert not checks.enter_context(
                    process_reservations.connection_slot(lifecycle.engine)
                )
                assert checks.enter_context(
                    process_reservations.connection_slot(lifecycle.engine, ConnectionUse.HEALTH)
                )
                assert checks.enter_context(
                    process_reservations.connection_slot(lifecycle.engine, ConnectionUse.PROBE)
                )
                assert not checks.enter_context(
                    process_reservations.connection_slot(lifecycle.engine, ConnectionUse.HEALTH)
                )
        with connection_process(lifecycle, ProcessRole.RUN), lifecycle.engine.connect() as conn:
            assert reservations(conn, datetime.now(UTC)).total == 15
    with lifecycle.engine.connect() as conn:
        assert reservations(conn, datetime.now(UTC)).total == 0


def test_transient_heartbeat_errors_reconnect_under_the_system_actor(lifecycle, monkeypatch):
    original = process_reservations.heartbeat
    renewed = threading.Event()
    calls = []

    def heartbeat(*args, **kwargs):
        calls.append(current_actor())
        if len(calls) == 1:
            raise OperationalError(None, None, OSError("temporary connection loss"))
        original(*args, **kwargs)
        renewed.set()

    monkeypatch.setattr(process_reservations, "HEARTBEAT_SECONDS", 0.01)
    monkeypatch.setattr(process_reservations, "heartbeat", heartbeat)
    with connection_process(lifecycle, ProcessRole.UI) as running:
        assert renewed.wait(timeout=5)
        assert not running.child.cancel.is_set()
    assert len(calls) >= 2
    assert all(actor == SYSTEM_ACTOR for actor in calls)


def test_a_refused_startup_does_not_leave_a_phantom_process_lifecycle(lifecycle, monkeypatch):
    def refuse(*args, **kwargs):
        raise ConfigurationError("Engine DB connection budget is full")

    with monkeypatch.context() as patch:
        patch.setattr(process_reservations, "reserve", refuse)
        with (
            pytest.raises(ConfigurationError, match="budget is full"),
            connection_process(lifecycle, ProcessRole.UI),
        ):
            pytest.fail("refused process started")
    with connection_process(lifecycle, ProcessRole.UI), lifecycle.engine.connect() as conn:
        assert reservations(conn, datetime.now(UTC)).total == 15
