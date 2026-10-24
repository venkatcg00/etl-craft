"""Process reservations enforce the deployment total rather than one host's guess."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from etl_craft.core.enums import ProcessRole
from etl_craft.core.errors import ConfigurationError
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.connection_budget import Connections, heartbeat, reservations, reserve
from fixtures.races import two_at_once


def wait_for_total(engine, now, expected):
    import threading
    from time import monotonic

    deadline = monotonic() + 5
    while True:
        with engine.connect() as conn:
            total = reservations(conn, now).total
        if total == expected:
            return
        assert monotonic() < deadline, f"connection total: expected {expected}, found {total}"
        threading.Event().wait(0.01)


def test_process_reservations_are_counted_and_stop_cleanly(engine_db, monkeypatch):
    now = datetime(2026, 1, 1, tzinfo=UTC)
    with engine_db.engine.begin() as conn:
        monkeypatch.setattr(for_engine(conn), "max_connections", lambda _: 100)
        identity = reserve(conn, ProcessRole.UI, Connections(10, 2, 0), now)
        assert reservations(conn, now).total == 12
        heartbeat(conn, identity, now + timedelta(seconds=15))
        conn.execute(
            text("UPDATE AUD_CONNECTION_RESERVATIONS SET STOPPED_AT=:now WHERE RESERVATION_ID=:id"),
            {"now": now, "id": identity},
        )
        assert reservations(conn, now).total == 0
        with pytest.raises(ConfigurationError, match=r"ownership ended.*restart"):
            heartbeat(conn, identity, now)


def test_over_budget_startup_fails_before_recording_the_process(engine_db, monkeypatch):
    now = datetime.now(UTC)
    with engine_db.engine.begin() as conn:
        monkeypatch.setattr(for_engine(conn), "max_connections", lambda _: 100)
        reserve(conn, ProcessRole.CONTROLLER, Connections(10, 3, 60), now)
        with pytest.raises(ConfigurationError, match=r"already reserved.*max_connections=100"):
            reserve(conn, ProcessRole.UI, Connections(10, 2, 0), now)
        assert reservations(conn, now).total == 73


def test_two_processes_cannot_spend_the_same_remaining_budget(engine_db, monkeypatch):
    engine = engine_db.engine
    monkeypatch.setattr(for_engine(engine), "max_connections", lambda _: 100)

    def start():
        try:
            with engine.begin() as conn:
                reserve(conn, ProcessRole.UI, Connections(45, 2, 0), datetime.now(UTC))
            return True
        except ConfigurationError:
            return False

    assert sorted(two_at_once(start, start, at=(engine, "begin"))) == [False, True]


def test_expired_process_cannot_renew_its_ownership(engine_db):
    now = datetime.now(UTC)
    with engine_db.engine.begin() as conn:
        identity = reserve(conn, ProcessRole.UI, Connections(2, 2, 0), now)
        with pytest.raises(ConfigurationError, match="ownership ended"):
            heartbeat(conn, identity, now + timedelta(seconds=61))


def test_live_sessions_keep_their_budget_after_a_missed_heartbeat(engine_db):
    engine = engine_db.engine
    dialect = for_engine(engine)
    now = datetime.now(UTC)
    with dialect.session_connection(engine, engine_db.config) as owner:
        with owner.begin():
            reserve(owner, ProcessRole.UI, Connections(2, 2, 0), now)
        with engine.connect() as observer:
            expected = 4 if dialect.supports_api_replicas else 0
            assert reservations(observer, now + timedelta(seconds=61)).total == expected
        owner.invalidate()
    wait_for_total(engine, now + timedelta(seconds=61), 0)


def test_process_lifecycle_releases_its_budget_on_exit(engine_db):
    import threading

    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.services.operations.context import OperationContext
    from etl_craft.services.operations.processes import connection_process

    engine = engine_db.engine
    ctx = OperationContext(engine, engine_db.config, SYSTEM_ACTOR)
    with connection_process(ctx, ProcessRole.UI) as scoped:
        if for_engine(engine).supports_api_replicas:
            assert isinstance(scoped.child.cancel, threading.Event)
        with engine.connect() as conn:
            expected = (
                engine_db.config.db_pool_size + 4 + engine_db.config.limits.max_parallel_tasks
                if for_engine(engine).supports_api_replicas
                else 0
            )
            assert reservations(conn, datetime.now(UTC)).total == expected
    wait_for_total(engine, datetime.now(UTC), 0)


@pytest.fixture
def postgres_budget(postgres_database):
    from fixtures.engine_db import apply_schema

    apply_schema(postgres_database.engine)
    return postgres_database


@pytest.mark.engine_postgres
def test_expired_owner_cannot_open_another_pooled_connection(postgres_budget, monkeypatch):
    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.services.operations.context import OperationContext
    from etl_craft.services.operations.processes import connection_process

    engine = postgres_budget.engine
    clock = [1000.0]
    monkeypatch.setattr("etl_craft.engine.process_reservations.monotonic", lambda: clock[0])
    with connection_process(
        OperationContext(engine, postgres_budget.config, SYSTEM_ACTOR), ProcessRole.UI
    ) as ctx:
        clock[0] += 61 + ctx.config.limits.engine_db_outage_seconds
        with (
            pytest.raises(ConfigurationError, match=r"ownership expired.*restart"),
            engine.connect(),
        ):
            pytest.fail("expired reservation opened a connection")
        assert ctx.child.cancel.is_set()


@pytest.mark.engine_postgres
def test_ui_callers_share_one_child_allowance_and_leave_excess_work_queued(
    postgres_budget, tmp_path
):
    from dataclasses import replace

    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.core.enums import AttemptStatus
    from etl_craft.engine.process_reservations import connection_slot
    from etl_craft.execution.attempt_types import ChildOptions, TaskRun
    from etl_craft.execution.runner import admit_attempt, try_execute_attempt
    from etl_craft.services.operations.context import OperationContext
    from etl_craft.services.operations.processes import connection_process
    from fixtures.metadata import add_pipeline, add_task, start_run

    engine = postgres_budget.engine
    config = replace(
        postgres_budget.config,
        config_path=tmp_path / "craft-connector.yml",
        limits=replace(postgres_budget.config.limits, max_parallel_tasks=1),
    )
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T", "PYTHON", SCRIPT_FILE="python_scripts/task.py")
        run = start_run(conn, pipeline)
    path = config.project_dir / "python_scripts"
    path.mkdir(exist_ok=True)
    (path / "task.py").write_text("def run(context):\n    pass\n")
    spec = admit_attempt(engine, config, TaskRun("P", "T", task, run, False))
    with connection_process(OperationContext(engine, config, SYSTEM_ACTOR), ProcessRole.UI):
        with connection_slot(engine) as occupied:
            assert occupied
            assert try_execute_attempt(engine, config, spec, ChildOptions()) is None
            with engine.connect() as conn:
                status = conn.execute(
                    text("SELECT STATUS AS status FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"),
                    {"id": spec.attempt_id},
                ).scalar_one()
                assert status == AttemptStatus.QUEUED
        with connection_slot(engine) as released:
            assert released


@pytest.mark.engine_postgres
def test_live_child_session_retains_capacity_after_the_parent_session_dies(postgres_budget):
    from dataclasses import replace

    from etl_craft.engine.connection import engine_db as connect_engine

    engine = postgres_budget.engine
    dialect = for_engine(engine)
    now = datetime.now(UTC)
    with dialect.session_connection(engine, postgres_budget.config) as owner:
        with owner.begin():
            identity = reserve(owner, ProcessRole.UI, Connections(2, 2, 1), now)
        import os

        previous = os.environ.get("ETL_CRAFT_PROCESS_RESERVATION")
        os.environ["ETL_CRAFT_PROCESS_RESERVATION"] = identity
        child = connect_engine(replace(postgres_budget.config, sizing=None), supervised=True)
        try:
            with child.connect() as child_conn:
                child_conn.execute(text("SELECT 1 AS connected"))
                with owner.begin():
                    from etl_craft.engine.queries import statement

                    owner.execute(
                        statement(owner, "connection_reservation_stop"),
                        {"identity": identity, "now": now},
                    )
                owner.invalidate()
                with engine.connect() as observer:
                    assert reservations(observer, now + timedelta(seconds=61)).total == 5
        finally:
            child.dispose()
            if previous is None:
                os.environ.pop("ETL_CRAFT_PROCESS_RESERVATION", None)
            else:
                os.environ["ETL_CRAFT_PROCESS_RESERVATION"] = previous
    wait_for_total(engine, now + timedelta(seconds=61), 0)


@pytest.mark.engine_postgres
def test_simultaneous_ui_requests_cannot_both_take_the_last_child_slot(postgres_budget):
    import threading
    from dataclasses import replace

    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.engine import process_reservations
    from etl_craft.services.operations.context import OperationContext
    from etl_craft.services.operations.processes import connection_process

    engine = postgres_budget.engine
    config = replace(
        postgres_budget.config, limits=replace(postgres_budget.config.limits, max_parallel_tasks=1)
    )
    checked = threading.Barrier(2)

    def take_slot():
        with process_reservations.connection_slot(engine) as available:
            checked.wait(timeout=5)
            return available

    with connection_process(OperationContext(engine, config, SYSTEM_ACTOR), ProcessRole.UI):
        results = two_at_once(
            take_slot, take_slot, at="etl_craft.engine.process_reservations.connection_slot"
        )
        assert sorted(results) == [False, True]


@pytest.mark.engine_postgres
def test_heartbeat_failure_stops_the_process_and_reports_the_lost_reservation(
    postgres_budget, monkeypatch
):
    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.services.operations.context import OperationContext
    from etl_craft.services.operations.processes import connection_process

    def fail_renewal(*args, **kwargs):
        raise ConfigurationError("test ownership loss")

    monkeypatch.setattr("etl_craft.engine.process_reservations.HEARTBEAT_SECONDS", 0)
    monkeypatch.setattr("etl_craft.engine.process_reservations.heartbeat", fail_renewal)
    ctx = OperationContext(postgres_budget.engine, postgres_budget.config, SYSTEM_ACTOR)
    with (
        pytest.raises(ConfigurationError, match=r"lost its database session.*restart"),
        connection_process(ctx, ProcessRole.UI) as running,
    ):
        assert running.child.cancel.wait(timeout=5)


@pytest.mark.engine_postgres
def test_a_dropped_reservation_session_is_replaced_without_stopping_work(
    postgres_budget, monkeypatch
):
    import threading
    from time import monotonic

    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.services.operations.context import OperationContext
    from etl_craft.services.operations.processes import connection_process

    engine = postgres_budget.engine
    monkeypatch.setattr("etl_craft.engine.process_reservations.HEARTBEAT_SECONDS", 0.01)
    with connection_process(
        OperationContext(engine, postgres_budget.config, SYSTEM_ACTOR), ProcessRole.UI
    ) as ctx:
        with engine.begin() as observer:
            backend = observer.execute(
                text(
                    "SELECT BACKEND_PID AS backend_pid FROM AUD_CONNECTION_RESERVATIONS "
                    "WHERE STOPPED_AT IS NULL"
                )
            ).scalar_one()
            observer.execute(text("SELECT pg_terminate_backend(:pid) AS ended"), {"pid": backend})
        deadline = monotonic() + 5
        while True:
            with engine.connect() as observer:
                current = observer.execute(
                    text(
                        "SELECT BACKEND_PID AS backend_pid FROM AUD_CONNECTION_RESERVATIONS "
                        "WHERE STOPPED_AT IS NULL"
                    )
                ).scalar_one()
            if current != backend:
                break
            assert monotonic() < deadline, "reservation did not reconnect"
            threading.Event().wait(0.01)
        assert not ctx.child.cancel.is_set()


def test_doctor_refuses_an_oversubscribed_budget_with_the_remedy(engine_db, monkeypatch):
    from etl_craft.services.doctor import Status, run_checks

    monkeypatch.setattr(for_engine(engine_db.engine), "max_connections", lambda _: 10)
    checks = {check.name: check for check in run_checks(engine_db.config)}
    budget = checks["Engine DB connection budget"]
    assert budget.status == Status.FAIL
    assert "max_connections=10" in budget.detail
    assert "expected at most 80%" in budget.detail
    assert "lower Db_pool_size" in budget.detail


@pytest.mark.chaos
@pytest.mark.parametrize(
    "empty_engine_db",
    [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
    indirect=True,
)
def test_a_kill_after_reservation_keeps_history_and_does_not_prevent_replacement(cli_project):
    from etl_craft.core.time import as_utc

    p = cli_project
    killed = p.start("controller", "--no-ui", fault="connections.after_reservation:kill")
    assert killed.wait() == 137, killed.output
    with p.engine.connect() as conn:
        reservation = conn.execute(
            text(
                "SELECT RESERVATION_ID AS identity,EXPIRES_AT AS expires "
                "FROM AUD_CONNECTION_RESERVATIONS WHERE PID=:pid"
            ),
            {"pid": killed.process.pid},
        ).one()
    after_expiry = as_utc(reservation.expires) + timedelta(seconds=1)
    import threading
    from time import monotonic

    deadline = monotonic() + 5
    while True:
        with p.engine.connect() as conn:
            remaining = reservations(conn, after_expiry).total
        if remaining == 0:
            break
        assert monotonic() < deadline, "killed owner still has an Engine DB session"
        threading.Event().wait(0.01)
    replacement = p.start("controller", "--no-ui")
    p.wait_for("SELECT COUNT(*) AS controllers FROM AUD_CONTROLLERS", expected=1)
    import signal

    replacement.signal(signal.SIGTERM)
    assert replacement.wait() == 0, replacement.output
    with p.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS retained FROM AUD_CONNECTION_RESERVATIONS "
                    "WHERE RESERVATION_ID=:id"
                ),
                {"id": reservation.identity},
            ).scalar_one()
            == 1
        )
