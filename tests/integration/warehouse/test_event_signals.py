"""Signal ids and payloads are consumed atomically on every warehouse and Engine DB."""

from dataclasses import replace
from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import InjectedFaultError
from etl_craft.engine.repository.events import run_event_data
from etl_craft.execution.events import check_event
from fixtures.engine_db import postgres_engine_db
from fixtures.event_waits import SIGNAL_KEY, check_signal_queries, send_signal, signal_scene
from fixtures.metadata import add_pipeline, start_run
from fixtures.races import two_at_once


# Plain values, not ENGINE_CASES: each case carries only its warehouse's marker, so it runs on
# that warehouse's CI shard, which also runs the PostgreSQL Engine DB.
@pytest.fixture(params=["sqlite", "postgresql"])
def signal_world(sql_world, request, monkeypatch):
    if request.param == "sqlite":
        yield sql_world
        return
    template = request.getfixturevalue("postgres_schema_template")
    with postgres_engine_db(monkeypatch, template) as db:
        with db.engine.begin() as conn:
            pipeline = add_pipeline(conn, "P", refresh_type="INCREMENTAL")
            run = start_run(conn, pipeline)
        yield replace(
            sql_world,
            engine_db=db.engine,
            config=replace(sql_world.config, engine=db.config.engine),
            pipeline_id=pipeline,
            pipeline_run_id=run,
            tasks={},
        )


def test_signal_delivery_is_consumed_once_with_exact_run_payload(signal_world):
    check_signal_queries(signal_world, "signals")


def test_failed_signal_completion_rolls_back_keys_and_evidence(signal_world, monkeypatch):
    w = signal_world
    task = signal_scene(w, "signals")
    send_signal(w, "signals")
    monkeypatch.setenv("ETL_CRAFT_FAULT", "event.after_evidence")
    with pytest.raises(InjectedFaultError):
        check_event(w.engine_db, w.config, w.pipeline_run_id, task, datetime.now(UTC))
    with w.engine_db.connect() as conn:
        assert conn.execute(text("SELECT count(*) AS n FROM AUD_EVENT_KEYS")).scalar_one() == 0
        assert run_event_data(conn, w.pipeline_run_id).keys == ()
        assert conn.execute(text("SELECT count(*) AS n FROM AUD_TASK_ATTEMPTS")).scalar_one() == 0
    monkeypatch.delenv("ETL_CRAFT_FAULT")
    assert (
        check_event(w.engine_db, w.config, w.pipeline_run_id, task, datetime.now(UTC)).status
        == RunStatus.SUCCESS
    )


def test_two_outcomes_recording_one_key_at_once_let_only_one_consume_it(signal_world):
    from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
    from etl_craft.engine import transitions
    from etl_craft.engine.repository.events import consumed_keys, record_event

    # One pipeline has one exclusive run in progress, so two runs normally never race for a
    # key; the unique index still decides if two outcomes ever record one at the same moment.
    w = signal_world
    task = signal_scene(w, "signals")
    with w.engine_db.begin() as conn:
        other = transitions.create_run(conn, w.pipeline_id, SYSTEM_ACTOR, status=RunStatus.QUEUED)
        task_runs = [
            transitions.find_or_create_task_run(conn, task, run).task_run_id
            for run in (w.pipeline_run_id, other)
        ]

    def record(task_run):
        with acting_as(SYSTEM_ACTOR), w.engine_db.begin() as conn:
            return record_event(conn, task, task_run, {"keys": [SIGNAL_KEY], "rows": [{}]})

    results = two_at_once(
        lambda: record(task_runs[0]),
        lambda: record(task_runs[1]),
        at=(w.engine_db, "begin"),
        timeout=30,
    )
    assert sorted(results) == [False, True]
    with w.engine_db.connect() as conn:
        assert consumed_keys(conn, task, [SIGNAL_KEY]) == {SIGNAL_KEY}
        recorded = conn.execute(
            text("SELECT COUNT(*) AS n FROM AUD_EVENT_KEYS WHERE TASK_ID = :t"), {"t": task}
        ).scalar_one()
    assert recorded == 1


def test_a_key_taken_before_the_outcome_commits_nothing_and_checks_again(signal_world, monkeypatch):
    from etl_craft.execution import events

    w = signal_world
    task = signal_scene(w, "signals")
    send_signal(w, "signals")
    now = datetime.now(UTC)
    with monkeypatch.context() as patch:
        patch.setattr(events, "record_event", lambda *args: False)
        result = events.check_event(w.engine_db, w.config, w.pipeline_run_id, task, now)
    # It checks again at once: the next check is the moment its own probe finished.
    assert result.status is None
    assert result.next_check_at is not None
    assert now <= result.next_check_at <= datetime.now(UTC)
    with w.engine_db.connect() as conn:
        assert run_event_data(conn, w.pipeline_run_id).keys == ()
        attempts = conn.execute(text("SELECT COUNT(*) AS n FROM AUD_TASK_ATTEMPTS")).scalar_one()
    assert attempts == 0
    retried = events.check_event(w.engine_db, w.config, w.pipeline_run_id, task, now)
    assert retried.status == RunStatus.SUCCESS


@pytest.mark.parametrize("problem", ["overflow", "missing", "aggregate"])
def test_bad_signal_queries_fail_validation_before_run_state_changes(signal_world, problem):
    from etl_craft.services.validate import validate

    w = signal_world
    task = signal_scene(w, "signals", admit=False)
    send_signal(w, "signals")
    if problem == "overflow":
        send_signal(w, "signals", "second", "later")
        changed = {"EVENT_MAX_ROWS": "1"}
        parts = ("EVENT_SQL", "2 rows", "EVENT_MAX_ROWS=1", "narrow EVENT_SQL")
    elif problem == "missing":
        # A wildcard's output must also be checked against the actual warehouse columns.
        changed = {"EVENT_KEY_COLUMN": "absent", "EVENT_SQL": f"SELECT * FROM {w.name('signals')}"}
        parts = ("EVENT_SQL", "absent", "returns columns", "correct EVENT_KEY_COLUMN")
    else:
        changed = {"EVENT_SQL": f"SELECT count(*) AS event_id FROM {w.name('signals')}"}
        parts = ("EVENT_SQL", "aggregate", "always returns a row", "HAVING count(*) > 0")
    with w.engine_db.begin() as conn:
        for name, value in changed.items():
            conn.execute(
                text(
                    "DELETE FROM CFG_TASK_PARAMETERS WHERE TASK_ID=:task AND PARAMETER_NAME=:name"
                ),
                {"task": task, "name": name},
            )
            conn.execute(
                text(
                    "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID,PARAMETER_NAME,PARAMETER_VALUE) "
                    "VALUES (:task,:name,:value)"
                ),
                {"task": task, "name": name, "value": value},
            )
    w.new_run()
    report = validate(w.engine_db, w.config)
    assert report.failed
    messages = " ".join(f"{f.where}: {f.message}" for f in report.findings)
    assert "signals_wait" in messages
    for part in parts:
        assert part in messages
    with w.engine_db.connect() as conn:
        assert conn.execute(text("SELECT count(*) AS n FROM AUD_TASK_ATTEMPTS")).scalar_one() == 0
        assert conn.execute(text("SELECT count(*) AS n FROM AUD_GATE_WAITS")).scalar_one() == 0
        assert conn.execute(text("SELECT count(*) AS n FROM AUD_EVENT_KEYS")).scalar_one() == 0
        assert (
            conn.execute(
                text(
                    "SELECT STATUS AS status FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:id"
                ),
                {"id": w.pipeline_run_id},
            ).scalar_one()
            == RunStatus.IN_PROGRESS
        )
    failed = check_event(w.engine_db, w.config, w.pipeline_run_id, task, datetime.now(UTC))
    assert failed.status == RunStatus.FAILED
    assert f"task_id={task}" in failed.message
    for part in parts:
        assert part in failed.message
    with w.engine_db.connect() as conn:
        row = conn.execute(
            text("SELECT STATUS AS status,ERROR_MESSAGE AS message FROM AUD_TASK_ATTEMPTS")
        ).one()
        assert row.status == RunStatus.FAILED
        assert row.message == failed.message
        assert conn.execute(text("SELECT count(*) AS n FROM AUD_EVENT_KEYS")).scalar_one() == 0


def test_retry_and_recovery_keep_the_successful_waits_original_keys(signal_world, monkeypatch):
    import yaml

    from etl_craft.config import load_config
    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.core.enums import ActionName, ApiRole, ControlAction
    from etl_craft.core.ownership import attempt_holder
    from etl_craft.engine import controls
    from etl_craft.engine.resource_locks import holder_tag
    from etl_craft.engine.runlog import RunSelector
    from etl_craft.execution.pipeline import run_pipeline
    from etl_craft.services.operations import OperationContext
    from etl_craft.services.operations.actions import invoke
    from etl_craft.services.operations.controls import ControlRequest
    from etl_craft.warehouse.connection import warehouse_dialect
    from etl_craft.warehouse.fencing import confirm_stopped
    from fixtures.engine_db import profile_block
    from fixtures.metadata import add_dependency, add_task

    w = signal_world
    wait = signal_scene(w, "signals", admit=False)
    send_signal(w, "signals")
    raw = {
        "Secrets": {"Source_type": "ENVIRONMENT"},
        "Orchestration": {},
        "Engine": {"dev": profile_block(w.config.engine, w.config.engine.schema or "main")},
        "Warehouse": {
            "Name": warehouse_dialect(w.config).spec.display_name,
            "Table_format": str(w.config.warehouse_table_format),
            "dev": profile_block(w.config.warehouse, w.config.warehouse.schema),
        },
    }
    for name, value in w.config.warehouse.extra.items():
        if name in {"secret_var", "s3_secret"}:
            continue
        variable = f"EVENT_TEST_{name.upper()}"
        monkeypatch.setenv(variable, value)
        raw["Warehouse"]["dev"][name.capitalize()] = variable
    w.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    config = load_config(w.config.config_path)
    folder = w.project / "python_scripts"
    folder.mkdir(exist_ok=True)
    (folder / "recover_signals.py").write_text(
        "from etl_craft.scripting import ScriptResult\n"
        "from etl_craft.core.errors import TransientError\n"
        "def run(task):\n"
        f"    assert task.event_keys == ({SIGNAL_KEY!r},)\n"
        "    assert task.input_params['keys'] == list(task.event_keys)\n"
        "    assert task.event_rows == ({'payload': 'first'},)\n"
        "    path = task.config.project_dir / 'calls'\n"
        "    calls = int(path.read_text()) + 1 if path.exists() else 1\n"
        "    path.write_text(str(calls))\n"
        "    if calls <= 2: raise TransientError('test source is temporarily down')\n"
        "    return ScriptResult(row_count=1)\n"
    )
    with w.engine_db.begin() as conn:
        consumer = add_task(
            conn,
            w.pipeline_id,
            "consume",
            "PYTHON",
            SCRIPT_FILE="python_scripts/recover_signals.py",
            INPUT_PARAMS='{"keys":"$$event_keys"}',
            RETRIES="1",
            RETRY_DELAY_SECONDS="0",
            RETRY_ON="TRANSIENT",
        )
        add_dependency(conn, w.pipeline_id, consumer, wait)
    w.new_run()
    selector = RunSelector(run_id=w.pipeline_run_id)
    first = run_pipeline(w.engine_db, config, "P", selector=selector)
    assert first.status == RunStatus.FAILED
    assert (w.project / "calls").read_text() == "2"
    send_signal(w, "signals", "second", "later")
    # Server-query acknowledgement remains explicit until operator cancellation in S5.D.
    with w.engine_db.connect() as conn:
        attempts = controls.scope_attempts(conn, w.pipeline_run_id, None)
    for attempt in attempts:
        if attempt.queries_ended_at is None:
            confirm_stopped(
                config, holder_tag(attempt_holder(attempt.attempt_id, attempt.owner_id))
            )
            with w.engine_db.begin() as conn:
                controls.observe_exit(conn, attempt.attempt_id, queries_ended=True)
    ctx = OperationContext(w.engine_db, config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    prepared = invoke(
        ctx,
        ActionName.CONTROL,
        ControlRequest(
            pipeline_code="P",
            run_id=w.pipeline_run_id,
            action=ControlAction.RECOVER,
            reason="source restored",
            request_key="recover-signals",
        ),
    )
    assert prepared.execution_run_id == w.pipeline_run_id
    from etl_craft.execution.reruns import rerun_task

    retried = rerun_task(w.engine_db, config, "P", "consume", "source restored", selector=selector)
    assert retried.status == RunStatus.IN_PROGRESS
    assert retried.pipeline_run_id == w.pipeline_run_id
    with w.engine_db.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT STATUS AS status FROM AUD_TASK_RUN_LOG "
                    "WHERE PIPELINE_RUN_ID=:run AND TASK_ID=:task"
                ),
                {"run": w.pipeline_run_id, "task": consumer},
            ).scalar_one()
            == RunStatus.SUCCESS
        )
    assert run_pipeline(w.engine_db, config, "P", selector=selector).status == RunStatus.SUCCESS
    assert (w.project / "calls").read_text() == "3"
    with w.engine_db.connect() as conn:
        assert run_event_data(conn, w.pipeline_run_id).keys == (SIGNAL_KEY,)
        assert run_event_data(conn, w.pipeline_run_id).rows == ({"payload": "first"},)
        assert (
            conn.execute(
                text(
                    "SELECT count(*) AS n FROM AUD_TASK_ATTEMPTS a JOIN AUD_TASK_RUN_LOG r "
                    "ON r.TASK_RUN_ID=a.TASK_RUN_ID WHERE r.TASK_ID=:task"
                ),
                {"task": wait},
            ).scalar_one()
            == 1
        )
    with w.engine_db.begin() as conn:
        next_run = start_run(conn, w.pipeline_id)
    assert (
        check_event(w.engine_db, config, next_run, wait, datetime.now(UTC)).status
        == RunStatus.SUCCESS
    )
    with w.engine_db.connect() as conn:
        assert run_event_data(conn, next_run).keys == ("second",)
