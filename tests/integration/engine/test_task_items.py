"""Item records survive retries and fence every operator/worker race on both Engine DBs."""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import AttemptStatus, ItemAction, ItemStatus
from etl_craft.core.errors import HandlerError, RunRefusedError, StaleTransitionError
from etl_craft.core.items import ItemRecord
from etl_craft.engine import transitions
from etl_craft.engine.items import latest
from etl_craft.engine.task_store import LocalTaskStore, TaskIdentity
from etl_craft.scripting import ScriptResult, ScriptTask
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.items import ItemControl, control
from fixtures.metadata import add_pipeline, add_task, start_run


@pytest.fixture
def scene(engine_db):
    db = engine_db.engine
    with db.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T", "PYTHON", CPU_REQUEST="2")
        run = start_run(conn, pipeline)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        attempt = transitions.queue_attempt(conn, summary, SYSTEM_ACTOR)
        transitions.claim_attempt(
            conn,
            attempt,
            SYSTEM_ACTOR,
            owner="items",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
        transitions.start_attempt(conn, attempt, SYSTEM_ACTOR, owner="items")
    store = LocalTaskStore(
        db,
        engine_db.config,
        TaskIdentity(pipeline, task, run, summary, attempt, "items"),
        SimpleNamespace(session_tag="items"),
    )
    helper = ScriptTask(
        "P",
        "T",
        run,
        pipeline,
        summary,
        "FULL",
        None,
        {},
        {"SCRIPT_KIND": "INGESTION"},
        False,
        engine_db.config,
        logging.getLogger("etl_craft.items"),
        _items=store,
    )
    return SimpleNamespace(
        db=db,
        config=engine_db.config,
        store=store,
        task=helper,
        attempt=attempt,
        run=run,
        summary=summary,
    )


def event(key, status, number=1):
    return ItemRecord(key, number, status, report_id=str(uuid4()))


def test_partial_failure_retry_keeps_success_and_its_row_count(scene):
    calls = []

    def item(key):
        calls.append(key)
        if key == "bad":
            raise HandlerError("source page bad: unavailable; expected a page; retry later")
        return ScriptResult(3)

    with pytest.raises(HandlerError, match=r"bad.*unavailable"):
        scene.task.fan_out(["ok", "bad"], item, max_items=2)
    assert sorted(calls) == ["bad", "ok"]
    with scene.db.connect() as conn:
        saved = {r.key: r for r in latest(conn, scene.summary)}
        assert saved["ok"].status == ItemStatus.SUCCESS
        assert saved["bad"].status == ItemStatus.FAILED
    calls.clear()
    result = scene.task.fan_out(
        ["ok", "bad"], lambda key: calls.append(key) or ScriptResult(2), max_items=2
    )
    assert calls == ["bad"]
    assert result.row_count == 5
    with scene.db.connect() as conn:
        saved = {r.key: r for r in latest(conn, scene.summary)}
        assert saved["ok"].status == ItemStatus.SKIPPED and saved["ok"].row_count == 3
        assert saved["bad"].number == 2


def test_per_item_retries_are_recorded_as_they_finish(scene):
    calls = []

    def item(key):
        calls.append(key)
        if len(calls) == 1:
            raise HandlerError("page transient: unavailable; expected a page; retry")
        return ScriptResult(7)

    assert scene.task.fan_out(["page"], item, max_items=1, retries=1).row_count == 7
    with scene.db.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT STATUS AS status, ITEM_ATTEMPT_NUMBER AS number FROM "
                "AUD_TASK_ITEMS ORDER BY ITEM_RECORD_ID"
            )
        ).all()
    assert [(r.status, r.number) for r in rows] == [
        ("QUEUED", 0),
        ("RUNNING", 1),
        ("FAILED", 1),
        ("RUNNING", 2),
        ("SUCCESS", 2),
    ]


@pytest.mark.parametrize(
    "keys,options,parts",
    [
        (["a", "b"], {"max_items": 1}, ["max_items", "1", "expected", "increase"]),
        (["a", "a"], {"max_items": 2}, ["key", "a", "unique", "stable"]),
        ([None], {"max_items": 2}, ["key", "None", "text", "stable"]),
        (["bad\x00key"], {"max_items": 2}, ["key", "bad", "NUL", "stable"]),
        (["bad\ud800key"], {"max_items": 2}, ["key", "bad", "UTF-8", "stable"]),
        (
            ["a"],
            {"max_items": 2, "max_parallel": 10000},
            ["max_parallel", "10000", "expected", "lower"],
        ),
    ],
)
def test_bad_fan_out_changes_no_item_or_attempt_state(scene, keys, options, parts):
    with pytest.raises(HandlerError) as caught:
        scene.task.fan_out(keys, lambda key: pytest.fail("bad request executed work"), **options)
    assert all(part in str(caught.value) for part in parts)
    with scene.db.connect() as conn:
        assert latest(conn, scene.summary) == ()
        assert (
            conn.execute(
                text("SELECT STATUS AS status FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"),
                {"id": scene.attempt},
            ).scalar_one()
            == AttemptStatus.RUNNING
        )


def test_empty_list_returns_without_a_store(scene):
    assert (
        replace(scene.task, _items=None)
        .fan_out([], lambda key: pytest.fail("empty list ran"), max_items=0)
        .row_count
        == 0
    )


def test_report_is_idempotent_and_an_expired_owner_cannot_record(scene):
    scene.store.prepare_items(("one",))
    start = event("one", ItemStatus.RUNNING)
    scene.store.item_event(start)
    scene.store.item_event(start)
    with scene.db.begin() as conn:
        assert conn.execute(text("SELECT COUNT(*) AS count FROM AUD_TASK_ITEMS")).scalar_one() == 2
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:expired WHERE ATTEMPT_ID=:id"),
            {"expired": datetime.now(UTC) - timedelta(seconds=1), "id": scene.attempt},
        )
    with pytest.raises(StaleTransitionError):
        scene.store.item_event(event("one", ItemStatus.SUCCESS))
    with scene.db.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) AS count FROM AUD_TASK_ITEMS")).scalar_one() == 2


def context(scene):
    return OperationContext(scene.db, scene.config, SYSTEM_ACTOR)


def test_running_cancel_stays_pending_until_the_thread_returns(scene):
    entered, release = threading.Event(), threading.Event()

    def item(key):
        entered.set()
        assert release.wait(10), "test did not release the running item"
        return ScriptResult(1)

    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(copy_context().run, scene.task.fan_out, ["one"], item, max_items=1)
        assert entered.wait(10)
        try:
            result = control(
                context(scene),
                ItemControl(
                    pipeline_code="P",
                    task_code="T",
                    run_id=scene.run,
                    action=ItemAction.CANCEL,
                    item_key="one",
                    reason="operator request",
                ),
            )
            assert result.records[0].status == ItemStatus.CANCEL_REQUESTED
            assert not future.done()
        finally:
            release.set()
        with pytest.raises(HandlerError, match=r"one.*CANCELLED"):
            future.result(10)
    with scene.db.connect() as conn:
        row = conn.execute(
            text(
                "SELECT ITEM_KEY AS key, REASON AS reason, REQUESTED_BY AS actor "
                "FROM AUD_RUN_INTERVENTIONS"
            )
        ).one()
        assert (
            row.key == "one" and row.reason == "operator request" and row.actor == SYSTEM_ACTOR.name
        )
        assert latest(conn, scene.summary)[0].status == ItemStatus.CANCELLED


def test_queued_cancel_prevents_the_function_from_starting(scene):
    scene.store.prepare_items(("one",))
    control(
        context(scene),
        ItemControl(
            pipeline_code="P",
            task_code="T",
            run_id=scene.run,
            action=ItemAction.CANCEL,
            item_key="one",
            reason="cancel queued",
        ),
    )
    with pytest.raises(HandlerError, match="CANCELLED"):
        scene.task.fan_out(["one"], lambda key: pytest.fail("cancelled key started"), max_items=1)


def test_a_mark_and_an_item_start_serialize_without_running_marked_work(scene):
    from fixtures.races import two_at_once

    scene.store.prepare_items(("one",))
    decision = ItemControl(
        pipeline_code="P",
        task_code="T",
        run_id=scene.run,
        action=ItemAction.MARK,
        item_key="one",
        status=ItemStatus.SUCCESS,
        rows=9,
        reason="verified output",
    )

    def mark():
        try:
            return control(context(scene), decision)
        except RunRefusedError:
            return None

    result, mark_result = two_at_once(
        lambda: scene.store.item_event(event("one", ItemStatus.RUNNING)),
        mark,
        at=(scene.db, "begin"),
    )
    if mark_result is None:
        assert result.status == ItemStatus.RUNNING
    else:
        assert result.status == ItemStatus.SUCCESS
    with scene.db.connect() as conn:
        records = latest(conn, scene.summary)
        assert len(records) == 1
        assert records[0].status in {ItemStatus.SUCCESS, ItemStatus.RUNNING}


def test_importable_process_function_uses_spawned_workers(scene):
    from fixtures.fan_out_tasks import one_row

    assert (
        scene.task.fan_out(
            ["one", "two"], one_row, max_items=2, max_parallel=2, processes=True
        ).row_count
        == 2
    )
    with scene.db.connect() as conn:
        assert {r.key for r in latest(conn, scene.summary) if r.status == ItemStatus.SUCCESS} == {
            "one",
            "two",
        }


def test_two_item_starts_cannot_run_the_same_key_twice(scene):
    from fixtures.races import two_at_once

    scene.store.prepare_items(("one",))

    def start():
        try:
            scene.store.item_event(event("one", ItemStatus.RUNNING))
            return True
        except StaleTransitionError:
            return False

    assert sorted(two_at_once(start, start, at=(scene.db, "begin"))) == [False, True]
    with scene.db.connect() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) AS count FROM AUD_TASK_ITEMS WHERE STATUS='RUNNING'")
            ).scalar_one()
            == 1
        )


@pytest.mark.parametrize(
    "outcome", [AttemptStatus.FAILED, AttemptStatus.TIMED_OUT, AttemptStatus.LOST]
)
def test_retry_after_task_failure_timeout_or_lost_keeps_finished_items(scene, outcome):
    scene.store.prepare_items(("finished", "unfinished"))
    scene.store.item_event(event("finished", ItemStatus.RUNNING))
    scene.store.item_event(replace(event("finished", ItemStatus.SUCCESS), row_count=5))
    scene.store.item_event(event("unfinished", ItemStatus.RUNNING))
    with scene.db.begin() as conn:
        if outcome == AttemptStatus.LOST:
            conn.execute(
                text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:expired WHERE ATTEMPT_ID=:id"),
                {"expired": datetime.now(UTC) - timedelta(seconds=1), "id": scene.attempt},
            )
            transitions.lose_attempt(
                conn, scene.attempt, SYSTEM_ACTOR, owner="items", error_message="test owner lost"
            )
        else:
            transitions.finish_attempt(conn, scene.attempt, outcome, SYSTEM_ACTOR, owner="items")
        new_attempt = transitions.queue_attempt(conn, scene.summary, SYSTEM_ACTOR)
        transitions.claim_attempt(
            conn,
            new_attempt,
            SYSTEM_ACTOR,
            owner="retry",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
        transitions.start_attempt(conn, new_attempt, SYSTEM_ACTOR, owner="retry")
    store = replace(
        scene.store, identity=replace(scene.store.identity, attempt_id=new_attempt, owner="retry")
    )
    helper = replace(scene.task, _items=store)
    ran = []
    assert (
        helper.fan_out(
            ["finished", "unfinished"], lambda key: ran.append(key) or ScriptResult(2), max_items=2
        ).row_count
        == 7
    )
    assert ran == ["unfinished"]
    with scene.db.connect() as conn:
        assert latest(conn, scene.summary)[0].status == ItemStatus.SKIPPED


def test_reusing_a_report_id_for_another_observation_is_refused(scene):
    scene.store.prepare_items(("one",))
    start = event("one", ItemStatus.RUNNING)
    scene.store.item_event(start)
    with pytest.raises(StaleTransitionError, match="original report"):
        scene.store.item_event(replace(start, row_count=999))
    with scene.db.connect() as conn:
        assert latest(conn, scene.summary)[0].row_count == start.row_count


def test_error_urls_are_masked_in_the_immediate_reply_and_persisted_evidence(scene):
    scene.store.prepare_items(("one",))
    scene.store.item_event(event("one", ItemStatus.RUNNING))
    reply = scene.store.item_event(
        replace(
            event("one", ItemStatus.FAILED),
            error="https://user:secret@source.example/page unavailable",
        )
    )
    assert "secret" not in reply.error
    with scene.db.connect() as conn:
        assert "secret" not in latest(conn, scene.summary)[0].error


def test_item_progress_is_identical_in_the_api_task_summary_and_catalog(scene):
    from etl_craft.engine.repository.catalog import fetch_task_runs
    from etl_craft.services.operations.items import ItemInput, inspect
    from etl_craft.services.operations.snapshots import task_run_view

    scene.task.fan_out(["one", "two"], lambda key: ScriptResult(1), max_items=2)
    view = inspect(context(scene), ItemInput(pipeline_code="P", task_code="T", run_id=scene.run))
    with scene.db.connect() as conn:
        task = task_run_view(conn, scene.task.pipeline_id, task_run_id=scene.summary)
        history = fetch_task_runs(conn)[scene.task._items.identity.task_id]
    assert task.item_progress == str(view.progress) == "items 2 / 2, 0 failed, 0 skipped"
    assert task.attempts[-1].item_progress == str(view.progress)
    assert history[0].item_progress == str(view.progress)


def test_a_committed_mark_prevents_execution_and_appears_in_the_attempt_summary(scene):
    from etl_craft.services.operations.inspect import get_attempt
    from etl_craft.services.operations.requests import AttemptId

    scene.store.prepare_items(("one",))
    control(
        context(scene),
        ItemControl(
            pipeline_code="P",
            task_code="T",
            run_id=scene.run,
            action=ItemAction.MARK,
            item_key="one",
            status=ItemStatus.SUCCESS,
            reason="verified landing batch",
            rows=9,
        ),
    )
    assert (
        scene.task.fan_out(
            ["one"], lambda key: pytest.fail("marked item executed"), max_items=1
        ).row_count
        == 9
    )
    assert (
        get_attempt(context(scene), AttemptId(attempt_id=scene.attempt)).item_progress
        == "items 1 / 1, 0 failed, 1 skipped"
    )


def test_a_thousand_keys_are_prepared_in_one_idempotent_statement(scene):
    from sqlalchemy import event as sql_event

    inserts = []

    def capture(conn, cursor, sql, parameters, context, many):
        if sql.lstrip().upper().startswith("INSERT INTO AUD_TASK_ITEMS"):
            inserts.append(sql)

    sql_event.listen(scene.db, "before_cursor_execute", capture)
    keys = tuple(str(i) for i in range(1000))
    try:
        scene.store.prepare_items(keys)
        assert len(inserts) == 1
        scene.store.prepare_items(keys)
        assert len(inserts) == 2
    finally:
        sql_event.remove(scene.db, "before_cursor_execute", capture)
    with scene.db.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) AS n FROM AUD_TASK_ITEMS")).scalar_one() == 1000


def test_item_http_actions_preserve_quoted_keys_and_report_the_operator(scene):
    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app
    from etl_craft.core.enums import ActionName, ApiRole
    from etl_craft.services.operations.items import ItemView
    from etl_craft.services.operations.serialization import from_json
    from fixtures.actions import act
    from fixtures.auth import service_token

    key = "' OR 1=1 --"
    scene.store.prepare_items((key, "untouched"))
    token = service_token(scene.db)
    headers = {"Authorization": f"Bearer {token}"}
    path = f"/api/v1/projects/default/pipelines/P/runs/{scene.run}/tasks/T/items"
    with TestClient(create_app(context(scene))) as client:
        refused = client.post(
            path + "/controls",
            headers=headers,
            json={"action": "MARK", "item_key": key, "status": "SUCCESS", "rows": 3},
        )
        assert (
            refused.status_code == 400 and "STRICT" in refused.text and "--reason" in refused.text
        )
        act(
            replace(context(scene), role=ApiRole.ADMIN),
            ActionName.PROJECT_SET,
            project="default",
            governance="RELAXED",
            reason="development",
        )
        changed = client.post(
            path + "/controls",
            headers=headers,
            json={
                "action": "MARK",
                "reason": "",
                "item_key": key,
                "status": "SUCCESS",
                "rows": 3,
            },
        )
        assert changed.status_code == 200, changed.text
        read = client.get(path, headers=headers)
        assert read.status_code == 200, read.text
        view = from_json(ItemView, read.json())
        assert view.progress.total == 2 and view.progress.done == 1
        records = {r.key: r for r in view.records}
        assert records[key].status == ItemStatus.SUCCESS
        assert records["untouched"].status == ItemStatus.QUEUED
    with scene.db.connect() as conn:
        actor = conn.execute(
            text("SELECT REQUESTED_BY AS actor,ITEM_KEY AS key FROM AUD_RUN_INTERVENTIONS")
        ).one()
        assert actor.actor == "test-admin" and actor.key == key


def test_two_prepare_batches_record_each_key_and_retained_skip_once(scene):
    from fixtures.races import two_at_once

    scene.store.prepare_items(("one",))
    scene.store.item_event(event("one", ItemStatus.RUNNING))
    scene.store.item_event(replace(event("one", ItemStatus.SUCCESS), row_count=5))

    def prepare():
        scene.store.prepare_items(("one", "new"))

    two_at_once(prepare, prepare, at=(scene.db, "begin"))
    with scene.db.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT ITEM_KEY AS key,STATUS AS status FROM AUD_TASK_ITEMS "
                "WHERE STATUS IN ('QUEUED','SKIPPED') ORDER BY ITEM_RECORD_ID"
            )
        ).all()
    assert [(r.key, r.status) for r in rows] == [
        ("one", "QUEUED"),
        ("one", "SKIPPED"),
        ("new", "QUEUED"),
    ]


@pytest.mark.parametrize("policy", ("STRICT", "RELAXED"))
def test_cli_item_reason_obeys_governance(
    scene, monkeypatch, capsys, cli_logging, tmp_path, policy
):
    import yaml

    from etl_craft.cli import main
    from etl_craft.core.enums import ActionName, ApiRole
    from etl_craft.core.errors import ExitCode
    from fixtures.actions import act
    from fixtures.auth import service_token
    from fixtures.engine_db import profile_block

    scene.store.prepare_items(("one",))
    act(
        replace(context(scene), role=ApiRole.ADMIN),
        ActionName.PROJECT_SET,
        project="default",
        governance=policy,
        reason="policy",
    )
    token = service_token(scene.db)
    path = tmp_path / "craft-connector.yml"
    block = profile_block(scene.config.engine, scene.config.engine.schema or "public")
    if scene.db.url.get_backend_name() == "sqlite":
        block.update(Jdbc_url=f"jdbc:sqlite:{scene.db.url.database}", Schema="main")
    path.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "ENVIRONMENT"},
                "Orchestration": {"Project": "default"},
                "Engine": {"test": block},
            },
            sort_keys=False,
        )
    )
    monkeypatch.setenv("ETL_CRAFT_TOKEN", token)
    result = main(
        [
            "--config",
            str(path),
            "items",
            "mark",
            "--pipeline-code",
            "P",
            "--task-code",
            "T",
            "--run-id",
            str(scene.run),
            "--item-key",
            "one",
            "--status",
            "SUCCESS",
        ]
    )
    error = capsys.readouterr().err
    if policy == "RELAXED":
        assert result == ExitCode.SUCCESS, error
    else:
        assert result == ExitCode.USAGE and "STRICT" in error and "--reason" in error
    with scene.db.connect() as conn:
        reasons = (
            conn.execute(text("SELECT REASON AS reason FROM AUD_RUN_INTERVENTIONS")).scalars().all()
        )
        assert reasons == ([""] if policy == "RELAXED" else [])
