"""Full stored logs retain byte identity, bounded reads and independent attempt outcomes."""

import tracemalloc
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etl_craft.config.model import Retention
from etl_craft.config.object_models import Logs
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole, AttemptStatus
from etl_craft.core.errors import ConfigurationError, ResourceNotFoundError
from etl_craft.core.text import sha256_hex
from etl_craft.engine import transitions
from etl_craft.engine.logs import log_key, log_location, publish_log, shared_log, stored_log
from etl_craft.engine.queries import statement
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.log_storage import publish, publish_pending
from etl_craft.services.operations.requests import LogInput
from etl_craft.services.operations.retention import PurgeInput
from etl_craft.services.retention import retaining
from fixtures.metadata import add_pipeline, add_task


@pytest.fixture
def log_scene(engine_db, tmp_path):
    config = replace(engine_db.config, logs=Logs((tmp_path / "store").as_uri(), tail_bytes=11))
    ctx = OperationContext(engine_db.engine, config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    with ctx.engine.begin() as conn:
        pipeline = add_pipeline(conn, "LOGS")
        task = add_task(conn, pipeline, "load")
        run = transitions.create_run(conn, pipeline, SYSTEM_ACTOR)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        attempt = transitions.queue_attempt(conn, summary, SYSTEM_ACTOR)
        transitions.claim_attempt(
            conn,
            attempt,
            SYSTEM_ACTOR,
            owner="7",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
    return ctx, attempt, run, summary


def append(ctx, attempt, sequence, content):
    with ctx.engine.begin() as conn:
        last = conn.execute(statement(conn, "agent_chunk_last"), {"attempt_id": attempt}).first()
        offset = 0 if last is None else last.byte_offset + last.byte_length
        conn.execute(
            statement(conn, "agent_chunk_insert"),
            {
                "attempt_id": attempt,
                "sequence": sequence,
                "hash": sha256_hex(content.encode()),
                "content": content,
                "offset": offset,
                "length": len(content.encode()),
                "now": datetime.now(UTC),
            },
        )


def finish(ctx, attempt):
    with ctx.engine.begin() as conn:
        transitions.finish_attempt(conn, attempt, AttemptStatus.SUCCESS, SYSTEM_ACTOR, owner="7")


def state(ctx, attempt):
    with ctx.engine.connect() as conn:
        return conn.execute(statement(conn, "log_attempt"), {"attempt_id": attempt}).one()


def read(ctx, attempt, offset=0):
    return b"".join(invoke(ctx, ActionName.LOGS, LogInput(attempt_id=attempt, offset=offset)))


def test_live_download_keeps_its_source_across_publication_without_gaps(log_scene):
    ctx, attempt, _, _ = log_scene
    content = "first é\n" + "a" * 65500
    for sequence in range(19):
        append(ctx, attempt, sequence, content)
    source = invoke(ctx, ActionName.LOGS, LogInput(attempt_id=attempt, offset=7))
    first = next(source)
    finish(ctx, attempt)
    publish(ctx, attempt)
    full = content.encode() * 19
    assert first + b"".join(source) == full[7:]
    assert read(ctx, attempt, 7) == full[7:]
    assert read(ctx, attempt, len(full)) == b""
    row = state(ctx, attempt)
    assert row.status == AttemptStatus.SUCCESS
    assert row.log_path == log_location(
        ctx.config,
        log_key(row.engine_id, row.project_id, row.pipeline_run_id, row.task_run_id, attempt),
    )


def test_store_failure_keeps_outcome_and_chunks_and_controller_retries(log_scene, tmp_path):
    import threading

    from etl_craft.controller.retention import purge_due

    ctx, attempt, _, _ = log_scene
    root = tmp_path / "store"
    root.write_text("store is a file, not a directory")
    append(ctx, attempt, 0, "complete log\n")
    finish(ctx, attempt)
    publish_pending(ctx)
    assert state(ctx, attempt).status == AttemptStatus.SUCCESS
    assert state(ctx, attempt).log_path is None
    assert read(ctx, attempt) == b"complete log\n"
    with ctx.engine.connect() as conn:
        failure = conn.execute(
            text(
                "SELECT ARGUMENTS AS arguments FROM AUD_ACTIONS WHERE COMMAND='agent log' AND "
                "OUTCOME='FAILURE'"
            )
        ).scalar_one()
    for part in ("attempt", "upload failed", "expected a writable", "restore permissions"):
        assert part in str(failure)
    root.unlink()
    purge_due(ctx, datetime.now(UTC), threading.Event())
    assert state(ctx, attempt).log_path is not None
    assert read(ctx, attempt) == b"complete log\n"


def test_pending_missing_and_retired_logs_have_distinct_messages(log_scene, tmp_path):
    ctx, attempt, run, _ = log_scene
    with pytest.raises(ResourceNotFoundError) as caught:
        read(ctx, attempt)
    assert (
        str(caught.value)
        == f"attempt_id={attempt}: log pending upload; expected acknowledged log bytes; "
        "wait for the agent or controller to publish the log"
    )
    append(ctx, attempt, 0, "full log")
    finish(ctx, attempt)
    publish(ctx, attempt)
    stored = next((tmp_path / "store").rglob("*.log"))
    stored.unlink()
    with pytest.raises(ConfigurationError) as caught:
        read(ctx, attempt)
    for part in (str(stored), "cannot open safely", "expected readable", "restore the path"):
        assert part in str(caught.value)
    # A missing stored object never falls back to the retained chunks.
    assert b"".join(shared_log(ctx.engine, attempt)) == b"full log"
    with retaining(), ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS='SUCCESS' WHERE PIPELINE_RUN_ID=:run"),
            {"run": run},
        )
    retired = replace(ctx, config=replace(ctx.config, retention=Retention(attempt_log_days=1)))
    with retaining(), ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET ENDED_AT=:old WHERE ATTEMPT_ID=:id"),
            {"old": datetime.now(UTC) - timedelta(days=2), "id": attempt},
        )
    invoke(retired, ActionName.PURGE, PurgeInput())
    assert read(ctx, attempt).startswith(b"removed by retention on ")
    assert b"".join(shared_log(ctx.engine, attempt)) == b""
    publish_pending(ctx)
    assert not stored.exists()


@pytest.mark.parametrize(
    "column,value",
    [
        ("STATUS", "FAILED"),
        ("ERROR_MESSAGE", "changed"),
        ("LOG_REMOVED_AT", "2026-01-01"),
        ("POOL", "changed"),
    ],
)
def test_terminal_log_acknowledgment_cannot_change_outcome_or_retention(log_scene, column, value):
    ctx, attempt, _, _ = log_scene
    finish(ctx, attempt)
    with pytest.raises(DBAPIError), ctx.engine.begin() as conn:
        conn.execute(
            text(
                f"UPDATE AUD_TASK_ATTEMPTS SET LOG_PATH='file:///fake', {column}=:value WHERE "
                "ATTEMPT_ID=:id"
            ),
            {"value": value, "id": attempt},
        )
    assert state(ctx, attempt).log_path is None
    assert state(ctx, attempt).status == AttemptStatus.SUCCESS


@pytest.mark.parametrize(
    "bad", ["../secret", "/secret", "project-1/run-1/task-run-1/attempt-999.log", "%2e%2e/secret"]
)
def test_recorded_external_paths_cannot_select_other_files(log_scene, bad, tmp_path):
    ctx, attempt, _, _ = log_scene
    secret = tmp_path / "secret"
    secret.write_text("never disclose")
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LOG_PATH=:path WHERE ATTEMPT_ID=:id"),
            {"id": attempt, "path": ctx.config.logs.store + "/" + bad},
        )
    with pytest.raises(ResourceNotFoundError, match="canonical identity"):
        read(ctx, attempt)
    assert secret.read_text() == "never disclose"


def test_large_file_streams_without_loading_the_log_into_memory(log_scene):
    ctx, attempt, run, summary = log_scene
    key = log_key(state(ctx, attempt).engine_id, 1, run, summary, attempt)
    block = b"z" * 65536
    publish_log(ctx.config, key, (block for _ in range(1024)))
    tracemalloc.start()
    try:
        total = sum(len(chunk) for chunk in stored_log(ctx.config, key, 0))
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert total == 64 * 1024 * 1024
    assert peak < 2 * 1024 * 1024


@pytest.mark.chaos
@pytest.mark.parametrize("retire", [False, True])
@pytest.mark.parametrize(
    "point", ["logs.during_upload", "logs.after_upload", "logs.after_publication"]
)
def test_killed_publication_worker_recovers_from_durable_chunks(log_scene, point, retire):
    import json
    import os
    import subprocess
    import sys
    import threading

    from etl_craft.controller.retention import purge_due
    from etl_craft.services.operations.serialization import encode

    ctx, attempt, _, _ = log_scene
    append(ctx, attempt, 0, "persisted before crash\n")
    finish(ctx, attempt)
    worker = """
import json, sys
from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.engine.connection import engine_db
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.log_storage import publish
from etl_craft.services.operations.serialization import from_json
config = from_json(ConnectorConfig, json.load(sys.stdin))
with acting_as(SYSTEM_ACTOR):
    engine = engine_db(config)
    publish(OperationContext(engine, config, SYSTEM_ACTOR), int(sys.argv[1]))
"""
    crashed = subprocess.run(
        [sys.executable, "-c", worker, str(attempt)],
        input=json.dumps(encode(ctx.config)),
        text=True,
        capture_output=True,
        timeout=15,
        env={**os.environ, "ETL_CRAFT_FAULT": point + ":kill"},
    )
    assert crashed.returncode == 137, crashed.stderr
    assert state(ctx, attempt).status == AttemptStatus.SUCCESS
    if retire:
        with retaining(), ctx.engine.begin() as conn:
            conn.execute(
                text("UPDATE AUD_TASK_ATTEMPTS SET ENDED_AT=:old WHERE ATTEMPT_ID=:id"),
                {"old": datetime.now(UTC) - timedelta(days=2), "id": attempt},
            )
            conn.execute(text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS='SUCCESS'"))
        ctx = replace(ctx, config=replace(ctx.config, retention=Retention(attempt_log_days=1)))
        invoke(ctx, ActionName.PURGE, PurgeInput())
        assert read(ctx, attempt).startswith(b"removed by retention on ")
    else:
        purge_due(ctx, datetime.now(UTC), threading.Event())
        assert state(ctx, attempt).log_path is not None
        assert read(ctx, attempt) == b"persisted before crash\n"
    from pathlib import Path
    from urllib.parse import urlsplit

    assert not list(Path(urlsplit(ctx.config.logs.store).path).rglob("*.upload"))


def test_external_store_symlink_is_refused_without_writing_outside(log_scene, tmp_path):
    ctx, attempt, _, _ = log_scene
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "store").symlink_to(outside, target_is_directory=True)
    append(ctx, attempt, 0, "must stay private")
    finish(ctx, attempt)
    with pytest.raises(ConfigurationError, match="upload failed"):
        publish(ctx, attempt)
    assert not list(outside.iterdir())
    assert state(ctx, attempt).log_path is None


@pytest.mark.chaos
def test_publication_and_retention_cannot_leave_a_late_object(log_scene):
    from sqlalchemy.engine import Engine

    from fixtures.races import two_at_once

    ctx, attempt, run, _ = log_scene
    append(ctx, attempt, 0, "retire this exact attempt")
    finish(ctx, attempt)
    with retaining(), ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET ENDED_AT=:old WHERE ATTEMPT_ID=:id"),
            {"old": datetime.now(UTC) - timedelta(days=2), "id": attempt},
        )
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS='SUCCESS' WHERE PIPELINE_RUN_ID=:run"),
            {"run": run},
        )
    retired = replace(ctx, config=replace(ctx.config, retention=Retention(attempt_log_days=1)))
    two_at_once(
        lambda: publish(ctx, attempt),
        lambda: invoke(retired, ActionName.PURGE, PurgeInput()),
        at=(Engine, "begin"),
    )
    assert state(ctx, attempt).log_removed_at is not None
    assert read(ctx, attempt).startswith(b"removed by retention on ")
    from pathlib import Path
    from urllib.parse import urlsplit

    assert not list(Path(urlsplit(ctx.config.logs.store).path).rglob("*.log"))
    assert b"".join(shared_log(ctx.engine, attempt)) == b""


def test_chunk_relay_memory_is_bounded_by_the_batch_not_the_log(log_scene):
    ctx, attempt, _, _ = log_scene
    content = "a" * 65536
    params = [
        {
            "attempt_id": attempt,
            "sequence": n,
            "hash": sha256_hex(content.encode()),
            "content": content,
            "offset": n * 65536,
            "length": 65536,
            "now": datetime.now(UTC),
        }
        for n in range(128)
    ]
    with ctx.engine.begin() as conn:
        conn.execute(statement(conn, "agent_chunk_insert"), params)
    tracemalloc.start()
    try:
        total = sum(len(chunk) for chunk in shared_log(ctx.engine, attempt))
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert total == 8 * 1024 * 1024
    assert peak < 4 * 1024 * 1024


def test_external_log_keys_separate_engine_databases_and_cleanup_refuses_other_identities(
    log_scene, tmp_path
):
    from etl_craft.engine.logs import remove_log

    ctx, attempt, run, summary = log_scene
    identity = state(ctx, attempt).engine_id
    key = log_key(identity, 1, run, summary, attempt)
    other = log_key("b" * 32, 1, run, summary, attempt)
    publish_log(ctx.config, key, iter([b"ours"]))
    publish_log(ctx.config, other, iter([b"another engine"]))
    assert b"".join(stored_log(ctx.config, key, 0)) == b"ours"
    with pytest.raises(ConfigurationError, match="invalid identity"):
        remove_log(ctx.config, log_location(ctx.config, other), 1, attempt, identity)
    assert b"".join(stored_log(ctx.config, other, 0)) == b"another engine"
    secret = tmp_path / "secret"
    secret.write_text("keep")
    with pytest.raises(ConfigurationError, match="invalid identity"):
        remove_log(ctx.config, ctx.config.logs.store + "/../secret", 1, attempt, identity)
    assert secret.read_text() == "keep"


def test_live_attempt_and_empty_pending_log_never_claim_a_stored_file(log_scene):
    from etl_craft.services.operations.log_storage import ensure_source

    ctx, attempt, _, _ = log_scene
    with ctx.engine.begin() as conn:
        ensure_source(conn, attempt)
    publish(ctx, attempt)
    assert state(ctx, attempt).log_path is None
    with pytest.raises(ResourceNotFoundError, match="log pending upload"):
        read(ctx, attempt)


def test_log_offset_is_bounded_before_a_query_or_file_seek(log_scene):
    from etl_craft.core.errors import UsageError

    ctx, attempt, _, _ = log_scene
    with pytest.raises(UsageError, match="expected 0"):
        read(ctx, attempt, 2**63)
