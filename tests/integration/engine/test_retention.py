"""Project purge protects recorded decisions, live recovery state and canonical files."""

from dataclasses import replace
from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from etl_craft.config.model import Retention
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.errors import ConfigurationError, PermissionDeniedError
from etl_craft.execution.attempt_records import attempt_log_path
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.retention import PurgeInput
from etl_craft.services.retention import retaining
from fixtures.metadata import add_pipeline, add_task, insert, upstream_run
from fixtures.retention import FUTURE, OLD, count


def test_purge_is_opt_in_and_dry_run_is_read_only(scene):
    ctx, _, _, _, _, _ = scene
    untouched = replace(ctx, config=replace(ctx.config, retention=Retention()))
    invoke(untouched, ActionName.PURGE, PurgeInput())
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1
    result = invoke(ctx, ActionName.PURGE, PurgeInput(dry_run=True))
    assert result.counts["AUD_TASK_ATTEMPTS"] == 1
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1
    done = invoke(ctx, ActionName.PURGE, PurgeInput())
    assert done.counts["AUD_TASK_ATTEMPTS"] == 1
    assert done.counts["AUD_TASK_RUN_LOG"] == 1
    assert done.counts["AUD_PIPELINES_RUN_LOG"] == 1
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 0


def test_purge_requires_admin_and_records_the_refusal(scene):
    ctx, *_ = scene
    with pytest.raises(PermissionDeniedError):
        invoke(replace(ctx, role=ApiRole.DEVELOPER), ActionName.PURGE, PurgeInput())
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT OUTCOME AS outcome FROM AUD_ACTIONS WHERE COMMAND='purge'")
            ).scalar_one()
            == "REFUSED"
        )


def test_live_and_recent_runs_keep_their_attempts(scene):
    ctx, _, _, run, _, _ = scene
    with retaining(), ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET END_DATE=:new WHERE PIPELINE_RUN_ID=:run"),
            {"new": FUTURE, "run": run},
        )
    invoke(ctx, ActionName.PURGE, PurgeInput())
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1
    with retaining(), ctx.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET END_DATE=:old,STATUS='IN-PROGRESS' "
                "WHERE PIPELINE_RUN_ID=:run"
            ),
            {"old": OLD, "run": run},
        )
    invoke(ctx, ActionName.PURGE, PurgeInput())
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1


def test_retained_gate_decision_pins_selected_run_and_task(scene):
    ctx, pipeline, task, run, task_run, _ = scene
    with ctx.engine.begin() as conn:
        other = add_task(conn, pipeline, "downstream", handler="PYTHON")
        dependency = insert(
            conn,
            "INSERT INTO CFG_TASK_DEPENDENCY "
            "(PIPELINE_ID,TASK_ID,DEPENDS_ON_PIPELINE_ID,DEPENDS_ON_TASK_ID,"
            "DEPENDENCY_TYPE) VALUES (:p,:other,:p,:t,'SUCCESS')",
            "TASK_DEPENDENCY_ID",
            p=pipeline,
            t=task,
            other=other,
        )
        downstream, _ = upstream_run(conn, pipeline, {})
        conn.execute(
            text(
                "INSERT INTO AUD_GATE_DECISIONS (PIPELINE_RUN_ID,TASK_DEPENDENCY_ID,"
                "SELECTED_PIPELINE_RUN_ID,SELECTED_TASK_RUN_ID,RESULT,REASON) "
                "VALUES (:down,:dep,:run,:task,'SATISFIED','recorded')"
            ),
            {"down": downstream, "dep": dependency, "run": run, "task": task_run},
        )
    invoke(ctx, ActionName.PURGE, PurgeInput())
    assert count(ctx, "AUD_GATE_DECISIONS") == 1
    assert count(ctx, "AUD_TASK_RUN_LOG") == 1
    assert count(ctx, "AUD_PIPELINES_RUN_LOG") == 2


def test_log_expiry_keeps_removal_evidence_and_deletes_only_canonical_files(scene):
    ctx, _, _, run, _, attempt = scene
    path = attempt_log_path(ctx.config, "retention", run, "load", 1)
    path.parent.mkdir(parents=True)
    path.write_text("expired secret-free log")
    with retaining(), ctx.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE AUD_TASK_ATTEMPTS SET LOG_PATH=:path,TASK_LOG='tail' WHERE ATTEMPT_ID=:id"
            ),
            {"path": str(path), "id": attempt},
        )
    config = replace(ctx.config, retention=Retention(attempt_log_days=30))
    ctx = replace(ctx, config=config)
    done = invoke(ctx, ActionName.PURGE, PurgeInput())
    assert done.counts["files"] == 1
    assert not path.exists()
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1
    from etl_craft.services.operations.requests import LogInput

    message = b"".join(invoke(ctx, ActionName.LOGS, LogInput(attempt_id=attempt)))
    assert b"removed by retention on" in message
    assert count(ctx, "AUD_RETENTION_FILES") == 0


def test_noncanonical_log_path_fails_before_any_log_is_retired(scene, tmp_path):
    ctx, _, _, _, _, attempt = scene
    outside = tmp_path / "keep.txt"
    outside.write_text("keep")
    with retaining(), ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LOG_PATH=:path WHERE ATTEMPT_ID=:id"),
            {"path": str(outside), "id": attempt},
        )
    ctx = replace(ctx, config=replace(ctx.config, retention=Retention(attempt_log_days=30)))
    with pytest.raises(ConfigurationError, match=r"LOG_PATH=.*expected.*restore"):
        invoke(ctx, ActionName.PURGE, PurgeInput())
    assert outside.read_text() == "keep"
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT LOG_REMOVED_AT AS removed FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"
                ),
                {"id": attempt},
            ).scalar_one()
            is None
        )


def test_project_purge_cannot_delete_another_projects_history(scene):
    ctx, pipeline, _, _, _, _ = scene
    with ctx.engine.begin() as conn:
        project = insert(
            conn,
            "INSERT INTO CFG_PROJECTS (PROJECT_CODE,PROJECT_NAME,DEFAULT_POOL) "
            "VALUES ('other','Other','default')",
            "PROJECT_ID",
        )
        other = add_pipeline(conn, "other")
        conn.execute(
            text("UPDATE CFG_PIPELINES SET PROJECT_ID=:project WHERE PIPELINE_ID=:id"),
            {"project": project, "id": other},
        )
        run, _ = upstream_run(conn, other, {})
    with retaining(), ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET END_DATE=:old WHERE PIPELINE_RUN_ID=:id"),
            {"old": OLD, "id": run},
        )
    invoke(ctx, ActionName.PURGE, PurgeInput())
    with ctx.engine.connect() as conn:
        rows = conn.execute(
            text("SELECT PIPELINE_ID AS pipeline_id FROM AUD_PIPELINES_RUN_LOG")
        ).all()
    assert [row.pipeline_id for row in rows] == [other]
    assert other != pipeline


def test_recorded_consumption_keeps_its_upstream_until_downstream_expires(scene):
    ctx, pipeline, _, run, task_run, _ = scene
    with ctx.engine.begin() as conn:
        other = add_pipeline(conn, "consumer")
        dependency = insert(
            conn,
            "INSERT INTO CFG_PIPELINE_DEPENDENCY "
            "(PIPELINE_ID,DEPENDS_ON_PIPELINE_ID,DEPENDENCY_TYPE) "
            "VALUES (:other,:pipeline,'SUCCESS')",
            "PIPELINE_DEPENDENCY_ID",
            other=other,
            pipeline=pipeline,
        )
        down, _ = upstream_run(conn, other, {})
        conn.execute(
            text(
                "INSERT INTO AUD_DEPENDENCY_CONSUMPTION "
                "(PIPELINE_DEPENDENCY_ID,PIPELINE_ID,PIPELINE_RUN_ID,DEPENDS_ON_PIPELINE_ID,"
                "CONSUMED_PIPELINE_RUN_ID,CONSUMED_TASK_RUN_ID,CONSUMED_AT) "
                "VALUES (:dep,:other,:down,:p,:run,:task,:old)"
            ),
            {
                "dep": dependency,
                "other": other,
                "down": down,
                "p": pipeline,
                "run": run,
                "task": task_run,
                "old": OLD,
            },
        )
    ctx = replace(
        ctx,
        config=replace(ctx.config, retention=Retention(run_history_days=90, consumption_days=90)),
    )
    invoke(ctx, ActionName.PURGE, PurgeInput())
    assert count(ctx, "AUD_DEPENDENCY_CONSUMPTION") == 1
    assert count(ctx, "AUD_TASK_RUN_LOG") == 1
    with retaining(), ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET END_DATE=:old WHERE PIPELINE_RUN_ID=:id"),
            {"old": OLD, "id": down},
        )
    invoke(ctx, ActionName.PURGE, PurgeInput())
    assert count(ctx, "AUD_DEPENDENCY_CONSUMPTION") == 0
    assert count(ctx, "AUD_TASK_RUN_LOG") == 0
    assert count(ctx, "AUD_PIPELINES_RUN_LOG") == 0


def test_purge_preserves_current_config_and_recovery_state(scene):
    ctx, pipeline, _, _, _, _ = scene
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PIPELINES SET DESCRIPTION='new' WHERE PIPELINE_ID=:id"),
            {"id": pipeline},
        )
    with retaining(), ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_CFG_PIPELINES_HISTORY SET VALID_TO=:old WHERE CURRENT_FLAG='N'"),
            {"old": OLD},
        )
    ctx = replace(ctx, config=replace(ctx.config, retention=Retention(config_history_days=30)))
    invoke(ctx, ActionName.PURGE, PurgeInput())
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT CURRENT_FLAG AS current_flag FROM AUD_CFG_PIPELINES_HISTORY")
            ).scalar_one()
            == "Y"
        )
    assert count(ctx, "CFG_PIPELINES") == 1
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1


def test_two_purges_serialize_without_double_deleting(scene):
    from etl_craft.engine.locks import EngineLock
    from fixtures.races import two_at_once

    ctx, *_ = scene
    results = two_at_once(
        lambda: invoke(ctx, ActionName.PURGE, PurgeInput()),
        lambda: invoke(ctx, ActionName.PURGE, PurgeInput()),
        at=(EngineLock, "hold"),
    )
    assert sorted(result.counts["AUD_TASK_ATTEMPTS"] for result in results) == [0, 1]
    assert count(ctx, "AUD_PIPELINES_RUN_LOG") == 0


def test_daily_purge_uses_project_time_and_a_durable_completion_cursor(scene):
    import threading

    from etl_craft.controller.retention import purge_due

    ctx, *_ = scene
    ctx = replace(
        ctx, config=replace(ctx.config, retention=Retention(run_history_days=90, daily_at="00:00"))
    )
    now = datetime.now(UTC)
    purge_due(ctx, now, threading.Event())
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 0
    requests = count(ctx, "AUD_ACTIONS")
    purge_due(ctx, now, threading.Event())
    assert count(ctx, "AUD_ACTIONS") == requests


def test_log_symlink_is_refused_without_removing_the_external_file(scene, tmp_path):
    ctx, _, _, run, _, attempt = scene
    path = attempt_log_path(ctx.config, "retention", run, "load", 1)
    path.parent.mkdir(parents=True)
    outside = tmp_path / "outside.log"
    outside.write_text("keep")
    path.symlink_to(outside)
    with retaining(), ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LOG_PATH=:path WHERE ATTEMPT_ID=:id"),
            {"path": str(path), "id": attempt},
        )
    ctx = replace(ctx, config=replace(ctx.config, retention=Retention(attempt_log_days=30)))
    with pytest.raises(ConfigurationError, match="symlink"):
        invoke(ctx, ActionName.PURGE, PurgeInput())
    assert outside.read_text() == "keep"
    assert count(ctx, "AUD_RETENTION_FILES") == 0


def test_daily_purge_runs_once_on_a_repeated_local_time(scene, monkeypatch):
    import threading

    from etl_craft.controller.retention import purge_due
    from etl_craft.services.operations import retention

    clock = [datetime(2026, 11, 1, 5, 29, tzinfo=UTC)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].astimezone(tz)

    monkeypatch.setattr(retention, "datetime", Clock)
    ctx, *_ = scene
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PROJECTS SET TIMEZONE='America/New_York' WHERE PROJECT_CODE='default'")
        )
    ctx = replace(
        ctx, config=replace(ctx.config, retention=Retention(run_history_days=90, daily_at="01:30"))
    )
    purge_due(ctx, clock[0], threading.Event())
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1
    clock[0] = datetime(2026, 11, 1, 5, 30, tzinfo=UTC)
    purge_due(ctx, clock[0], threading.Event())
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 0
    requests = count(ctx, "AUD_ACTIONS")
    clock[0] = datetime(2026, 11, 1, 6, 30, tzinfo=UTC)
    purge_due(ctx, clock[0], threading.Event())
    assert count(ctx, "AUD_ACTIONS") == requests


def test_inactive_project_history_can_still_expire(scene):
    ctx, *_ = scene
    with ctx.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_PROJECTS SET ACTIVE_FLAG='N' WHERE PROJECT_CODE='default'"))
    invoke(ctx, ActionName.PURGE, PurgeInput())
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 0


def test_shared_log_chunks_expire_with_the_same_removal_evidence(scene):
    ctx, _, _, _, _, attempt = scene
    with ctx.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO AUD_ATTEMPT_LOG_CHUNKS "
                "(ATTEMPT_ID,SEQUENCE_NUMBER,CONTENT_SHA256,CONTENT,BYTE_OFFSET,"
                "BYTE_LENGTH,CREATED_AT) VALUES (:id,0,:hash,'old log',0,7,:old)"
            ),
            {"id": attempt, "hash": "0" * 64, "old": OLD},
        )
    ctx = replace(ctx, config=replace(ctx.config, retention=Retention(attempt_log_days=30)))
    snapshot = invoke(ctx, ActionName.PURGE, PurgeInput(dry_run=True))
    assert snapshot.counts["attempt_logs"] == 1
    assert count(ctx, "AUD_ATTEMPT_LOG_CHUNKS") == 1
    result = invoke(ctx, ActionName.PURGE, PurgeInput())
    assert result.counts["AUD_ATTEMPT_LOG_CHUNKS"] == 1
    assert count(ctx, "AUD_ATTEMPT_LOG_CHUNKS") == 0
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 1
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT LOG_REMOVED_AT AS removed FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"
                ),
                {"id": attempt},
            ).scalar_one()
            is not None
        )


def test_latest_scheduled_run_keeps_the_controller_restart_cursor(scene):
    ctx, _, _, run, _, _ = scene
    with ctx.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET TRIGGER_KIND='SCHEDULE',"
                "RUN_KEY='schedule:2000-01-01T00:00:00Z' WHERE PIPELINE_RUN_ID=:id"
            ),
            {"id": run},
        )
    invoke(ctx, ActionName.PURGE, PurgeInput())
    assert count(ctx, "AUD_PIPELINES_RUN_LOG") == 1
    assert count(ctx, "AUD_TASK_ATTEMPTS") == 0


def test_empty_attempt_does_not_pin_an_expired_summary_tail(scene):
    ctx, _, _, _, task_run, attempt = scene
    with retaining(), ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET TASK_LOG='expired' WHERE ATTEMPT_ID=:id"),
            {"id": attempt},
        )
        conn.execute(
            text("UPDATE AUD_TASK_RUN_LOG SET TASK_LOG='expired' WHERE TASK_RUN_ID=:id"),
            {"id": task_run},
        )
        insert(
            conn,
            "INSERT INTO AUD_TASK_ATTEMPTS (TASK_RUN_ID,ATTEMPT_NUMBER,STATUS,ENDED_AT) "
            "VALUES (:task,2,'FAILED',:old)",
            "ATTEMPT_ID",
            task=task_run,
            old=OLD,
        )
    ctx = replace(ctx, config=replace(ctx.config, retention=Retention(attempt_log_days=30)))
    invoke(ctx, ActionName.PURGE, PurgeInput())
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT TASK_LOG AS log FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID=:id"),
                {"id": task_run},
            ).scalar_one()
            is None
        )


def test_gate_wait_batch_counts_distinct_wait_kinds(scene, engine_db, monkeypatch):
    from sqlalchemy import event

    from etl_craft.core.enums import WaitKind

    ctx, _, task, run, _, _ = scene
    with ctx.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO AUD_GATE_WAITS "
                "(PIPELINE_RUN_ID,TASK_ID,FIRST_CHECK_AT,LOOKS,WAIT_UNTIL,WAIT_KIND) "
                "VALUES (:run,:task,:old,1,:old,:kind)"
            ),
            [
                {"run": run, "task": task, "old": OLD, "kind": kind}
                for kind in (WaitKind.DEPENDENCY, WaitKind.EVENT)
            ],
        )
    original = type(engine_db.dialect).query

    def one_row(dialect, name):
        sql = original(dialect, name)
        return sql.replace("LIMIT 10000", "LIMIT 1") if name == "retention_aud_gate_waits" else sql

    monkeypatch.setattr(type(engine_db.dialect), "query", one_row)
    batches = []

    def observed(conn, cursor, sql, parameters, context, executemany):
        if sql.startswith("DELETE FROM AUD_GATE_WAITS"):
            batches.append(cursor.rowcount)

    event.listen(ctx.engine, "after_cursor_execute", observed)
    try:
        invoke(ctx, ActionName.PURGE, PurgeInput())
    finally:
        event.remove(ctx.engine, "after_cursor_execute", observed)
    assert max(batches) == 1
    assert sum(batches) == 2
    assert count(ctx, "AUD_GATE_WAITS") == 0
