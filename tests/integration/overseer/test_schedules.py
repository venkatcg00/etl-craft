"""Durable catch-up, overlap policy, guarded admission and timezone run dates."""

import signal
from dataclasses import replace
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.cron import timezone
from etl_craft.core.errors import StaleTransitionError
from etl_craft.engine import runlog, transitions
from etl_craft.execution.pipeline import init_pipeline_run
from etl_craft.overseer.schedules import Schedules
from etl_craft.overseer.server import WorkingSet
from etl_craft.services.generate_yml import pipeline_dag
from etl_craft.services.operations import OperationContext
from etl_craft.services.validate import validate
from fixtures.metadata import add_pipeline, start_run

NOW = datetime(2026, 1, 4, 12, tzinfo=UTC)


def configure(db, *, policy="SKIP", catchup="Y", zone="UTC", start="2026-01-02"):
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "SCHEDULED")
        conn.execute(
            text(
                "UPDATE CFG_PIPELINES SET RUN_SCHEDULE='@daily', CATCHUP=:catchup, "
                "MAX_CATCHUP_RUNS=2, OVERLAP_POLICY=:policy, SCHEDULE_TIMEZONE=:zone, "
                "SCHEDULE_START_DATE=:start WHERE PIPELINE_ID=:id"
            ),
            {"catchup": catchup, "policy": policy, "zone": zone, "start": start, "id": pipeline},
        )
    return pipeline, OperationContext(db.engine, db.config, SYSTEM_ACTOR)


def test_three_missed_ticks_keep_two_and_record_one_skipped_across_restarts(engine_db):
    _, ctx = configure(engine_db)
    Schedules().refresh(ctx, NOW)
    Schedules().refresh(ctx, NOW)
    with ctx.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT STATUS AS status, RUN_KEY AS run_key, STARTED_BY_KIND AS actor_kind FROM "
                "AUD_PIPELINES_RUN_LOG ORDER BY PIPELINE_RUN_ID"
            )
        ).all()
        reasons = (
            conn.execute(
                text("SELECT REASON AS reason FROM AUD_RUN_INTERVENTIONS ORDER BY INTERVENTION_ID")
            )
            .scalars()
            .all()
        )
    assert [r.status for r in rows] == ["SKIPPED", "QUEUED", "QUEUED"]
    assert len({r.run_key for r in rows}) == 3
    assert all(r.actor_kind == "SCHEDULE" for r in rows)
    assert reasons == ["missed while no overseer was running", "scheduled tick", "scheduled tick"]
    assert len(WorkingSet().refresh(ctx)) == 1


@pytest.mark.parametrize(("policy", "expected"), [("SKIP", "SKIPPED"), ("QUEUE", "QUEUED")])
def test_overlap_policy_records_the_tick_without_creating_an_active_sibling(
    engine_db, policy, expected
):
    pipeline, ctx = configure(engine_db, policy=policy, start="2026-01-04")
    with ctx.engine.begin() as conn:
        active = start_run(conn, pipeline)
    Schedules().refresh(ctx, NOW)
    with ctx.engine.begin() as conn:
        scheduled = conn.execute(
            text(
                "SELECT PIPELINE_RUN_ID AS pipeline_run_id, STATUS AS status FROM "
                "AUD_PIPELINES_RUN_LOG WHERE TRIGGER_KIND='SCHEDULE'"
            )
        ).one()
        assert scheduled.status == expected
        if policy == "QUEUE":
            with pytest.raises(StaleTransitionError):
                transitions.admit_run(conn, scheduled.pipeline_run_id, SYSTEM_ACTOR)
            transitions.finish_run(conn, active, "SUCCESS", SYSTEM_ACTOR)
            transitions.admit_run(conn, scheduled.pipeline_run_id, SYSTEM_ACTOR)
            assert (
                conn.execute(
                    text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG WHERE STATUS='IN-PROGRESS'")
                ).scalar_one()
                == 1
            )


def test_no_catchup_keeps_latest_tick_and_logical_date_uses_schedule_timezone(engine_db):
    _, ctx = configure(engine_db, catchup="N", zone="Pacific/Kiritimati")
    Schedules().refresh(ctx, NOW)
    with ctx.engine.connect() as conn:
        queued = conn.execute(
            text(
                "SELECT RUN_KEY AS run_key, RUN_DATE AS run_date FROM AUD_PIPELINES_RUN_LOG WHERE "
                "STATUS='QUEUED'"
            )
        ).one()
        assert queued.run_key == "schedule:2026-01-04T10:00:00+00:00"
        assert str(queued.run_date) == "2026-01-05"
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG WHERE STATUS='SKIPPED'")
            ).scalar_one()
            == 3
        )


def test_server_creates_and_finishes_a_scheduled_run(cli_project):
    p = cli_project
    today = datetime.now(UTC).date()
    with p.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PIPELINES SET RUN_SCHEDULE='@daily', SCHEDULE_START_DATE=:today"),
            {"today": today},
        )
    server = p.start("server")
    p.wait_for(
        "SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG "
        "WHERE TRIGGER_KIND='SCHEDULE' AND STATUS='SUCCESS'",
        expected=1,
        timeout=20,
    )
    server.signal(signal.SIGTERM)
    assert server.wait() == 0, server.output
    with p.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 1
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG")).scalar_one() == 1


def test_default_manual_run_date_uses_project_timezone_without_changing_audit_time(
    cli_project, monkeypatch
):
    p = cli_project
    fixed = datetime(2026, 1, 1, 0, 10, tzinfo=UTC)

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz)

    monkeypatch.setattr(runlog, "datetime", FrozenDatetime)
    config = replace(p.config, timezone="Pacific/Honolulu")
    assert runlog.today(config.timezone) == date(2025, 12, 31)
    result = init_pipeline_run(p.engine, config, "P")
    with p.engine.connect() as conn:
        row = conn.execute(
            text("SELECT RUN_DATE AS run_date, START_DATE AS start_date FROM AUD_PIPELINES_RUN_LOG")
        ).one()
    assert str(row.run_date) == "2025-12-31"
    assert result.pipeline_run_id is not None
    assert str(row.start_date).startswith(datetime.now(UTC).date().isoformat())
    from etl_craft.execution.interventions import record_stand_in_run

    with p.engine.begin() as conn:
        transitions.finish_run(conn, result.pipeline_run_id, "SUCCESS", SYSTEM_ACTOR)
    stand_in = record_stand_in_run(p.engine, config, "P", "SUCCESS", "external completion")
    with p.engine.connect() as conn:
        assert (
            str(
                conn.execute(
                    text("SELECT RUN_DATE FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:id"),
                    {"id": stand_in.pipeline_run_id},
                ).scalar_one()
            )
            == "2025-12-31"
        )


def test_validation_reports_bad_cron_and_zone_and_remote_yaml_localizes_date(cli_project):
    p = cli_project
    with p.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_PIPELINES SET RUN_SCHEDULE='0 0 30 FEB *', "
                "SCHEDULE_TIMEZONE='missing/zone'"
            )
        )
    report = validate(p.engine, p.config)
    messages = [f.message for f in report.findings]
    assert any("RUN_SCHEDULE" in m for m in messages)
    assert any("SCHEDULE_TIMEZONE" in m for m in messages)
    with p.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_PIPELINES SET RUN_SCHEDULE='@daily', "
                "SCHEDULE_TIMEZONE='America/New_York', SCHEDULE_START_DATE='2026-01-01'"
            )
        )
    with p.engine.connect() as conn:
        dag = pipeline_dag(conn, replace(p.config, mode="remote"), "P")
    assert dag["timezone"] == "America/New_York"
    assert dag["start_date"] == "2026-01-01"
    assert (
        'data_interval_end.in_timezone("America/New_York")'
        in dag["tasks"]["__init__"]["bash_command"]
    )
    from shlex import split
    from types import SimpleNamespace

    from jinja2 import Environment

    env = Environment()
    env.filters["ds"] = lambda value: value.strftime("%Y-%m-%d")
    instant = datetime(2026, 1, 2, 2, tzinfo=UTC)
    interval = SimpleNamespace(in_timezone=lambda name: instant.astimezone(timezone(name)))
    rendered = env.from_string(dag["tasks"]["__init__"]["bash_command"]).render(
        data_interval_end=interval, run_id="one"
    )
    assert split(rendered)[-1] == "2026-01-01"


def test_a_queued_scheduled_run_can_be_cancelled_before_admission(cli_project):
    p = cli_project
    with p.engine.begin() as conn:
        run_id = transitions.create_run(
            conn,
            p.pipeline_id,
            SYSTEM_ACTOR,
            trigger_kind="SCHEDULE",
            run_key="schedule:test-cancel",
            status="QUEUED",
        )
    code, output = p.run(
        "cancel",
        "--pipeline_code",
        "P",
        "--run-id",
        str(run_id),
        "--reason",
        "stop queued schedule",
    )
    assert code == 0, output
    p.wait_for("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG", expected="CANCELLED")


def test_upgrade_preserves_runs_and_audits_new_schedule_columns(
    empty_engine_db, tmp_path, monkeypatch
):
    import json

    from etl_craft.engine.migrations import apply_pending_migrations
    from fixtures.released_schema import install

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    db = empty_engine_db
    install(db, "0.2.0")
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        original = conn.execute(
            text(
                "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID,STATUS,RUN_DATE) "
                "VALUES (:id,'IN-PROGRESS',:day) RETURNING PIPELINE_RUN_ID AS pipeline_run_id"
            ),
            {"id": pipeline, "day": date(2026, 1, 1)},
        ).scalar_one()
    apply_pending_migrations(db.engine)
    with db.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PIPELINES SET SCHEDULE_TIMEZONE='Asia/Kolkata' WHERE PIPELINE_ID=:id"),
            {"id": pipeline},
        )
    with db.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG")).scalar_one()
            == original
        )
        after = conn.execute(
            text(
                "SELECT AFTER_JSON AS after_json FROM AUD_METADATA_CHANGES "
                "WHERE TABLE_NAME='CFG_PIPELINES' AND OPERATION='UPDATE' "
                "ORDER BY CHANGE_ID DESC LIMIT 1"
            )
        ).scalar_one()
        after = json.loads(after) if isinstance(after, str) else after
        assert after["schedule_timezone"] == "Asia/Kolkata"


def test_sqlite_catchup_bound_requires_a_positive_whole_number(engine_db):
    from sqlalchemy.exc import IntegrityError

    if engine_db.engine.dialect.name != "sqlite":
        return
    pipeline, _ = configure(engine_db)
    for value in (0, -1, 2.5):
        with engine_db.engine.begin() as conn, pytest.raises(IntegrityError):
            conn.execute(
                text("UPDATE CFG_PIPELINES SET MAX_CATCHUP_RUNS=:value WHERE PIPELINE_ID=:id"),
                {"value": value, "id": pipeline},
            )
