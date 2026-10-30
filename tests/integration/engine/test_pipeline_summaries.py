"""Pipeline reads add the controller's next scheduled run and each pipeline's newest runs."""

from datetime import UTC, datetime, time, timedelta

from sqlalchemy import text

from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole, ControlAction
from etl_craft.services.operations import OperationContext
from fixtures.actions import act
from fixtures.metadata import add_pipeline, finish_run, start_run

OWNER = Actor("owner", ActorKind.USER)


def scene(engine_db):
    with acting_as(OWNER), engine_db.engine.begin() as conn:
        daily = add_pipeline(conn, "DAILY")
        add_pipeline(conn, "PAUSED")
        add_pipeline(conn, "CLOSED")
        add_pipeline(conn, "MANUAL")
        conn.execute(
            text(
                "UPDATE CFG_PIPELINES SET RUN_SCHEDULE='0 6 * * *', SCHEDULE_TIMEZONE='UTC' "
                "WHERE PIPELINE_CODE IN ('DAILY','PAUSED','CLOSED')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO CFG_CALENDARS (PROJECT_ID, CALENDAR_CODE, WEEKEND_DAYS, ACTIVE_FLAG) "
                "SELECT PROJECT_ID, 'GONE', 'SAT|SUN', 'N' FROM CFG_PIPELINES "
                "WHERE PIPELINE_CODE='CLOSED'"
            )
        )
        conn.execute(
            text(
                "UPDATE CFG_PIPELINES SET CALENDAR_ID=(SELECT CALENDAR_ID FROM CFG_CALENDARS "
                "WHERE CALENDAR_CODE='GONE') WHERE PIPELINE_CODE='CLOSED'"
            )
        )
        runs = []
        for _ in range(7):
            run = start_run(conn, daily)
            finish_run(conn, run)
            runs.append(run)
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET SLA_STATUS='BREACHED' WHERE PIPELINE_RUN_ID=:r"),
            {"r": runs[-1]},
        )
    ctx = OperationContext(engine_db.engine, engine_db.config, OWNER, role=ApiRole.ADMIN)
    act(ctx, ActionName.CONTROL, pipeline_code="PAUSED", action=ControlAction.PAUSE, reason="x")
    return ctx, runs


def test_list_gives_next_run_problems_and_the_five_newest_runs(engine_db):
    ctx, runs = scene(engine_db)
    before = datetime.now(UTC)
    views = {v.pipeline_code: v for v in act(ctx, ActionName.PIPELINES).pipelines}
    today_six = datetime.combine(before.date(), time(6), UTC)
    expected = today_six if today_six > before else today_six + timedelta(days=1)
    assert views["DAILY"].next_run == expected and views["DAILY"].schedule_problem is None
    assert views["PAUSED"].next_run is None and views["PAUSED"].schedule_problem is None
    assert views["MANUAL"].next_run is None and views["MANUAL"].schedule_problem is None
    assert views["CLOSED"].next_run is None
    assert "GONE" in views["CLOSED"].schedule_problem
    newest = views["DAILY"].recent_runs
    assert [r.pipeline_run_id for r in newest] == sorted(runs, reverse=True)[:5]
    assert newest[0].sla_status == "BREACHED" and newest[1].sla_status is None
    assert views["MANUAL"].recent_runs == ()


def test_one_pipeline_read_gives_the_same_summary_as_the_list(engine_db):
    ctx, _ = scene(engine_db)
    listed = {v.pipeline_code: v for v in act(ctx, ActionName.PIPELINES).pipelines}
    for code in ("DAILY", "CLOSED", "PAUSED"):
        one = act(ctx, ActionName.PIPELINE, pipeline_code=code)
        assert (one.next_run, one.schedule_problem, one.recent_runs) == (
            listed[code].next_run,
            listed[code].schedule_problem,
            listed[code].recent_runs,
        )
