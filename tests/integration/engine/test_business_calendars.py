"""Business calendars skip or move scheduled ticks, and validate checks them and their lists."""

from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etl_craft.controller.schedules import Schedules
from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.services.operations import OperationContext
from etl_craft.services.validate import validate
from fixtures.actions import act
from fixtures.metadata import add_pipeline

OWNER = Actor("owner", ActorKind.USER)
# Christmas 2026 is a Friday: the tick then the weekend fall on closed days.
NOW = datetime(2026, 12, 29, 12, tzinfo=UTC)


def add_calendar(conn, holidays, *, project_id=1, code="UK"):
    conn.execute(
        text(
            "INSERT INTO CFG_CALENDARS (PROJECT_ID, CALENDAR_CODE, WEEKEND_DAYS) "
            "VALUES (:project, :code, 'SAT|SUN')"
        ),
        {"project": project_id, "code": code},
    )
    calendar = conn.execute(
        text("SELECT CALENDAR_ID FROM CFG_CALENDARS WHERE PROJECT_ID=:p AND CALENDAR_CODE=:c"),
        {"p": project_id, "c": code},
    ).scalar_one()
    for day, name in holidays.items():
        conn.execute(
            text(
                "INSERT INTO CFG_CALENDAR_HOLIDAYS (CALENDAR_ID, HOLIDAY_DATE, HOLIDAY_NAME) "
                "VALUES (:calendar, :day, :name)"
            ),
            {"calendar": calendar, "day": day, "name": name},
        )
    return calendar


def scheduled(db, *, schedule="0 6 * * *", policy="SKIP", holidays=None):
    with acting_as(OWNER), db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "DAILY")
        calendar = add_calendar(
            conn, holidays or {date(2026, 12, 25): "Christmas Day", date(2026, 12, 28): None}
        )
        conn.execute(
            text(
                "UPDATE CFG_PIPELINES SET RUN_SCHEDULE=:schedule, SCHEDULE_TIMEZONE='UTC', "
                "SCHEDULE_START_DATE='2026-12-24', CATCHUP='Y', MAX_CATCHUP_RUNS=10, "
                "CALENDAR_ID=:calendar, NON_BUSINESS_DAY=:policy WHERE PIPELINE_ID=:id"
            ),
            {"schedule": schedule, "calendar": calendar, "policy": policy, "id": pipeline},
        )
    return OperationContext(
        db.engine, db.config, Actor("scheduler", ActorKind.SERVICE), role=ApiRole.ADMIN
    )


def ticks(db):
    with db.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT r.RUN_DATE AS run_date, r.STATUS AS status, r.RUN_KEY AS run_key, "
                "i.REASON AS reason FROM AUD_PIPELINES_RUN_LOG r JOIN AUD_RUN_INTERVENTIONS i "
                "ON i.PIPELINE_RUN_ID=r.PIPELINE_RUN_ID ORDER BY r.RUN_KEY"
            )
        ).all()
    return [(str(r.run_date)[:10], r.status, r.reason) for r in rows]


def test_skip_records_each_closed_day_naming_the_calendar_and_date(engine_db):
    ctx = scheduled(engine_db)
    Schedules().refresh(ctx, NOW)
    Schedules().refresh(ctx, NOW)
    assert ticks(engine_db) == [
        ("2026-12-24", "QUEUED", "scheduled tick"),
        ("2026-12-25", "SKIPPED", "calendar UK: 2026-12-25 is a holiday (Christmas Day)"),
        ("2026-12-26", "SKIPPED", "calendar UK: 2026-12-26 is a weekend day (SAT)"),
        ("2026-12-27", "SKIPPED", "calendar UK: 2026-12-27 is a weekend day (SUN)"),
        ("2026-12-28", "SKIPPED", "calendar UK: 2026-12-28 is a holiday"),
        ("2026-12-29", "QUEUED", "scheduled tick"),
    ]


def test_next_business_day_moves_closed_ticks_into_that_days_one_run(engine_db):
    ctx = scheduled(engine_db, policy="NEXT_BUSINESS_DAY")
    Schedules().refresh(ctx, NOW)
    Schedules().refresh(ctx, NOW)
    assert ticks(engine_db) == [
        ("2026-12-24", "QUEUED", "scheduled tick"),
        ("2026-12-29", "QUEUED", "scheduled tick"),
    ]


def test_validate_refuses_unrunnable_schedules_and_holiday_lists_about_to_end(engine_db):
    soon = (datetime.now(UTC) + timedelta(days=10)).date()
    ctx = scheduled(engine_db, schedule="0 6 LBD * *", holidays={soon: "Soon"})
    with acting_as(OWNER), engine_db.engine.begin() as conn:
        other = add_pipeline(conn, "NO_CALENDAR")
        conn.execute(
            text("UPDATE CFG_PIPELINES SET RUN_SCHEDULE='0 6 3BD * *' WHERE PIPELINE_ID=:id"),
            {"id": other},
        )
    findings = {(f.where, f.message) for f in validate(ctx.engine, ctx.config).findings}
    assert (
        "calendar UK",
        f"the holiday list ends on {soon.isoformat()}, within 30 days; add the next year's "
        "holidays to calendar_holidays.csv",
    ) in findings
    assert (
        "NO_CALENDAR",
        "RUN_SCHEDULE='0 6 3BD * *' uses business days, but the pipeline names no "
        "CALENDAR_CODE; name its calendar",
    ) in findings
    assert not [where for where, _ in findings if where == "DAILY"]


def test_a_retired_calendar_fails_its_pipelines_schedule(engine_db):
    ctx = scheduled(engine_db)
    with acting_as(OWNER), engine_db.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_CALENDARS SET ACTIVE_FLAG='N'"))
    findings = {(f.where, f.message) for f in validate(ctx.engine, ctx.config).findings}
    assert (
        "DAILY",
        "DAILY: CALENDAR names calendar UK, which is missing or inactive; set the calendar's "
        "ACTIVE_FLAG to Y, or clear the pipeline's CALENDAR_CODE",
    ) in findings


def test_a_pipeline_cannot_name_another_projects_calendar(engine_db):
    admin = OperationContext(engine_db.engine, engine_db.config, OWNER, role=ApiRole.ADMIN)
    act(admin, ActionName.PROJECT_ADD, project="other", reason="Calendar fixture")
    with acting_as(OWNER), engine_db.engine.begin() as conn:
        other = conn.execute(
            text("SELECT PROJECT_ID FROM CFG_PROJECTS WHERE PROJECT_CODE='other'")
        ).scalar_one()
        calendar = add_calendar(conn, {}, project_id=other)
        pipeline = add_pipeline(conn, "MINE")
    with (
        pytest.raises(DBAPIError, match="calendar of the pipeline's own project"),
        acting_as(OWNER),
        engine_db.engine.begin() as conn,
    ):
        conn.execute(
            text("UPDATE CFG_PIPELINES SET CALENDAR_ID=:c WHERE PIPELINE_ID=:p"),
            {"c": calendar, "p": pipeline},
        )
