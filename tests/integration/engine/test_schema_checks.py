"""Warehouse schema checks: an optional per-project schedule and on-demand checks."""

import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from etl_craft.controller.schema_checks import schema_checks_due
from etl_craft.core.actor import SYSTEM_ACTOR, Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole, YesNo
from etl_craft.core.errors import PermissionDeniedError, UsageError
from etl_craft.services.operations import OperationContext
from fixtures.actions import act
from fixtures.metadata import add_pipeline, add_table, add_task

OWNER = Actor("owner", ActorKind.USER)


@pytest.fixture
def project(engine_db, tmp_path):
    config = replace(
        engine_db.config,
        docs_site=replace(engine_db.config.docs_site, output=tmp_path / "catalog"),
    )
    with acting_as(OWNER), engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "SALES")
        add_task(conn, pipeline, "LOAD")
        add_table(conn, "mart.sales")
    return OperationContext(engine_db.engine, config, OWNER, role=ApiRole.ADMIN), tmp_path


def settings(ctx, **fields):
    return act(ctx, ActionName.PROJECT_SET, project="default", reason="checks", **fields)


def test_project_settings_take_a_cron_schedule_none_and_on_demand(project):
    ctx, _ = project
    listed = settings(ctx, schema_check_schedule="0 2 * * *").projects[0]
    assert (listed.schema_check_schedule, listed.schema_check_on_demand) == ("0 2 * * *", "Y")
    listed = settings(ctx, schema_check_on_demand=YesNo.NO).projects[0]
    assert (listed.schema_check_schedule, listed.schema_check_on_demand) == ("0 2 * * *", "N")
    assert settings(ctx, schema_check_schedule="none").projects[0].schema_check_schedule is None
    with pytest.raises(UsageError, match=r"--schema-check-schedule='every night'.*cron"):
        settings(ctx, schema_check_schedule="every night")
    with pytest.raises(UsageError, match="--schema-check-on-demand"):
        settings(ctx, schema_check_on_demand="maybe")


def test_on_demand_checks_refused_when_off_but_the_schedule_still_runs(project):
    ctx, _ = project
    settings(ctx, schema_check_on_demand=YesNo.NO)
    with pytest.raises(PermissionDeniedError, match="on-demand schema checks are off"):
        act(ctx, ActionName.GENERATE_DOCS, with_warehouse=True)
    act(ctx, ActionName.GENERATE_DOCS)
    settings(ctx, schema_check_schedule="*/5 * * * *")
    due: dict[int, datetime] = {}
    now = datetime(2026, 10, 30, 10, 1, tzinfo=UTC)
    schema_checks_due(replace(ctx, actor=SYSTEM_ACTOR), now, threading.Event(), due)
    assert list(due.values()) == [datetime(2026, 10, 30, 10, 5, tzinfo=UTC)]
    before = act(ctx, ActionName.INSIGHTS).gaps_compared_at
    schema_checks_due(ctx, now + timedelta(minutes=3), threading.Event(), due)
    assert act(ctx, ActionName.INSIGHTS).gaps_compared_at == before
    schema_checks_due(ctx, now + timedelta(minutes=5), threading.Event(), due)
    assert list(due.values()) == [datetime(2026, 10, 30, 10, 10, tzinfo=UTC)]
    checked = act(ctx, ActionName.INSIGHTS)
    assert checked.gaps_compared_at is not None and before is None
    assert checked.schema_check_schedule == "*/5 * * * *"
    assert checked.schema_check_on_demand is False
    settings(ctx, schema_check_schedule="none")
    schema_checks_due(ctx, now + timedelta(minutes=30), threading.Event(), due)
    assert due == {}


def test_insights_reads_the_stored_check_only_for_whole_project_viewers(project, engine_db):
    from sqlalchemy import text

    from etl_craft.services.operations.identity import authenticated_context

    ctx, _ = project
    assert act(ctx, ActionName.INSIGHTS).gaps_compared_at is None
    act(ctx, ActionName.GENERATE_DOCS, with_warehouse=True)
    assert act(ctx, ActionName.INSIGHTS).gaps_compared_at is not None
    token = act(ctx, ActionName.TOKEN_CREATE, name="reader", role=ApiRole.VIEWER)
    reader = authenticated_context(ctx, token.metadata)
    with acting_as(OWNER), engine_db.engine.begin() as conn:
        add_pipeline(conn, "HIDDEN")
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_GRANTS (PIPELINE_CODE,GRANTEE_ROLE,PERMISSION) "
                "VALUES ('HIDDEN','operator','CAN_VIEW')"
            )
        )
    assert act(reader, ActionName.INSIGHTS).gaps_compared_at is None
