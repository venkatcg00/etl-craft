"""Orchestration.Enforce_sla: runs judged against CFG_PIPELINES.SLA_IN_HOURS (2026-09-24).

The setting existed since E2-17/E2-23 and nothing read it. On, a finishing run
records SLA_STATUS MET or BREACHED, the outcome message and `history` name a
breach, and an EMAIL_ALERT sent after the SLA has passed is amber rather than
green. Off -- the default -- nothing changes, and SLA_STATUS is never written.
Runs against the Engine DB the suite is using (PostgreSQL, or SQLite under
ETL_CRAFT_TEST_ENGINE=sqlite).
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from conftest import engine_profile, seed_active_run
from etl_craft.cli import main
from etl_craft.config import (
    CloningConfig,
    ConnectionSection,
    ConnectorConfig,
    ExecutionLimits,
    SourceConfig,
)
from etl_craft.email_alert import _sla_breach_so_far
from etl_craft.execution import TaskExecutionContext
from etl_craft.orchestrator import finalize_active_run
from etl_craft.runlog import SlaResult, elapsed_hours, finalize_pipeline_run


def _config(enforce_sla: bool) -> ConnectorConfig:
    return ConnectorConfig(
        mode="orchestrator",
        source=SourceConfig(type="environment"),
        postgres=ConnectionSection(active_profile="dev", profiles={"dev": engine_profile()}),
        cloning=CloningConfig(),
        limits=ExecutionLimits(enforce_sla=enforce_sla),
    )


def _started_hours_ago(engine, pipeline_run_id: int, hours: float) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET START_DATE = :start WHERE PIPELINE_RUN_ID = :id"
            ),
            {"start": datetime.now(UTC) - timedelta(hours=hours), "id": pipeline_run_id},
        )


def _set_sla(engine, pipeline_id: int, hours: float | None) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PIPELINES SET SLA_IN_HOURS = :h WHERE PIPELINE_ID = :id"),
            {"h": hours, "id": pipeline_id},
        )


def _run_row(engine, pipeline_run_id: int):
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT STATUS, SLA_STATUS FROM AUD_PIPELINES_RUN_LOG "
                "WHERE PIPELINE_RUN_ID = :id"
            ),
            {"id": pipeline_run_id},
        ).one()


def test_sla_result_describes_itself():
    assert SlaResult("BREACHED", 2.0, 2.5).describe() == "SLA of 2 h BREACHED (ran 2.50 h)"
    # A naive START_DATE is read as UTC, the engine's own convention.
    now = datetime(2026, 9, 24, 12, tzinfo=UTC)
    assert elapsed_hours(datetime(2026, 9, 24, 9), now) == 3.0


def test_a_run_is_judged_met_or_breached_and_keeps_its_own_status(
    postgres_engine, committed_pipeline
):
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    _started_hours_ago(postgres_engine, run_id, 3)
    with postgres_engine.begin() as conn:
        sla = finalize_pipeline_run(conn, run_id, "SUCCESS", sla_in_hours=2)
    assert sla is not None and sla.status == "BREACHED" and sla.elapsed_hours > 2.9
    # A late run still did its work: STATUS is its own, SLA_STATUS the verdict.
    assert tuple(_run_row(postgres_engine, run_id)) == ("SUCCESS", "BREACHED")

    with postgres_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET STATUS = 'IN-PROGRESS' "
                "WHERE PIPELINE_RUN_ID = :id"
            ),
            {"id": run_id},
        )
        met = finalize_pipeline_run(conn, run_id, "FAILED", sla_in_hours=4)
    assert met is not None and met.status == "MET"
    assert tuple(_run_row(postgres_engine, run_id)) == ("FAILED", "MET")


def test_without_enforcement_nothing_is_judged(postgres_engine, committed_pipeline):
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    with postgres_engine.begin() as conn:
        assert finalize_pipeline_run(conn, run_id, "SUCCESS") is None
    assert tuple(_run_row(postgres_engine, run_id)) == ("SUCCESS", None)


def test_finalize_reports_a_breach_only_when_enforce_sla_is_on(postgres_engine, committed_pipeline):
    _set_sla(postgres_engine, committed_pipeline, 1)
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    _started_hours_ago(postgres_engine, run_id, 3)
    outcome = finalize_active_run(postgres_engine, _config(True), "TEST_CONCURRENT_PL")
    assert outcome.status == "SUCCESS"
    assert "SLA of 1 h BREACHED" in outcome.message
    assert tuple(_run_row(postgres_engine, run_id)) == ("SUCCESS", "BREACHED")

    # The same late run with enforcement off: no verdict, no mention.
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    _started_hours_ago(postgres_engine, run_id, 3)
    outcome = finalize_active_run(postgres_engine, _config(False), "TEST_CONCURRENT_PL")
    assert "SLA" not in outcome.message
    assert tuple(_run_row(postgres_engine, run_id)) == ("SUCCESS", None)


def test_a_pipeline_without_an_sla_is_never_judged(postgres_engine, committed_pipeline):
    _set_sla(postgres_engine, committed_pipeline, None)
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    finalize_active_run(postgres_engine, _config(True), "TEST_CONCURRENT_PL")
    assert tuple(_run_row(postgres_engine, run_id)) == ("SUCCESS", None)


def test_history_names_the_verdict(
    postgres_engine, committed_pipeline, craft_connector_on_disk, capsys
):
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    _started_hours_ago(postgres_engine, run_id, 3)
    with postgres_engine.begin() as conn:
        finalize_pipeline_run(conn, run_id, "SUCCESS", sla_in_hours=2)
    # craft_connector_on_disk writes the config into the working directory.
    assert main(["history", "--pipeline_code", "TEST_CONCURRENT_PL"]) == 0
    (line,) = [row for row in capsys.readouterr().out.splitlines() if f"={run_id}\t" in row]
    assert line.startswith(f"pipeline_run_id={run_id}\tSUCCESS")
    assert line.endswith("\tSLA BREACHED")


def test_an_alert_sent_after_the_sla_has_passed_says_so(postgres_engine, committed_pipeline):
    _set_sla(postgres_engine, committed_pipeline, 1)
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    ctx = TaskExecutionContext(
        config=_config(True),
        task_run_id=1,
        pipeline_run_id=run_id,
        handler="EMAIL_ALERT",
        task_params={},
        pipeline_code="TEST_CONCURRENT_PL",
        task_code="ALERT",
        refresh_type="FULL",
        force=False,
        task_id=1,
        pipeline_id=committed_pipeline,
    )
    with postgres_engine.connect() as conn:
        # Still inside the SLA: nothing to say.
        assert _sla_breach_so_far(conn, ctx) is None
    _started_hours_ago(postgres_engine, run_id, 2)
    with postgres_engine.connect() as conn:
        note = _sla_breach_so_far(conn, ctx)
        assert note is not None and note.startswith("SLA of 1 h BREACHED")
        # Off, never.
        assert _sla_breach_so_far(conn, replace(ctx, config=_config(False))) is None
