"""An SLA breach is recorded and reported once, even when the first write fails."""

import time

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from etl_craft.engine import transitions
from etl_craft.execution import sla
from fixtures.metadata import add_pipeline, start_run


def test_a_refused_breach_write_is_retried_and_the_lapse_reported_once(engine_db, monkeypatch):
    engine = engine_db.engine
    with engine.begin() as conn:
        run = start_run(conn, add_pipeline(conn, "P", sla_in_hours=1))
    real = transitions.mark_sla_breached
    calls = []

    def refused_once(conn, pipeline_run_id):
        calls.append(pipeline_run_id)
        if len(calls) == 1:
            raise OperationalError("UPDATE", {}, Exception("server closed the connection"))
        return real(conn, pipeline_run_id)

    monkeypatch.setattr(transitions, "mark_sla_breached", refused_once)
    monkeypatch.setattr(sla, "RETRY_SECONDS", 0.01)
    lapses = []
    with sla.SlaWatch(engine, "P", run, 0.0, lapses.append):
        deadline = time.monotonic() + 10
        while not lapses and time.monotonic() < deadline:
            time.sleep(0.01)
    assert [(lapse.pipeline_code, lapse.pipeline_run_id) for lapse in lapses] == [("P", run)]
    assert len(calls) == 2
    with engine.connect() as conn:
        status = conn.execute(
            text("SELECT SLA_STATUS AS sla FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:id"),
            {"id": run},
        ).scalar_one()
    assert status == "BREACHED"


def test_a_watch_that_ends_before_the_write_lands_reports_nothing(engine_db, monkeypatch):
    engine = engine_db.engine
    with engine.begin() as conn:
        run = start_run(conn, add_pipeline(conn, "P", sla_in_hours=1))
    calls = []

    def always_refused(conn, pipeline_run_id):
        calls.append(pipeline_run_id)
        raise OperationalError("UPDATE", {}, Exception("server closed the connection"))

    monkeypatch.setattr(transitions, "mark_sla_breached", always_refused)
    monkeypatch.setattr(sla, "RETRY_SECONDS", 0.01)
    lapses = []
    with sla.SlaWatch(engine, "P", run, 0.0, lapses.append):
        deadline = time.monotonic() + 10
        while len(calls) < 3 and time.monotonic() < deadline:
            time.sleep(0.01)
    assert lapses == []
    assert len(calls) >= 3
