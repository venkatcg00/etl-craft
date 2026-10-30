"""The pipeline run log is the audit rows of one run, one timestamped line each, in time order."""

import json
from datetime import datetime

import pytest
from sqlalchemy import text

from etl_craft.core.actor import acting_as
from etl_craft.core.enums import ActionName, ApiRole, RunStatus
from etl_craft.core.errors import ResourceNotFoundError
from etl_craft.core.time import as_utc
from etl_craft.engine import transitions
from etl_craft.services.operations.identity import authenticated_context
from fixtures.actions import act
from fixtures.run_scene import OWNER, seed


@pytest.fixture
def ran(engine_db):
    return seed(engine_db.engine, engine_db.config)


def run_log(ctx, **selector):
    return b"".join(act(ctx, ActionName.RUN_LOG, pipeline_code="ORDERS", **selector)).decode()


def fields(line):
    at, code, _run, task, actor = line.split()[:5]
    return datetime.fromisoformat(at.replace("Z", "+00:00")), code, task, actor


def test_run_log_lists_each_audited_event_in_time_order(ran, engine_db):
    ctx, run, attempts = ran
    log = run_log(ctx, run_id=run).splitlines()
    parsed = [fields(line) for line in log]
    assert [at for at, *_ in parsed] == sorted(at for at, *_ in parsed)
    assert parsed[0][1] == "RUN_STARTED"
    assert [code for _, code, task, _ in parsed if task == "task=EXTRACT"] == [
        "TASK_QUEUED",
        "TASK_ADMITTED",
        "TASK_STARTED",
        "TASK_ENDED",
        "TASK_RETRY_DUE",
        "TASK_QUEUED",
        "TASK_ADMITTED",
        "TASK_STARTED",
        "TASK_ENDED",
    ]
    failure = next(line for line in log if "status=FAILED" in line)
    assert 'error="connection reset by source"' in failure and "exit=1" in failure
    started = next(line for line in log if " TASK_STARTED " in line)
    assert "attempt=1 host=worker-1 pid=41" in started
    held = next(line for line in log if " CONTROL_REQUESTED " in line)
    assert all(part in held for part in ("task=LOAD", "action=HOLD", "actor=user:owner"))
    assert 'reason="check the load"' in held
    with engine_db.engine.connect() as conn:
        audited = conn.execute(
            text(
                "SELECT QUEUED_AT AS queued, STARTED_AT AS started, ENDED_AT AS ended "
                "FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID IN (:a, :b)"
            ),
            {"a": attempts[0], "b": attempts[1]},
        ).all()
        breached = conn.execute(
            text(
                "SELECT SLA_BREACHED_AT AS at FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:r"
            ),
            {"r": run},
        ).scalar_one()
    times = {at for at, *_ in parsed}
    for row in audited:
        assert {as_utc(row.queued), as_utc(row.started), as_utc(row.ended)} <= times
    assert (as_utc(breached), "SLA_BREACHED") in {(at, code) for at, code, *_ in parsed}


def test_run_key_selects_the_same_run_and_an_ended_run_reports_its_outcome(ran, engine_db):
    ctx, run, _ = ran
    with engine_db.engine.connect() as conn:
        key = conn.execute(
            text("SELECT RUN_KEY AS k FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:r"),
            {"r": run},
        ).scalar_one()
    assert run_log(ctx, run_key=key) == run_log(ctx, run_id=run)
    with acting_as(OWNER), engine_db.engine.begin() as conn:
        transitions.finalize_pipeline_run(conn, run, RunStatus.FAILED, sla_in_hours=1)
    last = run_log(ctx, run_id=run).splitlines()[-1]
    assert last.split()[1] == "RUN_ENDED"
    assert "status=FAILED" in last and "tasks=SUCCESS:1" in last and "sla=BREACHED" in last


def test_a_viewer_without_the_pipeline_cannot_read_its_run_log(ran, engine_db):
    ctx, run, _ = ran
    token = act(ctx, ActionName.TOKEN_CREATE, name="reader", role=ApiRole.VIEWER)
    reader = authenticated_context(ctx, token.metadata)
    assert run_log(reader, run_id=run)
    with acting_as(OWNER), engine_db.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_GRANTS (PIPELINE_CODE,GRANTEE_ROLE,PERMISSION) "
                "VALUES ('ORDERS','operator','CAN_VIEW')"
            )
        )
    with pytest.raises(ResourceNotFoundError, match=r"ORDERS.*missing or not permitted"):
        run_log(reader, run_id=run)


def test_cli_and_api_print_the_same_run_log(metadata_project, monkeypatch, capsys):
    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app
    from etl_craft.cli import main

    p = metadata_project
    ctx, run, _ = seed(p.engine, p.config)
    expected = run_log(ctx, run_id=run)
    token = act(ctx, ActionName.TOKEN_CREATE, name="reader", role=ApiRole.VIEWER).token
    monkeypatch.chdir(p.root)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", token)
    assert main(["logs", "--pipeline-code", "ORDERS", "--run-id", str(run)]) == 0
    assert capsys.readouterr().out == expected
    assert main(["logs"]) != 0
    assert "give --attempt-id" in capsys.readouterr().err
    response = TestClient(create_app(ctx)).get(
        "/api/v1/projects/default/run-log",
        params={"pipeline_code": "ORDERS", "run_id": run},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200 and response.text == expected
    assert json.dumps(response.headers.get("content-type", "")).startswith('"text/plain')
