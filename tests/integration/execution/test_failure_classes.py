"""Real script attempts preserve classification and the admission policy."""

from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.core.actor import current_actor
from etl_craft.core.enums import ApiRole, FailureClass, RetryOn, RunStatus
from etl_craft.engine.repository.catalog import fetch_task_runs
from etl_craft.engine.runlog import RunSelector
from etl_craft.execution.pipeline import run_pipeline
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.status import explain_task, pipeline_status
from fixtures.metadata import add_pipeline, add_task
from integration.execution.test_pipeline import config as config


@pytest.mark.parametrize(
    "exception,policy,count,status,kind",
    [
        ("TransientError", RetryOn.TRANSIENT, 2, RunStatus.SUCCESS, FailureClass.TRANSIENT),
        ("PermanentError", RetryOn.TRANSIENT, 1, RunStatus.FAILED, FailureClass.PERMANENT),
        ("TransientError", RetryOn.NONE, 1, RunStatus.FAILED, FailureClass.TRANSIENT),
        ("PermanentError", RetryOn.ANY, 2, RunStatus.SUCCESS, FailureClass.PERMANENT),
        ("LookupError", RetryOn.TRANSIENT_AND_UNKNOWN, 2, RunStatus.SUCCESS, FailureClass.UNKNOWN),
        ("LookupError", RetryOn.TRANSIENT, 1, RunStatus.FAILED, FailureClass.UNKNOWN),
    ],
)
def test_script_failure_policy_and_historical_explanation(
    engine_db, config, exception, policy, count, status, kind
):
    project = config.project_dir
    (project / "ingestion_scripts").mkdir(exist_ok=True)
    (project / "ingestion_scripts" / "load.py").write_text(
        "from pathlib import Path\n"
        "from etl_craft.scripting import ScriptResult, TransientError, PermanentError\n"
        "def run():\n"
        "    flag = Path(__file__).with_suffix('.seen')\n"
        "    if not flag.exists():\n"
        "        flag.touch()\n"
        f"        raise {exception}('source unavailable')\n"
        "    return ScriptResult(row_count=1)\n"
    )
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task_id = add_task(
            conn,
            pipeline,
            "load",
            "PYTHON",
            SCRIPT_NAME="load.py",
            RETRIES="3",
            RETRY_DELAY_SECONDS="0",
            RETRY_ON=policy,
        )
    outcome = run_pipeline(engine, config, "P")
    assert outcome.status == status
    with engine.connect() as conn:
        attempts = conn.execute(
            text(
                "SELECT a.FAILURE_CLASS AS failure_class, a.FAILURE_SOURCE AS failure_source, "
                "a.FAILURE_CODE AS failure_code, a.RETRY_ON AS retry_on, a.RETRYABLE AS retryable "
                "FROM AUD_TASK_ATTEMPTS a JOIN AUD_TASK_RUN_LOG t ON t.TASK_RUN_ID=a.TASK_RUN_ID "
                "WHERE t.TASK_ID=:task ORDER BY a.ATTEMPT_NUMBER"
            ),
            {"task": task_id},
        ).all()
        history = fetch_task_runs(conn)[task_id][0]
    assert len(attempts) == count
    failed = attempts[0]
    assert failed.failure_class == kind
    assert failed.failure_source == exception
    assert failed.failure_code is None
    assert failed.retry_on == policy
    assert bool(failed.retryable) == (count > 1)
    ctx = OperationContext(
        engine, replace(config, retry_on=RetryOn.NONE), current_actor(), role=ApiRole.VIEWER
    )
    selector = RunSelector(run_id=outcome.pipeline_run_id)
    explanation = explain_task(ctx, "P", "load", selector=selector)
    reason = explanation.task.attempts[0].retry_reason
    assert f"{kind}: {exception}" in reason
    assert f"RETRY_ON={policy}" in reason
    assert reason in history.failure_reasons[0]
    status_view = pipeline_status(ctx, "P", selector=selector)
    assert status_view.tasks[0].explanation.task.attempts[0].retry_reason == reason


def test_validate_names_the_task_and_allowed_retry_policies(engine_db, config):
    from etl_craft.services.validate import validate

    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(conn, pipeline, "load", "PYTHON", RETRY_ON="sometimes")
    report = validate(engine_db.engine, config, "P")
    assert report.failed
    assert any(
        "RETRY_ON" in str(item) and "load" in str(item) and "TRANSIENT" in str(item)
        for item in report.findings
    )


@pytest.mark.parametrize("previous_policy", [None, "NONE"])
def test_admission_fills_only_an_unrecorded_queued_policy(engine_db, config, previous_policy):
    from etl_craft.engine import transitions
    from etl_craft.execution.runner import admit_attempt
    from fixtures.metadata import start_run

    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "load", "PYTHON", RETRY_ON="ANY")
        run = start_run(conn, pipeline)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        queued = transitions.queue_attempt(conn, summary, current_actor())
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET RETRY_ON=:policy WHERE ATTEMPT_ID=:id"),
            {"policy": previous_policy, "id": queued},
        )
    admitted = admit_attempt(engine, config, task, "load", "P", run, False)
    assert admitted.attempt_id == queued
    with engine.connect() as conn:
        policy = conn.execute(
            text("SELECT RETRY_ON AS retry_on FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"),
            {"id": queued},
        ).scalar_one()
    assert policy == ("ANY" if previous_policy is None else "NONE")
