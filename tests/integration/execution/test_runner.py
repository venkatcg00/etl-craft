"""``run --task_code`` end to end: real task processes against a real Engine DB, both dialects."""

import logging
from pathlib import Path

import pytest
import yaml
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from etl_craft.config import load_config
from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import HandlerError, MetadataError, RunRefusedError, RunStateError
from etl_craft.engine import runlog, transitions
from etl_craft.execution import runner
from etl_craft.execution.gates import CrossPipelineCheck
from etl_craft.execution.runner import ChildOptions, run_task
from fixtures.metadata import start_run

TESTS_DIR = Path(__file__).parents[2]
CHILD = ChildOptions(module="fixtures.task_child", log_level="DEBUG", kill_grace_seconds=2)


@pytest.fixture(autouse=True)
def child_can_import_fixtures(monkeypatch):
    monkeypatch.setenv("PYTHONPATH", str(TESTS_DIR))


@pytest.fixture(autouse=True)
def restore_logger():
    logger = logging.getLogger("etl_craft")
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


@pytest.fixture
def project(engine_db, tmp_path):
    """A craft-connector.yml for the test Engine DB, and a pipeline P with tasks."""
    profile = engine_db.config.engine
    schema = "public" if profile.jdbc_url.startswith("jdbc:postgresql") else "main"
    block = {"jdbc_url": profile.jdbc_url, "schema": schema}
    if profile.auth_mode != "none":
        block |= {
            "user": profile.user,
            "auth_mode": profile.auth_mode,
            "secret": profile.secret_var,
        }
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local", "Task_timeout_seconds": 60},
        "Engine": {"dev": block},
    }
    config_dir = (engine_db.config.config_path or tmp_path / "craft-connector.yml").parent
    path = config_dir / "craft-connector.yml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    config = load_config(path)
    engine = engine_db.engine
    ids = {}
    with engine.begin() as conn:
        ids["pipeline"] = conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
                "VALUES ('P', 'P', 'FULL') RETURNING PIPELINE_ID"
            )
        ).scalar_one()
        for code, behaviour in (
            ("ok", "succeed"),
            ("values", "variables"),
            ("fails", "fail"),
            ("raises", "raise"),
            ("exits", "exit"),
            ("killed", "kill"),
            ("slow", "sleep"),
            ("after_ok", "succeed"),
            ("on_failure", "succeed"),
        ):
            ids[code] = conn.execute(
                text(
                    "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
                    "VALUES (:code, 'ETL', :p, 'SQL') RETURNING TASK_ID"
                ),
                {"code": code, "p": ids["pipeline"]},
            ).scalar_one()
            conn.execute(
                text(
                    "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) "
                    "VALUES (:t, 'BEHAVIOUR', :b)"
                ),
                {"t": ids[code], "b": behaviour},
            )
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) "
                "VALUES (:t, 'TASK_TIMEOUT_SECONDS', '10')"
            ),
            {"t": ids["slow"]},
        )
        for task, upstream, kind in (
            ("after_ok", "ok", "SUCCESS"),
            ("on_failure", "ok", "FAILURE"),
        ):
            conn.execute(
                text(
                    "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_TASK_ID, "
                    "DEPENDENCY_TYPE) VALUES (:p, :t, :u, :k)"
                ),
                {"p": ids["pipeline"], "t": ids[task], "u": ids[upstream], "k": kind},
            )
        ids["run"] = start_run(conn, ids["pipeline"])
    return engine, config, ids


def row(engine, task_run_id):
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT STATUS AS status, ERROR_MESSAGE AS error_message, TASK_LOG AS task_log, "
                "SOURCE_COUNT AS source_count, TARGET_COUNT AS target_count, "
                "INSERT_COUNT AS insert_count, ATTEMPT_COUNT AS attempt_count "
                "FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID = :id"
            ),
            {"id": task_run_id},
        ).one()


def run(project, task, **kwargs):
    engine, config, _ = project
    return run_task(engine, config, "P", task, child=CHILD, **kwargs)


def attempt_log(project, task, attempt=1):
    _, config, ids = project
    return (config.log_dir / "P" / f"run-{ids['run']}" / f"{task}.attempt-{attempt}.log").read_text(
        encoding="utf-8"
    )


def test_a_task_succeeds_and_records_its_counts_and_output(project):
    engine, _, ids = project
    outcome = run(project, "ok")
    assert (outcome.status, outcome.message) == (RunStatus.SUCCESS, "ok: SUCCESS")
    recorded = row(engine, outcome.task_run_id)
    assert (
        recorded.status,
        recorded.source_count,
        recorded.target_count,
        recorded.insert_count,
    ) == (
        RunStatus.SUCCESS,
        10,
        9,
        7,
    )
    # TASK_LOG holds the reported counts, then the tail of everything the process wrote.
    assert recorded.task_log.startswith("SOURCE_COUNT = 10\nTARGET_COUNT = 9\nINSERT_COUNT = 7\n\n")
    assert "stdout from ok" in recorded.task_log
    assert "stderr from ok" in recorded.task_log
    log = attempt_log(project, "ok")
    # Every engine log record in the task process carries the run context.
    context = (
        f"[task_run_id={outcome.task_run_id} pipeline=P task=ok "
        f"pipeline_run_id={ids['run']} attempt=1]"
    )
    assert f"INFO etl_craft.handlers.fake {context}: fake handler doing succeed" in log
    assert f"INFO etl_craft.execution.child {context}: task succeeded: source 10" in log


def test_reported_values_are_listed_in_the_task_log(project):
    engine, _, _ = project
    outcome = run(project, "values")
    assert row(engine, outcome.task_run_id).task_log.startswith(
        "TARGET_COUNT = 5\nINGESTION_COUNT = 5\nOFFSET = 42"
    )


def test_a_handler_error_fails_the_task_with_its_message(project):
    engine, _, _ = project
    outcome = run(project, "fails")
    assert outcome.status == RunStatus.FAILED
    assert outcome.message == "fails: FAILED — the source file is missing"
    recorded = row(engine, outcome.task_run_id)
    assert recorded.error_message == "the source file is missing"
    assert "ERROR etl_craft.execution.child" in recorded.task_log
    assert "task failed: the source file is missing" in attempt_log(project, "fails")


def test_an_unexpected_error_fails_the_task_with_its_traceback_in_the_log(project):
    engine, _, _ = project
    outcome = run(project, "raises")
    recorded = row(engine, outcome.task_run_id)
    assert recorded.error_message == "ValueError: an unexpected value"
    assert "Traceback (most recent call last)" in recorded.task_log


@pytest.mark.parametrize(
    ("task", "reason"),
    [
        ("exits", "exited with code 3"),
        ("killed", "was killed by signal SIGKILL"),
        ("slow", "timed out after 10s and was killed"),
    ],
)
def test_a_task_process_that_ends_without_an_outcome_is_recorded_failed(project, task, reason):
    engine, _, _ = project
    outcome = run(project, task)
    recorded = row(engine, outcome.task_run_id)
    assert recorded.status == RunStatus.FAILED
    assert recorded.error_message.startswith(f"the task process {reason} before recording")
    assert f"stdout from {task}" in recorded.task_log


def test_a_retry_is_a_new_attempt_on_the_same_row_with_its_own_log(project):
    engine, _, ids = project
    first = run(project, "fails")
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = 'succeed' WHERE TASK_ID = :t"),
            {"t": ids["fails"]},
        )
    second = run(project, "fails")
    assert second.task_run_id == first.task_run_id
    recorded = row(engine, second.task_run_id)
    assert (recorded.status, recorded.attempt_count, recorded.error_message) == (
        RunStatus.SUCCESS,
        2,
        None,
    )
    assert "fake handler doing fail" in attempt_log(project, "fails", 1)
    assert "fake handler doing succeed" in attempt_log(project, "fails", 2)


def test_a_settled_task_is_not_run_again_unless_forced(project):
    engine, _, ids = project
    first = run(project, "ok")
    again = run(project, "ok")
    assert (again.status, again.message) == (
        RunStatus.SKIPPED,
        f"ok: already SUCCESS under pipeline_run_id={ids['run']}",
    )
    forced = run(project, "ok", force=True)
    assert forced.status == RunStatus.SUCCESS
    assert row(engine, first.task_run_id).attempt_count == 2


def test_a_task_waits_for_its_dependencies_and_is_skipped_when_they_can_never_be_met(project):
    engine, _, _ = project
    waiting = run(project, "after_ok")
    assert waiting.status == RunStatus.SKIPPED
    assert "its dependencies are not met yet" in waiting.message
    assert waiting.task_run_id is None
    run(project, "ok")
    assert run(project, "after_ok").status == RunStatus.SUCCESS
    # "ok" succeeded, so a FAILURE dependency on it can never be met: recorded SKIPPED.
    never = run(project, "on_failure")
    assert never.status == RunStatus.SKIPPED
    assert row(engine, never.task_run_id).status == RunStatus.SKIPPED
    assert "can never be met" in row(engine, never.task_run_id).error_message


def test_a_task_in_progress_is_not_started_twice(project):
    engine, _, ids = project
    with engine.begin() as conn:
        transitions.find_or_create_task_run(conn, ids["ok"], ids["run"])
    outcome = run(project, "ok")
    assert outcome.status == RunStatus.IN_PROGRESS
    assert "already IN-PROGRESS" in outcome.message


def test_errors_before_running(project):
    engine, config, ids = project
    with pytest.raises(MetadataError):
        run(project, "nope")
    remote = config.__class__(**{**config.__dict__, "mode": "remote"})
    with pytest.raises(RunRefusedError):
        run_task(engine, remote, "P", "ok", force=True, child=CHILD)
    with engine.begin() as conn:
        transitions.finalize_pipeline_run(conn, ids["run"], RunStatus.SUCCESS)
    with pytest.raises(RunStateError, match="matched 0 runs"):
        run(project, "ok")


def test_the_cross_pipeline_gate_decides_a_task_with_dependencies_elsewhere(project):
    engine, _, ids = project
    with engine.begin() as conn:
        other = conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
                "VALUES ('Q', 'Q', 'FULL') RETURNING PIPELINE_ID"
            )
        ).scalar_one()
        upstream = conn.execute(
            text(
                "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
                "VALUES ('publish', 'ETL', :q, 'SQL') RETURNING TASK_ID"
            ),
            {"q": other},
        ).scalar_one()
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, "
                "DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) VALUES (:p, :t, :q, :u, 'SUCCESS')"
            ),
            {"p": ids["pipeline"], "t": ids["values"], "q": other, "u": upstream},
        )
    # An unfinished admission leaves no task summary, so a later check can admit it.
    unchecked = run(
        project,
        "values",
        admission=CrossPipelineCheck(0, ("upstream X has not run",), definitive=False),
    )
    assert unchecked.status == RunStatus.SKIPPED and unchecked.task_run_id is None
    assert "not run" in unchecked.message
    refused = run(project, "values", admission=CrossPipelineCheck(0, ("upstream X has not run",)))
    assert row(engine, refused.task_run_id).error_message == "upstream X has not run"
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM AUD_TASK_RUN_LOG WHERE TASK_ID = :t"), {"t": ids["values"]})
    assert run(project, "values", admission=CrossPipelineCheck(1)).status == RunStatus.SUCCESS


def task_row_status(engine, task_id, run_id):
    with engine.connect() as conn:
        return runlog.fetch_task_run_status(conn, task_id, run_id)


def test_a_bad_time_limit_fails_before_the_task_is_bound(project):
    engine, _, ids = project
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) "
                "VALUES (:t, 'TASK_TIMEOUT_SECONDS', 'soon')"
            ),
            {"t": ids["ok"]},
        )
    with pytest.raises(HandlerError, match="TASK_TIMEOUT_SECONDS='soon' is not a whole number"):
        run(project, "ok")
    assert task_row_status(engine, ids["ok"], ids["run"]) is None


def test_a_task_process_that_cannot_start_is_recorded_failed(project, monkeypatch):
    engine, _, ids = project

    def no_process(*args, **kwargs):
        raise OSError(24, "Too many open files")

    monkeypatch.setattr(runner, "run_child", no_process)
    with pytest.raises(OSError):
        run(project, "ok")
    with engine.connect() as conn:
        found = runlog.fetch_task_run_result(conn, row_id(engine, ids["ok"], ids["run"]))
    assert found.status == "FAILED"
    assert (
        found.error_message
        == "could not run the task process: OSError: [Errno 24] Too many open files"
    )


def test_recording_an_outcome_survives_a_dropped_connection(project, monkeypatch):
    real = runlog.fetch_task_run_result
    calls = []

    def drops_once(conn, task_run_id):
        calls.append(task_run_id)
        if len(calls) == 1:
            raise OperationalError("SELECT", {}, Exception("server closed the connection"))
        return real(conn, task_run_id)

    monkeypatch.setattr(runner.runlog, "fetch_task_run_result", drops_once)
    outcome = run(project, "ok")
    assert outcome.status == RunStatus.SUCCESS and len(calls) == 2


def test_a_failed_task_is_never_rewritten_as_skipped(project):
    engine, _, ids = project
    failed = run(project, "fails")
    assert failed.status == RunStatus.FAILED
    kept = runner._record_skipped(engine, ids["fails"], ids["run"], "fails", "upstream failed")
    assert kept.status == RunStatus.FAILED
    assert "not run again: upstream failed" in kept.message
    assert row(engine, failed.task_run_id).error_message == "the source file is missing"


def row_id(engine, task_id, run_id):
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT TASK_RUN_ID FROM AUD_TASK_RUN_LOG "
                "WHERE TASK_ID = :t AND PIPELINE_RUN_ID = :r"
            ),
            {"t": task_id, "r": run_id},
        ).scalar_one()
