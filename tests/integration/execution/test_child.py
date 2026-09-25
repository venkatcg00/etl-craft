"""The task process entry point, called in-process, and ``etl-craft run`` on the command line."""

import logging

import pytest
import yaml
from sqlalchemy import text

from etl_craft.cli import main as cli_main
from etl_craft.config import load_config
from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import ExitCode, HandlerError
from etl_craft.engine import runlog
from etl_craft.execution import child
from etl_craft.execution.context import build_task_context
from etl_craft.handlers import registry
from etl_craft.handlers.registry import HandlerResult


@pytest.fixture(autouse=True)
def restore_logger():
    logger = logging.getLogger("etl_craft")
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


@pytest.fixture
def bound(engine_db, tmp_path):
    """A config file, a task T with parameter X=1, and its row bound under a run."""
    profile = engine_db.config.engine.active
    block = {"jdbc_url": profile.jdbc_url}
    if profile.auth_mode != "none":
        block |= {"user": profile.user, "auth_mode": "password", "secret": profile.secret_var}
    config_path = (engine_db.config.config_path or tmp_path / "craft-connector.yml").parent / (
        "craft-connector.yml"
    )
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local"},
        "Engine": {"dev": block},
    }
    config_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
                "VALUES ('P', 'P', 'INCREMENTAL') RETURNING PIPELINE_ID"
            )
        ).scalar_one()
        task = conn.execute(
            text(
                "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
                "VALUES ('T', 'ETL', :p, 'SQL') RETURNING TASK_ID"
            ),
            {"p": pipeline},
        ).scalar_one()
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) "
                "VALUES (:t, 'X', '1')"
            ),
            {"t": task},
        )
        run_id = runlog.find_or_create_active_run(conn, pipeline)
        binding = runlog.find_or_create_task_run(conn, task, run_id)
    return engine, config_path, binding.task_run_id, run_id


def status(engine, task_run_id):
    with engine.connect() as conn:
        return runlog.fetch_task_run_result(conn, task_run_id)


def install(monkeypatch, function):
    monkeypatch.setitem(registry.HANDLERS, "SQL", f"{__name__}:{function.__name__}")


seen = []


def succeeds(context, engine):
    seen.append(context)
    return HandlerResult(source_count=3, target_count=3, insert_count=3)


def fails(context, engine):
    raise HandlerError("nothing to load")


def breaks(context, engine):
    raise KeyError("missing")


def test_the_context_is_rebuilt_from_the_task_run(bound):
    engine, config_path, task_run_id, run_id = bound
    context = build_task_context(engine, load_config(config_path), task_run_id, force=True)
    assert (context.pipeline_code, context.task_code, context.handler) == ("P", "T", "SQL")
    assert (context.pipeline_run_id, context.task_run_id, context.attempt) == (
        run_id,
        task_run_id,
        1,
    )
    assert (context.refresh_type, dict(context.task_params), context.force) == (
        "INCREMENTAL",
        {"X": "1"},
        True,
    )


def args(config_path, task_run_id):
    return ["--config", str(config_path), "--task-run-id", str(task_run_id)]


def test_a_successful_handler_records_success(bound, monkeypatch):
    engine, config_path, task_run_id, _ = bound
    install(monkeypatch, succeeds)
    assert child.main(args(config_path, task_run_id)) == ExitCode.SUCCESS
    assert status(engine, task_run_id).status == RunStatus.SUCCESS


@pytest.mark.parametrize(
    ("function", "exit_code", "message"),
    [
        (fails, ExitCode.HANDLER, "nothing to load"),
        (breaks, ExitCode.UNEXPECTED, "KeyError: 'missing'"),
    ],
)
def test_a_failing_handler_records_failed(bound, monkeypatch, function, exit_code, message):
    engine, config_path, task_run_id, _ = bound
    install(monkeypatch, function)
    assert child.main(args(config_path, task_run_id)) == exit_code
    assert status(engine, task_run_id) == runlog.TaskRunResult(RunStatus.FAILED, message, 1)


def test_an_unknown_task_run(bound):
    _, config_path, _, _ = bound
    assert child.main(args(config_path, 999999)) == ExitCode.RUN_STATE


def test_the_command_line_runs_a_task_with_no_handler_installed(bound, capsys, monkeypatch):
    engine, config_path, task_run_id, _ = bound
    monkeypatch.chdir(config_path.parent)
    # An earlier attempt failed, so this run is a retry of the same row.
    with engine.begin() as conn:
        runlog.finish_task_run(conn, task_run_id, status=RunStatus.FAILED)
    code = cli_main(["run", "--pipeline_code", "P", "--task_code", "T"])
    assert code == ExitCode.FAILURE
    assert capsys.readouterr().out == ("T: FAILED — no handler is installed for HANDLER 'SQL'\n")
    assert status(engine, task_run_id).error_message == "no handler is installed for HANDLER 'SQL'"
    # A settled task is skipped and exits 0.
    with engine.begin() as conn:
        runlog.finish_task_run(conn, task_run_id, status=RunStatus.SUCCESS)
    assert cli_main(["run", "--pipeline_code", "P", "--task_code", "T"]) == ExitCode.SUCCESS
