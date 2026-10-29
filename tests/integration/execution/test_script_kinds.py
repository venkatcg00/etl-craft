"""Kinds preserve ingestion state and make utility counts and offsets honest."""

from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole, RunStatus
from etl_craft.engine.repository.offsets import StoredOffset, fetch_task_offset, save_task_offset
from etl_craft.execution.runner import run_task
from etl_craft.services.operations.context import OperationContext
from fixtures.actions import act
from fixtures.metadata import start_run
from integration.execution.test_python_scripts import project as project
from integration.execution.test_python_scripts import restore_logging as restore_logging
from integration.execution.test_python_scripts import task_row


def prepare(project, kind, source, *, admit=True):
    engine, config, pipeline, task = project
    (config.python_scripts_dir / "load.py").write_text(source, encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE=:kind "
                "WHERE TASK_ID=:task AND PARAMETER_NAME='SCRIPT_KIND'"
            ),
            {"kind": kind, "task": task},
        )
        save_task_offset(conn, task, StoredOffset("NUMBER", "42"))
        run = start_run(conn, pipeline) if admit else None
    return OperationContext(engine, config, SYSTEM_ACTOR, role=ApiRole.ADMIN), run


@pytest.mark.parametrize("project", [False], indirect=True)
@pytest.mark.parametrize("count", [None, 0, 3])
def test_utility_counts_are_nullable_and_stored_offsets_are_never_given(project, count):
    ctx, _ = prepare(
        project,
        "UTILITY",
        "from etl_craft.scripting import ScriptResult\n"
        f"def run(task):\n    assert task.offset is None\n    return ScriptResult({count!r})\n",
    )
    done = run_task(ctx.engine, ctx.config, "P", "load")
    assert done.status == RunStatus.SUCCESS, done.message
    row = task_row(ctx.engine, done.task_run_id)
    assert row.source == count and row.target == count and row.inserted == count
    with ctx.engine.connect() as conn:
        from etl_craft.engine.repository.catalog import fetch_task_runs
        from etl_craft.services.catalog.collect import run_kpis
        from etl_craft.services.catalog.metrics import counts

        summaries = fetch_task_runs(conn)[project[3]]
        assert run_kpis(summaries).average_rows == count
        if count is None:
            assert counts(summaries[0]) == ""
            measured = replace(summaries[0], target_count=8)
            assert run_kpis([*summaries, measured]).average_rows == 8
        else:
            assert f"target {count}" in counts(summaries[0])
        assert fetch_task_offset(conn, project[3]) == StoredOffset("NUMBER", "42")
        writes = conn.execute(
            text(
                "SELECT ROWS_WRITTEN AS rows_written FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID=:run"
            ),
            {"run": done.task_run_id},
        ).one()
        assert writes.rows_written == count
    explained = act(ctx, ActionName.EXPLAIN, pipeline_code="P", task_code="load")
    assert explained.retry_on == "NONE" and "default" in explained.retry_source
    if count is None:
        assert "SOURCE_COUNT = 0" not in (row.task_log or "")


@pytest.mark.parametrize("project", [False], indirect=True)
def test_utility_fan_out_counts_survive_storage_and_explain_reports_unmeasured_items(
    project, monkeypatch, capsys
):
    ctx, _ = prepare(
        project,
        "UTILITY",
        "from etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        "    assert task.offset is None\n"
        "    return task.fan_out(['a','b','c'], "
        "lambda key: ScriptResult({'a': 2, 'b': None, 'c': 0}[key]), max_items=3)\n",
    )
    done = run_task(ctx.engine, ctx.config, "P", "load")
    assert done.status == RunStatus.SUCCESS, done.message
    assert task_row(ctx.engine, done.task_run_id).target is None
    with ctx.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT ITEM_KEY AS key,ROW_COUNT AS count FROM AUD_TASK_ITEMS "
                "WHERE STATUS='SUCCESS' ORDER BY ITEM_KEY"
            )
        ).all()
        assert {row.key: row.count for row in rows} == {"a": 2, "b": None, "c": 0}
    explained = act(ctx, ActionName.EXPLAIN, pipeline_code="P", task_code="load")
    assert "Not measured: 1 of 3 items reported no count" in explained.task.item_progress
    assert "Not measured: 1 of 3 items reported no count" in explained.next_action
    metadata = act(ctx, ActionName.HISTORY, pipeline_code="P", task_code="load")
    assert "Not measured: 1 of 3 items reported no count" in metadata.entries[0].item_progress

    import json

    from etl_craft.cli import main
    from fixtures.auth import service_token

    monkeypatch.setenv("ETL_CRAFT_TOKEN", service_token(ctx.engine))
    args = ["status", "--config", str(ctx.config.config_path), "--pipeline-code", "P"]
    assert main(args) == 0
    assert "Not measured: 1 of 3 items reported no count" in capsys.readouterr().out
    assert main([*args, "--format", "json"]) == 0
    document = json.loads(capsys.readouterr().out)
    assert (
        "Not measured: 1 of 3 items reported no count"
        in document["tasks"][0]["explanation"]["task"]["item_progress"]
    )


@pytest.mark.parametrize("project", [False], indirect=True)
@pytest.mark.parametrize(
    "kind,result,parts",
    [
        ("UTILITY", "ScriptResult(offset=Offset.number(1))", ["UTILITY", "offset", "INGESTION"]),
        ("INGESTION", "ScriptResult()", ["row_count=None", "INGESTION", "return row_count"]),
    ],
)
def test_script_kind_contract_failures_are_recorded_in_attempt_and_log(
    project, kind, result, parts
):
    ctx, _ = prepare(
        project,
        kind,
        "from etl_craft.scripting import ScriptResult, Offset\n"
        f"def run(task):\n    return {result}\n",
    )
    done = run_task(ctx.engine, ctx.config, "P", "load")
    assert done.status == RunStatus.FAILED
    row = task_row(ctx.engine, done.task_run_id)
    for part in ["P.load", *parts]:
        assert part in row.error
    with ctx.engine.connect() as conn:
        attempt = conn.execute(
            text(
                "SELECT ERROR_MESSAGE AS message,LOG_PATH AS path FROM AUD_TASK_ATTEMPTS "
                "WHERE TASK_RUN_ID=:run"
            ),
            {"run": done.task_run_id},
        ).one()
        assert all(part in attempt.message for part in parts)
    from pathlib import Path

    assert all(part in Path(attempt.path).read_text(encoding="utf-8") for part in parts)


@pytest.mark.parametrize("project", [False], indirect=True)
def test_utility_offset_reset_refused_by_cli_and_api(project, monkeypatch, capsys):
    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app
    from etl_craft.cli import main
    from etl_craft.core.errors import UsageError
    from fixtures.auth import service_token

    ctx, _ = prepare(
        project,
        "UTILITY",
        "from etl_craft.scripting import ScriptResult\ndef run(task):\n    return ScriptResult()\n",
    )
    token = service_token(ctx.engine)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", token)
    arguments = [
        "mark",
        "--config",
        str(ctx.config.config_path),
        "--pipeline-code",
        "P",
        "--task-code",
        "load",
        "--offset",
        "99",
        "--reason",
        "replay",
    ]
    assert main(arguments) == UsageError.exit_code
    message = capsys.readouterr().err
    for part in ("P.load", "UTILITY", "no offset", "INGESTION"):
        assert part in message
    with TestClient(create_app(ctx)) as client:
        response = client.post(
            "/api/v1/projects/default/marks",
            headers={"Authorization": "Bearer " + token},
            json={"pipeline_code": "P", "task_code": "load", "offset": "99", "reason": "replay"},
        )
    assert response.status_code == 400
    for part in ("P.load", "UTILITY", "no offset", "INGESTION"):
        assert part in response.text
    with ctx.engine.connect() as conn:
        assert fetch_task_offset(conn, project[3]) == StoredOffset("NUMBER", "42")
        refusals = conn.execute(
            text(
                "SELECT OUTCOME AS outcome,EXIT_CODE AS code,ACTOR AS actor "
                "FROM AUD_ACTIONS WHERE COMMAND='mark'"
            )
        ).all()
        assert len(refusals) == 2
        assert all(row.code is None and row.outcome == "REQUESTED" for row in refusals)
        assert all(row.actor == "test-admin" for row in refusals)


@pytest.mark.parametrize("project", [False], indirect=True)
def test_utility_offsets_stay_absent_across_retry_and_resume(project):
    ctx, _ = prepare(
        project,
        "UTILITY",
        "from etl_craft.scripting import ScriptResult, TransientError\n"
        "def run(task):\n"
        "    assert task.offset is None\n"
        "    marker = task.config.host_dir / 'utility-attempt'\n"
        "    if not marker.exists():\n"
        "        marker.touch()\n"
        "        raise TransientError('external action failed; retry explicitly')\n"
        "    return ScriptResult()\n",
    )
    first = run_task(ctx.engine, ctx.config, "P", "load")
    assert first.status == RunStatus.FAILED
    second = run_task(ctx.engine, ctx.config, "P", "load")
    assert second.status == RunStatus.SUCCESS, second.message
    assert second.task_run_id == first.task_run_id
    assert task_row(ctx.engine, second.task_run_id).inserted is None
    with ctx.engine.connect() as conn:
        assert fetch_task_offset(conn, project[3]) == StoredOffset("NUMBER", "42")
        policies = (
            conn.execute(text("SELECT RETRY_ON AS policy FROM AUD_TASK_ATTEMPTS")).scalars().all()
        )
        assert policies == ["NONE", "NONE"]


@pytest.mark.parametrize("project", [False], indirect=True)
def test_standing_success_on_utility_has_unmeasured_counts_and_no_output(project):
    from etl_craft.core.enums import ControlAction
    from etl_craft.engine import standing_pauses

    ctx, run = prepare(
        project,
        "UTILITY",
        "def run(task):\n    raise AssertionError('must not execute')\n",
        admit=False,
    )
    act(
        ctx,
        ActionName.CONTROL,
        pipeline_code="P",
        task_code="load",
        action=ControlAction.PAUSE,
        outcome=RunStatus.SUCCESS,
        reason="external delivery was performed",
    )
    with ctx.engine.begin() as conn:
        pause = standing_pauses.current(conn, project[2])[0]
        run = start_run(conn, project[2])
        standing_pauses.apply(conn, pause, run)
        row = conn.execute(
            text(
                "SELECT SOURCE_COUNT AS source,TARGET_COUNT AS target,INSERT_COUNT AS inserted,"
                "ROWS_WRITTEN AS writes,ATTEMPT_COUNT AS attempts FROM AUD_TASK_RUN_LOG "
                "WHERE PIPELINE_RUN_ID=:run"
            ),
            {"run": run},
        ).one()
        assert (
            row.source is None
            and row.target is None
            and row.inserted is None
            and row.writes is None
        )
        assert row.attempts == 0
        assert (
            conn.execute(text("SELECT COUNT(*) AS count FROM AUD_TASK_ATTEMPTS")).scalar_one() == 0
        )


@pytest.mark.parametrize("project", [False], indirect=True)
@pytest.mark.parametrize("explicit", [False, True])
def test_utility_automatic_retries_require_an_explicit_policy(project, explicit):
    engine, _, _, task = project
    with engine.begin() as conn:
        params = {"RETRIES": "1", "RETRY_DELAY_SECONDS": "0"}
        if explicit:
            params["RETRY_ON"] = "ANY"
        for name, value in params.items():
            conn.execute(
                text(
                    "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID,PARAMETER_NAME,PARAMETER_VALUE) "
                    "VALUES (:task,:name,:value)"
                ),
                {"task": task, "name": name, "value": value},
            )
    ctx, _ = prepare(
        project,
        "UTILITY",
        "from etl_craft.scripting import ScriptResult, TransientError\n"
        "def run(task):\n"
        "    assert task.offset is None\n"
        "    marker = task.config.host_dir / 'retry-marker'\n"
        "    if not marker.exists():\n"
        "        marker.touch()\n"
        "        raise TransientError('utility side effect unavailable')\n"
        "    return ScriptResult()\n",
    )
    done = run_task(ctx.engine, ctx.config, "P", "load")
    assert done.status == (RunStatus.SUCCESS if explicit else RunStatus.FAILED), done.message
    with engine.connect() as conn:
        attempts = conn.execute(
            text("SELECT RETRY_ON AS policy FROM AUD_TASK_ATTEMPTS ORDER BY ATTEMPT_ID")
        ).all()
        assert len(attempts) == (2 if explicit else 1)
        assert all(row.policy == ("ANY" if explicit else "NONE") for row in attempts)
        assert fetch_task_offset(conn, task) == StoredOffset("NUMBER", "42")


@pytest.mark.parametrize("project", [False], indirect=True)
def test_explain_reports_the_policy_already_captured_on_a_queued_attempt(project):
    from etl_craft.engine import transitions

    ctx, run = prepare(
        project,
        "UTILITY",
        "from etl_craft.scripting import ScriptResult\ndef run(task):\n    return ScriptResult()\n",
    )
    with ctx.engine.begin() as conn:
        task = transitions.find_or_create_task_run(conn, project[3], run)
        attempt = transitions.queue_attempt(conn, task.task_run_id, SYSTEM_ACTOR)
        transitions.set_attempt_retry_policy(conn, attempt, "TRANSIENT")
    explained = act(ctx, ActionName.EXPLAIN, pipeline_code="P", task_code="load")
    assert explained.retry_on == "TRANSIENT"
    assert "default recorded on attempt" in explained.retry_source
    assert str(attempt) in explained.retry_source


@pytest.mark.parametrize("project", [False], indirect=True)
def test_script_cannot_change_its_admitted_kind_through_task_parameters(project):
    ctx, _ = prepare(
        project,
        "UTILITY",
        "from etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        "    try:\n"
        "        task.task_params['SCRIPT_KIND'] = 'INGESTION'\n"
        "    except TypeError:\n"
        "        pass\n"
        "    else:\n"
        "        raise AssertionError('admitted metadata was mutable')\n"
        "    assert task.offset is None\n"
        "    return ScriptResult()\n",
    )
    done = run_task(ctx.engine, ctx.config, "P", "load")
    assert done.status == RunStatus.SUCCESS, done.message
    assert task_row(ctx.engine, done.task_run_id).target is None
