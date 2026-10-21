"""Independent task execution through the installed CLI and real child processes."""

import json

import pytest
from sqlalchemy import text

from etl_craft.core.enums import RunStatus
from etl_craft.engine import runlog
from fixtures.metadata import add_dependency, add_task


@pytest.mark.parametrize("with_downstream", [False, True])
def test_standalone_cli_runs_only_selected_sql_and_preserves_exact_key(
    cli_project, with_downstream
):
    project = cli_project
    project.config.sql_scripts_dir.mkdir()
    (project.config.sql_scripts_dir / "one.sql").write_text("SELECT 1 AS id")
    with project.engine.begin() as conn:
        selected_id = add_task(
            conn,
            project.pipeline_id,
            "only",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="main.selected",
            SCRIPT_FILE="sql_scripts/one.sql",
        )
        downstream = add_task(
            conn,
            project.pipeline_id,
            "after",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="main.after",
            SCRIPT_FILE="sql_scripts/one.sql",
        )
        add_dependency(conn, project.pipeline_id, downstream, selected_id)
    args = (
        "run",
        "--pipeline-code",
        "P",
        "--task-code",
        "only",
        "--standalone",
        "--run-key",
        "operator:only",
        "--format",
        "json",
    )
    if with_downstream:
        args += ("--with-downstream",)
    code, output = project.run(*args)
    assert code == 0, output
    document = json.loads(output.splitlines()[-1])
    expected_ids = sorted([selected_id, downstream] if with_downstream else [selected_id])
    assert document["run"]["selected_task_ids"] == expected_ids
    assert document["run"]["exclusive_run"] is True
    with project.engine.connect() as conn:
        selected = runlog.select_run(
            conn, project.pipeline_id, runlog.RunSelector(run_key="operator:only")
        )
        assert selected.status == RunStatus.SUCCESS
        tasks = (
            conn.execute(
                text(
                    "SELECT t.TASK_CODE AS task_code FROM AUD_TASK_RUN_LOG r "
                    "JOIN CFG_TASKS t ON t.TASK_ID=r.TASK_ID WHERE r.PIPELINE_RUN_ID=:run_id"
                ),
                {"run_id": selected.pipeline_run_id},
            )
            .scalars()
            .all()
        )
        assert set(tasks) == ({"only", "after"} if with_downstream else {"only"})
    code, output = project.run(*args)
    assert code == 0 and "existing result is preserved" in output
    with project.engine.connect() as conn:
        attempts = conn.execute(
            text("SELECT COUNT(*) AS attempts FROM AUD_TASK_ATTEMPTS")
        ).scalar_one()
        assert attempts == (2 if with_downstream else 1)

    code, output = project.run(*args, "--run-date", "2020-01-01")
    assert code != 0 and "a run date cannot change" in output
