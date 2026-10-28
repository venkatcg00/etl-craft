"""Published task files survive working-tree edits and use host-local connection paths."""

import duckdb
import yaml
from sqlalchemy import text

from etl_craft.config import load_config
from fixtures.metadata import add_task


def test_sql_run_uses_admitted_files_and_the_next_publication_uses_edits(cli_project):
    project = cli_project
    raw = yaml.safe_load(project.config.config_path.read_text())
    raw["Artifacts"] = {"Store": (project.config.project_dir / "artifacts").as_uri()}
    project.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    project.config = load_config(project.config.config_path)
    scripts = project.config.sql_scripts_dir
    scripts.mkdir()
    script = scripts / "published.sql"
    script.write_text("SELECT 17 AS id")
    with project.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASKS SET ACTIVE_FLAG='N' WHERE TASK_ID=:id"), {"id": project.task_id}
        )
        add_task(
            conn,
            project.pipeline_id,
            "published",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="main.bundle_output",
            SCRIPT_FILE="sql_scripts/published.sql",
        )
    code, output = project.run("bundle", "publish")
    assert code == 0, output
    code, output = project.run("pipelines", "refresh")
    assert code == 0, output
    code, output = project.run("run", "--pipeline-code", "P", "--init-only")
    assert code == 0, output
    script.unlink()
    code, output = project.run("run", "--pipeline-code", "P")
    assert code == 0, output
    with duckdb.connect(str(project.config.project_dir / "warehouse.duckdb")) as conn:
        assert conn.execute("SELECT id FROM bundle_output").to_arrow_table().to_pylist() == [
            {"id": 17}
        ]
    script.write_text("SELECT 29 AS id")
    code, output = project.run("bundle", "publish")
    assert code == 0, output
    code, output = project.run("pipelines", "refresh")
    assert code == 0, output
    code, output = project.run("run", "--pipeline-code", "P")
    assert code == 0, output
    with duckdb.connect(str(project.config.project_dir / "warehouse.duckdb")) as conn:
        assert conn.execute("SELECT id FROM bundle_output").to_arrow_table().to_pylist() == [
            {"id": 29}
        ]
    with project.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT BUNDLE_ID AS bundle_id, STATUS AS status FROM AUD_PIPELINES_RUN_LOG "
                "ORDER BY PIPELINE_RUN_ID"
            )
        ).all()
    assert len(rows) == 2
    assert rows[0].bundle_id != rows[1].bundle_id
    assert all(row.status == "SUCCESS" for row in rows)


def test_recovery_keeps_successes_and_admitted_files_after_definition_edits(cli_project):
    project = cli_project
    raw = yaml.safe_load(project.config.config_path.read_text())
    raw["Artifacts"] = {"Store": (project.config.project_dir / "artifacts").as_uri()}
    project.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    project.config = load_config(project.config.config_path)
    completed = project.config.project_dir / "completed"
    attempts = project.config.project_dir / "attempts"
    scripts = project.config.python_scripts_dir
    (scripts / "load.py").write_text(
        "from pathlib import Path\n"
        "from etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        f"    Path({str(completed)!r}).write_text('once')\n"
        "    return ScriptResult(1)\n"
    )
    retry = scripts / "retry.py"
    retry.write_text(
        "from pathlib import Path\n"
        "from etl_craft.core.errors import HandlerError\n"
        "from etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        f"    path = Path({str(attempts)!r})\n"
        "    previous = path.read_text() if path.exists() else ''\n"
        "    path.write_text(previous + 'admitted\\n')\n"
        "    if not previous:\n"
        "        raise HandlerError('first attempt fails before recovery')\n"
        "    return ScriptResult(1)\n"
    )
    from fixtures.metadata import add_dependency

    with project.engine.begin() as conn:
        second = add_task(
            conn, project.pipeline_id, "retry", "PYTHON", SCRIPT_FILE="python_scripts/retry.py"
        )
        add_dependency(conn, project.pipeline_id, second, project.task_id)
    code, output = project.run("bundle", "publish")
    assert code == 0, output
    code, output = project.run("pipelines", "refresh")
    assert code == 0, output
    code, output = project.run("run", "--pipeline-code", "P")
    assert code != 0, output
    assert completed.read_text() == "once"
    completed.write_text("must not rerun")
    retry.write_text(
        "def run(task):\n    raise AssertionError('changed source must not execute')\n"
    )
    with project.engine.begin() as conn:
        run = conn.execute(
            text("SELECT PIPELINE_RUN_ID AS run_id FROM AUD_PIPELINES_RUN_LOG")
        ).scalar_one()
        conn.execute(text("UPDATE CFG_TASKS SET ACTIVE_FLAG='N'"))
        add_task(conn, project.pipeline_id, "new", "PYTHON", SCRIPT_FILE="python_scripts/retry.py")
    code, output = project.run("bundle", "publish")
    assert code == 0, output
    code, output = project.run("pipelines", "refresh")
    assert code == 0, output
    code, output = project.run(
        "run", "--pipeline-code", "P", "--run-id", str(run), "--recover", "--reason", "repair"
    )
    assert code == 0, output
    code, output = project.run("run", "--pipeline-code", "P", "--run-id", str(run))
    assert code == 0, output
    assert completed.read_text() == "must not rerun"
    assert attempts.read_text().splitlines() == ["admitted", "admitted"]
    with project.engine.connect() as conn:
        tasks = conn.execute(
            text(
                "SELECT TASK_ID AS task_id,STATUS AS status,ATTEMPT_COUNT AS attempts "
                "FROM AUD_TASK_RUN_LOG WHERE PIPELINE_RUN_ID=:run"
            ),
            {"run": run},
        ).all()
    assert {row.task_id: (row.status, row.attempts) for row in tasks} == {
        project.task_id: ("SUCCESS", 1),
        second: ("SUCCESS", 2),
    }
