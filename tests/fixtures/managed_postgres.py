"""Exercise a disposable PostgreSQL namespace through real CLI processes."""

from contextlib import contextmanager
from dataclasses import replace
from uuid import uuid4

import yaml
from sqlalchemy import text

from etl_craft.config import load_config
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.engine.connection import engine_db
from fixtures.auth import service_token
from fixtures.cli_project import CliProject
from fixtures.metadata import add_pipeline, add_task


@contextmanager
def managed_project(config, directory, monkeypatch, *, timeout=30):
    profile = replace(config.engine, schema="managed_" + uuid4().hex)
    fields = {
        "Jdbc_url": profile.jdbc_url,
        "Schema": profile.schema,
        "User": profile.user,
        "Auth_mode": str(profile.auth_mode),
        **{key.capitalize(): value for key, value in profile.extra.items() if key != "secret_var"},
        "Secret": profile.secret_var,
    }
    if profile.listen_jdbc_url:
        fields["Listen_jdbc_url"] = profile.listen_jdbc_url
    raw = {
        "Secrets": {"Source_type": str(config.source.type)},
        "Orchestration": {"Project": "default", "Local_python_slots": 1, "Log_dir": "logs"},
        "Engine": {"dev": fields},
    }
    if config.source.path:
        raw["Secrets"]["Path"] = config.source.path
    path = directory / "craft-connector.yml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    isolated = load_config(path)
    engine = engine_db(isolated)
    project = CliProject(engine, isolated, 0, 0, [])
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{profile.schema}"'))
    try:
        code, output = project.run("init-db", timeout=timeout)
        assert code == 0, output
        monkeypatch.setenv("ETL_CRAFT_TOKEN", service_token(engine))
        code, output = project.run("migrate", timeout=timeout)
        assert code == 0, output
        isolated.python_scripts_dir.mkdir()
        (isolated.python_scripts_dir / "load.py").write_text(
            "from etl_craft.scripting import ScriptResult\n"
            "def run(task):\n    return ScriptResult(1)\n"
        )
        with acting_as(SYSTEM_ACTOR), engine.begin() as conn:
            project.pipeline_id = add_pipeline(conn, "P")
            project.task_id = add_task(
                conn, project.pipeline_id, "load", "PYTHON", SCRIPT_FILE="python_scripts/load.py"
            )
        yield project
    finally:
        for command in reversed(project.processes):
            command.close()
        with engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{profile.schema}" CASCADE'))
        engine.dispose()


def run_and_recover(project, *, timeout=30):
    """A dead owner leaves history, and a committed child outcome survives process loss."""
    killed = project.start(
        "run", "--pipeline-code", "P", fault="connections.after_reservation:kill"
    )
    assert killed.wait() == 137, killed.output
    code, output = project.run(
        "run", "--pipeline-code", "P", fault="child.after_outcome:kill", timeout=timeout
    )
    assert code == 0, output
    with project.engine.connect() as conn:
        row = conn.execute(
            text("SELECT COUNT(*) AS attempts FROM AUD_TASK_ATTEMPTS WHERE STATUS='SUCCESS'")
        ).one()
        assert row.attempts == 1
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS retained FROM AUD_CONNECTION_RESERVATIONS "
                    "WHERE STOPPED_AT IS NULL"
                )
            ).scalar_one()
            == 1
        )
