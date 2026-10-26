"""A stopped PostgreSQL deployment restores and resumes the same interrupted work."""

import signal
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime, timedelta

import pytest
import yaml
from sqlalchemy import text

from etl_craft.config import load_config
from fixtures.engine_db import postgres_engine_db
from fixtures.metadata import add_dependency, add_task
from fixtures.services import POSTGRES_USER, REPO_ROOT, require

pytestmark = [
    pytest.mark.chaos,
    pytest.mark.skipif(
        sys.platform != "linux", reason="process fencing uses Linux birth identities"
    ),
    pytest.mark.parametrize(
        "empty_engine_db",
        [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
        indirect=True,
    ),
]


@pytest.fixture
def restored_database(monkeypatch):
    with postgres_engine_db(monkeypatch) as db:
        yield db


def test_dump_restore_recovers_exact_interrupted_run(cli_project, restored_database):
    require("postgres")
    p = cli_project
    prepare = p.config.python_scripts_dir / "prepare.py"
    prepare.write_text(
        "from pathlib import Path\nfrom etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        "    with Path('backup-prepared').open('a') as stream: stream.write('committed\\n')\n"
        "    return ScriptResult(1)\n"
    )
    with p.engine.begin() as conn:
        task = add_task(
            conn, p.pipeline_id, "prepare", "PYTHON", SCRIPT_FILE="python_scripts/prepare.py"
        )
        add_dependency(conn, p.pipeline_id, p.task_id, task, "SUCCESS")
    script = p.config.python_scripts_dir / "load.py"
    script.write_text(
        "from pathlib import Path\nfrom threading import Event\n"
        "from etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        "    marker=Path('backup-started')\n"
        "    if not marker.exists():\n"
        "        marker.write_text('started')\n"
        "        Event().wait(120)\n"
        "    with Path('backup-output').open('a') as stream: stream.write('one row\\n')\n"
        "    return ScriptResult(1)\n"
    )
    assert p.run("run", "--pipeline-code", "P", "--init-only", "--run-key", "restore")[0] == 0
    controller = p.start("controller", "--no-ui")
    p.wait_for("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS WHERE STATUS='RUNNING'", expected=1)
    # Poll a committed process boundary, then kill the controller and its observed descendants.
    deadline = time.monotonic() + 10
    while not (p.config.project_dir / "backup-started").exists():
        assert time.monotonic() < deadline, controller.output
        threading.Event().wait(0.02)
    controller.descendants()
    controller.signal(signal.SIGKILL)
    assert controller.wait() == -signal.SIGKILL
    controller.close()
    with p.engine.begin() as conn:
        expired = datetime.now(UTC) - timedelta(seconds=1)
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:at WHERE STATUS='RUNNING'"),
            {"at": expired},
        )
        conn.execute(
            text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET LEASE_EXPIRES_AT=:at WHERE STATUS='IN-PROGRESS'"
            ),
            {"at": expired},
        )
    source, target = p.engine.url.database, restored_database.engine.url.database
    base = ["docker", "compose", "exec", "-T", "postgres"]
    dump = subprocess.run(
        [*base, "pg_dump", "-U", POSTGRES_USER, "-Fc", "--schema=public", source],
        cwd=REPO_ROOT,
        capture_output=True,
        check=True,
    )
    restored = subprocess.run(
        [
            *base,
            "pg_restore",
            "-U",
            POSTGRES_USER,
            "--no-owner",
            "--no-acl",
            "--clean",
            "--if-exists",
            "-d",
            target,
        ],
        input=dump.stdout,
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
    )
    assert restored.returncode == 0, restored.stderr.decode()
    raw = yaml.safe_load(p.config.config_path.read_text())
    raw["Engine"]["dev"]["Jdbc_url"] = restored_database.config.engine.jdbc_url
    p.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    p.config = load_config(p.config.config_path)
    p.engine = restored_database.engine
    for command in ("doctor", "reconcile"):
        code, output = p.run(command)
        assert code == 0, output
    replacement = p.start("controller", "--no-ui")
    p.wait_for("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG", expected="SUCCESS", timeout=20)
    replacement.signal(signal.SIGTERM)
    assert replacement.wait() == 0, replacement.output
    with p.engine.connect() as conn:
        attempts = conn.execute(
            text(
                "SELECT a.STATUS AS status,a.TASK_RUN_ID AS task_run_id,a.TARGET_COUNT AS rows "
                "FROM AUD_TASK_ATTEMPTS a JOIN AUD_TASK_RUN_LOG t USING (TASK_RUN_ID) "
                "JOIN CFG_TASKS c USING (TASK_ID) WHERE c.TASK_CODE='load' "
                "ORDER BY ATTEMPT_NUMBER"
            )
        ).all()
        assert [row.status for row in attempts] == ["LOST", "SUCCESS"]
        assert len({row.task_run_id for row in attempts}) == 1
        assert next(row.rows for row in attempts if row.status == "SUCCESS") == 1
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG")).scalar_one() == 1
    assert (p.config.project_dir / "backup-output").read_text() == "one row\n"
    assert (p.config.project_dir / "backup-prepared").read_text() == "committed\n"
