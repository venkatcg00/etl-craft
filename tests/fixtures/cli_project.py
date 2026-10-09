"""Throwaway projects driven through the installed command line and real process trees."""

import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest
import yaml
from sqlalchemy import text

from etl_craft.config import load_config
from fixtures.metadata import add_pipeline, add_task


def descendants(pid):
    """Return Linux descendants from /proc; process-tree assertions require Linux."""
    if sys.platform != "linux":
        pytest.skip("process-tree inspection requires Linux /proc")
    parents = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            parents[int(entry.name)] = int(fields[1])
        except (OSError, IndexError, ValueError):
            continue
    found, pending = [], [pid]
    while pending:
        current = pending.pop()
        children = [child for child, parent in parents.items() if parent == current]
        found.extend(children)
        pending.extend(children)
    return found


@dataclass
class CliProcess:
    """A command whose stdout/stderr go to a file, with bounded waits and signal helpers."""

    process: subprocess.Popen
    log_path: Path
    observed: dict[int, str] = field(default_factory=dict)

    @property
    def output(self):
        return self.log_path.read_text(errors="replace")

    def signal(self, sig):
        self.process.send_signal(sig)

    def wait(self, timeout=30):
        try:
            return self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as error:
            raise AssertionError(
                f"CLI pid {self.process.pid} timed out; output:\n{self.output}"
            ) from error

    def descendants(self):
        """Remember descendant birth times so cleanup cannot signal a reused pid."""
        children = descendants(self.process.pid)
        for pid in children:
            try:
                fields = (
                    Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
                )
                self.observed[pid] = fields[19]
            except FileNotFoundError:
                continue
        return children

    def close(self):
        if self.process.poll() is None:
            if sys.platform == "linux":
                self.descendants()
            self.signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        for pid, started in self.observed.items():
            try:
                fields = (
                    Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
                )
                if fields[19] == started and fields[0] != "Z":
                    os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                continue
            except FileNotFoundError:
                continue


@dataclass
class CliProject:
    """A fresh Engine DB plus a Python task, configured through the real init-db command."""

    engine: object
    config: object
    pipeline_id: int
    task_id: int
    processes: list

    def start(self, *arguments, fault=None):
        log_path = self.config.project_dir / f"cli-{len(self.processes)}.log"
        env = {**os.environ, "ETL_CRAFT_CONFIG": str(self.config.config_path)}
        env.pop("ETL_CRAFT_FAULT", None)
        if fault is not None:
            env["ETL_CRAFT_FAULT"] = fault
        with log_path.open("wb") as output:
            process = subprocess.Popen(
                [sys.executable, "-m", "etl_craft", *arguments],
                cwd=self.config.project_dir,
                env=env,
                stdout=output,
                stderr=subprocess.STDOUT,
            )
        command = CliProcess(process, log_path)
        self.processes.append(command)
        return command

    def run(self, *arguments, fault=None):
        command = self.start(*arguments, fault=fault)
        return command.wait(), command.output

    def wait_for(self, sql, *, expected, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql)).scalar_one_or_none()
            if result == expected:
                return result
            time.sleep(0.02)
        raise AssertionError(f"row state timed out: expected {expected!r}, found {result!r}")


@pytest.fixture
def cli_project(empty_engine_db, tmp_path):
    db = empty_engine_db
    profile = db.config.engine.active
    block = {
        "jdbc_url": profile.jdbc_url,
        "schema": "public" if profile.auth_mode != "none" else "main",
    }
    if profile.auth_mode != "none":
        block |= {
            "auth_mode": profile.auth_mode,
            "user": profile.user,
            "secret": profile.extra["secret_var"],
        }
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local", "Log_dir": "logs", "Api_address": "127.0.0.1:0"},
        "Engine": {"dev": block},
        "Warehouse": {
            "Name": "duckdb",
            "dev": {"jdbc_url": "jdbc:duckdb:warehouse.duckdb", "schema": "main"},
        },
    }
    path = tmp_path / "craft-connector.yml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    config = load_config(path)
    project = CliProject(db.engine, config, 0, 0, [])
    code, output = project.run("init-db")
    assert code == 0, output
    config.ingestion_scripts_dir.mkdir()
    (config.ingestion_scripts_dir / "load.py").write_text(
        "from etl_craft.scripting import ScriptResult\ndef run(task):\n    return ScriptResult(1)\n"
    )
    with db.engine.begin() as conn:
        project.pipeline_id = add_pipeline(conn, "P")
        project.task_id = add_task(
            conn, project.pipeline_id, "load", "PYTHON", SCRIPT_NAME="load.py"
        )
    try:
        yield project
    finally:
        for command in reversed(project.processes):
            command.close()
