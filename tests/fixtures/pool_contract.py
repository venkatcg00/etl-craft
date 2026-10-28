"""The same admitted work and process evidence for local and API-only execution."""

import json
import signal
import threading
from dataclasses import asdict, dataclass
from pathlib import Path
from time import monotonic

import pytest
from sqlalchemy import text

from etl_craft.execution.attempt_types import TaskRun
from etl_craft.execution.pools.local import LocalPool
from etl_craft.execution.pools.queue import QueuePool
from etl_craft.execution.runner import admit_attempt
from fixtures.cluster import publish_project


def until(predicate, *, timeout=20):
    deadline = monotonic() + timeout
    while True:
        result = predicate()
        if result:
            return result
        assert monotonic() < deadline, "pool contract condition did not become true"
        threading.Event().wait(0.02)


@dataclass
class Provider:
    project: object
    pool: object
    directory: Path
    monkeypatch: object
    url: str | None = None
    token: object = None
    agent: object = None

    @property
    def queued(self):
        return self.pool.name == "queue"

    @property
    def evidence(self):
        path = self.project.config.project_dir / "executions"
        return path.read_text().splitlines() if path.exists() else []

    def prepare(self):
        root = self.project.config.project_dir
        (self.directory / "python_scripts" / "load.py").write_text(
            "from pathlib import Path\nimport os,threading\n"
            "from etl_craft.scripting import ScriptResult\n"
            "def run(task):\n"
            f"    with Path({str(root / 'executions')!r}).open('a') as log:\n"
            "        log.write(task.task_code + ':' + str(os.getpid()) + '\\n')\n"
            f"    while not Path({str(root / 'release')!r}).exists():\n"
            "        threading.Event().wait(0.02)\n"
            "    return ScriptResult(1)\n"
        )
        if self.queued:
            publish_project(self.project, self.directory)
        else:
            code, output = self.project.run("pipelines", "refresh")
            assert code == 0, output

    def admit(self):
        code, output = self.project.run(
            "run", "--pipeline-code", "P", "--init-only", "--run-key", "contract"
        )
        assert code == 0, output
        with self.project.engine.connect() as conn:
            run = conn.execute(
                text(
                    "SELECT PIPELINE_RUN_ID AS run_id FROM AUD_PIPELINES_RUN_LOG "
                    "WHERE RUN_KEY='contract'"
                )
            ).scalar_one()
        return admit_attempt(
            self.project.engine,
            self.project.config,
            TaskRun("P", "load", self.project.task_id, run, False),
        )

    def start_agent(self):
        assert self.queued
        with self.monkeypatch.context() as patch:
            patch.setenv("ETL_CRAFT_TOKEN", self.token.token)
            self.agent = self.project.start(
                "agent",
                "--api",
                self.url,
                "--project-dir",
                str(self.directory),
                "--drain-seconds",
                "0",
            )
        return self.agent

    def submit(self, spec):
        handle = self.pool.submit(spec)
        if self.queued and self.agent is None:
            self.start_agent()
        until(lambda: bool(self.evidence))
        return handle

    def release(self):
        (self.project.config.project_dir / "release").touch()

    def row(self, attempt_id):
        with self.project.engine.connect() as conn:
            return conn.execute(
                text(
                    "SELECT a.ATTEMPT_ID AS attempt_id,a.STATUS AS status,a.PID AS pid,"
                    "a.PROCESS_START AS birth,a.OWNER_ID AS owner_id,a.TASK_RUN_ID AS task_run_id,"
                    "r.PIPELINE_RUN_ID AS pipeline_run_id,"
                    "a.CANCEL_ACKNOWLEDGED_AT AS acknowledged,a.ERROR_MESSAGE AS reason "
                    "FROM AUD_TASK_ATTEMPTS a JOIN AUD_TASK_RUN_LOG r "
                    "ON r.TASK_RUN_ID=a.TASK_RUN_ID "
                    "WHERE a.ATTEMPT_ID=:id"
                ),
                {"id": attempt_id},
            ).one()

    def start_local_worker(self, spec, *, fault=None):
        root = self.project.config.project_dir
        (root / "pool_worker.py").write_text(Path(__file__).with_name("pool_worker.py").read_text())
        description = root / "attempt.json"
        description.write_text(json.dumps(asdict(spec)))
        return self.project.start(str(description), fault=fault, module="pool_worker")

    def expire(self, spec):
        from datetime import UTC, datetime, timedelta

        from etl_craft.execution.reconcile import reconcile

        with self.project.engine.begin() as conn:
            past = datetime.now(UTC) - timedelta(seconds=61)
            conn.execute(
                text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:past WHERE ATTEMPT_ID=:id"),
                {"past": past, "id": spec.attempt_id},
            )
            conn.execute(
                text(
                    "UPDATE AUD_PIPELINES_RUN_LOG SET LEASE_EXPIRES_AT=:past "
                    "WHERE PIPELINE_RUN_ID=:id"
                ),
                {"past": past, "id": spec.pipeline_run_id},
            )
            if self.queued:
                conn.execute(text("UPDATE AUD_AGENTS SET HEARTBEAT_AT=:past"), {"past": past})
        return reconcile(self.project.engine)

    def retry(self, spec):
        return admit_attempt(
            self.project.engine,
            self.project.config,
            TaskRun("P", "load", self.project.task_id, spec.pipeline_run_id, False),
            rerun=True,
        )

    def register(self, *, name=None):
        import httpx2 as httpx

        from etl_craft import __version__
        from etl_craft.core.actor import SYSTEM_ACTOR
        from etl_craft.core.enums import ActionName, ApiRole
        from etl_craft.services.operations import OperationContext
        from etl_craft.services.operations.actions import invoke
        from etl_craft.services.operations.identity import authenticated_context
        from etl_craft.services.operations.serialization import encode
        from etl_craft.services.operations.tokens import TokenCreate, authenticate
        from fixtures.agents import resources

        admin = OperationContext(
            self.project.engine, self.project.config, SYSTEM_ACTOR, role=ApiRole.ADMIN
        )
        token = (
            invoke(
                admin,
                ActionName.TOKEN_CREATE,
                TokenCreate(name=name, role=ApiRole.AGENT, pool="default"),
            )
            if name
            else self.token
        )
        headers = {"Authorization": f"Bearer {token.token}"}
        response = httpx.post(
            self.url + "/api/v1/agent/register",
            headers=headers,
            json={
                "version": __version__,
                "pool": "default",
                "resources": encode(resources(node=name or "manual-agent")),
            },
        )
        assert response.status_code == 200, response.text
        return authenticated_context(admin, authenticate(admin, token.token)), headers

    def close(self):
        self.release()
        self.pool.close()
        if self.agent is not None and self.agent.process.poll() is None:
            from etl_craft.core.actor import SYSTEM_ACTOR
            from etl_craft.core.enums import ActionName, AgentCommand, ApiRole
            from etl_craft.core.errors import RunRefusedError
            from etl_craft.services.operations import OperationContext
            from etl_craft.services.operations.actions import invoke
            from etl_craft.services.operations.agent_models import AgentControl

            ctx = OperationContext(
                self.project.engine, self.project.config, SYSTEM_ACTOR, role=ApiRole.ADMIN
            )
            try:
                invoke(
                    ctx,
                    ActionName.AGENT_CONTROL,
                    AgentControl(agent_id=self.token.metadata.token_id, command=AgentCommand.STOP),
                )
            except RunRefusedError:
                self.agent.signal(signal.SIGTERM)
            assert self.agent.wait(timeout=30) == 0, self.agent.output


@pytest.fixture
def queue_provider(cluster, monkeypatch):
    project, url, token, directory = cluster
    scene = Provider(
        project, QueuePool(project.engine, project.config), directory, monkeypatch, url, token
    )
    scene.prepare()
    try:
        yield scene
    finally:
        scene.close()


@pytest.fixture(params=["local", "queue"])
def provider(request, cli_project, monkeypatch):
    if request.param == "queue":
        yield request.getfixturevalue("queue_provider")
        return
    scene = Provider(
        cli_project,
        LocalPool(cli_project.engine, cli_project.config),
        cli_project.config.project_dir,
        monkeypatch,
    )
    scene.prepare()
    try:
        yield scene
    finally:
        scene.close()
