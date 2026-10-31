"""Register, claim, supervise and drain whole task processes through the API alone."""

from __future__ import annotations

import codecs
import json
import logging
import os
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from pathlib import Path

from etl_craft import __version__
from etl_craft.agent.client import AgentClient
from etl_craft.agent.task_store import RemoteTaskStore
from etl_craft.config import ConnectorConfig
from etl_craft.config.project import project_config
from etl_craft.core.agent_resources import NO_DECLARATION, DeclaredCapacity, report
from etl_craft.core.enums import (
    ActionName,
    AgentCommand,
    AgentReport,
    AgentStatus,
    AttemptStatus,
    Handler,
    SlotKind,
)
from etl_craft.core.errors import ConfigurationError, EtlCraftError, RunStateError, ServerError
from etl_craft.core.faults import fault_point
from etl_craft.core.ownership import attempt_holder
from etl_craft.core.placement import slot_kind
from etl_craft.core.resources import Ceilings
from etl_craft.core.text import sha256_hex
from etl_craft.engine.resource_locks import holder_tag
from etl_craft.execution.child_environment import child_environment
from etl_craft.execution.control_flow import process_local_queries
from etl_craft.execution.leases import process_start
from etl_craft.execution.processes import ChildResult, ChildSpec, run_child
from etl_craft.execution.query_cancellation import confirm
from etl_craft.services.operations.agent_models import (
    AgentClaim,
    AgentLog,
    AgentRegister,
    AgentRenew,
    AgentRequest,
    AgentTask,
)
from etl_craft.services.operations.serialization import from_json
from etl_craft.warehouse.fencing import cluster_problem

logger = logging.getLogger(__name__)
FENCE_MARGIN = 15


@dataclass(frozen=True)
class AgentOptions:
    """The agent's project and explicitly requested ceilings; no Engine DB settings."""

    api: str
    pool: str
    ceilings: Ceilings
    drain_seconds: float = 300
    declared: DeclaredCapacity = NO_DECLARATION


@dataclass(frozen=True)
class Work:
    """A claim's task metadata and remaining lease budget, supplied by the API."""

    task: dict[str, object]
    lease_seconds: float
    timeout_seconds: int
    credential: str
    holder_id: str


@dataclass
class RunningTask:
    """Local process supervision and upload progress; durable ownership remains in the API."""

    attempt_id: int
    kind: SlotKind
    folder: Path
    temporary: tempfile.TemporaryDirectory[str]
    deadline: float
    cancel: threading.Event = field(default_factory=threading.Event)
    future: Future[ChildResult] | None = None
    sequence: int = 0
    read_offset: int = 0
    pending: str = ""
    decoder: codecs.IncrementalDecoder = field(
        default_factory=lambda: codecs.getincrementaldecoder("utf-8")("replace")
    )
    forced_status: AttemptStatus | None = None
    forced_message: str | None = None
    fenced: bool = False
    trigger: bool = False
    decoded: bool = False

    def lease(self, deadline: float) -> None:
        """Publish a conservative monotonic deadline before the task's existing one expires."""
        self.deadline = deadline
        pending = self.folder / "lease.new"
        pending.write_text(str(deadline), encoding="utf-8")
        pending.replace(self.folder / "lease")


class AgentLoop:
    """Renew and fence independently of claim polls and slow log uploads."""

    def __init__(
        self, config: ConnectorConfig, options: AgentOptions, stop: threading.Event
    ) -> None:
        """Prepare local process supervision without constructing an Engine DB engine."""
        if problem := cluster_problem(config):
            raise ConfigurationError(problem)
        self.config, self.options, self.stop = config, options, stop
        self.client = AgentClient(options.api)
        self.tasks: dict[int, RunningTask] = {}
        self.lock = threading.RLock()
        self.closed = threading.Event()
        self.wake = threading.Event()
        self.status = AgentStatus.ACTIVE
        self.error: EtlCraftError | None = None
        self.owner = ""
        self.birth = process_start(os.getpid())
        if self.birth is None:
            raise RunStateError(
                "agent process birth identity is unavailable; run on a supported process host"
            )
        self.commands: tuple[int, ...] = ()
        measured = report(options.ceilings, options.declared)
        self.resources = measured
        self.executor = ThreadPoolExecutor(
            max(1, measured.sizing.task_slots.value), thread_name_prefix="agent-task"
        )

    def _background(self, call: Callable[[], None]) -> None:
        """Record unexpected supervisor-thread failures and stop the process safely."""
        try:
            call()
        except Exception as error:
            # This process boundary stops all supervised writers before returning an error.
            logger.exception("agent background supervision failed")
            self.error = ServerError(
                f"agent supervision failed ({type(error).__name__}); inspect the agent log"
            )
            self.stop.set()
            raise self.error from None

    def snapshot(self) -> tuple[RunningTask, ...]:
        """Keep heartbeat and log processing from racing task joins and departures."""
        with self.lock:
            return tuple(self.tasks.values())

    def serve(self) -> None:
        """Stay outbound-only; retain processes only while their conservative lease permits."""
        registered = self.client.call(
            ActionName.AGENT_REGISTER,
            AgentRegister(
                version=__version__,
                pool=self.options.pool,
                resources=self.resources,
            ),
        )
        if not isinstance(registered.data, dict):
            raise ServerError("agent registration: expected identity; check the API version")
        self.owner = str(registered.data["agent_id"])
        heartbeat = threading.Thread(
            target=self._background, args=(self._heartbeats,), name="agent-renew", daemon=True
        )
        monitor = threading.Thread(
            target=self._background, args=(self._monitor,), name="agent-monitor", daemon=True
        )
        heartbeat.start()
        monitor.start()
        try:
            while not self.stop.is_set():
                self._claim()
            self.status = AgentStatus.DRAINING
            self.wake.set()
            deadline = time.monotonic() + self.options.drain_seconds
            while self.snapshot() and time.monotonic() < deadline and self.error is None:
                self.closed.wait(0.1)
            for task in self.snapshot():
                task.forced_status, task.forced_message = AttemptStatus.FAILED, "agent drained"
                task.cancel.set()
            while self.snapshot() and self.error is None:
                self.closed.wait(0.1)
            if self.error is not None:
                raise self.error
            self.client.call(ActionName.AGENT_DEREGISTER, AgentRequest(version=__version__))
        finally:
            self.closed.set()
            self.wake.set()
            for task in self.snapshot():
                task.cancel.set()
            self.executor.shutdown(wait=True, cancel_futures=True)
            heartbeat.join(timeout=6)
            monitor.join(timeout=6)
            for task in self.snapshot():
                task.temporary.cleanup()

    def _claim(self) -> None:
        measured = self.resources
        live = self.snapshot()
        limits = {
            SlotKind.PYTHON: measured.sizing.python_slots.value,
            SlotKind.WAREHOUSE: measured.sizing.warehouse_slots.value,
        }
        kinds = tuple(
            kind
            for kind in SlotKind
            if self.status == AgentStatus.ACTIVE
            and len(live) < measured.sizing.task_slots.value
            and sum(t.kind == kind for t in live) < limits[kind]
        )
        started = time.monotonic()
        try:
            reply = self.client.call(
                ActionName.AGENT_CLAIM,
                AgentClaim(version=__version__, free_kinds=kinds),
                timeout=35,
            )
        except ServerError:
            self.stop.wait(1)
            return
        if not isinstance(reply.data, dict):
            return
        if reply.data.get("controls_pending"):
            for attempt in reply.data.get("cleanup", []):
                self._cleanup(int(str(attempt)))
            self.wake.set()
            self.closed.wait(0.1)
            return
        work = from_json(Work, reply.data)
        self._start(work, started)

    def _cleanup(self, attempt_id: int) -> None:
        """Confirm a prior owned process's queries without ever opening the Engine DB."""
        assert self.client.token is not None
        tag = holder_tag(attempt_holder(attempt_id, sha256_hex(self.client.token.encode())))
        reason = confirm(self.config, tag)
        self.client.call(
            ActionName.AGENT_REPORT,
            AgentTask(
                version=__version__,
                attempt_id=attempt_id,
                operation=AgentReport.CANCEL,
                queries_ended=reason is None,
                error_message=reason,
            ),
        )

    def _environment(self, work: Work, folder: Path) -> dict[str, str]:
        """Resolve allowed secret names from the pinned config before stripping the environment."""
        if work.task.get("bundle_id") is None:
            return child_environment(self.config)
        store = RemoteTaskStore(
            AgentClient(self.options.api, token=work.credential),
            int(str(work.task["attempt_id"])),
            lambda holder: None,
            work.holder_id,
        )
        path = folder / "craft-connector.yml"
        path.write_bytes(store.bundle_file("craft-connector.yml"))
        pinned = project_config(self.config, path)
        return child_environment(self.config) | child_environment(pinned)

    def _start(self, work: Work, started: float) -> None:
        attempt_id = int(str(work.task["attempt_id"]))
        temporary = tempfile.TemporaryDirectory(prefix=f"etl-craft-agent-{attempt_id}-")
        folder = Path(temporary.name)
        kind = slot_kind(str(work.task["handler"]))
        task = RunningTask(
            attempt_id, kind, folder, temporary, started + work.lease_seconds - FENCE_MARGIN
        )
        task.trigger = work.task["handler"] == Handler.RUN_PIPELINE
        task.lease(task.deadline)
        (folder / "work").write_text(
            json.dumps(
                {
                    "task": work.task,
                    "parent": os.getpid(),
                    "birth": self.birth,
                    "credential": work.credential,
                    "holder_id": work.holder_id,
                }
            ),
            encoding="utf-8",
        )
        assert self.config.config_path is not None
        spec = ChildSpec(
            (
                sys.executable,
                "-m",
                "etl_craft.agent.child",
                "--config",
                str(self.config.config_path),
                "--api",
                self.options.api,
                "--attempt-id",
                str(attempt_id),
                "--owner-id",
                self.owner,
                "--work-file",
                str(folder / "work"),
            ),
            timeout_seconds=work.timeout_seconds,
            log_path=folder / "output",
            cwd=self.config.project_dir,
            env=self._environment(work, folder),
        )
        with self.lock:
            self.tasks[attempt_id] = task
            task.future = self.executor.submit(
                run_child,
                spec,
                tail_bytes=self.config.logs.tail_bytes,
                cancel=task.cancel,
                kill_grace_seconds=1,
            )

    def _heartbeats(self) -> None:
        while not self.closed.is_set():
            try:
                self._renew()
            except ServerError:
                logger.warning("agent API unavailable; existing task lease budgets still apply")
            except EtlCraftError as error:
                self.error = error
                self.stop.set()
            now = time.monotonic()
            for task in self.snapshot():
                if now >= task.deadline:
                    task.fenced = True
                    task.cancel.set()
            self.wake.wait(1 if self.stop.is_set() else 10)
            self.wake.clear()

    def _renew(self) -> None:
        if self.stop.is_set():
            self.status = AgentStatus.DRAINING
        tasks = self.snapshot()
        started = time.monotonic()
        measured = report(self.options.ceilings, self.options.declared)
        reply = self.client.call(
            ActionName.AGENT_RENEW,
            AgentRenew(
                version=__version__,
                attempts=tuple(t.attempt_id for t in tasks),
                resources=measured,
                acknowledged_commands=self.commands,
                status=self.status,
            ),
        )
        if not isinstance(reply.data, dict):
            raise ServerError("agent renewal: expected lease budgets; check the API version")
        self.resources = measured
        renewed = reply.data.get("renewed", [])
        cancelled = reply.data.get("cancel", [])
        budget = float(str(reply.data.get("lease_seconds", 0)))
        for task in tasks:
            if task.attempt_id not in renewed:
                task.fenced = True
                task.cancel.set()
            else:
                with self.lock:
                    if self.tasks.get(task.attempt_id) is task:
                        task.lease(started + budget - FENCE_MARGIN)
            if task.attempt_id in cancelled:
                task.forced_status, task.forced_message = (
                    AttemptStatus.CANCELLED,
                    "cancelled by request",
                )
                task.cancel.set()
        acknowledged = []
        commands = reply.data.get("commands", [])
        if isinstance(commands, list):
            for item in commands:
                if not isinstance(item, dict):
                    raise ServerError(
                        "agent command: expected a command document; check the API version"
                    )
                command = AgentCommand(item["command"])
                if command == AgentCommand.STOP:
                    self.stop.set()
                self.status = (
                    AgentStatus.ACTIVE if command == AgentCommand.RESUME else AgentStatus.DRAINING
                )
                acknowledged.append(int(item["command_id"]))
        self.commands = tuple(acknowledged)
        if acknowledged:
            self.wake.set()

    def _monitor(self) -> None:
        while not self.closed.wait(0.1):
            for task in self.snapshot():
                try:
                    self._advance(task)
                except ServerError:
                    break
                except EtlCraftError as error:
                    self.error = error
                    self.stop.set()

    def _advance(self, task: RunningTask) -> None:
        future = task.future
        if future is None:
            return
        if task.fenced:
            task.cancel.set()
            if future.done():
                self._forget(task)
            return
        if task.trigger and not future.done():
            return
        if task.trigger and (task.folder / "result").is_file():
            self._complete(task, future.result())
            return
        if task.pending:
            self.client.call(
                ActionName.AGENT_LOG,
                AgentLog(
                    version=__version__,
                    attempt_id=task.attempt_id,
                    sequence=task.sequence,
                    content=task.pending,
                ),
            )
            fault_point("agent.after_log_upload")
            task.sequence += 1
            task.pending = ""
        output = task.folder / "output"
        if output.is_file():
            with output.open("rb") as stream:
                stream.seek(task.read_offset)
                data = stream.read(16384)
            if data:
                task.read_offset += len(data)
                task.pending = task.decoder.decode(data)
                return
        if not future.done():
            return
        if not task.decoded:
            task.pending = task.decoder.decode(b"", final=True)
            task.decoded = True
            if task.pending:
                return
        self._complete(task, future.result())

    def _complete(self, task: RunningTask, outcome: ChildResult) -> None:
        """Keep a pending cancellation owned until its queries are confirmed stopped."""
        if self._finish(task, outcome):
            self._forget(task)

    def _finish(self, task: RunningTask, outcome: ChildResult) -> bool:
        path = task.folder / "result"
        result = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None
        if isinstance(result, dict) and result.get("trigger"):
            return True
        if result is None:
            ending = AgentTask(
                version=__version__,
                attempt_id=task.attempt_id,
                operation=AgentReport.END,
                status=AttemptStatus.FAILED,
                exit_code=outcome.returncode,
                error_message=f"agent task process exited {outcome.returncode}; inspect its log",
            )
        else:
            ending = from_json(AgentTask, result)
        if outcome.timed_out:
            ending = replace(
                ending, status=AttemptStatus.TIMED_OUT, error_message="agent task timeout"
            )
        if task.forced_status is not None:
            ending = replace(ending, status=task.forced_status, error_message=task.forced_message)
        queries_ended = task.kind != SlotKind.PYTHON or process_local_queries(self.config)
        assert self.client.token is not None
        if outcome.cancelled or outcome.timed_out or outcome.returncode < 0:
            tag = holder_tag(
                attempt_holder(task.attempt_id, sha256_hex(self.client.token.encode()))
            )
            reason = confirm(self.config, tag)
            queries_ended = reason is None
            if reason and ending.status == AttemptStatus.CANCELLED:
                self.client.call(
                    ActionName.AGENT_REPORT,
                    AgentTask(
                        version=__version__,
                        attempt_id=task.attempt_id,
                        operation=AgentReport.CANCEL,
                        error_message=reason,
                    ),
                )
                return False
        ending = replace(ending, queries_ended=queries_ended)
        self.client.call(ActionName.AGENT_REPORT, ending)
        return True

    def _forget(self, task: RunningTask) -> None:
        with self.lock:
            self.tasks.pop(task.attempt_id, None)
            task.temporary.cleanup()
