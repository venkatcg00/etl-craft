"""One task process using its project files and the API's scoped task store."""

import argparse
import json
import logging
import os
import signal
import threading
import time
from dataclasses import replace
from pathlib import Path
from types import FrameType, TracebackType

from etl_craft import __version__
from etl_craft.agent.client import AgentClient
from etl_craft.agent.task_store import RemoteTaskStore
from etl_craft.config import load_config
from etl_craft.core import log
from etl_craft.core.counts import Counts
from etl_craft.core.enums import AgentReport, AttemptStatus, Handler
from etl_craft.core.errors import EtlCraftError, ExitCode
from etl_craft.core.text import mask_arguments
from etl_craft.engine.resource_locks import holder_tag
from etl_craft.execution.child import exit_now
from etl_craft.execution.classify import classify_failure
from etl_craft.execution.leases import process_start
from etl_craft.handlers.registry import TaskContext, format_task_log, resolve_handler
from etl_craft.services.operations.agent_models import AgentTask
from etl_craft.services.operations.serialization import encode, from_json
from etl_craft.warehouse.fencing import confirm_stopped

logger = logging.getLogger(__name__)


def _interrupt(signum: int, frame: FrameType | None) -> None:
    raise KeyboardInterrupt(signal.Signals(signum).name)


def _watch(parent: int, birth: str, lease: Path) -> None:
    """Fence the entire task group even when SIGKILL removes its supervising agent."""
    waiting = threading.Event()
    while not waiting.wait(0.1):
        try:
            deadline = float(lease.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            deadline = 0
        if process_start(parent) != birth or time.monotonic() >= deadline:
            os.killpg(os.getpid(), signal.SIGKILL)
            return


def main() -> int:
    """Run one handler, leaving the parent to upload final logs before committing its ending."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--api", required=True)
    parser.add_argument("--attempt-id", type=int, required=True)
    parser.add_argument("--owner-id", required=True)
    parser.add_argument("--work-file", type=Path, required=True)
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, _interrupt)
    log.configure("INFO", "text")
    work = json.loads(args.work_file.read_text(encoding="utf-8"))
    folder = args.work_file.parent
    threading.Thread(
        target=_watch, args=(work["parent"], work["birth"], folder / "lease"), daemon=True
    ).start()
    config = load_config(args.config, agent=True)
    context = from_json(TaskContext, work["task"] | {"config": encode(config)})
    store = RemoteTaskStore(
        AgentClient(args.api),
        args.attempt_id,
        lambda holder_id: confirm_stopped(config, holder_tag(holder_id)),
    )
    started = store.call(
        replace(
            store.request(AgentReport.START),
            pid=os.getpid(),
            process_start=process_start(os.getpid()),
        )
    )
    if isinstance(started, dict) and started.get("trigger"):
        (folder / "result").write_text(json.dumps({"trigger": True}), encoding="utf-8")
        return ExitCode.SUCCESS
    result = AgentTask(
        version=__version__,
        attempt_id=args.attempt_id,
        operation=AgentReport.END,
        status=AttemptStatus.FAILED,
        exit_code=ExitCode.UNEXPECTED,
    )
    try:
        counts = resolve_handler(context.handler)(context, store)
        result = replace(
            result,
            status=AttemptStatus.SUCCESS,
            counts=Counts(**counts.parameters()),
            offset=counts.offset,
            task_log=format_task_log(counts),
            exit_code=ExitCode.SUCCESS,
        )
    except KeyboardInterrupt:
        result = replace(result, error_message="task stopped by its agent")
    except Exception as error:
        # The process hook logs the failure after its classified ending has been saved.
        failure = classify_failure(error, config, script=context.handler == Handler.PYTHON)
        result = replace(
            result,
            error_message=str(error),
            failure=failure,
            exit_code=error.exit_code if isinstance(error, EtlCraftError) else ExitCode.UNEXPECTED,
        )
        raise
    finally:
        payload = encode(result)
        assert isinstance(payload, dict)
        (folder / "result").write_text(json.dumps(mask_arguments(payload)), encoding="utf-8")
    return int(result.exit_code or 0)


def failed(kind: type[BaseException], error: BaseException, trace: TracebackType | None) -> None:
    """Log a masked process-boundary failure and exit without waiting for script threads."""
    logger.error("agent task failed", exc_info=(kind, error, trace))
    exit_now(error.exit_code if isinstance(error, EtlCraftError) else ExitCode.UNEXPECTED)


if __name__ == "__main__":
    import sys

    sys.excepthook = failed
    exit_now(main())
