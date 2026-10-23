"""The only place etl-craft writes to the terminal.

Command results go to standard output, so they can be piped; errors go to standard error, next
to the log records.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable, Iterable
from typing import TextIO

from etl_craft.engine.controls import ControlView
from etl_craft.services.operations import to_json
from etl_craft.services.operations.agents import AgentsView
from etl_craft.services.operations.models import BackfillView, OperationResult, ReconciliationView


class Output:
    """Writes a command's results and errors.

    The streams default to the current ``sys.stdout`` and ``sys.stderr``, looked up at each
    write, so redirecting them after construction still takes effect.
    """

    def __init__(self, stdout: TextIO | None = None, stderr: TextIO | None = None) -> None:
        """Write to the given streams instead of the process's standard streams."""
        self._stdout = stdout
        self._stderr = stderr

    @property
    def stdout(self) -> TextIO:
        """The stream results are written to."""
        return sys.stdout if self._stdout is None else self._stdout

    @property
    def stderr(self) -> TextIO:
        """The stream errors are written to."""
        return sys.stderr if self._stderr is None else self._stderr

    def line(self, text: str = "") -> None:
        """Write one line of results."""
        print(text, file=self.stdout)

    def document(self, result: object) -> None:
        """Write the same JSON document returned by the service serializer."""
        self.line(json.dumps(to_json(result), ensure_ascii=False, allow_nan=False))

    def result(
        self,
        done: object,
        output_format: str,
        *,
        text: Callable[[], None] | None = None,
    ) -> None:
        """Write JSON or render the command's text, defaulting to its outcome message."""
        if output_format == "json":
            self.document(done)
        elif text is not None:
            text()
        elif isinstance(done, ControlView):
            self.line(
                f"control_id={done.control_id} {done.action} {done.status}; "
                f"run_id={done.pipeline_run_id} task={done.task_code or 'all'} "
                f"run_status={done.run_status} execution_run_id={done.execution_run_id}: "
                f"{done.message}"
            )
        else:
            assert isinstance(done, (OperationResult, BackfillView, ReconciliationView))
            self.line(done.message)

    def rows(self, rows: Iterable[Iterable[object]]) -> None:
        """Write one tab-separated line per row; ``None`` becomes an empty field."""
        for row in rows:
            self.line("\t".join("" if value is None else str(value) for value in row))

    def agents(self, view: AgentsView | None) -> None:
        """Render the shared registry's advertised, reserved and free resources."""
        if view is None:
            return
        self.rows(
            [
                (
                    "AGENT",
                    "POOL",
                    "HOST",
                    "STATUS",
                    "USED_SLOTS",
                    "FREE_SLOTS",
                    "CPU",
                    "CPU_RESERVED",
                    "CPU_FREE",
                    "MEMORY_MB",
                    "MEMORY_RESERVED_MB",
                    "MEMORY_FREE_MB",
                    "HEARTBEAT",
                )
            ]
        )
        self.rows(
            (
                a.agent_id,
                a.pool,
                a.host,
                a.status,
                a.used,
                a.free,
                a.advertised_cpu,
                a.reserved_cpu,
                a.free_cpu,
                a.advertised_memory_mb,
                a.reserved_memory_mb,
                a.free_memory_mb,
                a.heartbeat_at,
            )
            for a in view.agents
        )
        self.rows(
            [
                (
                    "POOL",
                    "AGENTS",
                    "CPU",
                    "CPU_RESERVED",
                    "CPU_FREE",
                    "MEMORY_MB",
                    "MEMORY_RESERVED_MB",
                    "MEMORY_FREE_MB",
                )
            ]
        )
        self.rows(
            (
                p.pool,
                p.agents,
                p.advertised_cpu,
                p.reserved_cpu,
                p.free_cpu,
                p.advertised_memory_mb,
                p.reserved_memory_mb,
                p.free_memory_mb,
            )
            for p in view.pools
        )

    def empty(self, what: str) -> None:
        """Report a result with nothing in it, such as ``(no active pipelines)``."""
        self.line(f"({what})")

    def error(self, message: str) -> None:
        """Write ``error: <message>`` to standard error."""
        print(f"error: {message}", file=self.stderr)
