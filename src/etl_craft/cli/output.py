"""The only place etl-craft writes to the terminal.

Command results go to standard output, so they can be piped; errors go to standard error, next
to the log records.
"""

from __future__ import annotations

import sys
from collections.abc import Iterable
from typing import TextIO


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

    def rows(self, rows: Iterable[Iterable[object]]) -> None:
        """Write one tab-separated line per row; ``None`` becomes an empty field."""
        for row in rows:
            self.line("\t".join("" if value is None else str(value) for value in row))

    def empty(self, what: str) -> None:
        """Report a result with nothing in it, such as ``(no active pipelines)``."""
        self.line(f"({what})")

    def error(self, message: str) -> None:
        """Write ``error: <message>`` to standard error."""
        print(f"error: {message}", file=self.stderr)
