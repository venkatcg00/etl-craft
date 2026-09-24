"""Fail when files carry change-history commentary instead of describing current behaviour.

Comments, docstrings and documentation say what the code does now. Why something
changed belongs in the pull request and the commit message. A line that must
quote one of the patterns (for example, test data) can end with the marker
``history-gate: allow``.

Usage: ``python scripts/check_no_history.py [PATH ...]``. With no paths, every
file tracked by git is checked.
"""

from __future__ import annotations

import re
import subprocess
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path

ALLOW_MARKER = "history-gate: allow"

# Files whose purpose is to record history.
EXEMPT = frozenset({"CHANGELOG.md"})

TEXT_SUFFIXES = frozenset(
    {".py", ".pyi", ".sql", ".md", ".toml", ".yml", ".yaml", ".cfg", ".ini", ".txt", ".sh"}
)
TEXT_NAMES = frozenset({"Makefile", "Dockerfile"})

PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\[(?:DEVIATION|ADDITION|CHOICE)\b"), "decision tag"),
    (re.compile(r"\bE\d-\d{2,}\b"), "review item id"),
    (
        re.compile(r"\bper explicit (?:instruction|decision|request|correction)", re.IGNORECASE),
        "decision provenance",
    ),
    (re.compile(r"\[Bug (?:caught|found)", re.IGNORECASE), "bug history"),
    (re.compile(r"\(\d{4}-\d{2}-\d{2}\b"), "dated note"),
)


def findings(path: Path, text: str) -> list[str]:
    """Return one message per offending line in ``text``."""
    messages = []
    for number, line in enumerate(text.splitlines(), start=1):
        if ALLOW_MARKER in line:
            continue
        for pattern, kind in PATTERNS:
            if pattern.search(line):
                messages.append(f"{path}:{number}: {kind}: {line.strip()}")
                break
    return messages


def tracked_files() -> list[Path]:
    """Return every file git tracks in the current repository."""
    output = subprocess.run(
        ["git", "ls-files", "-z"], check=True, capture_output=True, text=True
    ).stdout
    return [Path(name) for name in output.split("\0") if name]


def is_text_candidate(path: Path) -> bool:
    """Report whether ``path`` is a file type the gate reads."""
    if path.name in EXEMPT:
        return False
    return path.suffix in TEXT_SUFFIXES or path.name in TEXT_NAMES


def check(paths: Iterable[Path]) -> list[str]:
    """Check each readable text file in ``paths`` and return every finding."""
    messages: list[str] = []
    for path in paths:
        if not is_text_candidate(path) or not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        messages.extend(findings(path, text))
    return messages


def main(argv: Sequence[str] | None = None) -> int:
    """Run the gate and return its exit code."""
    args = list(sys.argv[1:] if argv is None else argv)
    paths = [Path(arg) for arg in args] if args else tracked_files()
    messages = check(paths)
    for message in messages:
        print(message)
    if messages:
        print(
            f"\n{len(messages)} line(s) describe history rather than behaviour: "
            "move that context to the commit message or pull request."
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
