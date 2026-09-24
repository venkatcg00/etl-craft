"""Record a test run as release evidence: which tests ran, on which commit, and how they ended.

Enabled by ``--evidence=PATH``. The JSON written there holds no failure text, only test ids
and outcomes, so a traceback that contains a connection string never reaches a committed
evidence file.
"""

from __future__ import annotations

import hashlib
import json
import platform
import subprocess
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import pytest

SCHEMA = 1
EVIDENCE_DIR = "release/evidence"

# When a test reports in several phases, the worst outcome wins.
RANK = {"passed": 0, "skipped": 1, "xfailed": 2, "xpassed": 3, "failed": 4, "error": 5}


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("evidence", "release evidence")
    group.addoption("--evidence", metavar="PATH", help="write this run's evidence JSON to PATH")
    group.addoption("--evidence-suite", metavar="NAME", help="the release suite being run")
    group.addoption("--evidence-wheel", metavar="PATH", help="the wheel this run tested")


def pytest_configure(config: pytest.Config) -> None:
    path = config.getoption("--evidence")
    if path:
        config.pluginmanager.register(EvidenceRecorder(config, Path(path)), "evidence-recorder")


def git_state(root: Path) -> tuple[str | None, bool | None]:
    """Return HEAD's commit and whether the tree has changes outside the evidence directory."""
    head = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True, check=False
    )
    if head.returncode != 0:
        return None, None
    status = subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            ".",
            f":(exclude){EVIDENCE_DIR}",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    return head.stdout.strip(), bool(status.stdout.strip())


def classify(report: pytest.TestReport | pytest.CollectReport) -> str:
    """Map one phase report to a test outcome."""
    if report.failed:
        return "failed" if getattr(report, "when", None) == "call" else "error"
    if report.skipped:
        return "xfailed" if hasattr(report, "wasxfail") else "skipped"
    return "xpassed" if hasattr(report, "wasxfail") else "passed"


def now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class EvidenceRecorder:
    """Collects outcomes during the session and writes them when it finishes."""

    def __init__(self, config: pytest.Config, path: Path) -> None:
        self.config = config
        self.path = path
        self.started_at = now()
        self.commit, self.dirty = git_state(config.rootpath)
        self.outcomes: dict[str, str] = {}
        self.durations: dict[str, float] = {}

    def record(self, nodeid: str, outcome: str, duration: float = 0.0) -> None:
        previous = self.outcomes.get(nodeid)
        if previous is None or RANK[outcome] > RANK[previous]:
            self.outcomes[nodeid] = outcome
        self.durations[nodeid] = self.durations.get(nodeid, 0.0) + duration

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if report.when == "call" or report.failed or report.skipped:
            self.record(report.nodeid, classify(report), report.duration)
        else:
            self.record(report.nodeid, "passed", report.duration)

    def pytest_collectreport(self, report: pytest.CollectReport) -> None:
        if report.failed:
            self.record(report.nodeid or "<collection>", "error")

    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        counts = dict.fromkeys(RANK, 0)
        for outcome in self.outcomes.values():
            counts[outcome] += 1
        wheel = self.config.getoption("--evidence-wheel")
        evidence: dict[str, Any] = {
            "schema": SCHEMA,
            "suite": self.config.getoption("--evidence-suite"),
            "marker": self.config.getoption("markexpr") or None,
            "package_version": installed_version(),
            "commit": self.commit,
            "dirty": self.dirty,
            "wheel_sha256": sha256_of(Path(wheel)) if wheel else None,
            "python": platform.python_version(),
            "platform": platform.platform(),
            "started_at": self.started_at,
            "finished_at": now(),
            "exit_status": int(exitstatus),
            "counts": counts,
            "tests": [
                {
                    "nodeid": nodeid,
                    "outcome": self.outcomes[nodeid],
                    "duration": round(self.durations.get(nodeid, 0.0), 3),
                }
                for nodeid in sorted(self.outcomes)
            ],
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")


def installed_version() -> str | None:
    try:
        return version("etl-craft")
    except PackageNotFoundError:
        return None


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
