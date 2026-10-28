"""CI-only demo failure evidence, captured before fixture cleanup removes the catalog."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

import psutil
import pytest
import yaml

logger = logging.getLogger(__name__)


def namespaces(schema: str) -> tuple[str, ...]:
    """Restrict catalog evidence to this demo's generated project namespaces."""
    match = re.fullmatch(r"(ec_[0-9a-f]{8}_)(?:aud)", schema)
    return (
        ()
        if match is None
        else tuple(match[1] + name for name in ("lnd", "prs", "ds", "cdc", "pre_dm", "dm", "aud"))
    )


def destination(nodeid: str) -> Path | None:
    """Each process and test writes a separate directory under the CI artifact root."""
    root = os.environ.get("ETL_CRAFT_CI_DIAGNOSTICS")
    if not root:
        return None
    identity = hashlib.sha256(nodeid.encode()).hexdigest()[:16]
    return Path(root) / identity


def catalog(project: Path, target: Path) -> None:
    """Save REST metadata without configuration credentials or data file contents."""
    config = yaml.safe_load((project / "craft-connector.yml").read_text())
    profile = config.get("Warehouse", {}).get("dev", {})
    names = namespaces(profile.get("Schema", ""))
    if not names:
        return
    host = os.environ.get("ETL_CRAFT_TEST_ICEBERG_REST", "127.0.0.1:58181")
    base = f"http://{host}/v1/namespaces/"

    def read(url: str) -> object:
        with urllib.request.urlopen(url, timeout=10) as response:
            return json.load(response)

    metadata = {}
    for name in names:
        tables = read(base + name + "/tables")
        assert isinstance(tables, dict)
        for table in tables.get("identifiers", []):
            key = name + "." + table["name"]
            metadata[key] = read(base + name + "/tables/" + table["name"])
    (target / "catalog.json").write_text(json.dumps(metadata, indent=2))


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Copy logs and metadata on failure, before the demo fixture deletes its tables."""
    outcome = yield
    report = outcome.get_result()
    target = destination(item.nodeid)
    temporary = item.funcargs.get("tmp_path")
    if target is None or temporary is None or not report.failed:
        return
    project = temporary / "support-insights"
    if not (project / "craft-connector.yml").is_file():
        return
    try:
        failure_artifacts(project, target)
    except Exception as error:
        # Diagnostic boundary: evidence failure must not replace the test's original failure.
        target.mkdir(parents=True, exist_ok=True)
        (target / "capture-error.txt").write_text(f"{type(error).__name__}: {error}")
        logger.exception("could not capture demo failure evidence")


def failure_artifacts(project: Path, target: Path) -> None:
    """Preserve logs first, then record the installed runtime and catalog metadata."""
    target.mkdir(parents=True, exist_ok=True)
    if (project / "logs").is_dir():
        shutil.copytree(project / "logs", target / "logs", dirs_exist_ok=True)
    for python in project.parent.parent.glob("venvs*/venv-*/bin/python"):
        versions = subprocess.run(
            [
                str(python),
                "-c",
                "from importlib.metadata import version; "
                "print({name: version(name) for name in "
                "('etl-craft', 'duckdb', 'duckdb-engine')})",
            ],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        (target / "installed-versions.txt").write_text(versions.stdout + versions.stderr)
    catalog(project, target)


def capture_stacks(target: Path, stop: threading.Event) -> None:
    """Attach once to long-running attempt processes, verifying their birth identity."""
    captured = set()
    parent = psutil.Process()
    while not stop.wait(2):
        for child in parent.children(recursive=True):
            try:
                birth = child.create_time()
                identity = (child.pid, birth)
                if identity in captured or time.time() - birth < 45:
                    continue
                if "etl_craft.execution.child" not in child.cmdline():
                    continue
                captured.add(identity)
                target.mkdir(parents=True, exist_ok=True)
                if psutil.Process(child.pid).create_time() != birth:
                    continue
                command = [
                    "gdb",
                    "-q",
                    "-nx",
                    "-batch",
                    "-ex",
                    "set debuginfod enabled off",
                    "-ex",
                    "set print frame-arguments none",
                    "-ex",
                    "thread apply all bt 24",
                    "-p",
                    str(child.pid),
                ]
                if os.environ.get("GITHUB_ACTIONS") == "true":
                    command = ["sudo", "-n", *command]
                with (target / f"process-{child.pid}.txt").open("w") as output:
                    subprocess.run(
                        command, stdout=output, stderr=subprocess.STDOUT, timeout=30, check=False
                    )
            except (psutil.Error, OSError, subprocess.TimeoutExpired):
                # Diagnostic boundary: a vanished process or unavailable debugger is evidence.
                logger.exception("could not capture a stalled demo process")


@pytest.fixture(autouse=True)
def demo_stack_capture(request):
    """Normal developer tests allocate no watcher; CI enables it for installed demos only."""
    target = destination(request.node.nodeid)
    if target is None or "tests/e2e/" not in request.node.nodeid:
        yield
        return
    stop = threading.Event()
    watcher = threading.Thread(target=capture_stacks, args=(target / "stacks", stop), daemon=True)
    watcher.start()
    try:
        yield
    finally:
        stop.set()
        watcher.join(timeout=35)
