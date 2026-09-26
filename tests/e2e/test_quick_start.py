"""The Quick Start, run as written, from the built wheel installed with pip.

The commands are read from ``docs/quick-start.md``: every line of its ``bash`` blocks that runs
``etl-craft`` or ``python prepare.py``, in order, in a fresh copy of ``examples/demo``. A line
whose comment says it ``fails`` must exit ``1``, every other ``0``; and every line of the page's
``text`` blocks must be in what the commands printed. Installing and starting Mailpit are left
to the suite, which installs the wheel and needs the local Mailpit. With
``ETL_CRAFT_E2E_WAREHOUSE`` naming another warehouse it is skipped, as its warehouse is DuckDB.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import textwrap

import pytest

from fixtures.demo import DEMO, REPO, installed_cli
from fixtures.services import require

pytestmark = [pytest.mark.e2e_pip, pytest.mark.timeout(1800)]

PAGE = REPO / "docs" / "quick-start.md"
BLOCK = re.compile(r"^( *)```(\w+)\n(.*?)^\1```", re.MULTILINE | re.DOTALL)
RUNS = ("etl-craft ", "python prepare.py ")


def blocks(kind: str) -> list[str]:
    """Return the page's fenced blocks of ``kind``, dedented."""
    text = PAGE.read_text(encoding="utf-8")
    return [textwrap.dedent(body) for _, found, body in BLOCK.findall(text) if found == kind]


def commands() -> list[str]:
    return [
        line.strip()
        for block in blocks("bash")
        for line in block.splitlines()
        if line.strip().startswith(RUNS)
    ]


def test_the_page_runs_the_whole_demo():
    assert commands()[:3] == [
        "python prepare.py warehouse",
        "etl-craft setup",
        "python prepare.py metadata",
    ]
    assert "etl-craft generate-docs" in commands()


def test_the_quick_start_runs_as_written(tmp_path):
    only = os.environ.get("ETL_CRAFT_E2E_WAREHOUSE")
    if only not in (None, "duckdb"):
        pytest.skip(f"the Quick Start's warehouse is DuckDB; this run covers {only}")
    require("mailpit_smtp")
    cli = installed_cli("pip", tmp_path / "venvs", extras="trino")
    python = cli.with_name(cli.name.replace("etl-craft", "python"))
    project = tmp_path / "demo"
    shutil.copytree(
        DEMO,
        project,
        ignore=shutil.ignore_patterns(
            "engine.db*", "warehouse.duckdb*", "logs", "catalog", ".flaky-has-failed"
        ),
    )
    printed = []
    for line in commands():
        command, _, comment = line.partition("#")
        argv = command.split()
        program = cli if argv[0] == "etl-craft" else python
        done = subprocess.run(
            [str(program), *argv[1:]],
            cwd=project,
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )
        expected = 1 if "fails" in comment else 0
        assert done.returncode == expected, (line, done.stdout[-3000:], done.stderr[-3000:])
        printed.append(done.stdout)
    output = "\n".join(printed)
    for block in blocks("text"):
        for shown in block.splitlines():
            assert shown in output, shown
    assert (project / "catalog" / "index.html").is_file()
