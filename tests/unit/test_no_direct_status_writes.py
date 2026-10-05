"""Lifecycle SQL is executed only by the module owning transitions."""

import ast
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit
ROOT = Path(__file__).resolve().parents[2] / "src" / "etl_craft"
STATUS_WRITE = re.compile(
    r"\bSET\s+(?:STATUS\s*=|(?:(?!\bWHERE\b)[^;])*?,\s*STATUS\s*=)", re.IGNORECASE | re.DOTALL
)


def test_no_direct_status_writes():
    writing = set()
    for path in ROOT.rglob("*.sql"):
        if STATUS_WRITE.search(path.read_text()):
            assert path.name.startswith("transition_"), path
            writing.add(path.stem)
    assert writing
    for path in ROOT.rglob("*.py"):
        if path == ROOT / "engine" / "transitions.py":
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                assert node.value not in writing, (path, node.lineno, node.value)
                assert not STATUS_WRITE.search(node.value), (path, node.lineno)


@pytest.mark.parametrize(
    "sql",
    [
        "SET STATUS = :status",
        "SET END_DATE = :now,\n STATUS = :status",
        "SET END_DATE = :now,STATUS=:status",
    ],
)
def test_status_write_detection(sql):
    assert STATUS_WRITE.search(sql)


def test_sla_status_is_independent():
    assert not STATUS_WRITE.search("SET SLA_STATUS = 'BREACHED'")


@pytest.mark.parametrize(
    "sql",
    [
        "SET REPAIR_PENDING = 'Y' WHERE STATUS = 'IN-PROGRESS'",
        "SET END_DATE = :now WHERE STATUS = 'SUCCESS'",
    ],
)
def test_status_filters_are_not_lifecycle_assignments(sql):
    assert not STATUS_WRITE.search(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT TASK_ID, STATUS = 'SUCCESS' AS succeeded FROM AUD_TASK_RUN_LOG",
        "UPDATE FLAGS SET FLAG = 'Y' WHERE FLAG IN ('Y', STATUS = 'SUCCESS')",
    ],
)
def test_comparisons_outside_set_are_not_lifecycle_assignments(sql):
    assert not STATUS_WRITE.search(sql)
