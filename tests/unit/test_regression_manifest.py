"""Regression traceability refuses missing defects and missing named tests."""

import pytest

from fixtures.scripts import load

pytestmark = pytest.mark.unit
regressions = load("check_regressions")


def test_assignments_include_shared_stabilization_fixes_and_exclude_later_work():
    roadmap = (
        "| B1 | High | ownership | S3.B |\n"
        "| B2 | Medium | setup | S2.A.7 |\n"
        "| B24 | High | overlap | S2.A.2, S3.D |\n"
    )
    assert regressions.assigned_defects(roadmap) == {"B2", "B24"}


@pytest.mark.parametrize(
    ("mapping", "nodes", "reason"),
    [
        ({}, [], "no regression mapping"),
        ({"B2": []}, [], "at least one named test"),
        ({"B2": ["tests/x.py::test_setup"]}, [], "test not collected"),
        (
            {"B2": ["tests/x.py::test_setup"]},
            ["tests/x.py::test_setup_other"],
            "test not collected",
        ),
        (
            {"B2": ["tests/x.py::test_setup"]},
            ["tests/x.py::test_setup_extra[pg]"],
            "test not collected",
        ),
        ({"B2": [False]}, [], "invalid test reference"),
        ({"B2": ["tests/x.py::test_setup"], "B1": []}, [], "not assigned"),
    ],
)
def test_incomplete_or_stale_mappings_are_rejected(mapping, nodes, reason):
    assert any(
        reason in failure for failure in regressions.problems({"B2"}, {"defects": mapping}, nodes)
    )


def test_named_test_reference_covers_its_collected_dialect_cases():
    mapping = {"defects": {"B2": ["tests/x.py::test_setup"]}}
    assert (
        regressions.problems(
            {"B2"}, mapping, ["tests/x.py::test_setup[sqlite]", "tests/x.py::test_setup[postgres]"]
        )
        == []
    )
