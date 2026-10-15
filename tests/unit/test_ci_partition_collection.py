"""The repository's normal pytest configuration applies CI partitions after collection."""

import json
import runpy
from pathlib import Path

import pytest

from plugins import ci_shards

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("markexpr", [None, "unit"])
def test_repository_configuration_partitions_a_collection_without_losing_ids(
    pytester, monkeypatch, markexpr
):
    root = Path(__file__).resolve().parents[1]
    plugins = runpy.run_path(str(root / "conftest.py"))["pytest_plugins"]
    pytester.makeconftest(f"pytest_plugins = {plugins!r}\n")
    pytester.makeini("[pytest]\nmarkers=\n    unit: unit tests\n    harness: service tests\n")
    sample = pytester.makepyfile(
        "import pytest\n@pytest.mark.unit\n"
        "@pytest.mark.parametrize('number', range(6))\ndef test_one(number): pass\n"
        "@pytest.mark.harness\n@pytest.mark.parametrize('number', range(2))\n"
        "def test_two(number): pass\n"
    )
    estimates = pytester.path / "estimates.json"
    estimates.write_text(
        json.dumps({"engine": {sample.name: {"test_one[0]": 100, "test_one[2]": 100}}})
    )
    monkeypatch.setattr(ci_shards, "TIMINGS", estimates)
    monkeypatch.setenv("ETL_CRAFT_CI_PROFILE", "engine")
    seen = set()
    complete = None
    heavy_shards = []
    expected_count = 6 if markexpr else 8
    for index in range(2):
        path = pytester.path / f"shard{index}.json"
        arguments = ["-m", markexpr] if markexpr else []
        result = pytester.runpytest("-q", f"--evidence={path}", f"--ci-shard={index}/2", *arguments)
        assert result.ret == 0
        result.assert_outcomes(passed=expected_count // 2, deselected=8 - expected_count // 2)
        evidence = json.loads(path.read_text())
        assert len(evidence["ci"]["expected"]) == expected_count
        assert not seen & set(evidence["collected"])
        heavy_shards.append(
            {
                node
                for node in evidence["collected"]
                if node.endswith(("test_one[0]", "test_one[2]"))
            }
        )
        seen.update(evidence["collected"])
        if complete is not None:
            assert evidence["ci"]["expected"] == complete
        complete = evidence["ci"]["expected"]
    assert seen == set(complete)
    assert all(len(nodes) == 1 for nodes in heavy_shards)
