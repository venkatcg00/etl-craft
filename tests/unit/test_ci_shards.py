"""CI partitions retain every test and reject incomplete outcome or matrix artifacts."""

import copy
import json
import subprocess
import threading
from types import SimpleNamespace

import pytest

from fixtures.scripts import load
from plugins.ci_shards import partitions

pytestmark = pytest.mark.unit
ci = load("ci_shards")


def reports():
    result = []
    for index, node in enumerate(("test_one", "test_two")):
        result.append(
            {
                "commit": "same-source",
                "dirty": False,
                "exit_status": 0,
                "marker": ci.SERVICE_MARKERS["engine"],
                "collected": [node],
                "tests": [{"nodeid": node, "outcome": "passed"}],
                "counts": {"passed": 1},
                "ci": {
                    "profile": "engine",
                    "index": index,
                    "count": 2,
                    "expected": ["test_one", "test_two"],
                    "services_required": True,
                },
            }
        )
    return result


def test_measured_partitions_cover_unique_ids_once_and_balance_long_tests():
    nodes = [f"case{i}" for i in range(10)]
    timings = {"case0": 100.0, "case1": 100.0}
    groups = partitions(nodes, 2, timings)
    assert sorted(node for group in groups for node in group) == nodes
    assert not set(groups[0]) & set(groups[1])
    assert ("case0" in groups[0]) != ("case1" in groups[0])
    assert partitions(list(reversed(nodes)), 2, timings) == groups


@pytest.mark.parametrize("timing", [-1.0, float("nan"), float("inf")])
def test_invalid_timing_estimates_fail_instead_of_changing_selection(timing):
    with pytest.raises(pytest.UsageError, match="finite and nonnegative"):
        partitions(["test_one"], 1, {"test_one": timing})


@pytest.mark.parametrize("count,nodes", [(0, ["one"]), (2, ["one", "one"])])
def test_invalid_partition_inputs_fail(count, nodes):
    with pytest.raises(pytest.UsageError, match="unique test IDs"):
        partitions(nodes, count, {})


def test_all_shards_and_their_outcomes_are_required():
    data = reports()
    assert ci.check_profile("engine", data, 2) == []
    assert "expected exactly shards" in ci.check_profile("engine", data[:1], 2)[0]
    data[1]["ci"]["index"] = 0
    assert "expected exactly shards" in ci.check_profile("engine", data, 2)[0]


@pytest.mark.parametrize(
    "field,value,message",
    [
        ("exit_status", 1, "failed shard"),
        ("dirty", True, "clean source provenance"),
        ("commit", "another-source", "different source commits"),
        ("tests", [], "outcomes do not cover exactly"),
        ("counts", {"passed": 9}, "summary counts"),
        ("marker", "unit", "unexpected test selection"),
    ],
)
def test_incomplete_or_failed_shard_evidence_is_rejected(field, value, message):
    data = reports()
    data[1][field] = value
    assert any(message in problem for problem in ci.check_profile("engine", data, 2))


def test_omitted_and_overlapping_tests_fail_even_with_passing_counts():
    data = reports()
    data[1]["collected"] = ["test_one"]
    data[1]["tests"] = [{"nodeid": "test_one", "outcome": "passed"}]
    problems = ci.check_profile("engine", data, 2)
    assert any("duplicate test IDs" in problem for problem in problems)
    assert any("missing or unexpected test IDs" in problem for problem in problems)


def test_fresh_collections_must_agree_between_shards():
    data = reports()
    data[1]["ci"]["expected"] = ["test_one", "test_two", "unrun_test"]
    assert any("collection/count disagrees" in p for p in ci.check_profile("engine", data, 2))


def test_required_service_suites_cannot_pass_by_skipping():
    data = reports()
    data[0]["tests"][0]["outcome"] = "skipped"
    data[0]["counts"] = {"skipped": 1}
    assert any("skipped" in p for p in ci.check_profile("engine", data, 2))
    data = reports()
    data[0]["ci"]["services_required"] = False
    assert any("not enforced" in p for p in ci.check_profile("engine", data, 2))


def test_missing_matrix_artifacts_fail(tmp_path):
    wheel = tmp_path / "package.whl"
    wheel.write_bytes(b"wheel")
    problems = ci.verify(tmp_path, wheel)
    assert any("profiles missing" in problem for problem in problems)
    assert any("package matrix" in problem for problem in problems)


def test_every_original_python_platform_and_warehouse_combination_is_planned():
    groups = ci.combinations()
    versions = {row["python"] for row in groups["coverage"]}
    assert {"3.11", "3.12", "3.13"} <= versions
    for version in versions:
        assert {"ubuntu-latest", "macos-latest"} <= {
            row["os"] for row in groups["coverage"] if row["python"] == version
        }
    assert {"duckdb", "postgres", "duckdb_iceberg", "trino_iceberg"} <= {
        row["warehouse"] for row in groups["e2e"]
    }
    for rows in ci.matrix().values():
        pairs = [(row["profile"], row["index"]) for row in rows]
        assert len(pairs) == len(set(pairs))


def test_serial_timing_runs_preserve_the_same_complete_combinations():
    parallel = ci.matrix()
    serial = ci.matrix(serial=True)
    for family in parallel:
        assert {row["profile"] for row in parallel[family]} == {
            row["profile"] for row in serial[family]
        }
        assert all(row["index"] == 0 and row["count"] == 1 for row in serial[family])


@pytest.mark.parametrize("failure", [None, -15, 1])
def test_parallel_workers_all_finish_and_any_failure_or_signal_fails_the_job(
    tmp_path, monkeypatch, failure
):
    barrier = threading.Barrier(4)
    seen = []

    def execute(profile, index, count, output):
        assert profile == "e2e-duckdb" and count == 4
        assert output == tmp_path / str(index)
        barrier.wait(timeout=5)
        seen.append(index)
        return failure if index == 0 and failure is not None else 0

    monkeypatch.setattr(ci, "run", execute)
    assert ci.main(["run-all", "e2e-duckdb", "4", str(tmp_path)]) == int(failure is not None)
    assert sorted(seen) == list(range(4))


def test_each_demo_warehouse_uses_one_job_with_all_logical_workers():
    rows = ci.matrix()["e2e"]
    assert len(rows) == len(ci.combinations()["e2e"])
    assert all(row["index"] == 0 and row["count"] == 4 for row in rows)


def test_complete_matrix_verifies_and_a_missing_shard_or_wrong_wheel_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(ci, "git", lambda *args: SimpleNamespace(stdout="same-source"))
    combined = {}
    monkeypatch.setattr(
        ci, "coverage_report", lambda profile, reports, _: combined.update({profile: reports}) or []
    )
    wheel = tmp_path / "package.whl"
    wheel.write_bytes(b"wheel")
    paths = []
    installed = None
    logical = [
        row | {"index": index}
        for family, rows in ci.matrix().items()
        for row in rows
        for index in (range(row["count"]) if family == "e2e" else [row["index"]])
    ]
    for row in logical:
        profile = row["profile"]
        report = copy.deepcopy(reports()[0])
        node = f"test_{row['index']}"
        report.update(collected=[node], tests=[{"nodeid": node, "outcome": "passed"}])
        report["ci"].update(
            profile=profile,
            index=row["index"],
            count=row["count"],
            expected=[f"test_{index}" for index in range(row["count"])],
        )
        if profile.startswith("coverage-"):
            report.update(
                python=profile.split("-", 2)[1] + ".1",
                platform_key="darwin" if profile.endswith("macos-latest") else "linux",
                marker=None,
            )
        if profile.startswith("e2e-"):
            report["ci"]["warehouse"] = profile.removeprefix("e2e-")
            report["wheel_sha256"] = ci.sha256_of(wheel)
            report["marker"] = "e2e_pip or e2e_uv"
        elif not profile.startswith("coverage-"):
            report["marker"] = ci.SERVICE_MARKERS[profile]
        folder = tmp_path / f"{profile}-{row['index']}"
        folder.mkdir()
        path = folder / "results.json"
        path.write_text(json.dumps(report))
        paths.append(path)
        if profile.startswith("e2e-"):
            installed = path
    for name in ci.VALIDATIONS:
        folder = tmp_path / name
        folder.mkdir()
        (folder / "validation.json").write_text(json.dumps({"name": name, "commit": "same-source"}))
    assert ci.verify(tmp_path, wheel) == []
    # Every combination's coverage also counts what the Engine DB shards reach.
    engine = {path for path in paths if path.parent.name.startswith("engine-")}
    assert engine and all(engine <= set(reports) for reports in combined.values())
    original = installed.read_text()
    wrong = json.loads(original)
    wrong["wheel_sha256"] = "different-wheel"
    installed.write_text(json.dumps(wrong))
    assert any("wrong installed-wheel hash" in problem for problem in ci.verify(tmp_path, wheel))
    installed.write_text(original)
    paths[0].unlink()
    assert any("expected exactly shards" in problem for problem in ci.verify(tmp_path, wheel))


def test_job_failure_or_omission_fails_before_artifact_accounting(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("ETL_CRAFT_CI_JOBS", json.dumps({"test": {"result": "success"}}))
    assert ci.main(["verify", str(tmp_path), str(tmp_path / "not-read.whl")]) == 1
    assert "jobs are missing" in capsys.readouterr().err


def test_python_and_platform_are_checked_for_each_coverage_matrix():
    data = copy.deepcopy(reports()[:1])
    data[0].update(python="3.12.1", platform_key="darwin", marker=None)
    data[0]["ci"].update(profile="coverage-3.11-ubuntu-latest", index=0, count=1)
    data[0]["ci"]["expected"] = ["test_one"]
    assert any(
        "wrong Python/platform" in p
        for p in ci.check_profile("coverage-3.11-ubuntu-latest", data, 1)
    )


def test_a_skipped_partition_does_not_hide_an_entirely_skipped_combination():
    data = reports()
    for report in data:
        report.update(python="3.11.1", platform_key="linux", marker=None)
        report["ci"]["profile"] = "coverage-3.11-ubuntu-latest"
    data[0]["tests"][0]["outcome"] = "skipped"
    data[0]["counts"] = {"skipped": 1}
    assert ci.check_profile("coverage-3.11-ubuntu-latest", data, 2) == []
    data[1]["tests"][0]["outcome"] = "skipped"
    data[1]["counts"] = {"skipped": 1}
    assert any(
        "no tests passed" in problem
        for problem in ci.check_profile("coverage-3.11-ubuntu-latest", data, 2)
    )


def test_missing_coverage_artifact_cannot_pass(tmp_path):
    report = tmp_path / "results.json"
    assert (
        "missing coverage artifacts" in ci.coverage_report("coverage-test", [report], tmp_path)[0]
    )


def test_combined_real_partial_coverage_still_fails_the_ninety_percent_gate(tmp_path):
    from coverage import Coverage

    from etl_craft.core.text import sha256_hex

    part = tmp_path / "part"
    part.mkdir()
    data = Coverage(
        data_file=str(part / "coverage.data"),
        source=["etl_craft"],
        branch=True,
        config_file=False,
    )
    data.set_option("run:relative_files", True)
    data.start()
    try:
        sha256_hex(b"a deliberately incomplete coverage run")
    finally:
        data.stop()
    data.save()
    assert any(
        "coverage report failed" in problem
        for problem in ci.coverage_report("coverage-test", [part / "results.json"], tmp_path)
    )


def test_each_supervision_module_must_reach_ninety_percent(tmp_path, monkeypatch):
    part = tmp_path / "part"
    part.mkdir()
    (part / "coverage.data").write_bytes(b"")
    calls = []

    def coverage(command, **_):
        calls.append(command[3:])
        low = "--include=src/etl_craft/controller/leadership.py" in command
        return subprocess.CompletedProcess(command, 2 if low else 0)

    monkeypatch.setattr(ci.subprocess, "run", coverage)
    problems = ci.coverage_report("coverage-test", [part / "results.json"], tmp_path)
    assert problems == [
        "coverage-test: src/etl_craft/controller/leadership.py is below 90% coverage; "
        "add tests for it"
    ]
    assert {f"--include={module}" for module in ci.SUPERVISION_MODULES} <= {
        argument for call in calls for argument in call
    }


@pytest.mark.parametrize("profile,measured", [("engine", True), ("connections", False)])
def test_the_engine_shards_measure_only_the_supervision_modules(
    tmp_path, monkeypatch, profile, measured
):
    commands = []
    monkeypatch.delenv("PYTEST_ADDOPTS", raising=False)
    monkeypatch.setattr(
        ci.subprocess,
        "run",
        lambda command, **_: commands.append(command) or SimpleNamespace(returncode=0, stdout=""),
    )
    assert ci.run(profile, 0, 1, tmp_path) == 0
    sources = [argument for argument in commands[0] if argument.startswith("--cov=")]
    expected = {"--cov=etl_craft.controller.leadership", "--cov=etl_craft.api.serve"}
    assert (set(sources) >= expected) == measured
    assert len(sources) == (len(ci.SUPERVISION_MODULES) if measured else 0)


def test_environment_selection_cannot_omit_tests_from_ci(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("PYTEST_ADDOPTS", "-k just_one_test")
    assert ci.run("engine", 0, 2, tmp_path / "unused") == 2
    assert "unset PYTEST_ADDOPTS" in capsys.readouterr().err
    assert not (tmp_path / "unused").exists()
