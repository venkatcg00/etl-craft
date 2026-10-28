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


def test_approved_pull_request_python_platform_and_warehouse_matrix_is_planned():
    groups = ci.combinations()
    versions = {row["python"] for row in groups["coverage"]}
    assert {"3.11", "3.12", "3.13"} <= versions
    for version in versions:
        assert any(
            row["python"] == version and row["os"] == "ubuntu-latest" for row in groups["coverage"]
        )
    assert {row["python"] for row in groups["macos"] if row["os"] == "macos-latest"} == {"3.13"}
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
    assert all(row["index"] == 0 and row["count"] == ci.SHARDS["e2e"] for row in rows)


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
        node = f"{profile}::test_{row['index']}"
        report.update(collected=[node], tests=[{"nodeid": node, "outcome": "passed"}])
        report["ci"].update(
            profile=profile,
            index=row["index"],
            count=row["count"],
            expected=[f"{profile}::test_{index}" for index in range(row["count"])],
        )
        if profile.startswith("coverage-"):
            report.update(
                python=profile.split("-", 2)[1] + ".1",
                platform_key="darwin" if profile.endswith("macos-latest") else "linux",
                marker=ci.selection(profile),
            )
        if profile.startswith("e2e-"):
            report["ci"]["warehouse"] = profile.removeprefix("e2e-")
            report["wheel_sha256"] = ci.sha256_of(wheel)
            report["marker"] = ci.selection(profile)
        elif not profile.startswith("coverage-"):
            report["marker"] = ci.SERVICE_MARKERS[profile]
        folder = tmp_path / f"{profile}-{row['index']}"
        folder.mkdir()
        path = folder / "results.json"
        path.write_text(json.dumps(report))
        paths.append(path)
        if profile.startswith("e2e-"):
            installed = path
    catalog = {}
    primary_nodes = [
        f"{ci.PRIMARY}::test_{index}" for index in range(ci.SHARDS["measured-coverage"])
    ]
    for path in paths:
        report = json.loads(path.read_text())
        profile = report["ci"]["profile"]
        if profile.startswith("coverage-") and profile != ci.PRIMARY:
            report.update(
                collected=primary_nodes,
                tests=[{"nodeid": node, "outcome": "passed"} for node in primary_nodes],
                counts={"passed": len(primary_nodes)},
            )
            report["ci"]["expected"] = primary_nodes
        else:
            marks = (
                ["unit"]
                if profile == ci.PRIMARY
                else [
                    {
                        "engine": "engine_postgres",
                        "warehouse-local": "harness",
                        "warehouse-iceberg": "warehouse_trino_iceberg",
                    }.get(profile, "e2e_pip")
                ]
            )
            if profile.startswith("e2e-"):
                old = report["collected"][0]
                node = old + "-" + profile.removeprefix("e2e-") + "]"
                report["collected"] = [node]
                report["tests"] = [{"nodeid": node, "outcome": "passed"}]
                report["ci"]["expected"] = [node]
            catalog.update(dict.fromkeys(report["ci"]["expected"], marks))
        path.write_text(json.dumps(report))
    for path in paths:
        report = json.loads(path.read_text())
        report["ci"]["catalog"] = catalog
        path.write_text(json.dumps(report))
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
    next(path for path in paths if path.parent.name.startswith("engine-")).unlink()
    assert any("expected exactly shards" in problem for problem in ci.verify(tmp_path, wheel))


def test_job_failure_or_omission_fails_before_artifact_accounting(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("ETL_CRAFT_CI_JOBS", json.dumps({"test": {"result": "success"}}))
    assert ci.main(["verify", str(tmp_path), str(tmp_path / "not-read.whl")]) == 1
    assert "jobs are missing" in capsys.readouterr().err


def test_python_and_platform_are_checked_for_each_coverage_matrix():
    data = copy.deepcopy(reports()[:1])
    data[0].update(
        python="3.12.1", platform_key="darwin", marker=ci.selection("coverage-3.11-ubuntu-latest")
    )
    data[0]["ci"].update(profile="coverage-3.11-ubuntu-latest", index=0, count=1)
    data[0]["ci"]["expected"] = ["test_one"]
    assert any(
        "wrong Python/platform" in p
        for p in ci.check_profile("coverage-3.11-ubuntu-latest", data, 1)
    )


def test_a_skipped_partition_does_not_hide_an_entirely_skipped_combination():
    data = reports()
    for report in data:
        report.update(
            python="3.11.1",
            platform_key="linux",
            marker=ci.selection("coverage-3.11-ubuntu-latest"),
        )
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


@pytest.mark.parametrize("profile,measured", [("engine", True), ("warehouse-local", False)])
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


def test_worker_missing_outcome_cannot_pass_verification():
    data = reports()
    data[1]["tests"] = []
    data[1]["counts"] = {"passed": 0}
    problems = ci.check_profile("engine", data, 2)
    assert any("outcomes do not cover exactly" in problem for problem in problems)


def test_only_one_linux_python_combination_measures_project_coverage(tmp_path, monkeypatch):
    commands = []
    monkeypatch.delenv("PYTEST_ADDOPTS", raising=False)
    monkeypatch.setattr(
        ci.subprocess,
        "run",
        lambda command, **_: commands.append(command) or SimpleNamespace(returncode=0, stdout=""),
    )
    profiles = ci.combinations()["coverage"]
    for row in profiles:
        assert ci.run(row["profile"], 0, 1, tmp_path / row["profile"]) == 0
    measured = [row for row, command in zip(profiles, commands, strict=True) if "--cov" in command]
    assert len(measured) == 1 and measured[0]["os"] == "ubuntu-latest"
    assert all(
        "-n" in command and "logical" in command and "worksteal" in command for command in commands
    )


def test_pull_request_matrix_fits_runner_concurrency_limits():
    plan = ci.matrix()
    # Package jobs wait for static, so static and package cannot overlap.
    peak = sum(len(rows) for rows in plan.values()) + max(len(ci.VALIDATIONS), 1)
    assert peak <= 20
    mac = sum(row.get("os") == "macos-latest" for rows in plan.values() for row in rows)
    assert mac + 1 <= 5


def test_verify_rejects_a_result_lost_from_an_xdist_worker(pytester, monkeypatch):
    from pathlib import Path

    tests = Path(__file__).parents[1]
    monkeypatch.setenv("ETL_CRAFT_CI_PROFILE", "engine")
    monkeypatch.setenv("ETL_CRAFT_REQUIRE_SERVICES", "1")
    pytester.makeconftest(f"""
import sys
sys.path.insert(0, {str(tests)!r})
pytest_plugins = ["plugins.evidence", "plugins.ci_shards"]
from plugins.evidence import EvidenceRecorder
original = EvidenceRecorder.pytest_runtest_logreport

def omit_one(self, report):
    if not report.nodeid.endswith("test_lost"):
        original(self, report)

EvidenceRecorder.pytest_runtest_logreport = omit_one
""")
    pytester.makeini("[pytest]\nmarkers = engine_postgres: local test\n")
    pytester.makepyfile("""
import pytest
pytestmark = pytest.mark.engine_postgres

def test_kept():
    assert True

def test_lost():
    assert True
""")
    evidence = pytester.path / "results.json"
    result = pytester.runpytest_subprocess(
        "-q",
        "-n",
        "2",
        "--dist",
        "worksteal",
        "--ci-shard=0/1",
        "--evidence=" + str(evidence),
        "-m",
        ci.SERVICE_MARKERS["engine"],
    )
    result.assert_outcomes(passed=2)
    report = json.loads(evidence.read_text())
    assert len(report["collected"]) == 2
    assert len(report["tests"]) == 1
    assert any(
        "outcomes do not cover exactly" in problem
        for problem in ci.check_profile("engine", [report], 1)
    )


def test_complete_pr_ownership_refuses_missing_duplicate_and_unknown_cases():
    catalog = {
        "unit_case": ["unit"],
        "sqlite_case": ["engine_sqlite"],
        "postgres_case": ["engine_postgres"],
        "cloud_case": ["cloud_snowflake"],
    }
    groups = {
        ci.PRIMARY: [{"ci": {"catalog": catalog, "expected": ["unit_case", "sqlite_case"]}}],
        "engine": [{"ci": {"expected": ["postgres_case"]}}],
    }
    assert ci.check_collections(groups) == []
    groups["engine"][0]["ci"]["expected"].append("sqlite_case")
    assert any("duplicated ownership" in p for p in ci.check_collections(groups))
    groups["engine"][0]["ci"]["expected"] = []
    assert any("missing" in p for p in ci.check_collections(groups))
    catalog["new_case"] = ["new_suite"]
    assert any("unassigned" in p for p in ci.check_collections(groups))


def test_platform_jobs_run_units_and_sqlite_transitions_without_complete_engine_duplication(
    tmp_path, monkeypatch
):
    commands = []
    monkeypatch.delenv("PYTEST_ADDOPTS", raising=False)
    monkeypatch.setattr(
        ci.subprocess,
        "run",
        lambda command, **_: commands.append(command) or SimpleNamespace(returncode=0, stdout=""),
    )
    ci.run("coverage-3.13-macos-latest", 0, 1, tmp_path)
    assert "tests/unit" not in commands[0]
    assert commands[0][commands[0].index("-m", 3) + 1] == "unit or engine_sqlite"
    assert ci.selection("engine") == "engine_postgres and not warehouse_postgres"


def test_every_collected_marker_has_a_complete_pr_or_existing_cloud_owner():
    from plugins.suites import suite_markers

    for marker in suite_markers() - {"chaos", "cluster"}:
        assert ci.assigned_profile("test_case", [marker]) != "unassigned", marker


@pytest.mark.parametrize("profile", ["coverage-3.11-ubuntu-latest", "coverage-3.13-macos-latest"])
def test_unfiltered_catalog_survives_marker_selection_and_xdist(pytester, monkeypatch, profile):
    from pathlib import Path

    monkeypatch.setenv("ETL_CRAFT_CI_PROFILE", profile)
    tests = Path(__file__).parents[1]
    pytester.makeconftest(
        f"import sys\nsys.path.insert(0, {str(tests)!r})\n"
        "pytest_plugins = ['plugins.evidence', 'plugins.ci_shards']\n"
    )
    pytester.makeini("[pytest]\nmarkers = unit: units\n engine_postgres: engine\n")
    pytester.makepyfile(
        "import pytest\n@pytest.mark.unit\ndef test_kept(): pass\n"
        "@pytest.mark.engine_postgres\ndef test_elsewhere(): pass\n"
    )
    evidence = pytester.path / "results.json"
    result = pytester.runpytest_subprocess(
        "-q", "-n", "2", "-m", "unit", "--ci-shard=0/1", "--evidence=" + str(evidence)
    )
    result.assert_outcomes(passed=1)
    report = json.loads(evidence.read_text())
    assert len(report["ci"]["expected"]) == 1
    assert len(report["ci"]["catalog"]) == 2


def test_unfiltered_catalog_disagreement_between_shards_fails():
    data = reports()
    data[0]["ci"]["catalog"] = {"test_one": ["engine_postgres"]}
    data[1]["ci"]["catalog"] = {"test_two": ["engine_postgres"]}
    assert any("unfiltered collections disagree" in p for p in ci.check_profile("engine", data, 2))
