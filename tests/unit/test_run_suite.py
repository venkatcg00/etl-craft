import json
import sys
from pathlib import Path

import pytest

from fixtures.scripts import load

pytestmark = pytest.mark.unit

run_suite = load("run_suite")
suites = load("suites")


def test_an_unknown_suite_is_a_usage_error(capsys):
    assert run_suite.main(["nightly"]) == 2
    assert "unknown suite 'nightly'; known: unit" in capsys.readouterr().err


def test_a_wheel_suite_needs_an_existing_wheel(capsys, tmp_path):
    assert run_suite.main(["package"]) == 2
    assert "pass --wheel PATH" in capsys.readouterr().err
    assert run_suite.main(["package", "--wheel", str(tmp_path / "missing.whl")]) == 2
    assert "wheel not found" in capsys.readouterr().err


def test_the_pytest_command_selects_the_suite_and_writes_evidence(tmp_path):
    suite = suites.load_suites()["package"]
    wheel = tmp_path / "etl_craft.whl"
    command = run_suite.pytest_command(suite, tmp_path / "e.json", wheel, ["-x"])
    assert command[:5] == [sys.executable, "-m", "pytest", "-m", "package"]
    assert f"--evidence={tmp_path / 'e.json'}" in command
    assert "--evidence-suite=package" in command
    assert f"--evidence-wheel={wheel}" in command
    assert command[-1] == "-x"


def test_running_a_suite_records_its_complete_evidence(tmp_path, capsys, monkeypatch):
    root = tmp_path / "repo"
    (root / "release").mkdir(parents=True)
    (root / "release" / "required-suites.toml").write_text('[suites.unit]\nmarker="unit"\n')
    (root / "pyproject.toml").write_text(
        '[project]\nversion="0.1.0"\n[tool.pytest.ini_options]\nmarkers=["unit"]\n'
        'addopts="-k no_test_should_match"\n'
    )
    tests_path = Path(__file__).resolve().parents[1]
    (root / "conftest.py").write_text(
        f"import sys\nsys.path.insert(0, {str(tests_path)!r})\n"
        "pytest_plugins = ['plugins.evidence']\n"
    )
    (root / "test_complete.py").write_text(
        "import pytest\npytestmark=pytest.mark.unit\n"
        "@pytest.mark.parametrize('n', range(3))\ndef test_one(n): pass\n"
    )
    monkeypatch.setattr(run_suite, "REPO_ROOT", root)
    status = run_suite.main(["unit", "--evidence-dir", str(tmp_path), "--", "-q"])
    assert status == 0
    evidence = json.loads((tmp_path / "unit.json").read_text(encoding="utf-8"))
    assert evidence["suite"] == evidence["marker"] == "unit"
    assert evidence["counts"]["passed"] == 3
    assert evidence["collected"] == [f"test_complete.py::test_one[{i}]" for i in range(3)]
    assert evidence["collected"] == [test["nodeid"] for test in evidence["tests"]]
    assert f"evidence for unit: {tmp_path / 'unit.json'}" in capsys.readouterr().out


@pytest.mark.parametrize(
    "extra",
    [
        ["-k", "one"],
        ["-m", "unit"],
        ["-munit"],
        ["--deselect=x"],
        ["tests/unit/test_cli.py"],
        ["--ignore=tests"],
        ["--lf"],
        ["--ff"],
        ["-o", "addopts=-k one"],
        ["--evidence-suite=other"],
        ["-p", "custom"],
    ],
)
def test_evidence_runs_refuse_extra_selection(extra, tmp_path, capsys):
    assert run_suite.main(["unit", "--evidence-dir", str(tmp_path), "--", *extra]) == 2
    assert "not allowed for release evidence" in capsys.readouterr().err
    assert not (tmp_path / "unit.json").exists()


def test_environment_selection_is_refused(monkeypatch, capsys):
    monkeypatch.setenv("PYTEST_ADDOPTS", "-k test_one")
    assert run_suite.main(["unit"]) == 2
    assert "unset PYTEST_ADDOPTS" in capsys.readouterr().err


def test_only_cloud_suites_use_parallel_workers(tmp_path):
    for name, suite in suites.load_suites().items():
        command = run_suite.pytest_command(suite, tmp_path / "e.json", None, [])
        assert ("-n" in command) == (name in {"cloud-snowflake", "cloud-databricks"})
        if "-n" in command:
            assert command[command.index("-n") : command.index("-n") + 4] == [
                "-n",
                "6",
                "--dist",
                "loadgroup",
            ]


def test_parallel_evidence_matches_serial_including_grouped_ids(pytester):
    import subprocess

    tests_path = Path(__file__).resolve().parents[1]
    pytester.makeconftest(
        f"import sys\nsys.path.insert(0, {str(tests_path)!r})\n"
        "pytest_plugins = ['plugins.evidence']\n"
    )
    pytester.makepyfile(
        """import pytest
@pytest.mark.xdist_group('serialized')
@pytest.mark.parametrize('value', ['one@literal', 'two'])
def test_pass(value): pass
@pytest.mark.skip(reason='fixture unavailable')
def test_skip(): pass
@pytest.mark.xfail(reason='expected failure')
def test_xfail(): assert False
def test_failure(): assert False
"""
    )
    recordings = []
    for workers in (0, 4):
        path = pytester.path / f"evidence-{workers}.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "-o",
                "addopts=",
                "-n",
                str(workers),
                "--dist",
                "loadgroup",
                f"--evidence={path}",
            ],
            cwd=pytester.path,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1, result.stdout + result.stderr
        evidence = json.loads(path.read_text())
        recordings.append(
            (
                evidence["collected"],
                evidence["counts"],
                [(row["nodeid"], row["outcome"]) for row in evidence["tests"]],
            )
        )
    assert recordings[0] == recordings[1]
    assert recordings[0][1] == {
        "passed": 2,
        "skipped": 1,
        "xfailed": 1,
        "xpassed": 0,
        "failed": 1,
        "error": 0,
    }


@pytest.mark.parametrize("problem", ["crashed", "missing", "different"])
def test_parallel_evidence_refuses_incomplete_worker_collections(pytestconfig, tmp_path, problem):
    from types import SimpleNamespace

    from plugins.evidence import EvidenceRecorder

    recorder = EvidenceRecorder(pytestconfig, tmp_path / "evidence.json")
    recorder.collected = ["test_one.py::test_one"]
    output = (
        {}
        if problem == "missing"
        else {
            "evidence_collected": [
                "different" if problem == "different" else "test_one.py::test_one"
            ]
        }
    )
    node = SimpleNamespace(workeroutput=output, gateway=SimpleNamespace(id="gw0"))
    recorder.pytest_testnodedown(node, "crashed" if problem == "crashed" else None)
    assert set(recorder.outcomes.values()) == {"error"}
    assert recorder.collected == ["test_one.py::test_one"]
