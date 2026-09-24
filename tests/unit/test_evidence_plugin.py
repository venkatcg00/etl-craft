import hashlib
import json
import subprocess

import pytest

import etl_craft

pytestmark = pytest.mark.unit

OUTCOMES = """
import pytest

@pytest.fixture
def broken():
    raise RuntimeError("postgresql://etl:hunter2@db/prod")

def test_pass():
    pass

def test_fail():
    assert "password=hunter2" == ""

def test_skip():
    pytest.skip("service not running")

def test_error(broken):
    pass

@pytest.mark.xfail(reason="known gap")
def test_xfail():
    assert False

@pytest.mark.xfail(reason="fixed already")
def test_xpass():
    pass
"""


def run_with_evidence(pytester, *args):
    path = pytester.path / "release" / "evidence" / "run.json"
    result = pytester.runpytest(
        "-p", "plugins.evidence", "-p", "no:cacheprovider", f"--evidence={path}", *args
    )
    return result, path


def git(repo, *args):
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
            *args,
        ],
        cwd=repo,
        check=True,
        capture_output=True,
    )


def test_every_outcome_is_counted_and_listed(pytester):
    pytester.makepyfile(test_outcomes=OUTCOMES)
    result, path = run_with_evidence(pytester, "--evidence-suite=unit", "-m", "not slow")
    evidence = json.loads(path.read_text(encoding="utf-8"))

    assert evidence["schema"] == 1
    assert evidence["suite"] == "unit"
    assert evidence["marker"] == "not slow"
    assert evidence["package_version"] == etl_craft.__version__
    assert evidence["exit_status"] == result.ret == 1
    assert evidence["counts"] == {
        "passed": 1,
        "skipped": 1,
        "xfailed": 1,
        "xpassed": 1,
        "failed": 1,
        "error": 1,
    }
    outcomes = {test["nodeid"].split("::")[1]: test["outcome"] for test in evidence["tests"]}
    assert outcomes == {
        "test_pass": "passed",
        "test_fail": "failed",
        "test_skip": "skipped",
        "test_error": "error",
        "test_xfail": "xfailed",
        "test_xpass": "xpassed",
    }


def test_no_failure_text_reaches_the_evidence_file(pytester):
    pytester.makepyfile(test_outcomes=OUTCOMES)
    _, path = run_with_evidence(pytester)
    text = path.read_text(encoding="utf-8")
    assert "hunter2" not in text
    assert "service not running" not in text


def test_a_collection_error_is_recorded_as_an_error(pytester):
    pytester.makepyfile(test_broken="import missing_module_for_evidence_test\n")
    _, path = run_with_evidence(pytester)
    evidence = json.loads(path.read_text(encoding="utf-8"))
    assert evidence["counts"]["error"] == 1
    assert evidence["tests"] == [{"nodeid": "test_broken.py", "outcome": "error", "duration": 0.0}]


def test_outside_a_git_repository_there_is_no_commit(pytester):
    pytester.makepyfile(test_one="def test_one():\n    pass\n")
    _, path = run_with_evidence(pytester)
    evidence = json.loads(path.read_text(encoding="utf-8"))
    assert evidence["commit"] is None
    assert evidence["dirty"] is None


def test_the_commit_and_uncommitted_changes_are_recorded(pytester):
    pytester.makepyfile(test_one="def test_one():\n    pass\n")
    (pytester.path / ".gitignore").write_text("__pycache__/\n", encoding="utf-8")
    git(pytester.path, "init", "-q")
    git(pytester.path, "add", ".")
    git(pytester.path, "commit", "-q", "-m", "tests")
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=pytester.path, capture_output=True, text=True, check=True
    ).stdout.strip()

    # Evidence files themselves never make the tree dirty.
    _, path = run_with_evidence(pytester)
    clean = json.loads(path.read_text(encoding="utf-8"))
    assert clean["commit"] == head
    assert clean["dirty"] is False

    (pytester.path / "test_one.py").write_text("def test_one():\n    assert 1\n", encoding="utf-8")
    _, path = run_with_evidence(pytester)
    assert json.loads(path.read_text(encoding="utf-8"))["dirty"] is True


def test_the_tested_wheel_is_identified_by_its_hash(pytester):
    pytester.makepyfile(test_one="def test_one():\n    pass\n")
    wheel = pytester.path / "etl_craft-0.1.0-py3-none-any.whl"
    wheel.write_bytes(b"wheel bytes")
    _, path = run_with_evidence(pytester, f"--evidence-wheel={wheel}")
    evidence = json.loads(path.read_text(encoding="utf-8"))
    assert evidence["wheel_sha256"] == hashlib.sha256(b"wheel bytes").hexdigest()
