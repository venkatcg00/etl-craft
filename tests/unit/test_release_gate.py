import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from fixtures.scripts import load

pytestmark = pytest.mark.unit

gate = load("release_gate")
suites_module = load("suites")

VERSION = "1.0.0"
SUITES_TOML = """
[suites.unit]
marker = "unit"

[suites.package]
marker = "package"
wheel = true
"""
WHEEL_SHA = hashlib.sha256(b"the wheel").hexdigest()


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
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
        text=True,
    ).stdout.strip()


def commit_all(repo: Path, message: str) -> str:
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", message)
    return git(repo, "rev-parse", "HEAD")


@pytest.fixture
def repo(tmp_path):
    root = tmp_path / "repo"
    (root / "release").mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        f'[project]\nname = "demo"\nversion = "{VERSION}"\n', encoding="utf-8"
    )
    (root / "release" / "required-suites.toml").write_text(SUITES_TOML, encoding="utf-8")
    (root / "module.py").write_text("x = 1\n", encoding="utf-8")
    git(root, "init", "-q", "-b", "main")
    commit_all(root, "initial")
    return root


def suites(repo):
    return suites_module.load_suites(repo / "release" / "required-suites.toml")


def write_evidence(repo: Path, name: str, **overrides) -> None:
    evidence = {
        "schema": 1,
        "suite": name,
        "marker": name,
        "package_version": VERSION,
        "commit": git(repo, "rev-parse", "HEAD"),
        "dirty": False,
        "wheel_sha256": WHEEL_SHA if name == "package" else None,
        "exit_status": 0,
        "counts": {"passed": 3, "failed": 0, "error": 0, "skipped": 0, "xfailed": 0, "xpassed": 0},
        "tests": [],
    }
    evidence.update(overrides)
    path = repo / "release" / "evidence" / VERSION / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(evidence), encoding="utf-8")


def problems(repo, wheel=None):
    return {
        result.suite: result.problems for result in gate.check(VERSION, repo, suites(repo), wheel)
    }


def test_passing_evidence_for_every_suite_is_releasable(repo, capsys):
    write_evidence(repo, "unit")
    write_evidence(repo, "package")
    commit_all(repo, "record evidence")
    assert problems(repo) == {"unit": [], "package": []}
    assert gate.main(["--repo", str(repo)]) == 0
    assert capsys.readouterr().out.splitlines() == [
        "release gate for 1.0.0",
        "  OK    unit",
        "  OK    package",
        "releasable",
    ]


def test_a_suite_without_evidence_blocks_the_release(repo, capsys):
    write_evidence(repo, "unit")
    assert problems(repo)["package"] == ["no evidence (release/evidence/1.0.0/package.json)"]
    assert gate.main(["--repo", str(repo)]) == 1
    assert capsys.readouterr().out.endswith("not releasable\n")


@pytest.mark.parametrize(
    ("overrides", "problem"),
    [
        ({"dirty": True}, "recorded from a working tree with uncommitted changes"),
        ({"dirty": None}, "recorded from a working tree with uncommitted changes"),
        ({"counts": {"passed": 2, "skipped": 1}}, "1 skipped"),
        ({"counts": {"passed": 2, "failed": 1}}, "1 failed"),
        ({"counts": {"passed": 2, "error": 1}}, "1 error"),
        ({"counts": {"passed": 2, "xfailed": 1}}, "1 xfailed"),
        ({"counts": {"passed": 0}}, "no tests ran"),
        ({"exit_status": 1}, "pytest exited with status 1"),
        ({"package_version": "0.9.0"}, "recorded at version '0.9.0', not 1.0.0"),
        ({"suite": "other"}, "evidence is for suite 'other'"),
        ({"schema": 2}, "unsupported evidence schema 2"),
        ({"commit": None}, "no commit recorded"),
    ],
)
def test_evidence_that_does_not_show_a_clean_pass_is_rejected(repo, overrides, problem):
    write_evidence(repo, "unit", **overrides)
    assert problem in problems(repo)["unit"]


def test_a_wheel_suite_must_record_its_wheel(repo):
    write_evidence(repo, "package", wheel_sha256=None)
    assert "no wheel recorded; run the suite with --wheel" in problems(repo)["package"]


def test_unreadable_evidence_is_rejected(repo):
    write_evidence(repo, "unit")
    path = repo / "release" / "evidence" / VERSION / "unit.json"
    path.write_text("{not json", encoding="utf-8")
    assert problems(repo)["unit"][0].startswith("unreadable evidence")
    path.write_text("[1, 2]", encoding="utf-8")
    assert problems(repo)["unit"] == ["unreadable evidence: not a JSON object"]


def test_evidence_from_a_commit_outside_the_history_is_rejected(repo):
    git(repo, "checkout", "-q", "-b", "side")
    (repo / "module.py").write_text("x = 2\n", encoding="utf-8")
    side = commit_all(repo, "side change")
    git(repo, "checkout", "-q", "main")
    write_evidence(repo, "unit", commit=side)
    assert problems(repo)["unit"] == [f"commit {side[:12]} is not an ancestor of HEAD"]


def test_code_changed_after_the_evidence_invalidates_it(repo):
    write_evidence(repo, "unit")
    (repo / "module.py").write_text("x = 3\n", encoding="utf-8")
    commit_all(repo, "change code after testing")
    assert problems(repo)["unit"] == ["changed since the evidence was recorded: module.py"]


def test_changelog_and_release_notes_may_change_after_the_evidence(repo):
    write_evidence(repo, "unit")
    (repo / "CHANGELOG.md").write_text("## 1.0.0\n", encoding="utf-8")
    (repo / "docs" / "release-notes").mkdir(parents=True)
    (repo / "docs" / "release-notes" / "1.0.0.md").write_text("Notes\n", encoding="utf-8")
    commit_all(repo, "release notes")
    assert problems(repo)["unit"] == []


def test_wheel_suites_must_all_test_the_same_wheel(repo, tmp_path):
    two_wheels = SUITES_TOML + '\n[suites.e2e]\nmarker = "e2e"\nwheel = true\n'
    (repo / "release" / "required-suites.toml").write_text(two_wheels, encoding="utf-8")
    write_evidence(repo, "package")
    write_evidence(repo, "e2e", wheel_sha256="0" * 64)
    found = problems(repo)
    assert "wheel-bound suites tested different wheels" in found["package"]
    assert "wheel-bound suites tested different wheels" in found["e2e"]


def test_the_released_wheel_must_be_the_tested_one(repo, tmp_path):
    write_evidence(repo, "unit")
    write_evidence(repo, "package")
    tested = tmp_path / "tested.whl"
    tested.write_bytes(b"the wheel")
    other = tmp_path / "other.whl"
    other.write_bytes(b"a rebuilt wheel")
    assert problems(repo, wheel=tested)["package"] == []
    assert problems(repo, wheel=other)["package"] == [f"tested a different wheel than {other}"]
