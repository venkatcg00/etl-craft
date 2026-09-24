"""Decide whether HEAD can be released: every required suite needs passing evidence.

Usage: ``python scripts/release_gate.py [--version V] [--wheel PATH] [--repo DIR]``

Each suite in release/required-suites.toml must have an evidence file under
``release/evidence/<version>/`` that:

- names the suite and the version being released;
- was recorded from a clean working tree, on a commit that is an ancestor of HEAD;
- ran at least one test, and every test passed (no failures, errors, skips or xfails);
- has, since its commit, only evidence, changelog or release-note changes on top;
- for suites that test the wheel, records the same wheel as every other such suite, and the
  wheel given with ``--wheel`` when there is one.

Exits 0 when HEAD is releasable and 1 when it is not.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from suites import REPO_ROOT, Suite, evidence_dir, load_suites, project_version

SCHEMA = 1
NOT_PASSING = ("failed", "error", "skipped", "xfailed", "xpassed")
ALLOWED_AFTER_EVIDENCE = ("release/evidence/", "CHANGELOG.md", "docs/release-notes/")


@dataclass
class SuiteResult:
    """The gate's verdict on one suite."""

    suite: str
    problems: list[str] = field(default_factory=list)
    wheel_sha256: str | None = None

    @property
    def ok(self) -> bool:
        """Report whether the suite's evidence passed every check."""
        return not self.problems


def git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Run a git command in ``repo`` and return the completed process."""
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=False
    )


def sha256_of(path: Path) -> str:
    """Return the hex sha256 of a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def changes_since(repo: Path, commit: str) -> list[str] | None:
    """Return the paths changed between ``commit`` and HEAD, or None when git cannot tell."""
    result = git(repo, "diff", "--name-only", commit, "HEAD")
    if result.returncode != 0:
        return None
    return [line for line in result.stdout.splitlines() if line]


def check_evidence(suite: Suite, evidence: dict[str, Any], version: str, repo: Path) -> list[str]:
    """Return every reason ``evidence`` does not show ``suite`` passing at ``version``."""
    problems = []
    if evidence.get("schema") != SCHEMA:
        problems.append(f"unsupported evidence schema {evidence.get('schema')!r}")
    if evidence.get("suite") != suite.name:
        problems.append(f"evidence is for suite {evidence.get('suite')!r}")
    if evidence.get("package_version") != version:
        problems.append(f"recorded at version {evidence.get('package_version')!r}, not {version}")
    if evidence.get("dirty") is not False:
        problems.append("recorded from a working tree with uncommitted changes")

    counts = evidence.get("counts") or {}
    if sum(int(value) for value in counts.values()) == 0:
        problems.append("no tests ran")
    for outcome in NOT_PASSING:
        if counts.get(outcome):
            problems.append(f"{counts[outcome]} {outcome}")
    if evidence.get("exit_status") != 0:
        problems.append(f"pytest exited with status {evidence.get('exit_status')!r}")
    if suite.wheel and not evidence.get("wheel_sha256"):
        problems.append("no wheel recorded; run the suite with --wheel")

    commit = evidence.get("commit")
    if not commit:
        problems.append("no commit recorded")
    elif git(repo, "merge-base", "--is-ancestor", str(commit), "HEAD").returncode != 0:
        problems.append(f"commit {str(commit)[:12]} is not an ancestor of HEAD")
    else:
        changed = changes_since(repo, str(commit))
        if changed is None:
            problems.append(f"cannot compare commit {str(commit)[:12]} with HEAD")
        else:
            other = [path for path in changed if not path.startswith(ALLOWED_AFTER_EVIDENCE)]
            if other:
                problems.append(f"changed since the evidence was recorded: {', '.join(other)}")
    return problems


def check_suite(suite: Suite, version: str, directory: Path, repo: Path) -> SuiteResult:
    """Load and check one suite's evidence file."""
    result = SuiteResult(suite.name)
    path = directory / f"{suite.name}.json"
    if not path.is_file():
        result.problems.append(f"no evidence ({path.relative_to(repo)})")
        return result
    try:
        evidence = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        result.problems.append(f"unreadable evidence: {error}")
        return result
    if not isinstance(evidence, dict):
        result.problems.append("unreadable evidence: not a JSON object")
        return result
    result.problems.extend(check_evidence(suite, evidence, version, repo))
    if suite.wheel and evidence.get("wheel_sha256"):
        result.wheel_sha256 = str(evidence["wheel_sha256"])
    return result


def check_wheels(results: list[SuiteResult], wheel: Path | None) -> None:
    """Require every wheel-bound suite to have tested the same wheel, and ``wheel`` if given."""
    recorded = {result.wheel_sha256 for result in results if result.wheel_sha256}
    expected = sha256_of(wheel) if wheel is not None else None
    for result in results:
        if result.wheel_sha256 is None:
            continue
        if expected is not None and result.wheel_sha256 != expected:
            result.problems.append(f"tested a different wheel than {wheel}")
        elif expected is None and len(recorded) > 1:
            result.problems.append("wheel-bound suites tested different wheels")


def check(
    version: str, repo: Path, suites: dict[str, Suite], wheel: Path | None = None
) -> list[SuiteResult]:
    """Check every suite's evidence for ``version`` and return one result per suite."""
    directory = evidence_dir(version, repo)
    results = [check_suite(suite, version, directory, repo) for suite in suites.values()]
    check_wheels(results, wheel)
    return results


def report(results: list[SuiteResult], version: str) -> str:
    """Render the per-suite verdicts and the overall decision."""
    lines = [f"release gate for {version}"]
    for result in results:
        if result.ok:
            lines.append(f"  OK    {result.suite}")
        else:
            lines.append(f"  FAIL  {result.suite}: {'; '.join(result.problems)}")
    releasable = all(result.ok for result in results)
    lines.append("releasable" if releasable else "not releasable")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the gate and return 0 when releasable, 1 when not."""
    parser = argparse.ArgumentParser(description="Check release evidence for every suite.")
    parser.add_argument("--version", help="the version to release (default: pyproject.toml)")
    parser.add_argument("--wheel", type=Path, help="the wheel being released")
    parser.add_argument("--repo", type=Path, default=REPO_ROOT, help="repository root")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    repo = args.repo.resolve()
    version = args.version or project_version(repo)
    suites = load_suites(repo / "release" / "required-suites.toml")
    results = check(version, repo, suites, args.wheel)
    print(report(results, version))
    return 0 if all(result.ok for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
