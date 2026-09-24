"""Build the versioned documentation site that the Docs workflow deploys to GitHub Pages.

Usage: ``python scripts/build_docs_site.py OUT_DIR [--repo PATH]``

The site has one directory per version and a version selector:

- ``dev`` is built from the repository's working tree;
- each release line ``X.Y`` is built from its newest ``vX.Y.Z`` tag, in a temporary worktree
  with that tag's own documentation dependencies;
- the newest release line also gets the alias ``latest``.

The site root redirects to ``latest``, or to ``dev`` before the first release. mike assembles
the versions on the local branch ``docs-site-build``, which exists only while the script runs;
the finished site is written to OUT_DIR, which must not exist yet.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BUILD_BRANCH = "docs-site-build"
RELEASE_TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
# mike commits each version to the build branch; this identity signs those local commits.
COMMITTER = {
    "GIT_COMMITTER_NAME": "etl-craft documentation build",
    "GIT_COMMITTER_EMAIL": "docs-build@localhost",
}


def release_lines(tags: Iterable[str]) -> list[tuple[str, str]]:
    """Return ``(version, tag)`` per release line, oldest line first, using each newest tag.

    Only ``vMAJOR.MINOR.PATCH`` tags count; the version of ``v0.1.2`` is ``0.1``.
    """
    newest: dict[tuple[int, int], tuple[int, str]] = {}
    for tag in tags:
        match = RELEASE_TAG.match(tag)
        if match is None:
            continue
        major, minor, patch = (int(part) for part in match.groups())
        line = (major, minor)
        if line not in newest or patch > newest[line][0]:
            newest[line] = (patch, tag)
    return [(f"{major}.{minor}", newest[(major, minor)][1]) for major, minor in sorted(newest)]


def deploy_args(version: str, *, latest: bool) -> list[str]:
    """Build the ``mike deploy`` arguments that add ``version`` to the build branch."""
    args = ["deploy", "--branch", BUILD_BRANCH, "--alias-type", "copy"]
    if latest:
        return [*args, "--update-aliases", version, "latest"]
    return [*args, version]


def mike_executable() -> str | None:
    """Return the ``mike`` command installed next to the running interpreter, if any."""
    return shutil.which("mike", path=str(Path(sys.executable).parent))


def _run(command: Sequence[str], cwd: Path, env: Mapping[str, str] | None = None) -> str:
    result = subprocess.run(
        list(command), cwd=cwd, env=env, check=True, capture_output=True, text=True
    )
    return result.stdout


def _branch_exists(repo: Path) -> bool:
    probe = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", f"refs/heads/{BUILD_BRANCH}"],
        cwd=repo,
        capture_output=True,
        check=False,
    )
    return probe.returncode == 0


def _build_release(
    repo: Path, version: str, tag: str, *, latest: bool, env: Mapping[str, str]
) -> None:
    with tempfile.TemporaryDirectory() as scratch:
        tree = Path(scratch) / "tree"
        _run(["git", "worktree", "add", "--detach", str(tree), tag], cwd=repo)
        try:
            uv_mike = ["uv", "run", "--locked", "--no-default-groups", "--group", "docs", "mike"]
            _run([*uv_mike, *deploy_args(version, latest=latest)], cwd=tree, env=env)
        finally:
            subprocess.run(
                ["git", "worktree", "remove", "--force", str(tree)],
                cwd=repo,
                capture_output=True,
                check=False,
            )


def _export(repo: Path, out: Path) -> None:
    out.mkdir(parents=True)
    archive = subprocess.Popen(
        ["git", "archive", "--format=tar", BUILD_BRANCH], cwd=repo, stdout=subprocess.PIPE
    )
    subprocess.run(["tar", "-x", "-C", str(out)], stdin=archive.stdout, check=True)
    if archive.stdout is not None:
        archive.stdout.close()
    if archive.wait() != 0:
        raise subprocess.CalledProcessError(archive.returncode, "git archive")


def build(repo: Path, out: Path, mike: str) -> list[str]:
    """Build every version into ``out`` and return the version names, newest release first."""
    lines = release_lines(_run(["git", "tag", "--list"], cwd=repo).split())
    env = {**os.environ, **COMMITTER}
    # Each release is built in its own worktree environment, not the one running this script.
    env.pop("VIRTUAL_ENV", None)
    try:
        for index, (version, tag) in enumerate(lines):
            _build_release(repo, version, tag, latest=index == len(lines) - 1, env=env)
        _run([mike, *deploy_args("dev", latest=False)], cwd=repo, env=env)
        default = "latest" if lines else "dev"
        _run([mike, "set-default", "--branch", BUILD_BRANCH, default], cwd=repo, env=env)
        _export(repo, out)
    finally:
        subprocess.run(
            ["git", "branch", "-D", BUILD_BRANCH], cwd=repo, capture_output=True, check=False
        )
    return [version for version, _ in reversed(lines)] + ["dev"]


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description="Build the versioned documentation site.")
    parser.add_argument("out", type=Path, help="directory to write the site to (must not exist)")
    parser.add_argument("--repo", type=Path, default=REPO_ROOT, help="the repository to build")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Build the site; return 0, 1 when a build step fails, or 2 for a usage error."""
    args = parse_args(sys.argv[1:] if argv is None else argv)
    out: Path = args.out.resolve()
    repo: Path = args.repo.resolve()
    if out.exists():
        print(f"{out} already exists; remove it or choose another directory", file=sys.stderr)
        return 2
    if _branch_exists(repo):
        print(
            f"branch {BUILD_BRANCH} already exists in {repo}; delete it with "
            f"`git branch -D {BUILD_BRANCH}` and run again",
            file=sys.stderr,
        )
        return 2
    mike = mike_executable()
    if mike is None:
        print("mike is not installed; run `uv sync --group docs`", file=sys.stderr)
        return 2
    try:
        versions = build(repo, out, mike)
    except subprocess.CalledProcessError as error:
        command = error.cmd if isinstance(error.cmd, str) else " ".join(error.cmd)
        print(f"documentation build failed: {command}", file=sys.stderr)
        if error.stderr:
            print(error.stderr, file=sys.stderr)
        return 1
    print(f"site with versions {', '.join(versions)} written to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
