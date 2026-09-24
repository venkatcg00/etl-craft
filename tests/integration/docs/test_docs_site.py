"""Build the published documentation site from a clone of this repository."""

import json
import subprocess
from pathlib import Path

import pytest

from fixtures.scripts import SCRIPTS_DIR, load

pytestmark = pytest.mark.docs

build_docs_site = load("build_docs_site")
REPO_ROOT = SCRIPTS_DIR.parent
SITE_URL = "https://venkatcg00.github.io/etl-craft"


def _git(repo, *args):
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture
def clone(tmp_path):
    """A clone of the current commit with no tags, so the test controls the releases."""
    head = _git(REPO_ROOT, "rev-parse", "HEAD")
    repo = tmp_path / "repo"
    _git(tmp_path, "clone", "--quiet", "--shared", "--no-checkout", str(REPO_ROOT), str(repo))
    _git(repo, "checkout", "--quiet", "--detach", head)
    tags = _git(repo, "tag", "--list").split()
    if tags:
        _git(repo, "tag", "--delete", *tags)
    return repo


def _versions(site: Path):
    entries = json.loads((site / "versions.json").read_text(encoding="utf-8"))
    return {entry["version"]: entry["aliases"] for entry in entries}


def test_before_the_first_release_the_site_is_the_dev_version(clone, tmp_path):
    site = tmp_path / "site"
    assert build_docs_site.main([str(site), "--repo", str(clone)]) == 0

    assert _versions(site) == {"dev": []}
    assert 'url=dev/"' in (site / "index.html").read_text(encoding="utf-8")
    page = (site / "dev" / "reference" / "exit-codes" / "index.html").read_text(encoding="utf-8")
    assert f'<link rel="canonical" href="{SITE_URL}/dev/reference/exit-codes/">' in page
    assert _git(clone, "branch", "--list", "docs-site-build") == ""


def test_each_release_line_is_built_and_the_newest_is_latest(clone, tmp_path):
    _git(clone, "tag", "v0.1.0")
    _git(clone, "tag", "v0.1.1")
    site = tmp_path / "site"
    assert build_docs_site.main([str(site), "--repo", str(clone)]) == 0

    assert _versions(site) == {"0.1": ["latest"], "dev": []}
    assert 'url=latest/"' in (site / "index.html").read_text(encoding="utf-8")
    for version in ("0.1", "latest", "dev"):
        assert (site / version / "index.html").is_file()
    assert _git(clone, "worktree", "list").count("\n") == 0
