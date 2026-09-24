import subprocess

import pytest

from fixtures.scripts import load

pytestmark = pytest.mark.unit

build_docs_site = load("build_docs_site")


def test_each_release_line_uses_its_newest_patch_tag():
    tags = ["v0.1.0", "v0.1.2", "v0.1.1", "v0.2.0"]
    assert build_docs_site.release_lines(tags) == [("0.1", "v0.1.2"), ("0.2", "v0.2.0")]


def test_release_lines_are_ordered_by_number_not_text():
    tags = ["v0.10.0", "v0.9.3", "v1.0.0"]
    versions = [version for version, _ in build_docs_site.release_lines(tags)]
    assert versions == ["0.9", "0.10", "1.0"]


def test_only_release_tags_count():
    tags = ["archive/iteration-2", "v0.1.0rc1", "v0.1", "0.1.0", "v0.1.0-beta", "release"]
    assert build_docs_site.release_lines(tags) == []


def test_deploy_arguments_add_the_latest_alias_only_when_asked():
    common = ["deploy", "--branch", "docs-site-build", "--alias-type", "copy"]
    assert build_docs_site.deploy_args("dev", latest=False) == [*common, "dev"]
    assert build_docs_site.deploy_args("0.1", latest=True) == [
        *common,
        "--update-aliases",
        "0.1",
        "latest",
    ]


def _repo(path):
    subprocess.run(["git", "init", "--quiet", str(path)], check=True)
    identity = ["-c", "user.name=t", "-c", "user.email=t@t"]
    subprocess.run(
        ["git", *identity, "commit", "--quiet", "--allow-empty", "-m", "root"],
        cwd=path,
        check=True,
    )
    return path


def test_an_existing_output_directory_is_a_usage_error(tmp_path, capsys):
    out = tmp_path / "site"
    out.mkdir()
    assert build_docs_site.main([str(out), "--repo", str(tmp_path)]) == 2
    assert "already exists" in capsys.readouterr().err


def test_a_leftover_build_branch_is_reported_not_deleted(tmp_path, capsys):
    repo = _repo(tmp_path / "repo")
    subprocess.run(["git", "branch", "docs-site-build"], cwd=repo, check=True)
    assert build_docs_site.main([str(tmp_path / "site"), "--repo", str(repo)]) == 2
    assert "git branch -D docs-site-build" in capsys.readouterr().err
    listed = subprocess.run(
        ["git", "branch", "--list", "docs-site-build"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    assert "docs-site-build" in listed.stdout


def test_missing_mike_is_a_usage_error(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(build_docs_site, "mike_executable", lambda: None)
    repo = _repo(tmp_path / "repo")
    assert build_docs_site.main([str(tmp_path / "site"), "--repo", str(repo)]) == 2
    assert "uv sync --group docs" in capsys.readouterr().err


def test_a_failed_build_step_is_reported_with_its_output(tmp_path, capsys, monkeypatch):
    def fail(repo, out, mike):
        raise subprocess.CalledProcessError(1, ["mike", "deploy"], stderr="Config value error")

    monkeypatch.setattr(build_docs_site, "build", fail)
    repo = _repo(tmp_path / "repo")
    assert build_docs_site.main([str(tmp_path / "site"), "--repo", str(repo)]) == 1
    err = capsys.readouterr().err
    assert "documentation build failed: mike deploy" in err
    assert "Config value error" in err
