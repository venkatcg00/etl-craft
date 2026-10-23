import subprocess
from pathlib import Path

import pytest
import yaml

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


def test_mike_subprocess_finds_tools_beside_the_running_interpreter(tmp_path, monkeypatch):
    import os
    import sys
    from pathlib import Path

    calls = []
    (tmp_path / "mkdocs.yml").write_text("site_name: test\n")

    def run(command, cwd, env=None):
        if env is not None:
            calls.append(env)
        return ""

    monkeypatch.setenv("PATH", "/other/tools")
    monkeypatch.setattr(build_docs_site, "_run", run)
    monkeypatch.setattr(build_docs_site, "_export", lambda repo, out: None)
    monkeypatch.setattr(build_docs_site.subprocess, "run", lambda *args, **kwargs: None)
    build_docs_site.build(tmp_path, tmp_path / "out", "/env/bin/mike")
    assert calls
    assert all(
        env["PATH"].split(os.pathsep) == [str(Path(sys.executable).parent), "/other/tools"]
        for env in calls
    )


def test_release_uses_shared_api_renderer_but_its_own_source(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    (repo / "docs").mkdir(parents=True)
    (repo / "docs" / "gen_python_api.py").write_text("maintained renderer")
    calls = []

    def run(command, cwd, env=None):
        calls.append(command)
        if command[:3] == ["git", "worktree", "add"]:
            tree = Path(command[4])
            (tree / "docs").mkdir(parents=True)
            (tree / "src").mkdir()
            (tree / "src" / "release.py").write_text("released source")
            (tree / "docs" / "guide.md").write_text("released guide")
            (tree / "docs" / "gen_ref_pages.py").write_text("old renderer")
            (tree / "mkdocs.yml").write_text("site_name: released\nstrict: false\n")
        else:
            assert (cwd / "docs" / "gen_ref_pages.py").read_text() == "maintained renderer"
            assert (cwd / "src" / "release.py").read_text() == "released source"
            assert (cwd / "docs" / "guide.md").read_text() == "released guide"
            config = Path(command[command.index("--config-file") + 1])
            assert yaml.safe_load(config.read_text(encoding="utf-8")) == {
                "site_name": "released",
                "strict": True,
            }
        return ""

    monkeypatch.setattr(build_docs_site, "_run", run)
    monkeypatch.setattr(build_docs_site.subprocess, "run", lambda *args, **kwargs: None)
    build_docs_site._build_release(repo, "0.1", "v0.1.0", latest=True, env={})
    assert len(calls) == 2
    assert calls[0][-1] == "v0.1.0"


def test_every_rendered_version_and_latest_alias_get_content_checks(tmp_path, monkeypatch):
    calls = []
    (tmp_path / "mkdocs.yml").write_text("site_name: test\n")

    def run(command, cwd, env=None):
        calls.append(command)
        return "v0.1.0 v0.2.0" if command == ["git", "tag", "--list"] else ""

    monkeypatch.setattr(build_docs_site, "_run", run)
    monkeypatch.setattr(build_docs_site, "_build_release", lambda *args, **kwargs: None)
    monkeypatch.setattr(build_docs_site, "_export", lambda *args: None)
    monkeypatch.setattr(build_docs_site.subprocess, "run", lambda *args, **kwargs: None)
    assert build_docs_site.build(tmp_path, tmp_path / "out", "mike") == ["0.2", "0.1", "dev"]
    checked = [Path(command[-1]).name for command in calls if "check_docs_content.py" in command[1]]
    assert checked == ["0.1", "0.2", "dev", "latest"]


@pytest.mark.parametrize("fail", [False, True])
def test_deploy_builds_once_in_strict_mode_and_keeps_yaml_tags(tmp_path, monkeypatch, fail):
    original = (
        "site_name: test\nstrict: false\n"
        "extra: !ENV DOCS_LABEL\n"
        "formatter: !!python/name:pymdownx.superfences.fence_code_format\n"
    )
    (tmp_path / "mkdocs.yml").write_text(original)
    builds = []

    def run(command, cwd, env=None):
        assert "mkdocs" not in command, "a separate render duplicates mike deploy"
        if "deploy" in command:
            config = Path(command[command.index("--config-file") + 1])
            builds.append(config)
            assert config.parent == tmp_path
            node = yaml.compose(config.read_text(encoding="utf-8"))
            fields = {key.value: value for key, value in node.value}
            assert fields["strict"].tag == "tag:yaml.org,2002:bool"
            assert fields["strict"].value == "true"
            assert fields["extra"].tag == "!ENV"
            assert fields["formatter"].tag.endswith(
                "python/name:pymdownx.superfences.fence_code_format"
            )
            if fail:
                raise subprocess.CalledProcessError(1, command, stderr="strict warning")
        return ""

    monkeypatch.setattr(build_docs_site, "_run", run)
    monkeypatch.setattr(build_docs_site, "_export", lambda *args: None)
    monkeypatch.setattr(build_docs_site.subprocess, "run", lambda *args, **kwargs: None)
    if fail:
        with pytest.raises(subprocess.CalledProcessError, match="returned non-zero"):
            build_docs_site.build(tmp_path, tmp_path / "out", "mike")
    else:
        build_docs_site.build(tmp_path, tmp_path / "out", "mike")
    assert len(builds) == 1
    assert not builds[0].exists()
    assert (tmp_path / "mkdocs.yml").read_text() == original
