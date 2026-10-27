"""Project selection is explicit, agrees with the loaded file and never comes from a folder."""

import pytest

from etl_craft.cli import main
from etl_craft.cli.client import ApiClient
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.services.operations.actions import ACTIONS
from fixtures.actions import build

pytestmark = pytest.mark.unit


def config(tmp_path, project=None):
    path = tmp_path / "craft-connector.yml"
    setting = "" if project is None else f"Orchestration:\n  Project: {project}\n"
    path.write_text("Secrets:\n  Source_type: ENVIRONMENT\n" + setting)
    return path


def test_no_default_project_and_conflicting_selection_name_the_file(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("ETL_CRAFT_CONFIG", raising=False)
    path = config(tmp_path)
    args = ["--api", "http://127.0.0.1:9", "--config", str(path), "pipelines"]
    assert main(args) == ExitCode.USAGE
    error = capsys.readouterr().err
    for part in (str(path), "Orchestration.Project", "--project", "set"):
        assert part in error
    config(tmp_path, "sales")
    assert main(["--project", "finance", *args]) == ExitCode.USAGE
    error = capsys.readouterr().err
    for part in (str(path), "sales", "finance", "conflicts"):
        assert part in error


def test_project_dir_selects_its_file_and_not_its_folder_name(tmp_path, capsys):
    path = config(tmp_path, "sales")
    assert (
        main(
            [
                "--api",
                "http://127.0.0.1:9",
                "--project",
                "finance",
                "config",
                "plan",
                "--project-dir",
                str(tmp_path),
            ]
        )
        == ExitCode.USAGE
    )
    error = capsys.readouterr().err
    assert "sales" in error and "finance" in error and str(path) in error


def test_remote_project_actions_require_selection_but_admin_actions_do_not(monkeypatch):
    class ReachedTransportError(Exception):
        pass

    def transport(*args, **kwargs):
        raise ReachedTransportError

    monkeypatch.setattr(ApiClient, "_open", transport)
    client = ApiClient("http://127.0.0.1:9")
    for name in (
        ActionName.ACCOUNT_LIST,
        ActionName.GROUP_LIST,
        ActionName.POOL_LIST,
        ActionName.PROJECT_LIST,
    ):
        with pytest.raises(ReachedTransportError):
            client.invoke(name, build(ACTIONS[name].request))
    with pytest.raises(UsageError, match=r"Orchestration\.Project"):
        client.invoke(ActionName.PIPELINES, build(ACTIONS[ActionName.PIPELINES].request))


def test_remote_bundle_project_directory_is_one_source(tmp_path, monkeypatch, capsys):
    from etl_craft.services.operations.bundles import BundleView

    config(tmp_path, "sales")
    calls = []

    def publish(client, name, request):
        calls.append((client.project, name, request))
        return BundleView("published", {})

    monkeypatch.setattr(ApiClient, "invoke", publish)
    assert (
        main(["bundle", "publish", "--project-dir", str(tmp_path), "--api", "http://localhost:9"])
        == ExitCode.SUCCESS
    )
    assert calls[0][0:2] == ("sales", ActionName.BUNDLE_PUBLISH)
    assert "craft-connector.yml" in calls[0][2].files
    assert "Published" in capsys.readouterr().out

    assert (
        main(
            [
                "bundle",
                "publish",
                "--project-dir",
                str(tmp_path),
                "--api",
                "http://localhost:9",
                "--project",
                "finance",
            ]
        )
        == ExitCode.USAGE
    )
    error = capsys.readouterr().err
    assert "sales" in error and "finance" in error and "conflicts" in error
    assert len(calls) == 1
