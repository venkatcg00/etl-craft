"""Unattended and interactive bootstrap never silently omit an administrator."""

import io
import sys

import pytest
import yaml

from etl_craft.cli import main
from etl_craft.core.errors import ExitCode
from fixtures.engine_db import profile_block


@pytest.fixture
def setup_project(empty_engine_db, tmp_path, monkeypatch):
    from etl_craft.engine.schema import init_db

    engine_db = empty_engine_db
    init_db(engine_db.engine)
    path = tmp_path / "craft-connector.yml"
    path.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "ENVIRONMENT"},
                "Orchestration": {"Project": "default"},
                "Engine": {
                    "dev": profile_block(
                        engine_db.config.engine, engine_db.config.engine.schema or "main"
                    )
                },
                "Artifacts": {"Store": (tmp_path / "artifacts").as_uri()},
            },
            sort_keys=False,
        )
    )
    monkeypatch.setenv("ETL_CRAFT_CONFIG", str(path))
    monkeypatch.delenv("ETL_CRAFT_TOKEN", raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "credentials"))
    return engine_db


def test_unattended_setup_admin_contract(setup_project, monkeypatch, capsys):
    monkeypatch.setattr(
        sys, "stdin", io.StringIO("secret for first administrator\nignored second line\n")
    )
    assert main(["setup"]) == ExitCode.USAGE
    assert "--admin-name N --password-stdin" in capsys.readouterr().err
    assert main(["setup", "--admin-name", "owner", "--password-stdin"]) == ExitCode.SUCCESS
    output = capsys.readouterr()
    assert "created admin owner" in output.out
    assert "secret for first" not in output.out + output.err
    assert sys.stdin.readline() == "ignored second line\n"
    assert main(["setup", "--admin-name", "another", "--password-stdin"]) == ExitCode.USAGE
    assert "account add or account reset-password" in capsys.readouterr().err
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    assert main(["setup", "--admin-name", "another", "--password-stdin"]) == ExitCode.USAGE
    assert "account reset-password" in capsys.readouterr().err


def test_setup_retries_password_policy(setup_project, monkeypatch, capsys):
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "owner")
    answers = iter(
        [
            "short",
            "short",
            "also short",
            "also short",
            "strong administrator password",
            "strong administrator password",
        ]
    )
    monkeypatch.setattr("getpass.getpass", lambda _: next(answers))
    assert main(["setup"]) == ExitCode.SUCCESS
    out = capsys.readouterr().out
    assert out.count("try again") == 2
    assert "created admin owner" in out
