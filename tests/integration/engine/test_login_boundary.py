"""Commands reject unproven identity even when the caller claims an actor name."""

import pytest
import yaml

from etl_craft.cli import main


@pytest.fixture
def project(engine_db, tmp_path, monkeypatch, cli_logging):
    profile = engine_db.config.engine
    block = {"jdbc_url": profile.jdbc_url, "schema": profile.schema or "main"}
    if profile.auth_mode != "none":
        block.update(user=profile.user, auth_mode=profile.auth_mode, secret=profile.secret_var)
    path = tmp_path / "craft-connector.yml"
    path.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "environment"},
                "Orchestration": {"Mode": "local"},
                "Engine": {"dev": block},
            },
            sort_keys=False,
        )
    )
    monkeypatch.setenv("ETL_CRAFT_CONFIG", str(path))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "person"))
    monkeypatch.delenv("ETL_CRAFT_TOKEN", raising=False)


def test_read_commands_require_a_proven_account(project, capsys):
    assert main(["list"]) == 3
    assert "no signed-in account" in capsys.readouterr().err


def test_a_supplied_unknown_token_cannot_claim_an_identity(project, capsys, monkeypatch):
    monkeypatch.setenv("ETL_CRAFT_TOKEN", "unregistered-token")
    assert main(["list"]) == 3
    assert "set ETL_CRAFT_TOKEN for a service account" in capsys.readouterr().err
