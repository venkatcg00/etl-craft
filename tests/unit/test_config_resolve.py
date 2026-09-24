"""Secrets and variables: resolving them at load time and again at connect time."""

from pathlib import Path

import pytest

from etl_craft.config import (
    load_config,
    profile_needs_secret,
    profile_secret,
    resolve_config_path,
    resolve_secret,
)
from etl_craft.config.model import ConnectionProfile, SettingSource
from etl_craft.config.resolve import Resolver, profile_variable_name, read_secrets_file
from etl_craft.core.errors import ConfigurationError

pytestmark = pytest.mark.unit

ENGINE_VARS = "TEST_ENGINE_USER=etl_engine\nTEST_ENGINE_AUTH_MODE=password\n"

VALID_YAML = """
Secrets:
  Source_type: environment

Orchestration:
  Mode: local

Engine:
  dev:
    jdbc_url: jdbc:postgresql://localhost:5432/etl_craft
    user: TEST_ENGINE_USER
    auth_mode: TEST_ENGINE_AUTH_MODE
    secret: ETL_CRAFT_POSTGRES_DEV_SECRET
"""

SQLITE_ENGINE = "  dev:\n    jdbc_url: jdbc:sqlite:e.db\n"


@pytest.fixture(autouse=True)
def engine_variables(monkeypatch):
    monkeypatch.setenv("TEST_ENGINE_USER", "etl_engine")
    monkeypatch.setenv("TEST_ENGINE_AUTH_MODE", "password")


def write_config(tmp_path: Path, contents: str) -> Path:
    path = tmp_path / "craft-connector.yml"
    path.write_text(contents, encoding="utf-8")
    return path


def test_resolve_secret_from_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("ETL_CRAFT_POSTGRES_DEV_SECRET", "s3cr3t")
    config = load_config(write_config(tmp_path, VALID_YAML))
    assert resolve_secret(config, config.engine.active) == "s3cr3t"
    assert profile_secret(config, config.engine.active) == "s3cr3t"


def test_a_secret_is_checked_at_load_and_again_when_resolved(tmp_path, monkeypatch):
    monkeypatch.delenv("ETL_CRAFT_POSTGRES_DEV_SECRET", raising=False)
    with pytest.raises(ConfigurationError, match="not set"):
        load_config(write_config(tmp_path, VALID_YAML))
    monkeypatch.setenv("ETL_CRAFT_POSTGRES_DEV_SECRET", "s3cr3t")
    config = load_config(write_config(tmp_path, VALID_YAML))
    monkeypatch.delenv("ETL_CRAFT_POSTGRES_DEV_SECRET")
    with pytest.raises(ConfigurationError, match="'ETL_CRAFT_POSTGRES_DEV_SECRET' not found"):
        resolve_secret(config, config.engine.active)


def test_a_secret_can_name_any_variable(tmp_path, monkeypatch):
    overridden = VALID_YAML.replace(
        "secret: ETL_CRAFT_POSTGRES_DEV_SECRET", "secret: MY_CUSTOM_SECRET"
    )
    monkeypatch.setenv("MY_CUSTOM_SECRET", "hunter2")
    config = load_config(write_config(tmp_path, overridden))
    assert resolve_secret(config, config.engine.active) == "hunter2"


def test_resolve_secret_from_a_file_source(tmp_path):
    env_file = tmp_path / "secrets.env"
    env_file.write_text(
        ENGINE_VARS + "ETL_CRAFT_POSTGRES_DEV_SECRET=filesecret\n# comment\n\nOTHER=1\n",
        encoding="utf-8",
    )
    file_source_yaml = VALID_YAML.replace(
        "Secrets:\n  Source_type: environment\n",
        f"Secrets:\n  Source_type: file\n  Path: {env_file}\n",
    )
    config = load_config(write_config(tmp_path, file_source_yaml))
    assert resolve_secret(config, config.engine.active) == "filesecret"


def test_the_file_source_path_is_relative_to_the_config_file(tmp_path, monkeypatch):
    secrets = tmp_path / "secrets.env"
    secrets.write_text(ENGINE_VARS + "ETL_CRAFT_POSTGRES_DEV_SECRET=filesecret\n", "utf-8")
    connector_dir = tmp_path / "project"
    connector_dir.mkdir()
    connector = connector_dir / "craft-connector.yml"
    connector.write_text(
        VALID_YAML.replace(
            "Secrets:\n  Source_type: environment\n",
            "Secrets:\n  Source_type: file\n  Path: ../secrets.env\n",
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    config = load_config(connector)
    assert config.source.path == str(secrets)
    assert resolve_secret(config, config.engine.active) == "filesecret"


def test_read_secrets_file_errors(tmp_path):
    with pytest.raises(ConfigurationError, match=r"Secrets\.Path is required"):
        read_secrets_file(None)
    with pytest.raises(ConfigurationError, match="could not read secrets file"):
        read_secrets_file(str(tmp_path / "missing.env"))


@pytest.mark.parametrize(
    ("auth_mode", "extra", "needs"),
    [
        ("password", {}, True),
        ("token", {}, True),
        ("oauth", {}, True),
        ("none", {}, False),
        ("sts", {}, False),
        ("sso", {}, False),
        ("key_file", {"secret_var": "KEY_PASSPHRASE"}, True),
    ],
)
def test_profile_needs_secret(auth_mode, extra, needs):
    profile = ConnectionProfile("ENGINE", "dev", "jdbc:postgresql://h/db", "u", auth_mode, extra)
    assert profile_needs_secret(profile) is needs


def test_a_profile_without_a_secret_resolves_to_empty(tmp_path):
    config = load_config(write_config(tmp_path, VALID_YAML.split("  dev:")[0] + SQLITE_ENGINE))
    assert profile_secret(config, config.engine.active) == ""
    assert config.engine.active.secret_var == "ETL_CRAFT_ENGINE_DEV_SECRET"


@pytest.mark.parametrize(
    ("var_name", "profile", "field_name", "expected"),
    [
        ("ENGINE_SECRET", "prod", "secret", "ENGINE_PROD_SECRET"),
        ("ENGINE_PROD_SECRET", "prod", "secret", "ENGINE_PROD_SECRET"),
        ("engine_secret", "dev", "secret", "engine_DEV_secret"),
        ("EMAIL_FROM", "uat", "from_address", "EMAIL_UAT_FROM"),
        ("MY_TOKEN", "dev", "secret", None),
    ],
)
def test_profile_variable_name(var_name, profile, field_name, expected):
    assert profile_variable_name(var_name, profile, field_name) == expected


def test_the_resolver_records_every_lookup_and_explains_missing_variables(tmp_path):
    resolver = Resolver(values={"SET_ONE": "x"}, origin="the tests", path=tmp_path)
    assert resolver.resolve("SET_ONE", "A.a") == "x"
    assert resolver.resolve("MISSING_ONE", "A.b") == "MISSING_ONE"
    assert resolver.resolve(["SET_ONE", "plain"], "A.c") == ["x", "plain"]
    assert resolver.resolve(3, "A.d") == 3
    assert resolver.sources[:2] == [
        SettingSource("A.a", "SET_ONE", "SET_ONE"),
        SettingSource("A.b", "MISSING_ONE"),
    ]
    assert "no variable named MISSING_ONE is set in the tests" in resolver.hint("A.b")
    assert resolver.hint("A.a") == ""
    assert resolver.hint("A.never") == ""
    with pytest.raises(ConfigurationError, match=r"A\.e must be a non-empty string"):
        resolver.text("  ", "A.e")


def test_resolve_config_path_precedence(tmp_path, monkeypatch):
    # --config first, then $ETL_CRAFT_CONFIG, then the nearest file searching upward.
    project = tmp_path / "project"
    nested = project / "a" / "b"
    nested.mkdir(parents=True)
    (project / "craft-connector.yml").write_text("{}", encoding="utf-8")
    monkeypatch.delenv("ETL_CRAFT_CONFIG", raising=False)
    assert resolve_config_path("explicit.yml") == Path("explicit.yml")
    assert resolve_config_path(start=nested) == project / "craft-connector.yml"
    monkeypatch.chdir(nested)
    assert resolve_config_path() == (project / "craft-connector.yml").resolve()
    monkeypatch.setenv("ETL_CRAFT_CONFIG", "/etc/etl/craft-connector.yml")
    assert resolve_config_path() == Path("/etc/etl/craft-connector.yml")
    assert resolve_config_path("explicit.yml") == Path("explicit.yml")


def test_resolve_config_path_without_a_file_names_where_it_was_expected(tmp_path, monkeypatch):
    monkeypatch.delenv("ETL_CRAFT_CONFIG", raising=False)
    empty = tmp_path / "empty"
    empty.mkdir()
    found = resolve_config_path(start=empty)
    if found != empty.resolve() / "craft-connector.yml":  # pragma: no cover - a stray parent file
        pytest.skip(f"a craft-connector.yml above the temporary directory: {found}")
    with pytest.raises(ConfigurationError, match=r"craft-connector\.yml not found at"):
        load_config(found)
