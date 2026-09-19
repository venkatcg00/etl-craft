"""Tests for etl_craft.config."""

import pytest

from etl_craft.config import ConfigError, load_config, resolve_secret

VALID_YAML = """
Execution:
  Mode: local

Source:
  Type: environment

Postgres:
  Active_profile: dev
  Profiles:
    dev:
      jdbc_url: jdbc:postgresql://localhost:5432/etl_craft
      user: etl_engine
      auth_mode: password
    prod:
      jdbc_url: jdbc:postgresql://prod-host:5432/etl_craft
      user: etl_engine
      auth_mode: key_file
      key_file: /etc/etl-craft/prod.key

Cloning:
  Enabled: true
  Scope: cfg
"""


def write_config(tmp_path, contents: str):
    path = tmp_path / "craft-connector.yml"
    path.write_text(contents)
    return path


def test_load_valid_config(tmp_path):
    config = load_config(write_config(tmp_path, VALID_YAML))
    assert config.mode == "local"
    assert config.source.type == "environment"
    assert config.postgres.active_profile == "dev"
    assert config.postgres.active.jdbc_url == "jdbc:postgresql://localhost:5432/etl_craft"
    assert config.postgres.active.auth_mode == "password"
    assert config.postgres.profiles["prod"].extra["key_file"] == "/etc/etl-craft/prod.key"
    assert config.cloning.enabled is True
    assert config.cloning.scope == "cfg"


def test_missing_file_raises(tmp_path):
    with pytest.raises(ConfigError):
        load_config(tmp_path / "does-not-exist.yml")


def test_invalid_mode_rejected(tmp_path):
    bad = VALID_YAML.replace("Mode: local", "Mode: bogus")
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, bad))


def test_active_profile_must_exist_in_profiles(tmp_path):
    bad = VALID_YAML.replace("Active_profile: dev", "Active_profile: staging")
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, bad))


def test_invalid_auth_mode_rejected(tmp_path):
    bad = VALID_YAML.replace("auth_mode: password", "auth_mode: bogus")
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, bad))


def test_file_source_requires_path(tmp_path):
    bad = VALID_YAML.replace("Type: environment", "Type: file")
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, bad))


def test_cloning_defaults_when_section_absent(tmp_path):
    no_cloning = VALID_YAML.replace("Cloning:\n  Enabled: true\n  Scope: cfg\n", "")
    config = load_config(write_config(tmp_path, no_cloning))
    assert config.cloning.enabled is False
    assert config.cloning.scope == "cfg"


def test_resolve_secret_from_environment(tmp_path, monkeypatch):
    config = load_config(write_config(tmp_path, VALID_YAML))
    monkeypatch.setenv("ETL_CRAFT_POSTGRES_DEV_SECRET", "s3cr3t")
    assert resolve_secret(config, config.postgres.active) == "s3cr3t"


def test_resolve_secret_missing_raises(tmp_path, monkeypatch):
    config = load_config(write_config(tmp_path, VALID_YAML))
    monkeypatch.delenv("ETL_CRAFT_POSTGRES_DEV_SECRET", raising=False)
    with pytest.raises(ConfigError):
        resolve_secret(config, config.postgres.active)


def test_resolve_secret_explicit_var_override(tmp_path, monkeypatch):
    overridden = VALID_YAML.replace(
        "auth_mode: password\n", "auth_mode: password\n      secret_var: MY_CUSTOM_SECRET\n"
    )
    config = load_config(write_config(tmp_path, overridden))
    monkeypatch.setenv("MY_CUSTOM_SECRET", "hunter2")
    assert resolve_secret(config, config.postgres.active) == "hunter2"


def test_resolve_secret_from_file_source(tmp_path):
    env_file = tmp_path / "secrets.env"
    env_file.write_text("ETL_CRAFT_POSTGRES_DEV_SECRET=filesecret\n# comment\n\nOTHER=1\n")
    file_source_yaml = VALID_YAML.replace(
        "Source:\n  Type: environment\n",
        f"Source:\n  Type: file\n  Path: {env_file}\n",
    )
    config = load_config(write_config(tmp_path, file_source_yaml))
    assert resolve_secret(config, config.postgres.active) == "filesecret"
