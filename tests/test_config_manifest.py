"""Regression coverage for the commit-safe craft-connector manifest format."""

from __future__ import annotations

from pathlib import Path

import pytest

from etl_craft.config import ConfigError, load_config, resolve_secret

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS = REPO_ROOT / "docs"


def _write_example(tmp_path: Path, filename: str, *, secrets_path: Path | None = None) -> Path:
    """Copy a shipped example into a temporary, runnable connector file."""
    contents = (DOCS / filename).read_text(encoding="utf-8")
    if secrets_path is not None:
        contents = contents.replace("/etc/etl-craft/.env", str(secrets_path))
    config_path = tmp_path / "craft-connector.yml"
    config_path.write_text(contents, encoding="utf-8")
    return config_path


def _set_environment(monkeypatch: pytest.MonkeyPatch, values: dict[str, str]) -> None:
    for profile_override in (
        "ETL_CRAFT_ENGINE_PROFILE",
        "ETL_CRAFT_WAREHOUSE_PROFILE",
        "ETL_CRAFT_EMAIL_PROFILE",
    ):
        monkeypatch.delenv(profile_override, raising=False)
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def _file_values() -> str:
    return """\
ENGINE_JDBC_URL=jdbc:postgresql://engine.internal:5432/etl_craft
ENGINE_USER=engine_user
ENGINE_AUTH_MODE=password
ENGINE_SECRET=engine-secret
WAREHOUSE_JDBC_URL=jdbc:postgresql://warehouse.internal:5432/analytics
WAREHOUSE_USER=warehouse_user
WAREHOUSE_AUTH_MODE=password
WAREHOUSE_SECRET=warehouse-secret
EMAIL_HOST=mail.internal
EMAIL_PORT=2525
EMAIL_FROM=etl-craft@example.com
EMAIL_AUTH_MODE=none
EMAIL_USER=
EMAIL_USE_TLS=false
EMAIL_SECRET=unused
"""


def _environment_values() -> dict[str, str]:
    return {
        "ENGINE_JDBC_URL": "jdbc:postgresql://engine.internal:5432/etl_craft",
        "ENGINE_USER": "engine_user",
        "ENGINE_AUTH_MODE": "password",
        "ENGINE_SECRET": "engine-secret",
        "WAREHOUSE_JDBC_URL": (
            "jdbc:databricks://workspace.cloud.databricks.com:443/default;"
            "httpPath=/sql/1.0/warehouses/warehouse-id"
        ),
        "WAREHOUSE_CATALOG": "analytics",
        "WAREHOUSE_SCHEMA": "default",
        "WAREHOUSE_TOKEN": "databricks-token",
        "EMAIL_HOST": "mail.internal",
        "EMAIL_PORT": "2525",
        "EMAIL_FROM": "etl-craft@example.com",
        "EMAIL_AUTH_MODE": "none",
        "EMAIL_USER": "",
        "EMAIL_USE_TLS": "false",
        "EMAIL_SECRET": "unused",
    }


def test_file_secrets_example_loads_and_resolves_all_active_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secrets_path = tmp_path / "secrets.env"
    secrets_path.write_text(_file_values(), encoding="utf-8")
    config_path = _write_example(
        tmp_path,
        "craft-connector.file-secrets.example.yml",
        secrets_path=secrets_path,
    )
    _set_environment(monkeypatch, {})

    config = load_config(config_path)

    assert config.mode == "orchestrator"
    assert config.source.type == "file"
    assert config.postgres.active_profile == "dev"
    assert config.postgres.active.jdbc_url.endswith("/etl_craft")
    assert config.postgres.active.user == "engine_user"
    assert resolve_secret(config, config.postgres.active) == "engine-secret"
    assert config.warehouse is not None
    assert config.warehouse.active.user == "warehouse_user"
    assert resolve_secret(config, config.warehouse.active) == "warehouse-secret"
    assert config.warehouse_table_format == "iceberg"
    assert config.email is not None
    assert config.email.active.port == 2525
    assert config.email.active.use_tls is False


def test_environment_secrets_example_loads_and_uses_databricks_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_environment(monkeypatch, _environment_values())
    config = load_config(_write_example(tmp_path, "craft-connector.env-secrets.example.yml"))

    assert config.mode == "orchestrator"
    assert config.source.type == "environment"
    assert config.postgres.active_profile == "prod"
    assert config.warehouse is not None
    assert config.warehouse.active.auth_mode == "token"
    assert config.warehouse.active.user == ""
    assert resolve_secret(config, config.warehouse.active) == "databricks-token"
    assert config.email is not None
    assert config.email.active.use_tls is False


def test_manifest_profile_override_prefers_tier_scoped_variables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "craft-connector.yml"
    config_path.write_text(
        """\
Orchestration:
  Mode: local
Secrets:
  Source_type: environment
Engine:
  Profile: prod
  Variables:
    jdbc_url: ENGINE_JDBC_URL
    user: ENGINE_USER
    auth_mode: ENGINE_AUTH_MODE
    secret: ENGINE_SECRET
""",
        encoding="utf-8",
    )
    _set_environment(
        monkeypatch,
        {
            "ENGINE_JDBC_URL": "jdbc:postgresql://plain:5432/etl_craft",
            "ENGINE_USER": "plain_user",
            "ENGINE_AUTH_MODE": "password",
            "ENGINE_SECRET": "plain-secret",
            "ENGINE_DEV_JDBC_URL": "jdbc:postgresql://dev:5432/etl_craft",
            "ENGINE_DEV_USER": "dev_user",
            "ENGINE_DEV_AUTH_MODE": "password",
            "ENGINE_DEV_SECRET": "dev-secret",
            "ETL_CRAFT_ENGINE_PROFILE": "dev",
        },
    )

    config = load_config(config_path)

    assert config.postgres.active_profile == "dev"
    assert config.postgres.active.jdbc_url == "jdbc:postgresql://dev:5432/etl_craft"
    assert config.postgres.active.user == "dev_user"
    assert resolve_secret(config, config.postgres.active) == "dev-secret"


def test_manifest_warehouse_native_table_format_is_retained(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secrets_path = tmp_path / "secrets.env"
    secrets_path.write_text(_file_values(), encoding="utf-8")
    config_path = _write_example(
        tmp_path,
        "craft-connector.file-secrets.example.yml",
        secrets_path=secrets_path,
    )
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "Table_format: iceberg", "Table_format: native"
        ),
        encoding="utf-8",
    )
    _set_environment(monkeypatch, {})

    assert load_config(config_path).warehouse_table_format == "native"


def test_legacy_config_still_loads_and_retains_native_table_format(tmp_path: Path) -> None:
    config_path = tmp_path / "craft-connector.yml"
    config_path.write_text(
        """\
Execution:
  Mode: local
Source:
  Type: environment
Postgres:
  Active_profile: dev
  Profiles:
    dev:
      jdbc_url: jdbc:postgresql://engine.internal:5432/etl_craft
      user: engine_user
      auth_mode: password
Warehouse:
  Table_format: native
  Active_profile: dev
  Profiles:
    dev:
      jdbc_url: jdbc:postgresql://warehouse.internal:5432/analytics
      user: warehouse_user
      auth_mode: password
""",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.mode == "local"
    assert config.warehouse_table_format == "native"


def test_manifest_rejects_engine_auth_mode_without_an_implemented_connector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_environment(monkeypatch, _environment_values())
    monkeypatch.setenv("ENGINE_AUTH_MODE", "token")
    config_path = _write_example(tmp_path, "craft-connector.env-secrets.example.yml")

    with pytest.raises(ConfigError, match="Engine.Variables.auth_mode"):
        load_config(config_path)


# [DEVIATION, 2026-09-23] The shipped env-secrets example now leads with the
# Databricks *preferred* connection shape (catalog/schema/token as separate
# Variables, see docs/craft-connector.env-secrets.example.yml), which has no
# `auth_mode`/`jdbc_url`-vs-`Name` validation of its own -- Name itself
# selects which fields to read, so it can't disagree with a URL it builds.
# The two tests below exercise that validation on the older, still-fully-
# supported plain jdbc_url/user/auth_mode/secret shape directly, rather than
# through a shipped example whose lead shape no longer takes that path.
_LEGACY_WAREHOUSE_MANIFEST = """\
Orchestration:
  Mode: local
Secrets:
  Source_type: environment
Engine:
  Profile: dev
  Variables:
    jdbc_url: ENGINE_JDBC_URL
    user: ENGINE_USER
    auth_mode: ENGINE_AUTH_MODE
    secret: ENGINE_SECRET
Warehouse:
  Name: {name}
  Profile: dev
  Variables:
    jdbc_url: WAREHOUSE_JDBC_URL
    user: WAREHOUSE_USER
    auth_mode: WAREHOUSE_AUTH_MODE
    secret: WAREHOUSE_SECRET
"""


def test_manifest_rejects_warehouse_auth_mode_without_an_implemented_connector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_environment(monkeypatch, _environment_values())
    monkeypatch.setenv(
        "WAREHOUSE_JDBC_URL",
        "jdbc:databricks://workspace.cloud.databricks.com:443/default;"
        "httpPath=/sql/1.0/warehouses/warehouse-id;ConnCatalog=analytics",
    )
    monkeypatch.setenv("WAREHOUSE_USER", "token")
    monkeypatch.setenv("WAREHOUSE_AUTH_MODE", "sso")
    monkeypatch.setenv("WAREHOUSE_SECRET", "unused")
    config_path = tmp_path / "craft-connector.yml"
    config_path.write_text(_LEGACY_WAREHOUSE_MANIFEST.format(name="Databricks"), encoding="utf-8")

    with pytest.raises(ConfigError, match="Warehouse.Variables.auth_mode"):
        load_config(config_path)


def test_manifest_rejects_a_warehouse_name_that_disagrees_with_its_jdbc_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_environment(monkeypatch, _environment_values())
    monkeypatch.setenv(
        "WAREHOUSE_JDBC_URL",
        "jdbc:databricks://workspace.cloud.databricks.com:443/default;"
        "httpPath=/sql/1.0/warehouses/warehouse-id;ConnCatalog=analytics",
    )
    monkeypatch.setenv("WAREHOUSE_USER", "token")
    monkeypatch.setenv("WAREHOUSE_AUTH_MODE", "token")
    monkeypatch.setenv("WAREHOUSE_SECRET", "databricks-token")
    config_path = tmp_path / "craft-connector.yml"
    # Name says Snowflake; the jdbc_url is actually Databricks.
    config_path.write_text(_LEGACY_WAREHOUSE_MANIFEST.format(name="Snowflake"), encoding="utf-8")

    with pytest.raises(ConfigError, match="Warehouse.Name is 'Snowflake'"):
        load_config(config_path)
