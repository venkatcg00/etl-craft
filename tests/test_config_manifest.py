"""craft-connector.yml: the one format, as a team writes it (2026-09-24).

Sections in order -- Secrets, Orchestration (DAG defaults and Email inside it),
Engine, Warehouse, Cloning -- each with one block per profile. These are pure
parsing tests: no database is touched.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from etl_craft.config import ConfigError, load_config

EXAMPLE = Path(__file__).parents[1] / "docs" / "craft-connector.example.yml"

PROFILE_VARS = (
    "ETL_CRAFT_PROFILE",
    "ETL_CRAFT_ENGINE_PROFILE",
    "ETL_CRAFT_WAREHOUSE_PROFILE",
    "ETL_CRAFT_ORCHESTRATION_PROFILE",
    "ETL_CRAFT_CLONING_PROFILE",
)


@pytest.fixture(autouse=True)
def _no_profile_overrides(monkeypatch):
    for name in PROFILE_VARS:
        monkeypatch.delenv(name, raising=False)


def _write(tmp_path: Path, raw: dict) -> Path:
    path = tmp_path / "craft-connector.yml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return path


def _minimal(**sections) -> dict:
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local"},
        "Engine": {"dev": {"jdbc_url": "jdbc:sqlite:engine.db"}},
    }
    raw.update(sections)
    return raw


def test_the_shipped_example_loads_for_dev_with_nothing_set():
    config = load_config(EXAMPLE)
    assert config.mode == "local"
    assert config.postgres.active.jdbc_url == "jdbc:sqlite:etl-craft-engine.db"
    assert config.postgres.active.auth_mode == "none"
    assert config.warehouse is not None
    assert config.warehouse.active.jdbc_url == "jdbc:duckdb:warehouse.duckdb"
    assert config.warehouse_table_format == "native"
    assert config.orchestrator.allow_schedule is False
    assert config.email is None
    assert config.cloning.enabled is False


def test_the_shipped_example_prod_profile_overrides_and_resolves(monkeypatch):
    monkeypatch.setenv("ETL_CRAFT_PROFILE", "prod")
    for name, value in {
        "ENGINE_JDBC_URL": "jdbc:postgresql://db/etl",
        "ENGINE_USER": "etl",
        "ENGINE_AUTH_MODE": "password",
        "WAREHOUSE_JDBC_URL": "jdbc:postgresql://db/wh",
        "WAREHOUSE_USER": "wh",
        "WAREHOUSE_AUTH_MODE": "password",
        "EMAIL_HOST": "smtp.internal",
        "EMAIL_PORT": "587",
        "EMAIL_FROM": "etl@example.com",
        "EMAIL_AUTH_MODE": "password",
        "EMAIL_USER": "etl",
        "EMAIL_USE_TLS": "true",
    }.items():
        monkeypatch.setenv(name, value)
    config = load_config(EXAMPLE)
    # A profile block overrides the section's own settings, Mode included.
    assert config.mode == "orchestrator"
    assert config.postgres.active.secret_var == "ENGINE_SECRET"
    assert config.warehouse is not None and config.warehouse.active.user == "wh"
    assert config.email is not None
    assert config.email.active.host == "smtp.internal"
    assert config.email.active.port == 587
    assert config.email.active.secret_var == "EMAIL_SECRET"
    assert config.orchestrator.email_recipients == ["data-alerts@example.com"]
    assert config.cloning.enabled is True and config.cloning.scope == "all"


def test_profile_selection_order(tmp_path, monkeypatch):
    raw = _minimal(
        Secrets={"Source_type": "environment", "Profile": "sit"},
        Engine={
            "Profile": "uat",
            "sit": {"jdbc_url": "jdbc:sqlite:sit.db"},
            "uat": {"jdbc_url": "jdbc:sqlite:uat.db"},
            "prod": {"jdbc_url": "jdbc:sqlite:prod.db"},
        },
    )
    path = _write(tmp_path, raw)
    # The section's own Profile beats Secrets.Profile...
    assert load_config(path).postgres.active.jdbc_url == "jdbc:sqlite:uat.db"
    # ...the global environment switch beats the file...
    monkeypatch.setenv("ETL_CRAFT_PROFILE", "prod")
    assert load_config(path).postgres.active.jdbc_url == "jdbc:sqlite:prod.db"
    # ...and the section's own environment switch beats everything.
    monkeypatch.setenv("ETL_CRAFT_ENGINE_PROFILE", "sit")
    assert load_config(path).postgres.active.jdbc_url == "jdbc:sqlite:sit.db"


def test_an_unselected_multi_profile_section_is_refused(tmp_path):
    raw = _minimal(
        Engine={"dev": {"jdbc_url": "jdbc:sqlite:a.db"}, "prod": {"jdbc_url": "jdbc:sqlite:b.db"}}
    )
    with pytest.raises(ConfigError, match="none is selected"):
        load_config(_write(tmp_path, raw))


def test_a_missing_profile_names_the_ones_that_exist(tmp_path, monkeypatch):
    monkeypatch.setenv("ETL_CRAFT_PROFILE", "qa")
    with pytest.raises(ConfigError, match=r"no profile 'qa' \(it has \['dev'\]\)"):
        load_config(_write(tmp_path, _minimal()))


def test_tier_scoped_variables_win_over_the_plain_name(tmp_path, monkeypatch):
    raw = _minimal(
        Engine={
            "prod": {
                "jdbc_url": "ENGINE_JDBC_URL",
                "user": "ENGINE_USER",
                "auth_mode": "ENGINE_AUTH_MODE",
                "secret": "ENGINE_SECRET",
            }
        }
    )
    monkeypatch.setenv("ENGINE_JDBC_URL", "jdbc:postgresql://shared/etl")
    monkeypatch.setenv("ENGINE_PROD_JDBC_URL", "jdbc:postgresql://prod/etl")
    monkeypatch.setenv("ENGINE_USER", "etl")
    monkeypatch.setenv("ENGINE_AUTH_MODE", "password")
    monkeypatch.setenv("ENGINE_PROD_SECRET", "p")
    engine = load_config(_write(tmp_path, raw)).postgres.active
    assert engine.jdbc_url == "jdbc:postgresql://prod/etl"
    assert engine.secret_var == "ENGINE_PROD_SECRET"


def test_values_come_from_a_secrets_file_relative_to_the_config(tmp_path):
    (tmp_path / "secrets.env").write_text(
        "WH_URL=jdbc:postgresql://wh/analytics\nWH_USER=analyst\nWH_AUTH=password\n",
        encoding="utf-8",
    )
    raw = _minimal(
        Secrets={"Source_type": "File", "Path": "secrets.env"},
        Warehouse={
            "Name": "Postgres",
            "dev": {
                "jdbc_url": "WH_URL",
                "user": "WH_USER",
                "auth_mode": "WH_AUTH",
                "secret": "WH_SECRET",
            },
        },
    )
    config = load_config(_write(tmp_path, raw))
    assert config.source.type == "file"
    assert config.warehouse is not None
    assert config.warehouse.active.user == "analyst"


def test_a_literal_jdbc_url_is_accepted_but_a_literal_secret_is_not(tmp_path, monkeypatch):
    monkeypatch.setenv("ENGINE_AUTH_MODE", "password")
    monkeypatch.setenv("ENGINE_USER", "etl")
    raw = _minimal(
        Engine={
            "dev": {
                "jdbc_url": "jdbc:postgresql://db/etl",
                "user": "ENGINE_USER",
                "auth_mode": "ENGINE_AUTH_MODE",
                "secret": "hunter2!",
            }
        }
    )
    with pytest.raises(ConfigError, match="must be the name of a variable"):
        load_config(_write(tmp_path, raw))


def test_the_earlier_layouts_are_refused_with_a_pointer(tmp_path):
    legacy = {"Execution": {"Mode": "local"}, "Source": {"Type": "environment"}}
    with pytest.raises(ConfigError, match="earlier craft-connector.yml layout"):
        load_config(_write(tmp_path, legacy))
    variables = _minimal(Engine={"Profile": "dev", "Variables": {"jdbc_url": "X"}})
    with pytest.raises(ConfigError, match="Variables blocks"):
        load_config(_write(tmp_path, variables))


def test_sections_must_appear_in_the_documented_order(tmp_path):
    # Secrets, Orchestration, Engine, Warehouse, then Cloning -- the order is
    # part of the format. An omitted optional section does not count.
    raw = _minimal()
    reordered = {"Engine": raw["Engine"], "Secrets": raw["Secrets"]}
    reordered["Orchestration"] = raw["Orchestration"]
    with pytest.raises(ConfigError, match="write them as \\['Secrets', 'Orchestration', 'Engine'"):
        load_config(_write(tmp_path, reordered))
    cloning_first = {"Cloning": {"Enabled": False}, **_minimal()}
    with pytest.raises(ConfigError, match="then Cloning"):
        load_config(_write(tmp_path, cloning_first))
    with_cloning = _minimal(Cloning={"Enabled": False})
    assert load_config(_write(tmp_path, with_cloning)).cloning.enabled is False


def test_unknown_sections_and_keys_are_refused(tmp_path):
    with pytest.raises(ConfigError, match="unknown top-level section"):
        load_config(_write(tmp_path, _minimal(Warehosue={})))
    with pytest.raises(ConfigError, match=r"unknown key\(s\) \['Retires'\]"):
        load_config(_write(tmp_path, _minimal(Orchestration={"Mode": "local", "Retires": 1})))
    typo = _minimal(Engine={"dev": {"jdbc_url": "jdbc:sqlite:e.db", "usr": "X"}})
    with pytest.raises(ConfigError, match=r"unknown key\(s\) \['usr'\] in Engine.dev"):
        load_config(_write(tmp_path, typo))


@pytest.mark.parametrize(
    ("engine", "message"),
    [
        ({"Name": "Postgres", "dev": {"jdbc_url": "jdbc:sqlite:e.db"}}, "is a sqlite URL"),
        ({"dev": {"jdbc_url": "jdbc:mysql://h/db"}}, "not a supported Engine DB URL"),
        (
            {"dev": {"jdbc_url": "jdbc:sqlite:e.db", "auth_mode": "ENGINE_AUTH_MODE"}},
            "takes \\['none'\\]",
        ),
    ],
)
def test_engine_validation(tmp_path, monkeypatch, engine, message):
    monkeypatch.setenv("ENGINE_AUTH_MODE", "password")
    with pytest.raises(ConfigError, match=message):
        load_config(_write(tmp_path, _minimal(Engine=engine)))


def test_warehouse_table_format_defaults_to_native_and_postgres_refuses_iceberg(tmp_path):
    raw = _minimal(Warehouse={"dev": {"jdbc_url": "jdbc:duckdb:wh.duckdb"}})
    assert load_config(_write(tmp_path, raw)).warehouse_table_format == "native"
    iceberg_postgres = _minimal(
        Warehouse={
            "Table_format": "iceberg",
            "dev": {
                "jdbc_url": "jdbc:postgresql://h/wh",
                "user": "WH_USER",
                "auth_mode": "WH_AUTH",
                "secret": "WH_SECRET",
            },
        }
    )
    with pytest.raises(ConfigError, match="always native"):
        load_config(_write(tmp_path, iceberg_postgres))


def test_warehouse_name_must_match_its_jdbc_url(tmp_path):
    raw = _minimal(Warehouse={"Name": "Snowflake", "dev": {"jdbc_url": "jdbc:duckdb:w.duckdb"}})
    with pytest.raises(ConfigError, match="resolves to 'duckdb'"):
        load_config(_write(tmp_path, raw))


def test_warehouse_key_file_is_refused_where_it_is_not_implemented(tmp_path, monkeypatch):
    # Only Snowflake's key-pair auth is built; anywhere else key_file used to
    # load cleanly and fail at the first connection with NotImplementedError.
    monkeypatch.setenv("WAREHOUSE_JDBC_URL", "jdbc:postgresql://db/wh")
    monkeypatch.setenv("WAREHOUSE_USER", "wh")
    monkeypatch.setenv("WAREHOUSE_AUTH_MODE", "key_file")
    block = {
        "jdbc_url": "WAREHOUSE_JDBC_URL",
        "user": "WAREHOUSE_USER",
        "auth_mode": "WAREHOUSE_AUTH_MODE",
        "secret": "WAREHOUSE_SECRET",
        "key_file": "WAREHOUSE_KEY_FILE",
    }
    raw = _minimal(Warehouse={"dev": block})
    with pytest.raises(ConfigError, match="implemented only for a Snowflake warehouse"):
        load_config(_write(tmp_path, raw))


def test_databricks_token_fields_build_a_credential_free_url(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "DBX_URL",
        "jdbc:databricks://adb.example.net:443/default;httpPath=/sql/1.0/warehouses/x;"
        "AuthMech=3;UID=token;PWD=dapi-leaked",
    )
    monkeypatch.setenv("DBX_CATALOG", "main")
    monkeypatch.setenv("DBX_SCHEMA", "analytics")
    raw = _minimal(
        Warehouse={
            "Name": "Databricks",
            "Table_format": "iceberg",
            "dev": {
                "jdbc_url": "DBX_URL",
                "catalog": "DBX_CATALOG",
                "schema": "DBX_SCHEMA",
                "token": "DBX_TOKEN",
            },
        }
    )
    config = load_config(_write(tmp_path, raw))
    assert config.warehouse is not None
    profile = config.warehouse.active
    assert profile.auth_mode == "token"
    assert profile.secret_var == "DBX_TOKEN"
    assert "dapi-leaked" not in profile.jdbc_url
    assert profile.jdbc_url.endswith(";ConnCatalog=main;ConnSchema=analytics")
    assert config.warehouse_table_format == "iceberg"


def test_token_fields_are_refused_for_other_warehouses_and_unused_fields(tmp_path, monkeypatch):
    raw = _minimal(Warehouse={"Name": "Postgres", "dev": {"jdbc_url": "X", "token": "T"}})
    with pytest.raises(ConfigError, match="only for Warehouse.Name Databricks or Snowflake"):
        load_config(_write(tmp_path, raw))
    extra_field = _minimal(
        Warehouse={
            "Name": "Databricks",
            "dev": {"jdbc_url": "U", "catalog": "C", "schema": "S", "token": "T", "user": "X"},
        }
    )
    with pytest.raises(ConfigError, match=r"unknown key\(s\) \['user'\]"):
        load_config(_write(tmp_path, extra_field))


def test_duckdb_iceberg_profile_carries_its_catalog_fields(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_URI", "http://localhost:58181")
    monkeypatch.setenv("LAKE_NAME", "lake")
    monkeypatch.setenv("LAKE_WAREHOUSE", "s3://warehouse/")
    raw = _minimal(
        Warehouse={
            "Name": "DuckDB",
            "Table_format": "iceberg",
            "dev": {
                "jdbc_url": "jdbc:duckdb:",
                "catalog": "LAKE_NAME",
                "catalog_uri": "LAKE_URI",
                "iceberg_warehouse": "LAKE_WAREHOUSE",
            },
        }
    )
    config = load_config(_write(tmp_path, raw))
    assert config.warehouse is not None
    assert config.warehouse.active.extra == {
        "catalog": "lake",
        "catalog_uri": "http://localhost:58181",
        "iceberg_warehouse": "s3://warehouse/",
    }


def test_orchestration_settings_are_type_checked(tmp_path):
    raw = _minimal(Orchestration={"Mode": "local", "dev": {"Retries": "three"}})
    with pytest.raises(ConfigError, match="Retries must be a whole number"):
        load_config(_write(tmp_path, raw))
    with pytest.raises(ConfigError, match="Mode must be local or remote"):
        load_config(_write(tmp_path, _minimal(Orchestration={"Mode": "cron"})))


def test_cloning_scope_none_disables_it(tmp_path):
    raw = _minimal(Cloning={"dev": {"Enabled": True, "Scope": "none"}})
    cloning = load_config(_write(tmp_path, raw)).cloning
    assert cloning.enabled is False and cloning.scope == "none"


def test_email_needs_user_and_secret_for_password_auth(tmp_path, monkeypatch):
    for name, value in {"H": "smtp", "P": "25", "F": "a@b", "A": "password"}.items():
        monkeypatch.setenv(name, value)
    raw = _minimal(
        Orchestration={
            "Mode": "local",
            "dev": {"Email": {"host": "H", "port": "P", "from_address": "F", "auth_mode": "A"}},
        }
    )
    with pytest.raises(ConfigError, match="needs user and secret"):
        load_config(_write(tmp_path, raw))
