"""craft-connector.yml: the one format, as a team writes it.

Sections in order -- Secrets, Orchestration (DAG defaults and Email inside it),
Engine, Warehouse, Cloning -- each with one block per profile. These are pure
parsing tests: no database is touched.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from etl_craft.config import SettingSource, load_config
from etl_craft.config.auth import warehouse_by_key
from etl_craft.core.errors import ConfigurationError

pytestmark = pytest.mark.unit

EXAMPLE = Path(__file__).parents[2] / "docs" / "craft-connector.example.yml"

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
    assert config.engine.active.jdbc_url == "jdbc:sqlite:etl-craft-engine.db"
    assert config.engine.active.auth_mode == "none"
    assert config.warehouse is not None
    assert config.warehouse.active.jdbc_url == "jdbc:duckdb:warehouse.duckdb"
    assert config.warehouse_table_format == "native"
    assert config.dag_defaults.allow_schedule is False
    assert config.email is None
    assert config.cloning.enabled is False


def test_the_shipped_example_prod_profile_overrides_and_resolves(tmp_path, monkeypatch):
    # The shipped file selects `dev` as a value; naming a variable instead
    # (Profile: ETL_CRAFT_PROFILE) is how each environment picks its own.
    text = EXAMPLE.read_text(encoding="utf-8")
    text = re.sub(r"^  Profile: dev\b", "  Profile: ETL_CRAFT_PROFILE", text, count=1, flags=re.M)
    example = tmp_path / "craft-connector.yml"
    example.write_text(text, encoding="utf-8")
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
        # Secrets must be set for the selected profile; their values are
        # never read back into the config object.
        "ENGINE_SECRET": "x",
        "WAREHOUSE_SECRET": "x",
        "EMAIL_SECRET": "x",
    }.items():
        monkeypatch.setenv(name, value)
    config = load_config(example)
    # A profile block overrides the section's own settings, Mode included.
    assert config.mode == "remote"
    assert config.engine.active.secret_var == "ENGINE_SECRET"
    assert config.warehouse is not None and config.warehouse.active.user == "wh"
    assert config.email is not None
    assert config.email.active.host == "smtp.internal"
    assert config.email.active.port == 587
    assert config.email.active.secret_var == "EMAIL_SECRET"
    assert config.dag_defaults.email_recipients == ["data-alerts@example.com"]
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
    # The section's own Profile beats Secrets.Profile.
    assert load_config(path).engine.active.jdbc_url == "jdbc:sqlite:uat.db"
    # Nothing outside the file switches it: a Profile names a variable when that is wanted.
    monkeypatch.setenv("ETL_CRAFT_PROFILE", "prod")
    monkeypatch.setenv("ETL_CRAFT_ENGINE_PROFILE", "prod")
    assert load_config(path).engine.active.jdbc_url == "jdbc:sqlite:uat.db"
    # Without its own Profile, a section takes Secrets.Profile.
    del raw["Engine"]["Profile"]
    assert load_config(_write(tmp_path, raw)).engine.active.jdbc_url == "jdbc:sqlite:sit.db"


def test_a_profile_can_name_a_variable(tmp_path, monkeypatch):
    raw = _minimal(
        Secrets={"Source_type": "environment", "Profile": "ACTIVE_PROFILE"},
        Engine={
            "dev": {"jdbc_url": "jdbc:sqlite:dev.db"},
            "prod": {"jdbc_url": "jdbc:sqlite:prod.db"},
        },
    )
    path = _write(tmp_path, raw)
    monkeypatch.setenv("ACTIVE_PROFILE", "prod")
    config = load_config(path)
    assert config.engine.active.jdbc_url == "jdbc:sqlite:prod.db"
    assert SettingSource("Secrets.Profile", "ACTIVE_PROFILE", "ACTIVE_PROFILE") in config.settings
    # Unset, the text itself is the profile -- which does not exist, and the
    # error says the variable was missing rather than leaving it to guesswork.
    monkeypatch.delenv("ACTIVE_PROFILE")
    with pytest.raises(ConfigurationError, match="no variable named ACTIVE_PROFILE is set"):
        load_config(path)


def test_an_unselected_multi_profile_section_is_refused(tmp_path):
    raw = _minimal(
        Engine={"dev": {"jdbc_url": "jdbc:sqlite:a.db"}, "prod": {"jdbc_url": "jdbc:sqlite:b.db"}}
    )
    with pytest.raises(ConfigurationError, match="none is selected"):
        load_config(_write(tmp_path, raw))


def test_a_missing_profile_names_the_ones_that_exist(tmp_path):
    raw = _minimal(Secrets={"Source_type": "environment", "Profile": "qa"})
    with pytest.raises(ConfigurationError, match=r"no profile 'qa' \(it has \['dev'\]\)"):
        load_config(_write(tmp_path, raw))


def test_every_value_is_a_variable_when_one_is_set_and_the_text_otherwise(tmp_path, monkeypatch):
    raw = _minimal(
        Orchestration={
            "Mode": "RUN_MODE",
            "Task_timeout_seconds": "TASK_TIMEOUT",
            "Max_parallel_tasks": 4,
            "dev": {"Tags": "DAG_TAGS", "Allow_schedule": "ALLOW_SCHEDULE", "Retries": "2"},
        }
    )
    path = _write(tmp_path, raw)
    for name, value in {
        "RUN_MODE": "remote",
        "TASK_TIMEOUT": "3600",
        "DAG_TAGS": "finance, daily",
        "ALLOW_SCHEDULE": "false",
    }.items():
        monkeypatch.setenv(name, value)
    config = load_config(path)
    # Variables' values, typed as their setting needs.
    assert config.mode == "remote"
    assert config.limits.task_timeout_seconds == 3600
    assert config.dag_defaults.tags == ["finance", "daily"]
    assert config.dag_defaults.allow_schedule is False
    # Values as written, whether YAML typed them or not.
    assert config.limits.max_parallel_tasks == 4
    assert config.dag_defaults.retries == 2
    sources = {source.where: source for source in config.settings}
    assert sources["Orchestration.dev.Mode"].variable == "RUN_MODE"
    assert sources["Orchestration.dev.Retries"].variable is None

    # A variable that is not set leaves its name as the value -- here an
    # invalid Mode, and the error names the missing variable.
    monkeypatch.delenv("RUN_MODE")
    with pytest.raises(ConfigurationError, match="no variable named RUN_MODE is set"):
        load_config(path)


def test_the_source_itself_can_come_from_the_environment(tmp_path, monkeypatch):
    (tmp_path / "deploy.env").write_text("ENGINE_URL=jdbc:sqlite:from-file.db\n", "utf-8")
    raw = _minimal(
        Secrets={"Source_type": "SECRETS_SOURCE", "Path": "SECRETS_FILE"},
        Engine={"dev": {"jdbc_url": "ENGINE_URL"}},
    )
    monkeypatch.setenv("SECRETS_SOURCE", "file")
    monkeypatch.setenv("SECRETS_FILE", "deploy.env")
    # Everything past Secrets resolves against the file, not the environment.
    monkeypatch.setenv("ENGINE_URL", "jdbc:sqlite:from-environment.db")
    config = load_config(_write(tmp_path, raw))
    assert config.source.type == "file"
    assert config.engine.active.jdbc_url == "jdbc:sqlite:from-file.db"


def test_a_secret_is_never_taken_as_written(tmp_path):
    raw = _minimal(
        Engine={
            "dev": {
                "jdbc_url": "jdbc:postgresql://db/etl",
                "user": "etl",
                "auth_mode": "password",
                "secret": "hunter2!",
            }
        }
    )
    with pytest.raises(
        ConfigurationError, match="must be the name of a variable holding the secret"
    ):
        load_config(_write(tmp_path, raw))


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
    engine = load_config(_write(tmp_path, raw)).engine.active
    assert engine.jdbc_url == "jdbc:postgresql://prod/etl"
    assert engine.secret_var == "ENGINE_PROD_SECRET"


def test_values_come_from_a_secrets_file_relative_to_the_config(tmp_path):
    (tmp_path / "secrets.env").write_text(
        "WH_URL=jdbc:postgresql://wh/analytics\nWH_USER=analyst\nWH_AUTH=password\n"
        "WH_SECRET=hunter2\n",
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
    with pytest.raises(ConfigurationError, match="must be the name of a variable"):
        load_config(_write(tmp_path, raw))


def test_a_secret_variable_that_is_not_set_is_a_load_time_error(tmp_path, monkeypatch):
    # Checked at load, not at the first connection.
    monkeypatch.delenv("ENGINE_SECRET", raising=False)
    monkeypatch.delenv("ENGINE_PROD_SECRET", raising=False)
    monkeypatch.setenv("ENGINE_USER", "etl")
    raw = _minimal(
        Secrets={"Source_type": "environment", "Profile": "prod"},
        Engine={
            "dev": {"jdbc_url": "jdbc:sqlite:engine.db"},
            "prod": {
                "jdbc_url": "jdbc:postgresql://db/etl",
                "user": "ENGINE_USER",
                "auth_mode": "password",
                "secret": "ENGINE_SECRET",
            },
        },
    )
    path = _write(tmp_path, raw)
    # Both names it would accept are given: the profile's own and the plain one.
    with pytest.raises(
        ConfigurationError,
        match=r"Engine\.prod\.secret names the secret variable 'ENGINE_PROD_SECRET' or "
        r"'ENGINE_SECRET', which is not set in the process environment",
    ):
        load_config(path)
    monkeypatch.setenv("ENGINE_SECRET", "x")
    assert load_config(path).engine.active.secret_var == "ENGINE_SECRET"

    # Only the selected profile's secrets are checked: dev needs none of them.
    monkeypatch.delenv("ENGINE_SECRET")
    raw["Secrets"]["Profile"] = "dev"
    assert load_config(_write(tmp_path, raw)).engine.active.auth_mode == "none"

    # The same rule for a secrets file, and for token and s3_secret fields.
    (tmp_path / "s.env").write_text("DBX_URL=jdbc:databricks://h:443/default\n", encoding="utf-8")
    dbx = _minimal(
        Secrets={"Source_type": "file", "Path": "s.env"},
        Warehouse={
            "Name": "Databricks",
            "dev": {"jdbc_url": "DBX_URL", "catalog": "main", "schema": "a", "token": "DBX_TOKEN"},
        },
    )
    with pytest.raises(
        ConfigurationError, match=r"'DBX_DEV_TOKEN' or 'DBX_TOKEN', which is not set in "
    ):
        load_config(_write(tmp_path, dbx))
    iceberg = _minimal(
        Warehouse={
            "Name": "DuckDB",
            "Table_format": "iceberg",
            "dev": {
                "jdbc_url": "jdbc:duckdb:",
                "catalog": "lake",
                "catalog_uri": "http://localhost:8181",
                "iceberg_warehouse": "s3://lake",
                "s3_key_id": "minio",
                "s3_secret": "LAKE_S3_SECRET",
            },
        }
    )
    monkeypatch.delenv("LAKE_S3_SECRET", raising=False)
    with pytest.raises(ConfigurationError, match="'LAKE_S3_SECRET', which is not set"):
        load_config(_write(tmp_path, iceberg))


def test_the_earlier_layouts_are_refused_with_a_pointer(tmp_path):
    legacy = {"Execution": {"Mode": "local"}, "Source": {"Type": "environment"}}
    with pytest.raises(ConfigurationError, match=r"earlier craft-connector\.yml layout"):
        load_config(_write(tmp_path, legacy))
    variables = _minimal(Engine={"Profile": "dev", "Variables": {"jdbc_url": "X"}})
    with pytest.raises(ConfigurationError, match="Variables blocks"):
        load_config(_write(tmp_path, variables))


def test_sections_must_appear_in_the_documented_order(tmp_path):
    # Secrets, Orchestration, Engine, Warehouse, then Cloning -- the order is
    # part of the format. An omitted optional section does not count.
    raw = _minimal()
    reordered = {"Engine": raw["Engine"], "Secrets": raw["Secrets"]}
    reordered["Orchestration"] = raw["Orchestration"]
    with pytest.raises(
        ConfigurationError, match="write them as \\['Secrets', 'Orchestration', 'Engine'"
    ):
        load_config(_write(tmp_path, reordered))
    cloning_first = {"Cloning": {"Enabled": False}, **_minimal()}
    with pytest.raises(ConfigurationError, match="then Cloning"):
        load_config(_write(tmp_path, cloning_first))
    with_cloning = _minimal(Cloning={"Enabled": False})
    assert load_config(_write(tmp_path, with_cloning)).cloning.enabled is False


def test_unknown_sections_and_keys_are_refused(tmp_path):
    with pytest.raises(ConfigurationError, match="unknown top-level section"):
        load_config(_write(tmp_path, _minimal(Warehosue={})))
    with pytest.raises(ConfigurationError, match=r"unknown key\(s\) \['Retires'\]"):
        load_config(_write(tmp_path, _minimal(Orchestration={"Mode": "local", "Retires": 1})))
    typo = _minimal(Engine={"dev": {"jdbc_url": "jdbc:sqlite:e.db", "usr": "X"}})
    with pytest.raises(ConfigurationError, match=r"unknown key\(s\) \['usr'\] in Engine.dev"):
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
    with pytest.raises(ConfigurationError, match=message):
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
    with pytest.raises(ConfigurationError, match="always native"):
        load_config(_write(tmp_path, iceberg_postgres))


def test_warehouse_name_must_match_its_jdbc_url(tmp_path):
    raw = _minimal(Warehouse={"Name": "Snowflake", "dev": {"jdbc_url": "jdbc:duckdb:w.duckdb"}})
    with pytest.raises(ConfigurationError, match="resolves to 'duckdb'"):
        load_config(_write(tmp_path, raw))


def test_an_auth_mode_a_warehouse_does_not_offer_is_refused_at_load(tmp_path, monkeypatch):
    # Refused while loading, not at the first connection: a DuckDB file has
    # nothing to present a key to, and Trino has no AWS IAM login.
    monkeypatch.setenv("WAREHOUSE_AUTH_MODE", "key_file")
    duckdb = _minimal(
        Warehouse={"dev": {"jdbc_url": "jdbc:duckdb:w.duckdb", "auth_mode": "WAREHOUSE_AUTH_MODE"}}
    )
    with pytest.raises(ConfigurationError, match=r"DuckDB warehouse takes \['none'\]"):
        load_config(_write(tmp_path, duckdb))
    trino = _minimal(
        Warehouse={
            "Name": "Trino",
            "dev": {"jdbc_url": "jdbc:trino://t:8080/iceberg/a", "auth_mode": "sts"},
        }
    )
    with pytest.raises(ConfigurationError, match="Trino warehouse takes"):
        load_config(_write(tmp_path, trino))


@pytest.mark.parametrize(
    ("warehouse", "missing"),
    [
        # Each auth mode names the fields it needs; a missing one is reported.
        (
            {
                "jdbc_url": "jdbc:postgresql://db/wh",
                "user": "u",
                "auth_mode": "oauth",
                "secret": "S",
            },
            "client_id",
        ),
        (
            {"jdbc_url": "jdbc:postgresql://db/wh", "user": "u", "auth_mode": "sts"},
            "region",
        ),
        (
            {"jdbc_url": "jdbc:trino://t:8080/iceberg/a", "auth_mode": "key_file", "key_file": "k"},
            "cert_file",
        ),
    ],
)
def test_an_auth_mode_needs_its_own_fields(tmp_path, monkeypatch, warehouse, missing):
    monkeypatch.setenv("S", "x")
    raw = _minimal(Warehouse={"dev": warehouse})
    with pytest.raises(ConfigurationError, match=f"needs {missing}"):
        load_config(_write(tmp_path, raw))


def test_every_auth_mode_loads_with_its_fields(tmp_path, monkeypatch):
    # One profile per (warehouse, auth mode) the dialects accept, built from
    # the fields each declares: the loader and the dialects agree.
    monkeypatch.setenv("S", "x")

    urls = {
        "postgres": ("Postgres", "jdbc:postgresql://db/wh"),
        "trino_iceberg": ("Trino", "jdbc:trino://t:8080/iceberg/a"),
        "snowflake": ("Snowflake", "jdbc:snowflake://acct.snowflakecomputing.com/?db=DB"),
    }
    loaded = 0
    for key, (name, url) in urls.items():
        dialect = warehouse_by_key(key)
        for mode, needed in dialect.auth_fields.items():
            block = {"jdbc_url": url, "auth_mode": mode}
            for field_name in needed:
                block[field_name] = "S" if field_name == "secret" else "value"
            raw = _minimal(Warehouse={"Name": name, "dev": block})
            config = load_config(_write(tmp_path, raw))
            assert config.warehouse is not None
            assert config.warehouse.active.auth_mode == mode
            loaded += 1
    assert loaded == 18


def test_databricks_token_fields_build_a_credential_free_url(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "DBX_URL",
        "jdbc:databricks://adb.example.net:443/default;httpPath=/sql/1.0/warehouses/x;"
        "AuthMech=3;UID=token;PWD=dapi-leaked",
    )
    monkeypatch.setenv("DBX_CATALOG", "main")
    monkeypatch.setenv("DBX_SCHEMA", "analytics")
    monkeypatch.setenv("DBX_TOKEN", "dapi-real")
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
    with pytest.raises(
        ConfigurationError, match=r"only for Warehouse\.Name Databricks or Snowflake"
    ):
        load_config(_write(tmp_path, raw))
    extra_field = _minimal(
        Warehouse={
            "Name": "Databricks",
            "dev": {"jdbc_url": "U", "catalog": "C", "schema": "S", "token": "T", "user": "X"},
        }
    )
    with pytest.raises(ConfigurationError, match=r"unknown key\(s\) \['user'\]"):
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
    with pytest.raises(ConfigurationError, match="Retries must be a whole number"):
        load_config(_write(tmp_path, raw))
    with pytest.raises(ConfigurationError, match="Mode must be local or remote"):
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
    with pytest.raises(ConfigurationError, match="needs user for auth_mode password"):
        load_config(_write(tmp_path, raw))


def test_log_dir_defaults_beside_the_config_and_can_be_set(tmp_path):
    assert load_config(_write(tmp_path, _minimal())).log_dir == (tmp_path / "logs").resolve()
    raw = _minimal(Orchestration={"Mode": "local", "Log_dir": "../task-logs"})
    assert load_config(_write(tmp_path, raw)).log_dir == (tmp_path.parent / "task-logs").resolve()
    raw = _minimal(Orchestration={"Mode": "local", "Log_dir": "/var/log/etl"})
    assert load_config(_write(tmp_path, raw)).log_dir == Path("/var/log/etl")
