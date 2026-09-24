"""Each way craft-connector.yml can be wrong gets its own message naming the setting."""

import pytest
import yaml

from etl_craft.config import load_config
from etl_craft.config.targets import parse_warehouse_url
from etl_craft.core.errors import ConfigurationError, ExitCode

pytestmark = pytest.mark.unit

SECRETS = {"Source_type": "environment"}
ORCHESTRATION = {"Mode": "local"}
ENGINE = {"dev": {"jdbc_url": "jdbc:sqlite:e.db"}}


def config(**sections):
    raw = {"Secrets": SECRETS, "Orchestration": ORCHESTRATION, "Engine": ENGINE}
    raw.update(sections)
    return {key: value for key, value in raw.items() if value is not None}


def postgres_warehouse(**fields):
    return {"dev": {"jdbc_url": "jdbc:postgresql://h/wh", **fields}}


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (["a", "list"], "must contain a top-level mapping"),
        (config(Secrets=None), "missing the Secrets section"),
        (config(Secrets="environment"), "Secrets must be a mapping"),
        (config(Secrets={"Source_type": "vault"}), "Secrets.Source_type must be one of"),
        (
            config(Secrets={"Source_type": "environment", "Path": "x.env"}),
            "Secrets.Path is only used with Source_type: file",
        ),
        (config(Secrets={"Source_type": "file"}), "Secrets.Path is required"),
        (config(Engine={"dev": {"user": "u"}}), "Engine.dev needs jdbc_url"),
        (config(Engine={"Name": "Postgres"}), "Engine needs at least one profile block"),
        (
            config(Warehouse=postgres_warehouse(user="u", auth_mode="password")),
            r"needs secret \(a variable name\) for auth_mode password",
        ),
        (
            config(Orchestration={"Mode": "local", "Tags": 5}),
            "Orchestration.Tags must be a list of strings",
        ),
        (
            config(Orchestration={"Mode": "local", "Email": "smtp.internal"}),
            "Orchestration.Email must be a mapping",
        ),
        (
            config(
                Orchestration={
                    "Mode": "local",
                    "Email": {"host": "h", "port": "twenty-five", "from_address": "a@b"},
                }
            ),
            "port resolved to 'twenty-five', not a number",
        ),
        (
            config(
                Orchestration={
                    "Mode": "local",
                    "Email": {"host": "h", "port": 25, "from_address": "a@b", "auth_mode": "sso"},
                }
            ),
            "auth_mode resolved to 'sso', which is not one of",
        ),
        (
            config(Warehouse={"Table_format": "delta", **postgres_warehouse()}),
            "Warehouse.Table_format must be native or iceberg",
        ),
        (
            config(Warehouse={"Name": "Oracle", **postgres_warehouse()}),
            r"Warehouse.Name 'Oracle' must be one of \['Databricks', 'DuckDB', 'Postgres'",
        ),
        (
            config(
                Warehouse={
                    "Name": "Databricks",
                    "dev": {
                        "jdbc_url": "jdbc:databricks://h/default;httpPath=/p",
                        "catalog": "main",
                        "schema": "s",
                        "token": "T",
                        "secret": "T",
                    },
                }
            ),
            "names its token under `token`, not `secret`",
        ),
        (
            config(
                Warehouse={
                    "Name": "Snowflake",
                    "dev": {
                        "user": "u",
                        "account": "https://acct.snowflakecomputing.com",
                        "database": "D",
                        "schema": "S",
                        "warehouse": "W",
                        "role": "R",
                        "auth_mode": "sts",
                    },
                }
            ),
            r"Warehouse\.dev: Snowflake account must be an account identifier",
        ),
        (
            config(Warehouse={"dev": {"jdbc_url": "jdbc:duckdb:/d/my-wh.duckdb"}}),
            r"Warehouse\.dev: DuckDB warehouse file",
        ),
        (config(Cloning={"Scope": "everything"}), "Cloning.Scope must be one of"),
        (config(Cloning={"Enabled": "maybe"}), "Cloning.Enabled must be true or false"),
        (config(Orchestration={"Mode": "local", "Enforce_sla": 3}), "must be true or false"),
    ],
)
def test_a_configuration_error_names_the_setting(tmp_path, monkeypatch, raw, message):
    monkeypatch.setenv("T", "token-value")
    path = tmp_path / "craft-connector.yml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    with pytest.raises(ConfigurationError, match=message) as error:
        load_config(path)
    assert error.value.exit_code is ExitCode.USAGE


def test_invalid_yaml_is_a_configuration_error(tmp_path):
    path = tmp_path / "craft-connector.yml"
    path.write_text("Secrets: [unclosed\n", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="is not valid YAML"):
        load_config(path)


def test_an_empty_file_is_missing_its_sections(tmp_path):
    path = tmp_path / "craft-connector.yml"
    path.write_text("", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="missing the Secrets section"):
        load_config(path)


@pytest.mark.parametrize(("written", "enabled"), [("", False), ("yes", True), ("0", False)])
def test_boolean_spellings(tmp_path, written, enabled):
    path = tmp_path / "craft-connector.yml"
    raw = config(Cloning={"Enabled": written, "Scope": "all"})
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    assert load_config(path).cloning.enabled is enabled


def test_databricks_parameters_without_a_value_are_ignored():
    url = parse_warehouse_url("jdbc:databricks://h/default;flag;httpPath=/p")
    assert url.query == {"http_path": "/p"}
