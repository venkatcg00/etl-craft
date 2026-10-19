"""Each way craft-connector.yml can be wrong gets its own message naming the setting."""

import pytest
import yaml

from etl_craft.config import load_config
from etl_craft.config.targets import parse_warehouse_url
from etl_craft.core.errors import ConfigurationError, ExitCode

pytestmark = pytest.mark.unit

SECRETS = {"Source_type": "ENVIRONMENT"}
ORCHESTRATION = {}
ENGINE = {"dev": {"Jdbc_url": "jdbc:sqlite:e.db", "Schema": "main"}}


def config(**sections):
    raw = {"Secrets": SECRETS, "Orchestration": ORCHESTRATION, "Engine": ENGINE}
    raw.update(sections)
    return {key: value for key, value in raw.items() if value is not None}


def postgres_warehouse(**fields):
    return {"dev": {"Jdbc_url": "jdbc:postgresql://h/wh", "Schema": "main", **fields}}


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (["a", "list"], "must contain a top-level mapping"),
        (config(Secrets=None), "missing the Secrets section"),
        (config(Secrets="environment"), "Secrets must be a mapping"),
        (config(Secrets={"Source_type": "vault"}), "Secrets.Source_type must be one of"),
        (
            config(Secrets={"Source_type": "ENVIRONMENT", "Path": "x.env"}),
            "Secrets.Path is only used with Source_type: FILE",
        ),
        (config(Secrets={"Source_type": "FILE"}), "Secrets.Path is required"),
        (config(Engine={"dev": {"User": "u"}}), "Engine.dev needs Jdbc_url"),
        (config(Engine={"Name": "Postgres"}), "Engine needs at least one profile block"),
        (
            config(Warehouse=postgres_warehouse(User="u", Auth_mode="PASSWORD")),
            r"needs Secret \(a variable name\) for Auth_mode PASSWORD",
        ),
        (
            config(Orchestration={"Email_recipients": 5}),
            "Orchestration.Email_recipients must be a list of strings",
        ),
        (
            config(Orchestration={"Email": "smtp.internal"}),
            "Orchestration.Email must be a mapping",
        ),
        (
            config(
                Orchestration={
                    "Email": {"Host": "h", "Port": "twenty-five", "From_address": "a@b"},
                }
            ),
            "port must be a whole number 1 to 65535, got 'twenty-five'",
        ),
        (
            config(
                Orchestration={
                    "Email": {"Host": "h", "Port": 25, "From_address": "a@b", "Auth_mode": "SSO"},
                }
            ),
            "Auth_mode resolved to 'SSO', which is not one of",
        ),
        (
            config(Warehouse={"Table_format": "delta", **postgres_warehouse()}),
            "Warehouse.Table_format must be NATIVE or ICEBERG",
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
                        "Jdbc_url": "jdbc:databricks://h/default;httpPath=/p",
                        "Catalog": "main",
                        "Schema": "s",
                        "Token": "T",
                        "Secret": "T",
                    },
                }
            ),
            "names its token under `Token`, not `Secret`",
        ),
        (
            config(
                Warehouse={
                    "Name": "Snowflake",
                    "dev": {
                        "User": "u",
                        "Account": "https://acct.snowflakecomputing.com",
                        "Database": "D",
                        "Schema": "S",
                        "Warehouse": "W",
                        "Role": "R",
                        "Auth_mode": "STS",
                    },
                }
            ),
            r"Warehouse\.dev: Snowflake account must be an account identifier",
        ),
        (
            config(
                Warehouse={"dev": {"Jdbc_url": "jdbc:duckdb:/d/my-wh.duckdb", "Schema": "main"}}
            ),
            r"Warehouse\.dev: DuckDB warehouse file",
        ),
        (config(Cloning={"Scope": "everything"}), "Cloning.Scope must be one of"),
        (config(Cloning={"Enabled": "maybe"}), "Cloning.Enabled must be true or false"),
        (config(Orchestration={"Enforce_sla": 3}), "must be true or false"),
    ],
)
def test_a_configuration_error_names_the_setting(tmp_path, monkeypatch, raw, message):
    monkeypatch.setenv("T", "token-value")
    path = tmp_path / "craft-connector.yml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    with pytest.raises(ConfigurationError, match=message) as error:
        load_config(path)
    assert error.value.exit_code is ExitCode.CONFIGURATION


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
    raw = config(Cloning={"Enabled": written, "Scope": "ALL"})
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    assert load_config(path).cloning.enabled is enabled


def test_databricks_parameters_without_a_value_are_ignored():
    url = parse_warehouse_url("jdbc:databricks://h/default;flag;httpPath=/p")
    assert url.query == {"http_path": "/p"}
