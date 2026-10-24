"""Managed Engine DB contracts and fresh, secret-safe database credentials."""

import io
import json
from dataclasses import replace
from unittest.mock import MagicMock

import pytest

from etl_craft.config import parse_config
from etl_craft.core.enums import ManagedProvider
from etl_craft.core.errors import ConfigurationError
from etl_craft.dialects import credentials, managed_postgres
from etl_craft.dialects.engine import build_engine
from etl_craft.dialects.engine.postgres import parse_postgres_url, postgres_creator
from etl_craft.services.cloning import cloning_problem

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def engine_secret(monkeypatch):
    monkeypatch.setenv("ENGINE_SECRET", "test-client-secret")


def configuration(tmp_path, provider="DATABRICKS", **fields):
    extra = (
        {
            "Workspace_url": "https://workspace.example",
            "Endpoint": "projects/p/branches/b/endpoints/e",
        }
        if provider == "DATABRICKS"
        else {"Account": "org-account", "Instance": "metadata"}
    )
    return parse_config(
        {
            "Secrets": {"Source_type": "ENVIRONMENT"},
            "Engine": {
                "dev": {
                    "Jdbc_url": "jdbc:postgresql://db.example/metadata?sslmode=verify-full",
                    "Schema": "metadata",
                    "User": "service",
                    "Auth_mode": "OAUTH",
                    "Client_id": "client",
                    "Secret": "ENGINE_SECRET",
                    "Token_url": "https://identity.example/token",
                    "Managed_provider": provider,
                    **extra,
                    **fields,
                }
            },
        },
        tmp_path / "craft-connector.yml",
    )


@pytest.mark.parametrize("provider", ["DATABRICKS", "SNOWFLAKE"])
def test_managed_profiles_preserve_explicit_provider_and_recycle_connections(tmp_path, provider):
    config = configuration(tmp_path, provider)
    assert config.engine.managed_provider == ManagedProvider(provider)
    engine = build_engine(config)
    try:
        assert engine.pool._recycle == credentials.MINTED_CREDENTIAL_POOL_RECYCLE_SECONDS
    finally:
        engine.dispose()
    enabled = replace(config, cloning=replace(config.cloning, enabled=True))
    assert "Cloning.Enabled=True" in cloning_problem(enabled)
    assert "set Cloning.Enabled=False" in cloning_problem(enabled)


@pytest.mark.parametrize(
    "fields, expected",
    [
        ({"Managed_provider": "GUESS"}, "Managed_provider"),
        ({"Jdbc_url": "jdbc:postgresql://db.example/metadata?sslmode=prefer"}, "sslmode='prefer'"),
        ({"Listen_jdbc_url": "jdbc:postgresql://db.example/metadata"}, "sslmode=None"),
        ({"Workspace_url": "http://workspace.example"}, "HTTPS"),
        ({"Workspace_url": "https://[malformed"}, "HTTPS"),
        ({"Workspace_url": "https://workspace.example:bad"}, "HTTPS"),
        ({"Workspace_url": "https://work space.example"}, "HTTPS"),
        ({"Workspace_url": "https://user:password@workspace.example"}, "credential-free"),
        ({"Workspace_url": "https://workspace.example/path"}, "HTTPS origin"),
        ({"Endpoint": "instance-guess"}, "projects/<project>"),
        ({"Account": "unexpected"}, "expected absent"),
        ({"Auth_mode": "PASSWORD"}, "expected OAUTH"),
    ],
)
def test_invalid_managed_profiles_fail_before_connecting(tmp_path, fields, expected):
    with pytest.raises(ConfigurationError) as error:
        configuration(tmp_path, **fields)
    assert expected in str(error.value)
    assert "Engine.dev" in str(error.value)


def test_lakebase_exchanges_the_workspace_token_for_a_database_password(tmp_path, monkeypatch):
    config = configuration(tmp_path)
    grant = MagicMock(return_value="workspace-token")
    monkeypatch.setattr(managed_postgres, "client_credentials_token", grant)
    requests = []

    def open_request(request, timeout):
        requests.append(request)
        assert timeout == 30
        return io.StringIO(json.dumps({"token": "database-token"}))

    monkeypatch.setattr(managed_postgres, "open_request", open_request)
    assert managed_postgres.database_token(config.engine, "private-secret") == "database-token"
    assert grant.call_args.kwargs == {"basic_auth": True}
    assert requests[0].full_url == "https://workspace.example/api/2.0/postgres/credentials"
    assert requests[0].get_header("Authorization") == "Bearer workspace-token"
    assert json.loads(requests[0].data) == {"endpoint": "projects/p/branches/b/endpoints/e"}


def test_snowflake_issues_a_database_token_with_bound_instance_and_user(tmp_path, monkeypatch):
    config = configuration(tmp_path, "SNOWFLAKE", Instance="name'with quote")
    monkeypatch.setattr(managed_postgres, "client_credentials_token", lambda *a, **k: "oauth-token")
    connector = MagicMock()
    connection = connector.connect.return_value.__enter__.return_value
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = ("database-token",)
    driver = MagicMock()
    driver.import_dbapi.return_value = connector
    monkeypatch.setattr(managed_postgres.registry, "load", lambda name: driver)
    assert managed_postgres.database_token(config.engine, "private-secret") == "database-token"
    assert cursor.execute.call_args.args[1] == ("name'with quote", "service")
    assert connector.connect.call_args.kwargs["token"] == "oauth-token"


@pytest.mark.parametrize("provider", ["DATABRICKS", "SNOWFLAKE"])
def test_every_new_postgres_connection_mints_a_fresh_token(tmp_path, monkeypatch, provider):
    profile = configuration(tmp_path, provider).engine
    tokens = iter(["first-token", "second-token"])
    monkeypatch.setattr(managed_postgres, "database_token", lambda *a: next(tokens))
    connect = MagicMock()
    monkeypatch.setattr("psycopg.connect", connect)
    create = postgres_creator(profile, "secret", parse_postgres_url(profile.jdbc_url))
    create()
    create()
    assert [call.kwargs["password"] for call in connect.call_args_list] == [
        "first-token",
        "second-token",
    ]
    assert all(call.kwargs["sslmode"] == "verify-full" for call in connect.call_args_list)


def test_provider_refusals_never_repeat_credential_bearing_errors(tmp_path, monkeypatch):
    def refused(*args, **kwargs):
        raise OSError("private-secret workspace-token signed-url")

    monkeypatch.setattr(managed_postgres, "client_credentials_token", refused)
    with pytest.raises(ConfigurationError) as error:
        managed_postgres.database_token(configuration(tmp_path).engine, "private-secret")
    message = str(error.value)
    assert "Engine.dev" in message
    assert "expected a fresh database token" in message
    assert "check provider OAuth settings" in message
    assert "private-secret" not in message
    assert "workspace-token" not in message
    assert "signed-url" not in message


@pytest.mark.parametrize("payload", [{}, {"token": ""}, ["unexpected"]])
def test_lakebase_refuses_missing_database_tokens(tmp_path, monkeypatch, payload):
    monkeypatch.setattr(managed_postgres, "client_credentials_token", lambda *a, **k: "token")
    monkeypatch.setattr(
        managed_postgres, "open_request", lambda *a, **k: io.StringIO(json.dumps(payload))
    )
    with pytest.raises(ConfigurationError, match="database credential issuance failed"):
        managed_postgres.database_token(configuration(tmp_path).engine, "private-secret")


def test_managed_cloning_is_refused_while_loading_the_configuration(tmp_path):
    import yaml

    from etl_craft.config.loader import read_config

    config = configuration(tmp_path)
    raw = {
        "Secrets": {"Source_type": "ENVIRONMENT"},
        "Engine": {
            "dev": {
                "Jdbc_url": config.engine.jdbc_url,
                "Schema": "metadata",
                "User": "service",
                "Auth_mode": "OAUTH",
                "Secret": "ENGINE_SECRET",
                "Client_id": "client",
                "Token_url": "https://identity.example/token",
                "Managed_provider": "SNOWFLAKE",
                "Account": "org-account",
                "Instance": "metadata",
            }
        },
        "Cloning": {"Enabled": True},
    }
    path = tmp_path / "craft-connector.yml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    with pytest.raises(ConfigurationError, match=r"set Cloning.Enabled=False"):
        parse_config(read_config(path), path)


def test_a_bundle_cannot_enable_cloning_on_the_managed_host(tmp_path):
    from etl_craft.config.project import project_config

    host = configuration(tmp_path)
    bundle = tmp_path / "bundle.yml"
    bundle.write_text("Secrets:\n  Source_type: ENVIRONMENT\nCloning:\n  Enabled: True\n")
    with pytest.raises(ConfigurationError, match=r"set Cloning.Enabled=False"):
        project_config(host, bundle)
