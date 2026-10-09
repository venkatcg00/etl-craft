"""Configuration errors name settings without retaining credentials."""

from pathlib import Path

import pytest
import yaml

from etl_craft.config import load_config, profile_secret
from etl_craft.core.errors import ConfigurationError
from etl_craft.dialects.engine.sqlite import resolve_sqlite_path

pytestmark = pytest.mark.unit


def write_config(tmp_path, **sections):
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local"},
        "Engine": {"dev": {"jdbc_url": "jdbc:sqlite:engine.db", "schema": "main"}},
    }
    raw.update(sections)
    path = tmp_path / "craft-connector.yml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return path


@pytest.mark.parametrize("section", ["Engine", "Warehouse", "Orchestration.Email"])
def test_duplicate_keys_name_path_and_both_lines(tmp_path, section):
    path = tmp_path / "craft-connector.yml"
    prefix = "Orchestration:\n  Email:\n" if section.endswith("Email") else f"{section}:\n"
    indent = "    " if section.endswith("Email") else "  "
    path.write_text(prefix + f"{indent}dev: {{}}\n{indent}dev: {{}}\n")
    with pytest.raises(ConfigurationError) as caught:
        load_config(path)
    message = str(caught.value)
    assert f"{section}.dev" in message
    assert "line" in message
    assert str(len(prefix.splitlines()) + 1) in message
    assert str(len(prefix.splitlines()) + 2) in message


@pytest.mark.parametrize("value", ["", " ", "\t\n"])
def test_blank_secrets_are_refused_at_load_and_connection(tmp_path, monkeypatch, value):
    fields = {
        "jdbc_url": "jdbc:postgresql://localhost/db",
        "schema": "public",
        "user": "etl",
        "auth_mode": "password",
        "secret": "ETL_CRAFT_ENGINE_SECRET",
    }
    path = write_config(tmp_path, Engine={"dev": fields})
    monkeypatch.setenv("ETL_CRAFT_ENGINE_SECRET", value)
    with pytest.raises(ConfigurationError, match=r"ETL_CRAFT_ENGINE_SECRET.*empty"):
        load_config(path)
    monkeypatch.setenv("ETL_CRAFT_ENGINE_SECRET", "valid")
    config = load_config(path)
    monkeypatch.setenv("ETL_CRAFT_ENGINE_SECRET", value)
    with pytest.raises(ConfigurationError, match=r"ETL_CRAFT_ENGINE_SECRET.*empty"):
        profile_secret(config, config.engine)


@pytest.mark.parametrize(
    "key", ["password", "PWD", "passwd", "token", "Access_Token", "secret", "private_key_file_pwd"]
)
@pytest.mark.parametrize("section", ["Engine", "Warehouse"])
def test_jdbc_query_credentials_are_refused_without_echoing_value(tmp_path, key, section):
    profile = {
        "jdbc_url": f"jdbc:postgresql://localhost/db?{key}=never-echo-me",
        "schema": "public",
        "auth_mode": "none",
    }
    path = write_config(tmp_path, **{section: {"dev": profile}})
    with pytest.raises(ConfigurationError) as caught:
        load_config(path)
    assert "never-echo-me" not in str(caught.value)
    assert "put the secret in a variable" in str(caught.value)
    assert key in str(caught.value)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("Max_parallel_tasks", 0),
        ("Max_parallel_tasks", "²"),
        ("Task_timeout_seconds", -1),
        ("Gate_wait_minutes", "\u0661"),
        ("Retries", True),
    ],
)
def test_numeric_settings_are_bounded_ascii_numbers(tmp_path, key, value):
    path = write_config(tmp_path, Orchestration={"Mode": "local", key: value})
    with pytest.raises(ConfigurationError, match=key):
        load_config(path)


@pytest.mark.parametrize("port", [0, 65536, "²", "+587", True])
def test_smtp_ports_are_bounded_ascii_numbers(tmp_path, port):
    path = write_config(
        tmp_path,
        Orchestration={
            "Mode": "local",
            "Email": {"host": "localhost", "port": port, "from_address": "etl@example.com"},
        },
    )
    with pytest.raises(ConfigurationError, match="port"):
        load_config(path)


def test_symlink_config_uses_link_folder_for_secrets_and_all_project_paths(tmp_path, monkeypatch):
    target = tmp_path / "target"
    link = tmp_path / "link"
    target.mkdir()
    link.mkdir()
    path = write_config(target, Secrets={"Source_type": "file", "Path": ".env"})
    (link / ".env").write_text("\ufeffexport SOME_VALUE=ok\n", encoding="utf-8")
    config_link = link / path.name
    config_link.symlink_to(path)
    monkeypatch.chdir(tmp_path)
    config = load_config(Path("link") / path.name)
    assert config.project_dir == link
    assert config.source.path == str(link / ".env")
    assert config.log_dir == link / "logs"
    assert resolve_sqlite_path(config.engine.jdbc_url, config.config_path) == str(
        link / "engine.db"
    )


def test_auth_and_url_paths_are_absolute_from_config_directory(tmp_path, monkeypatch):
    path = write_config(
        tmp_path,
        Engine={
            "dev": {
                "jdbc_url": "jdbc:postgresql://localhost/db?sslrootcert=certs/root.pem",
                "schema": "public",
                "user": "etl",
                "auth_mode": "key_file",
                "cert_file": "certs/client.pem",
                "key_file": "certs/key.pem",
            }
        },
    )
    monkeypatch.chdir(tmp_path.parent)
    profile = load_config(path).engine
    assert profile.extra["cert_file"] == str(tmp_path / "certs/client.pem")
    assert profile.extra["key_file"] == str(tmp_path / "certs/key.pem")
    from etl_craft.core.text import parse_jdbc_url

    assert parse_jdbc_url(profile.jdbc_url).query["sslrootcert"] == str(tmp_path / "certs/root.pem")


def test_env_bom_export_and_missing_secret_suggestions(tmp_path, monkeypatch):
    fields = {
        "jdbc_url": "jdbc:postgresql://localhost/db",
        "schema": "public",
        "user": "etl",
        "auth_mode": "password",
        "secret": "ETL_CRAFT_ENGINE_SECRET",
    }
    path = write_config(
        tmp_path, Secrets={"Source_type": "file", "Path": ".env"}, Engine={"dev": fields}
    )
    env = tmp_path / ".env"
    env.write_text("\ufeffexport ETL_CRAFT_ENGINE_SECRET=valid\n", encoding="utf-8")
    assert profile_secret(load_config(path), load_config(path).engine) == "valid"
    env.write_text("export ETL_CRAFT_ENGINE_SECRETT=valid\n", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="ETL_CRAFT_ENGINE_SECRETT"):
        load_config(path)


def test_iceberg_storage_secret_is_a_name_in_config(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_S3_SECRET", "never-keep-me")
    path = write_config(
        tmp_path,
        Warehouse={
            "Name": "DuckDB",
            "Table_format": "iceberg",
            "dev": {
                "jdbc_url": "jdbc:duckdb:",
                "schema": "public",
                "catalog": "lake",
                "catalog_uri": "http://localhost:8181",
                "iceberg_warehouse": "s3://warehouse/",
                "s3_key_id": "etl",
                "s3_secret": "LAKE_S3_SECRET",
            },
        },
    )
    config = load_config(path)
    assert "never-keep-me" not in repr(config)
    assert config.warehouse.extra["s3_secret"] == "LAKE_S3_SECRET"


@pytest.mark.parametrize("key", ["sslrootcert", "sslcert", "sslkey", "private_key_file"])
def test_doctor_reports_missing_and_readable_resolved_files(tmp_path, key):
    from etl_craft.services.doctor import Status, _files

    path = write_config(
        tmp_path,
        Engine={
            "dev": {
                "jdbc_url": f"jdbc:postgresql://localhost/db?{key}=missing.pem",
                "schema": "public",
                "user": "etl",
                "auth_mode": "key_file",
                "key_file": "key.pem",
            }
        },
    )
    config = load_config(path)
    checks = _files(config)
    assert len(checks) == 2
    assert all(check.status is Status.FAIL for check in checks)
    assert str(tmp_path / "missing.pem") in checks[1].detail
    (tmp_path / "missing.pem").write_text("certificate")
    (tmp_path / "key.pem").write_text("key")
    assert all(check.status is Status.OK for check in _files(config))


@pytest.mark.parametrize("port", [0, 65536])
@pytest.mark.parametrize(
    "url",
    [
        "jdbc:postgresql://localhost:{port}/db",
        "jdbc:snowflake://account:{port}/?db=db",
        "jdbc:databricks://localhost:{port}/default;httpPath=/sql;ConnCatalog=db",
    ],
)
def test_database_ports_are_bounded(url, port):
    from etl_craft.config.targets import parse_warehouse_url

    with pytest.raises(ConfigurationError, match="port"):
        parse_warehouse_url(url.format(port=port))


def test_logged_engine_urls_drop_credential_query_values(tmp_path, monkeypatch):
    from dataclasses import replace

    from etl_craft.engine.connection import engine_db
    from etl_craft.warehouse.connection import build_warehouse_engine

    monkeypatch.setenv("ETL_CRAFT_ENGINE_SECRET", "password")
    path = write_config(
        tmp_path,
        Engine={
            "dev": {
                "jdbc_url": "jdbc:postgresql://localhost/db",
                "schema": "public",
                "user": "etl",
                "auth_mode": "password",
                "secret": "ETL_CRAFT_ENGINE_SECRET",
            }
        },
    )
    config = load_config(path)
    profile = replace(
        config.engine,
        jdbc_url="jdbc:postgresql://localhost/db?password=never-log-me&api_key=never-log-me&sslmode=require",
    )
    config = replace(config, engine=profile, warehouse=profile)
    for engine in (engine_db(config), build_warehouse_engine(config)):
        try:
            assert "never-log-me" not in repr(engine.url)
            assert engine.url.query == {"sslmode": "require"}
        finally:
            engine.dispose()


def test_relative_sendmail_executable_is_project_relative(tmp_path, monkeypatch):
    from etl_craft.services.doctor import Status, _files

    path = write_config(
        tmp_path,
        Orchestration={
            "Mode": "local",
            "Email": {
                "transport": "sendmail",
                "from_address": "etl@example.com",
                "sendmail_path": "bin/sendmail",
            },
        },
    )
    monkeypatch.chdir(tmp_path.parent)
    config = load_config(path)
    assert config.email.sendmail_path == str(tmp_path / "bin/sendmail")
    (check,) = _files(config)
    assert check.status is Status.FAIL
    assert str(tmp_path / "bin/sendmail") in check.detail
