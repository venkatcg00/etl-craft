"""Object-store settings preserve variable names and reject ambiguous public values."""

from copy import deepcopy

import pytest

from etl_craft.config import parse_config
from etl_craft.core.enums import ObjectStoreAuth, ObjectUrlStyle
from etl_craft.core.errors import ConfigurationError

pytestmark = pytest.mark.unit

RAW = {
    "Secrets": {"Source_type": "ENVIRONMENT"},
    "Engine": {"dev": {"Jdbc_url": "jdbc:sqlite:engine.db", "Schema": "main"}},
    "Object_stores": {"artifacts": {"Auth_mode": "DEFAULT", "Url_style": "PATH"}},
    "Artifacts": {"Store": "s3://bundles/project", "Profile": "artifacts"},
}


def test_profiles_keep_names_and_root_file_locations(tmp_path, monkeypatch):
    raw = deepcopy(RAW)
    monkeypatch.setenv("BUNDLE_KEY", "private-access")
    monkeypatch.setenv("BUNDLE_SECRET", "private-secret")
    raw["Object_stores"]["artifacts"].update(
        Auth_mode="KEYS", Access_key_id="BUNDLE_KEY", Secret_access_key="BUNDLE_SECRET"
    )
    config = parse_config(raw, tmp_path / "craft-connector.yml")
    profile = config.object_stores["artifacts"]
    assert profile.auth_mode == ObjectStoreAuth.KEYS
    assert profile.url_style == ObjectUrlStyle.PATH
    assert profile.access_key_id == "BUNDLE_KEY"
    assert profile.secret_access_key == "BUNDLE_SECRET"
    assert "private-secret" not in repr(config)
    raw["Artifacts"] = {"Store": "file:///"}
    assert parse_config(raw, tmp_path / "craft-connector.yml").artifacts.store == "file:///"


@pytest.mark.parametrize(
    "field,value",
    [
        ("Auth_mode", "keys"),
        ("Url_style", "path"),
        ("Endpoint", "https://user:password@host"),
        ("Endpoint", "https://["),
        ("Unknown", "x"),
        ("Access_key_id", "UNEXPECTED"),
    ],
)
def test_invalid_profiles_fail_without_echoing_secrets(tmp_path, field, value):
    raw = deepcopy(RAW)
    raw["Object_stores"]["artifacts"][field] = value
    with pytest.raises(ConfigurationError) as caught:
        parse_config(raw, tmp_path / "craft-connector.yml")
    assert "Object_stores.artifacts" in str(caught.value)
    assert "password" not in str(caught.value)


@pytest.mark.parametrize(
    "store,profile",
    [
        ("https://bucket/a", "artifacts"),
        ("s3://bucket/a", "missing"),
        ("file://server/a", None),
        ("file:///a", "artifacts"),
        ("s3://key:secret@bucket/a", "artifacts"),
    ],
)
def test_bad_store_locations_name_the_remedy(tmp_path, store, profile):
    raw = deepcopy(RAW)
    raw["Artifacts"] = {"Store": store, "Profile": profile}
    with pytest.raises(ConfigurationError, match="correct the location/profile"):
        parse_config(raw, tmp_path / "craft-connector.yml")


def test_pinned_config_keeps_host_infrastructure_and_project_warehouse_limits(tmp_path):
    from dataclasses import replace

    import yaml

    from etl_craft.config.project import project_config

    raw = deepcopy(RAW)
    raw.pop("Object_stores")
    raw.pop("Artifacts")
    raw["Warehouse"] = {"dev": {"Jdbc_url": "jdbc:duckdb:local.duckdb", "Schema": "main"}}
    host_path = tmp_path / "host" / "craft-connector.yml"
    host = parse_config(raw, host_path)
    host = replace(host, max_warehouse_queries=7, limits=replace(host.limits, local_python_slots=2))
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    raw = {
        "Secrets": raw["Secrets"],
        "Orchestration": {
            "Task_timeout_seconds": 3,
            "Local_python_slots": 1,
        },
        "Engine": raw["Engine"],
        "Warehouse": raw["Warehouse"],
    }
    raw["Warehouse"]["Max_warehouse_queries"] = 4
    path = bundle / "craft-connector.yml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    pinned = project_config(host, path)
    assert pinned.project_dir == bundle
    assert pinned.engine == host.engine
    assert pinned.warehouse.jdbc_url == host.warehouse.jdbc_url
    assert pinned.max_warehouse_queries == 4
    assert pinned.limits.local_python_slots == 2
    assert pinned.limits.task_timeout_seconds == 3
    assert pinned._host_config_path == host_path


def test_an_agent_can_load_profiles_without_artifact_credentials(tmp_path, monkeypatch):
    raw = deepcopy(RAW)
    monkeypatch.delenv("ABSENT_BUNDLE_KEY", raising=False)
    monkeypatch.delenv("ABSENT_BUNDLE_SECRET", raising=False)
    raw["Object_stores"]["artifacts"].update(
        Auth_mode="KEYS",
        Access_key_id="ABSENT_BUNDLE_KEY",
        Secret_access_key="ABSENT_BUNDLE_SECRET",
    )
    raw.pop("Engine")
    config = parse_config(raw, tmp_path / "craft-connector.yml", agent=True)
    assert config.engine.jdbc_url == "api-only:"
    assert config.object_stores["artifacts"].secret_access_key == "ABSENT_BUNDLE_SECRET"
    from etl_craft.dialects.credentials import S3Objects

    with pytest.raises(ConfigurationError, match="ABSENT_BUNDLE_KEY"):
        S3Objects(config, config.object_stores["artifacts"])


@pytest.mark.parametrize("budget", [0, 11, 8192])
def test_log_settings_use_explicit_profiles_and_byte_budgets(tmp_path, budget):
    raw = deepcopy(RAW)
    raw["Logs"] = {"Store": "s3://logs/attempts", "Profile": "artifacts", "Tail_bytes": budget}
    config = parse_config(raw, tmp_path / "craft-connector.yml")
    assert config.logs.store == "s3://logs/attempts"
    assert config.logs.profile == "artifacts"
    assert config.logs.tail_bytes == budget
    raw.pop("Logs")
    assert parse_config(raw, tmp_path / "craft-connector.yml").logs.tail_bytes == 8192


@pytest.mark.parametrize(
    "values",
    [
        {"Store": "s3://logs/attempts", "Profile": "missing"},
        {"Store": "file:///logs", "Profile": "artifacts"},
        {"Profile": "artifacts"},
        {"Tail_bytes": -1},
        {"Tail_bytes": True},
        {"Tail_bytes": 1.5},
    ],
)
def test_bad_log_settings_fail_before_any_store_access(tmp_path, values):
    raw = deepcopy(RAW)
    raw["Logs"] = values
    with pytest.raises(ConfigurationError) as caught:
        parse_config(raw, tmp_path / "craft-connector.yml")
    assert "Logs." in str(caught.value)


def test_sqlite_bundle_store_defaults_to_the_host_project_and_keeps_explicit_store(tmp_path):
    from dataclasses import replace

    from etl_craft.config.object_models import Artifacts
    from etl_craft.dialects.engine import for_jdbc_url

    raw = deepcopy(RAW)
    raw.pop("Artifacts")
    config = parse_config(raw, tmp_path / "craft-connector.yml")
    dialect = for_jdbc_url(config.engine.jdbc_url)
    expected = (tmp_path / ".etl-craft" / "artifacts").as_uri()
    assert dialect.artifact_location(config) == expected
    pinned = replace(
        config,
        config_path=tmp_path / "cache" / "craft-connector.yml",
        _host_config_path=config.config_path,
    )
    assert dialect.artifact_location(pinned) == expected
    configured = replace(config, artifacts=Artifacts("s3://durable/bundles", "artifacts"))
    assert dialect.artifact_location(configured) == "s3://durable/bundles"


def test_postgres_bundle_store_requires_an_explicit_location(tmp_path):
    from etl_craft.dialects.engine import for_jdbc_url

    raw = deepcopy(RAW)
    raw.pop("Artifacts")
    config = parse_config(raw, tmp_path / "craft-connector.yml")
    dialect = for_jdbc_url("jdbc:postgresql://localhost/engine")
    with pytest.raises(
        ConfigurationError, match=r"Artifacts\.Store is unset for the PostgreSQL"
    ) as refused:
        dialect.artifact_location(config)
    assert str(config.config_path) in str(refused.value)
    assert "configure Artifacts.Store" in str(refused.value)
