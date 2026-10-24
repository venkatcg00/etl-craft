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
