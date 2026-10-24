"""Bad artifacts fail before execution, without leaking uploaded configuration bytes."""

import base64
import io
from types import SimpleNamespace

import pytest

from etl_craft.config import parse_config
from etl_craft.config.object_models import ObjectStore
from etl_craft.config.project import project_config
from etl_craft.core.errors import ConfigurationError, UsageError
from etl_craft.core.file_bytes import read_regular
from etl_craft.core.text import sha256_hex
from etl_craft.dialects.credentials import S3Objects
from etl_craft.engine import project_files
from etl_craft.engine.artifacts import ArtifactStore

pytestmark = pytest.mark.unit


def config(tmp_path):
    return parse_config(
        {
            "Secrets": {"Source_type": "ENVIRONMENT"},
            "Engine": {"dev": {"Jdbc_url": "jdbc:sqlite:engine.db", "Schema": "main"}},
        },
        tmp_path / "craft-connector.yml",
    )


@pytest.mark.parametrize("kind", ["symlink", "fifo", "oversize", "missing"])
def test_regular_reads_reject_unsafe_files_and_name_the_remedy(tmp_path, kind):
    import os

    path = tmp_path / "file"
    if kind == "symlink":
        (tmp_path / "outside").write_bytes(b"confidential")
        path.symlink_to(tmp_path / "outside")
    elif kind == "fifo":
        os.mkfifo(path)
    elif kind == "oversize":
        path.write_bytes(b"12345")
    with pytest.raises(ConfigurationError) as caught:
        read_regular(tmp_path, "file", 4)
    for part in (str(path), "expected", "restore"):
        assert part in str(caught.value)
    assert "confidential" not in str(caught.value)


def test_upload_size_and_count_limits_apply_before_materialization(monkeypatch):
    encoded = base64.b64encode(b"12345").decode()
    monkeypatch.setattr(project_files, "MAX_BYTES", 4)
    with pytest.raises(UsageError, match="at most 4"):
        project_files.decode_files({"craft-connector.yml": encoded})
    monkeypatch.setattr(project_files, "MAX_FILES", 1)
    with pytest.raises(UsageError, match="at most 1 files"):
        project_files.decode_files({"craft-connector.yml": "", "seeds/x": ""})


def test_source_size_limit_and_a_missing_config_do_not_publish(tmp_path, monkeypatch):
    with pytest.raises(UsageError, match="missing"):
        project_files.collect(tmp_path)
    (tmp_path / "craft-connector.yml").write_bytes(b"12345")
    monkeypatch.setattr(project_files, "MAX_BYTES", 4)
    with pytest.raises(UsageError, match="at most 4"):
        project_files.collect(tmp_path)


def test_cache_identity_and_permissions_are_checked_before_downloading(tmp_path):
    with pytest.raises(UsageError, match="64 lowercase hex"):
        project_files.materialize("wrong", {}, lambda _: b"", cache=tmp_path / "cache")
    cache = tmp_path / "cache"
    cache.mkdir(mode=0o777)
    cache.chmod(0o777)
    with pytest.raises(ConfigurationError, match="0700"):
        project_files.materialize("e" * 64, {}, lambda _: b"", cache=cache)
    assert not list(cache.iterdir())


def test_missing_cache_files_are_not_repaired(tmp_path):
    data = b"Secrets: {}"
    manifest = {"craft-connector.yml": sha256_hex(data)}
    root = project_files.materialize("f" * 64, manifest, lambda _: data, cache=tmp_path / "cache")
    (root / "craft-connector.yml").unlink()
    with pytest.raises(ConfigurationError, match=r"missing=.*craft-connector.yml"):
        project_files.materialize("f" * 64, manifest, lambda _: data, cache=tmp_path / "cache")
    assert not (root / "craft-connector.yml").exists()


def test_malformed_config_diagnostics_never_echo_uploaded_secrets(tmp_path):
    path = tmp_path / "bad.yml"
    path.write_text('Secrets: "do-not-log-uploaded-secret')
    with pytest.raises(ConfigurationError) as caught:
        project_config(config(tmp_path), path)
    for part in (str(path), "invalid", "UTF-8", "republish"):
        assert part in str(caught.value)
    assert "do-not-log-uploaded-secret" not in str(caught.value)


def test_bad_python_is_reported_without_executing_it(tmp_path):
    scripts = tmp_path / "python_scripts"
    scripts.mkdir()
    (scripts / "broken.py").write_text("if not valid python !")
    with pytest.raises(ConfigurationError, match=r"broken.py.*invalid Python"):
        project_files.script_imports(tmp_path)


def test_artifact_addresses_are_verified_before_writing(tmp_path):
    store = ArtifactStore(config(tmp_path), (tmp_path / "store").as_uri())
    with pytest.raises(ConfigurationError, match="SHA-256"):
        store.write("not-a-hash", b"data")
    with pytest.raises(ConfigurationError, match="content hash differs"):
        store.write("a" * 64, b"data")
    assert not (tmp_path / "store").exists()


def test_file_store_refuses_linked_object_directories(tmp_path):
    root, outside = tmp_path / "store", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "objects").symlink_to(outside, target_is_directory=True)
    store = ArtifactStore(config(tmp_path), root.as_uri())
    with pytest.raises(ConfigurationError, match="without symlinks"):
        store.write(sha256_hex(b"data"), b"data")
    assert not list(outside.iterdir())


def s3(monkeypatch, tmp_path, client):
    import boto3

    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: client)
    return S3Objects(config(tmp_path), ObjectStore("artifacts"))


def test_default_credentials_use_the_host_chain_and_reads_are_bounded(tmp_path, monkeypatch):
    body = io.BytesIO(b"12345")
    client = SimpleNamespace(get_object=lambda **kwargs: {"Body": body})
    store = s3(monkeypatch, tmp_path, client)
    with pytest.raises(ConfigurationError, match=r"s3://bucket/object.*read failed"):
        store.read("bucket", "object", 4)
    assert body.closed


@pytest.mark.parametrize("operation", ["read", "write", "probe"])
def test_provider_errors_hide_driver_secrets(tmp_path, monkeypatch, operation):
    def refused(**kwargs):
        raise RuntimeError("credential-value-must-never-appear")

    client = SimpleNamespace(
        get_object=refused, put_object=refused, delete_object=lambda **kwargs: None
    )
    store = s3(monkeypatch, tmp_path, client)
    with pytest.raises(ConfigurationError) as caught:
        if operation == "read":
            store.read("bucket", "object", 4)
        elif operation == "write":
            store.write("bucket", "object", b"data")
        else:
            store.probe("bucket", "prefix")
    assert "credential-value-must-never-appear" not in str(caught.value)
    for part in ("Object_stores.artifacts", "s3://bucket", "expected", "permissions"):
        assert part in str(caught.value)


def test_provider_initialization_never_exposes_credentials(tmp_path, monkeypatch):
    import boto3

    def refused(*args, **kwargs):
        raise RuntimeError("provider-url-with-confidential-token")

    monkeypatch.setattr(boto3, "client", refused)
    with pytest.raises(ConfigurationError) as caught:
        S3Objects(config(tmp_path), ObjectStore("artifacts"))
    assert "provider-url-with-confidential-token" not in str(caught.value)
    for part in ("Object_stores.artifacts", "initialization failed", "expected", "check"):
        assert part in str(caught.value)


@pytest.mark.parametrize(
    "table",
    [
        None,
        {"columns": "task_id", "rows": []},
        {"columns": ["unsafe)"], "rows": []},
        {"columns": ["task_id"], "rows": [{"different": 1}]},
    ],
)
def test_malformed_definition_is_refused_before_sql_projection(table):
    import json

    from etl_craft.core.definitions import Definition
    from etl_craft.core.errors import RunStateError

    with pytest.raises(RunStateError, match=r"snapshot is malformed.*restore"):
        Definition.read(json.dumps({"CFG_TASKS": table}), 7)


def test_resolved_host_values_never_escape_bundle_validation(tmp_path, monkeypatch):
    import yaml

    monkeypatch.setenv("BUNDLE_HOST_PRIVATE", "confidential-host-value-for-test")
    path = tmp_path / "published.yml"
    path.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "ENVIRONMENT"},
                "Orchestration": {"Task_timeout_seconds": "BUNDLE_HOST_PRIVATE"},
            },
            sort_keys=False,
        )
    )
    with pytest.raises(ConfigurationError) as caught:
        project_config(config(tmp_path), path)
    assert "confidential-host-value-for-test" not in str(caught.value)
    for part in (str(path), "rejected project settings", "expected", "validate", "republish"):
        assert part in str(caught.value)


@pytest.mark.parametrize("profiled", [False, True])
def test_bundle_connections_cannot_resolve_or_redirect_host_credentials(
    tmp_path, monkeypatch, profiled
):
    from dataclasses import replace

    import yaml

    from etl_craft.config import ConnectionProfile

    monkeypatch.setenv("BUNDLE_HOST_PRIVATE", "confidential-host-value-for-test")
    host = replace(
        config(tmp_path),
        warehouse=ConnectionProfile(
            "WAREHOUSE", "dev", "jdbc:duckdb:trusted.duckdb", "", "NONE", schema="main"
        ),
    )
    path = tmp_path / "published.yml"
    path.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "ENVIRONMENT"},
                "Engine": {"dev": {"Jdbc_url": "BUNDLE_HOST_PRIVATE"}},
                "Warehouse": {
                    "dev": {
                        "Jdbc_url": "jdbc:postgresql://untrusted.invalid/db",
                        "Auth_mode": "PASSWORD",
                        "Secret": "BUNDLE_HOST_PRIVATE",
                    }
                },
                "Orchestration": {
                    "Email": {"Host": "untrusted.invalid", "Secret": "BUNDLE_HOST_PRIVATE"}
                },
                "Cloning": {"Enabled": True},
            },
            sort_keys=False,
        )
    )
    if profiled:
        raw = yaml.safe_load(path.read_text())
        raw["Orchestration"] = {"dev": raw["Orchestration"]}
        path.write_text(yaml.safe_dump(raw, sort_keys=False))
    pinned = project_config(host, path)
    assert pinned.warehouse_table_format == host.warehouse_table_format
    assert pinned.engine == host.engine
    assert pinned.warehouse == host.warehouse
    assert pinned.email == host.email
    assert pinned.cloning == host.cloning
    assert "confidential-host-value-for-test" not in repr(pinned)
