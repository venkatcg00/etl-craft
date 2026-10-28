"""Publication boundaries and caches never execute or silently repair project bytes."""

import base64

import pytest

from etl_craft.core.errors import ConfigurationError, UsageError
from etl_craft.core.text import sha256_hex
from etl_craft.engine.project_files import (
    collect,
    decode_files,
    materialize,
    missing_imports,
    safe_path,
    script_imports,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "name",
    [
        "../x",
        "/tmp/x",
        "sql_scripts/../x",
        "sql_scripts\\x",
        "config/a.csv",
        "python_scripts//a.py",
        "sql_scripts/./a.sql",
    ],
)
def test_publication_refuses_noncanonical_paths(name):
    with pytest.raises(UsageError):
        safe_path(name)


def test_publication_hashes_exact_bytes_and_refuses_symlinks(tmp_path):
    (tmp_path / "craft-connector.yml").write_text("Secrets: {}")
    scripts = tmp_path / "sql_scripts"
    scripts.mkdir()
    data = b"select 1\r\n"
    (scripts / "a.sql").write_bytes(data)
    hashes, files = decode_files(collect(tmp_path))
    assert hashes["sql_scripts/a.sql"] == sha256_hex(data)
    assert files["sql_scripts/a.sql"] == data
    (scripts / "linked.sql").symlink_to(scripts / "a.sql")
    with pytest.raises(UsageError, match="link"):
        collect(tmp_path)


def test_download_is_atomic_and_corruption_is_refused(tmp_path):
    files = {"craft-connector.yml": b"Secrets: {}", "sql_scripts/a.sql": b"select 1"}
    manifest = {name: sha256_hex(data) for name, data in files.items()}
    identity = "a" * 64
    from etl_craft.engine import project_files
    from fixtures.races import two_at_once

    def download():
        return project_files.materialize(
            identity, manifest, files.__getitem__, cache=tmp_path / "cache"
        )

    roots = two_at_once(download, download, at=(type(tmp_path), "rename"))
    assert roots[0] == roots[1]
    script = roots[0] / "sql_scripts/a.sql"
    script.chmod(0o600)
    script.write_text("select 2")
    with pytest.raises(ConfigurationError, match="SHA-256"):
        materialize(identity, manifest, files.__getitem__, cache=tmp_path / "cache")


def test_download_refuses_bad_content_before_publishing(tmp_path):
    with pytest.raises(ConfigurationError, match="SHA-256"):
        materialize(
            "b" * 64,
            {"craft-connector.yml": sha256_hex(b"good")},
            lambda _: b"bad",
            cache=tmp_path / "cache",
        )
    assert not (tmp_path / "cache" / ("b" * 64)).exists()


def test_import_inspection_does_not_execute_scripts(tmp_path):
    scripts = tmp_path / "python_scripts"
    scripts.mkdir()
    (scripts / "local.py").write_text("raise AssertionError('never execute')")
    (scripts / "main.py").write_text(
        "import json\nimport local\nimport missing_bundle_package_xyz.sub\nraise "
        "AssertionError('never execute')"
    )
    imports = script_imports(tmp_path)
    assert imports == ("json", "missing_bundle_package_xyz")
    assert missing_imports(imports) == ("missing_bundle_package_xyz",)


def test_upload_requires_config_and_valid_base64():
    with pytest.raises(UsageError):
        decode_files({"sql_scripts/a.sql": base64.b64encode(b"select 1").decode()})
    with pytest.raises(UsageError):
        decode_files({"craft-connector.yml": "not base64!"})


def test_cache_refuses_unrecorded_special_files(tmp_path):
    import os

    files = {"craft-connector.yml": b"Secrets: {}"}
    manifest = {name: sha256_hex(data) for name, data in files.items()}
    root = materialize("c" * 64, manifest, files.__getitem__, cache=tmp_path / "cache")
    os.mkfifo(root / "unrecorded")
    with pytest.raises(ConfigurationError, match="special file"):
        materialize("c" * 64, manifest, files.__getitem__, cache=tmp_path / "cache")
