"""Concurrent installed demos own their Iceberg namespaces and clean only those namespaces."""

import json
import urllib.request

import pytest
from sqlalchemy import text

from fixtures.demo import PASSWORD_VAR, S3_VAR, SCHEMAS, Demo
from fixtures.services import MINIO_PASSWORD, POSTGRES_PASSWORD, require

pytestmark = pytest.mark.harness


@pytest.mark.parametrize("warehouse", ["duckdb_iceberg", "trino_iceberg"])
def test_building_and_closing_another_demo_cannot_remove_the_first_demos_rows(
    tmp_path, monkeypatch, warehouse
):
    monkeypatch.setenv(PASSWORD_VAR, POSTGRES_PASSWORD)
    monkeypatch.setenv(S3_VAR, MINIO_PASSWORD)
    first = Demo(tmp_path / "cli", tmp_path / "first", "sqlite", warehouse)
    second = Demo(tmp_path / "cli", tmp_path / "second", "sqlite", warehouse)
    try:
        first.build()
        with first.warehouse() as engine, engine.begin() as conn:
            conn.execute(text(f"CREATE TABLE {first.table('lnd.probe')} AS SELECT 1 AS id"))
        second.build()
        assert first.rows("SELECT id AS id FROM {c}.lnd.probe") == [(1,)]
        with second.warehouse() as engine, engine.begin() as conn:
            conn.execute(text(f"CREATE TABLE {second.table('lnd.probe')} AS SELECT 2 AS id"))
        assert second.rows("SELECT id AS id FROM {c}.lnd.probe") == [(2,)]
        second.close()
        assert first.rows("SELECT id AS id FROM {c}.lnd.probe") == [(1,)]
    finally:
        second.close()
        first.close()
    catalog = require("iceberg_rest")
    with urllib.request.urlopen(f"{catalog.http_url}/v1/namespaces", timeout=10) as reply:
        namespaces = {tuple(name) for name in json.load(reply)["namespaces"]}
    owned = {(f"{demo.prefix}{schema}",) for demo in (first, second) for schema in SCHEMAS}
    assert not namespaces & owned
