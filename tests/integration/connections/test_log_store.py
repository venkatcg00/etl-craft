"""Full log publication, byte-range download and retention on a real S3-compatible store."""

from dataclasses import replace
from uuid import uuid4

import pytest

from etl_craft.config.object_models import Logs, ObjectStore
from etl_craft.core.enums import ObjectStoreAuth, ObjectUrlStyle
from etl_craft.core.errors import ConfigurationError
from etl_craft.engine.logs import log_key, publish_log, remove_log, stored_log
from fixtures.services import MINIO_BUCKET, MINIO_PASSWORD, MINIO_USER, require

pytestmark = pytest.mark.connections


@pytest.mark.parametrize("empty_engine_db", ["sqlite", "postgresql"], indirect=True)
def test_s3_logs_stream_ranges_and_remove_exact_engine_keys(engine_db, monkeypatch):
    minio = require("minio")
    monkeypatch.setenv("LOG_ACCESS", MINIO_USER)
    monkeypatch.setenv("LOG_SECRET", MINIO_PASSWORD)
    profile = ObjectStore(
        "logs",
        minio.http_url,
        "us-east-1",
        ObjectUrlStyle.PATH,
        ObjectStoreAuth.KEYS,
        "LOG_ACCESS",
        "LOG_SECRET",
    )
    config = replace(
        engine_db.config,
        object_stores={"logs": profile},
        logs=Logs(f"s3://{MINIO_BUCKET}/logs/{uuid4().hex}", "logs"),
    )
    key = log_key("a" * 32, 1, 419, 719, 812)
    block = ("é" * 32000 + "\\n").encode()
    try:
        publish_log(config, key, (block for _ in range(150)))
        total = sum(len(chunk) for chunk in stored_log(config, key, 0))
        assert total == len(block) * 150
        offset = len(block) * 149 + 3
        assert b"".join(stored_log(config, key, offset)) == block[3:]
        assert b"".join(stored_log(config, key, total)) == b""
        with pytest.raises(ConfigurationError, match="missing profile"):
            b"".join(stored_log(replace(config, object_stores={}), key, 0))
        # A missing acknowledged object is inaccessible, not empty or pending.
        remove_log(config, config.logs.store + "/" + key, 1, 812, "a" * 32)
        with pytest.raises(
            ConfigurationError, match=r"log read failed.*expected a readable stored log"
        ):
            b"".join(stored_log(config, key, 0))
    finally:
        remove_log(config, config.logs.store + "/" + key, 1, 812, "a" * 32)
