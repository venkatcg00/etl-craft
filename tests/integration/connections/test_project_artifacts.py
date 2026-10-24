"""Artifact-store clients share immutable objects through a real S3-compatible service."""

import pytest

pytestmark = pytest.mark.connections


@pytest.mark.parametrize("empty_engine_db", ["sqlite", "postgresql"], indirect=True)
def test_profiled_s3_objects_share_verified_content_across_clients(engine_db, monkeypatch):
    from dataclasses import replace
    from uuid import uuid4

    from etl_craft.config.object_models import ObjectStore
    from etl_craft.core.enums import ObjectStoreAuth, ObjectUrlStyle
    from etl_craft.core.text import sha256_hex
    from etl_craft.engine.artifacts import ArtifactStore
    from fixtures.services import MINIO_BUCKET, MINIO_PASSWORD, MINIO_USER, require

    minio = require("minio")
    monkeypatch.setenv("BUNDLE_ACCESS", MINIO_USER)
    monkeypatch.setenv("BUNDLE_SECRET", MINIO_PASSWORD)
    profile = ObjectStore(
        "artifacts",
        minio.http_url,
        "us-east-1",
        ObjectUrlStyle.PATH,
        ObjectStoreAuth.KEYS,
        "BUNDLE_ACCESS",
        "BUNDLE_SECRET",
    )
    config = replace(engine_db.config, object_stores={"artifacts": profile})
    location = f"s3://{MINIO_BUCKET}/bundles/{uuid4().hex}"
    first = ArtifactStore(config, location, "artifacts")
    second = ArtifactStore(config, location, "artifacts")
    data = b"select 'published through one API, read through another' AS value"
    digest = sha256_hex(data)
    try:
        first.probe()
        first.write(digest, data)
        second.write(digest, data)
        assert second.read(digest) == data
    finally:
        from etl_craft.dialects.credentials import S3Objects

        S3Objects(config, profile).client.delete_object(
            Bucket=MINIO_BUCKET, Key=location.split(MINIO_BUCKET + "/", 1)[1] + "/objects/" + digest
        )
