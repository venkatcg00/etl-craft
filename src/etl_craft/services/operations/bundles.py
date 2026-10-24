"""Publish reviewed project bytes once through the same developer action on CLI and API."""

import base64
import json
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import ClassVar
from uuid import uuid4

from etl_craft.config import ConnectorConfig
from etl_craft.config.project import project_config
from etl_craft.core.actor import current_actor
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.text import sha256_hex
from etl_craft.engine.artifacts import ArtifactStore
from etl_craft.engine.bundles import Bundle
from etl_craft.engine.project_files import MAX_BYTES, decode_files
from etl_craft.engine.queries import statement
from etl_craft.handlers.registry import TaskContext
from etl_craft.services.operations.agent_models import AgentTask
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import Request


@dataclass(frozen=True, kw_only=True)
class PublishBundle(Request):
    """An upload contains file bytes, never paths to files on an API host."""

    MAX_BODY_BYTES: ClassVar[int] = MAX_BYTES * 2
    files: dict[str, str] = field(metadata={"audit": False})


@dataclass(frozen=True)
class BundleView:
    """Publication identity and hashes, shared by API and CLI."""

    SCHEMA: ClassVar[str] = "etl-craft/bundle/1"
    bundle_id: str
    files: dict[str, str]


def publish(ctx: OperationContext, request: PublishBundle) -> BundleView:
    """Validate before publishing; only a committed row makes a bundle admissible."""
    location = ctx.config.artifacts.store
    if location is None:
        raise ConfigurationError(
            "Artifacts.Store is unset; expected file:///shared/path or a "
            "profiled s3:// location; configure it on the API/controller host before publishing"
        )
    manifest, content = decode_files(request.files)
    with tempfile.TemporaryDirectory(prefix="etl-craft-publish-") as scratch:
        config_file = Path(scratch) / "craft-connector.yml"
        config_file.write_bytes(content["craft-connector.yml"])
        project_config(ctx.config, config_file)
    bundle_id = sha256_hex(uuid4().bytes)
    store = ArtifactStore(ctx.config, location, ctx.config.artifacts.profile)
    for name, digest in manifest.items():
        store.write(digest, content[name])
    recorded = json.dumps(
        {
            "schema": "etl-craft/bundle-manifest/1",
            "files": manifest,
            "store": location,
            "profile": ctx.config.artifacts.profile,
        },
        sort_keys=True,
    )
    with ctx.engine.begin() as conn:
        conn.execute(
            statement(conn, "bundle_insert"),
            {
                "bundle_id": bundle_id,
                "manifest": recorded,
                "actor": current_actor().name,
                "now": datetime.now(UTC),
            },
        )
    ctx.engine.update_execution_options(bundles_enabled=True)
    return BundleView(bundle_id, manifest)


def file_reply(bundle: Bundle, config: ConnectorConfig, path: str | None) -> dict[str, object]:
    """Return hashes or one file without store credentials or local paths."""
    if path is None:
        return {"bundle_id": bundle.bundle_id, "files": dict(bundle.files)}
    return {"content": base64.b64encode(bundle.read(config, path)).decode("ascii")}


def bundle_report(
    ctx: OperationContext, task: TaskContext, request: AgentTask
) -> dict[str, object]:
    """Serve only the bundle already resolved from the token's fenced task context."""
    if task.bundle_id is None:
        raise ConfigurationError(
            f"attempt {request.attempt_id}: no pinned bundle; expected a published project; "
            "publish a bundle and start a new run"
        )
    with ctx.engine.connect() as conn:
        bundle = Bundle.load(conn, task.bundle_id)
    return file_reply(bundle, ctx.config, request.bundle_path)
