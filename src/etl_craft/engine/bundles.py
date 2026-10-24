"""Recorded bundle identity and materialization for local execution and API delivery."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.engine import Connection

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import ConfigurationError, ResourceNotFoundError
from etl_craft.engine.artifacts import ArtifactStore
from etl_craft.engine.project_files import materialize
from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class Bundle:
    """An immutable manifest plus its recorded store address, never credential values."""

    bundle_id: str
    files: Mapping[str, str]
    store: str
    profile: str | None

    @classmethod
    def load(cls, conn: Connection, bundle_id: str) -> Bundle:
        """Select an exact published identity, not the current project publication."""
        row = conn.execute(statement(conn, "bundle_get"), {"bundle_id": bundle_id}).one_or_none()
        if row is None:
            raise ResourceNotFoundError(
                f"bundle {bundle_id}: missing; expected the run's recorded "
                f"publication; restore the bundle row and artifact store"
            )
        try:
            data = json.loads(row.manifest)
            return cls(bundle_id, data["files"], data["store"], data["profile"])
        except (ValueError, TypeError, KeyError) as error:
            raise ConfigurationError(
                f"bundle {bundle_id}: invalid manifest; expected its recorded "
                f"files and store; restore the manifest from backup"
            ) from error

    def read(self, config: ConnectorConfig, path: str) -> bytes:
        """Only a named manifest file can be read from the immutable content store."""
        if path not in self.files:
            raise ResourceNotFoundError(
                f"bundle {self.bundle_id}, path={path!r}: not in manifest; "
                f"expected a recorded file; select a manifest path"
            )
        return ArtifactStore(config, self.store, self.profile).read(self.files[path])

    def materialize(self, config: ConnectorConfig, *, cache: Path | None = None) -> Path:
        """Share the same verifier and cache protocol with API-only agents."""
        return materialize(
            self.bundle_id, self.files, lambda path: self.read(config, path), cache=cache
        )
