"""Recorded pipeline validation outcomes, shared by the CLI and HTTP interfaces."""

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from etl_craft.core.enums import RunStatus


@dataclass(frozen=True)
class RefreshFinding:
    """A file location where applicable, or an exact Engine DB metadata identity."""

    file: str
    line: int | None
    value: str
    message: str
    remedy: str
    failed: bool = True


@dataclass(frozen=True)
class PipelineRefresh:
    """One durable result; the bundle and metadata version identify the checked bytes."""

    refresh_id: int
    pipeline_code: str
    refreshed_at: datetime
    actor: str
    status: RunStatus
    metadata_version: str
    bundle_id: str | None
    findings: tuple[RefreshFinding, ...]
    changed: bool = True
    changes: tuple[str, ...] = ()
    previous_refreshed_at: datetime | None = None
    previous_status: RunStatus | None = None


@dataclass(frozen=True)
class RefreshResults:
    """Project refresh results, or the recorded history for a pipeline."""

    SCHEMA: ClassVar[str] = "etl-craft/pipeline-refresh/1"
    refreshes: tuple[PipelineRefresh, ...]

    @property
    def failed(self) -> bool:
        """Whether any checked pipeline failed validation."""
        return any(result.status == RunStatus.FAILED for result in self.refreshes)
