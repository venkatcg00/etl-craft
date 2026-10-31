"""Upload identities carried from admission to warehouse writes."""

from dataclasses import dataclass, field
from pathlib import Path

from etl_craft.core.enums import ActorKind
from etl_craft.core.enums_uploads import UploadStatus as UploadStatus


@dataclass(frozen=True)
class UploadWriter:
    """One admitted file's identity and authenticated account."""

    upload_id: int
    account: str


@dataclass(frozen=True)
class UploadInfo(UploadWriter):
    """Recorded file facts safe to send to an API-only task host."""

    file_name: str
    size_bytes: int
    sha256: str
    actor_kind: ActorKind = field(default=ActorKind.USER, kw_only=True)


@dataclass(frozen=True)
class UploadFile(UploadInfo):
    """A verified local file made available to a task script."""

    path: Path
