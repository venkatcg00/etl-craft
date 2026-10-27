"""Access command contracts shared by CLI, HTTP and service validation."""

from dataclasses import dataclass
from typing import ClassVar

from etl_craft.core.enums import AccessChange
from etl_craft.services.operations.requests import Request

FIELDS = ("PROJECT", "GROUP_NAME", "ROLE", "PIPELINE_CODE", "TAG", "PERMISSION", "ACTION")


@dataclass(frozen=True, kw_only=True)
class AccessFilter(Request):
    """Explicit project scope and optional exact identity filters."""

    project: str | None = None
    group: str | None = None
    account: str | None = None
    role: str | None = None
    pipeline: str | None = None
    tag: str | None = None


@dataclass(frozen=True, kw_only=True)
class AccessWrite(AccessFilter):
    """One access identity or submitted bulk content; no server-side client path."""

    reason: str
    permission: str | None = None
    content: str | None = None
    dry_run: bool = False


@dataclass(frozen=True)
class Grant:
    """One current grant, retained even after revocation."""

    project: str
    group: str | None
    account: str | None
    role: str | None
    pipeline: str | None
    tag: str | None
    permission: str
    active_flag: str


@dataclass(frozen=True)
class Diff:
    """A complete submitted row and its actual change, including its previous permission."""

    line: int
    grant: Grant
    previous_permission: str | None
    change: AccessChange


@dataclass(frozen=True)
class AccessView:
    """Access administration output shared by the CLI and API."""

    SCHEMA: ClassVar[str] = "etl-craft/access/1"
    grants: tuple[Grant, ...] = ()
    effective: tuple[tuple[str, str | None, tuple[str, ...]], ...] = ()
    changes: tuple[Diff, ...] = ()
    dry_run: bool = False
