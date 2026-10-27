"""Configuration file rows, attributed changes and operation results."""

import json
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import ClassVar, Literal

from etl_craft.core.enums import Permission, YesNo
from etl_craft.engine.repository.config_rows import ACTIVE_FLAG, Key, Value
from etl_craft.services.validate import Finding


@dataclass(frozen=True)
class FileRow:
    """One row with its ending line, parsed values and explicitly supplied header fields."""

    line: int
    values: dict[str, Value]
    provided: frozenset[str] | None = None

    @property
    def active(self) -> bool:
        """Whether the row's ``ACTIVE_FLAG`` is ``Y``."""
        return self.values[ACTIVE_FLAG.name] == YesNo.YES


@dataclass(frozen=True)
class ConfigFiles:
    """The config files, read and checked, with the revision of their content."""

    directory: Path
    revision: str
    rows: dict[str, dict[Key, FileRow]]


@dataclass(frozen=True)
class ColumnChange:
    """A column a change sets, as the files write its value before and after."""

    column: str
    before: str
    after: str


@dataclass(frozen=True)
class Change:
    """One row ``config apply`` inserts, updates, makes active again or retires, and why."""

    file: str
    table: str
    operation: Literal["insert", "update", "reactivate", "retire"]
    row: str
    columns: tuple[ColumnChange, ...] = ()
    reason: str = ""


@dataclass(frozen=True)
class TagGrantMatch:
    """One existing grant that starts or stops matching a pipeline's tags."""

    principal: str
    tag: str
    permission: Permission
    starts_matching: bool


@dataclass(frozen=True)
class TagAccessChange:
    """Grant matches and open/restricted policy changed by a submitted tag edit."""

    pipeline: str
    was_restricted: bool
    is_restricted: bool
    grants: tuple[TagGrantMatch, ...]
    policy_change: str | None = None


@dataclass(frozen=True)
class ConfigSync:
    """What loading the config files changed, or would change, and what ``validate`` found."""

    SCHEMA: ClassVar[str] = "etl-craft/config-sync/1"
    directory: str
    revision: str
    applied: bool
    failed: bool
    """Whether ``validate`` failed the resulting rows, so nothing was applied."""
    changes: tuple[Change, ...]
    findings: tuple[Finding, ...] = ()
    tag_access_changes: tuple[TagAccessChange, ...] = ()


@dataclass(frozen=True)
class ConfigExport:
    """Config files written from the ``CFG_`` rows, rebuilt ``as_of`` a moment or now (``None``)."""

    SCHEMA: ClassVar[str] = "etl-craft/config-export/1"
    directory: str
    rows: dict[str, int] = field(default_factory=dict)
    as_of: str | None = None


def cell(value: Value) -> str:
    """Write a value as a config file holds it: empty for NULL, JSON objects as JSON."""
    if value is None:
        return ""
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else format(Decimal(repr(value)), "f")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)
