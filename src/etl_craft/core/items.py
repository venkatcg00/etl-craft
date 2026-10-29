"""Item evidence shared by Python helpers, local task stores and the agent protocol."""

import itertools
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol

from etl_craft.core.enums import ItemStatus
from etl_craft.core.errors import HandlerError

COUNTED = frozenset({ItemStatus.SUCCESS, ItemStatus.SKIPPED})


@dataclass(frozen=True)
class ItemRecord:
    """One immutable observation of an item's execution or an operator decision."""

    key: str
    number: int
    status: ItemStatus
    row_count: int | None = None
    error: str | None = None
    started: str | None = None
    ended: str | None = None
    report_id: str = ""
    attempt_id: int | None = None


@dataclass(frozen=True)
class ItemState:
    """Latest item observations and the current task's admitted memory reservation."""

    records: tuple[ItemRecord, ...]
    memory_mb: int
    cpu: float = 1


class ItemStore(Protocol):
    """Record progress without giving scripts an Engine DB connection."""

    def items(self) -> ItemState:
        """Read this task run's items, including earlier attempts."""
        ...

    def prepare_items(self, keys: tuple[str, ...]) -> None:
        """Register bounded keys and retain successes in one fenced batch."""
        ...

    def item_event(self, event: ItemRecord) -> ItemRecord:
        """Fence and append one observation, recognizing a duplicate report."""
        ...


@dataclass(frozen=True)
class ItemProgress:
    """One shared count of the latest observations, independent of the interface."""

    total: int
    done: int
    failed: int
    skipped: int
    measured: int = 0
    unmeasured: int = 0

    def __str__(self) -> str:
        """Use the same progress text in logs, status, explain and catalog pages."""
        text = f"items {self.done} / {self.total}, {self.failed} failed, {self.skipped} skipped"
        if self.unmeasured:
            text += (
                f"; Not measured: {self.unmeasured} of {self.measured + self.unmeasured} "
                "items reported no count"
            )
        return text


def progress(records: tuple[ItemRecord, ...]) -> ItemProgress:
    """Count persisted results, including retained successes and operator decisions."""
    statuses = Counter(r.status for r in records)
    done = statuses[ItemStatus.SUCCESS] + statuses[ItemStatus.SKIPPED]
    unmeasured = sum(r.row_count is None for r in records if r.status in COUNTED)
    return ItemProgress(
        len(records),
        done,
        statuses[ItemStatus.FAILED] + statuses[ItemStatus.CANCELLED],
        statuses[ItemStatus.SKIPPED],
        done - unmeasured,
        unmeasured,
    )


def bounded_keys(items: Iterable[str], maximum: int) -> list[str]:
    """Bound discovery and refuse malformed or duplicated keys before recording work."""
    try:
        keys = list(itertools.islice(items, maximum + 1))
    except TypeError as error:
        raise HandlerError(
            f"fan_out items={items!r}; expected an iterable of text keys; supply stable keys"
        ) from error
    if len(keys) > maximum:
        raise HandlerError(
            f"fan_out: found at least {len(keys)} keys; expected max_items={maximum} "
            "or fewer; increase max_items or bound discovery"
        )
    seen: set[str] = set()
    for key in keys:
        if not valid_item_key(key) or key in seen:
            raise HandlerError(
                f"fan_out: item key={key!r}; expected unique UTF-8 text of "
                "1-512 characters without NUL; give every item a stable nonempty key"
            )
        seen.add(key)
    return keys


def valid_item_key(value: object) -> bool:
    """Require text that both Engine DBs can store without truncation or encoding loss."""
    if not isinstance(value, str) or not 1 <= len(value) <= 512 or "\x00" in value:
        return False
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True
