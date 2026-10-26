"""The named cross-process locks held in the Engine DB."""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from hashlib import blake2b

from sqlalchemy.engine import Engine

from etl_craft.core.ownership import ResourceLocker
from etl_craft.core.text import split_object_ref
from etl_craft.dialects.engine import for_engine


@dataclass(frozen=True)
class EngineLock:
    """A lock every process reaching the same Engine DB contends for."""

    name: str
    key: int

    def hold(self, engine: Engine, wait_seconds: float = 0) -> AbstractContextManager[None]:
        """Hold the lock for the ``with`` body; 0 waits indefinitely."""
        return for_engine(engine).lock(engine, self.key, self.name, wait_seconds)


MIGRATE = EngineLock("migrate", 8_241_007)
"""Serializes ``init-db`` and ``migrate``, so two runs never apply the same file twice."""

CLONE = EngineLock("clone", 8_241_008)
"""Serializes cloning, so two runs finishing together never write the same mirror at once."""

ACCOUNTS = EngineLock("accounts", 8_241_010)
"""Serialize account recovery and credential changes with sign-in."""

BUNDLES = EngineLock("bundle publication", 8_241_011)
"""Serialize connection approval and publication so superseded contracts cannot return."""

LINEAGE = EngineLock("lineage", 8_241_009)
"""Serializes task cache computations and table lineage version publication."""


@dataclass(frozen=True)
class ResourceLock:
    """One durable lock for a warehouse target, event probe or single writer."""

    name: str
    key: int

    def hold(
        self, locker: ResourceLocker, wait_seconds: float = 30
    ) -> AbstractContextManager[None]:
        """Keep resource ownership independent of any API process or DB session."""
        return locker.hold(self.key, self.name, wait_seconds)


def target(object_ref: str, catalog: str) -> ResourceLock:
    """Serialize target mutations and hash-contract publication across processes.

    The lock comes from the canonical resolved identity, ``catalog.schema.table`` in lowercase,
    so a target written with and without its catalog takes one lock.
    """
    database, schema, table = split_object_ref(object_ref)
    key = advisory_key(f"{database or catalog}.{schema}.{table}".lower())
    return ResourceLock(f"target {database or catalog}.{schema}.{table}", key)


def event(task_id: int) -> ResourceLock:
    """Serialize evaluation of one event-wait task across processes."""
    key = advisory_key(f"event:{task_id}")
    return ResourceLock(f"target-{key & ((1 << 64) - 1):016x}", key)


def advisory_key(name: str) -> int:
    """Hash an exact lock namespace to PostgreSQL's signed 64-bit advisory key."""
    return int.from_bytes(blake2b(name.encode(), digest_size=8).digest(), "big", signed=True)


def business_rule(rule_id: int, name: str) -> ResourceLock:
    """Serialize one rule's warehouse snapshot and Engine DB publication."""
    return ResourceLock(
        f"business rule {name!r} (BUSINESS_RULE_ID={rule_id})",
        advisory_key(f"business-rule:{rule_id}"),
    )
