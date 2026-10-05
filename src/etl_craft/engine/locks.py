"""The named cross-process locks held in the Engine DB."""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from hashlib import blake2b

from sqlalchemy.engine import Engine

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


def target(name: str) -> EngineLock:
    """Serialize target mutations and hash-contract publication across processes."""
    normalized = name.lower()
    key = int.from_bytes(blake2b(normalized.encode(), digest_size=8).digest(), "big", signed=True)
    return EngineLock(f"target-{key & ((1 << 64) - 1):016x}", key)
