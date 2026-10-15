"""How long a process waits out an Engine DB that stopped answering before it gives up.

A database restart or managed failover takes seconds to minutes. A process that has already
reached the Engine DB waits up to ``Orchestration.Engine_db_outage_seconds`` for it to come
back, then fails with ``EngineDbError`` naming how long it waited and the setting.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from etl_craft.core.errors import EngineDbError

logger = logging.getLogger(__name__)

SETTING = "Orchestration.Engine_db_outage_seconds"


@dataclass
class OutageBudget:
    """One continuous outage, measured from its first failure until a success ends it."""

    budget_seconds: float
    clock: Callable[[], float] = time.monotonic
    started: float | None = field(default=None, init=False)

    def failed(self, what: str, error: BaseException) -> float:
        """Record a failure of ``what``; return the next wait, or raise once over budget."""
        now = self.clock()
        if self.started is None:
            self.started = now
        waited = now - self.started
        if waited >= self.budget_seconds:
            raise EngineDbError(
                f"the Engine DB has not answered for {waited:.0f}s while {what} ({error}); "
                f"{SETTING} allows {self.budget_seconds:g}s. Restore the Engine DB, or raise "
                f"{SETTING} for longer failovers"
            ) from error
        logger.warning(
            "%s: the Engine DB is unavailable (%s); waiting, %.0fs of %gs used",
            what,
            error,
            waited,
            self.budget_seconds,
        )
        return min(1.0, self.budget_seconds - waited)

    def recovered(self, what: str) -> None:
        """End the outage, if one was running."""
        if self.started is not None:
            logger.warning(
                "%s: the Engine DB answers again after %.0fs", what, self.clock() - self.started
            )
            self.started = None
