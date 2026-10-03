"""A bounded retry for Engine DB calls that are safe to repeat.

A connection that drops for a moment (a database restart or failover) should not abort a whole
run. Only errors that mean "could not talk to the database" are retried; any other error, and the
last attempt's error, is raised as it is.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from typing import TypeVar

from sqlalchemy.exc import InterfaceError, OperationalError

logger = logging.getLogger(__name__)

T = TypeVar("T")

RETRY_DELAYS_SECONDS = (0.5, 1.0, 2.0)
"""The waits between attempts: up to three retries over about 3.5 seconds."""


def retrying(
    what: str,
    call: Callable[[], T],
    *,
    delays: Sequence[float] = RETRY_DELAYS_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Return ``call()``, retrying after each delay while the database can't be reached.

    ``call`` must be idempotent: it may have run, or half-run, before it failed.
    """
    for delay in delays:
        try:
            return call()
        except (OperationalError, InterfaceError) as error:
            logger.warning("%s failed (%s); retrying in %gs", what, error.orig or error, delay)
            sleep(delay)
    return call()
