"""A task process for tests: every handler is a fake whose behaviour a task parameter picks.

Run as ``python -m fixtures.task_child`` with the child's own arguments; ``tests`` must be on
``PYTHONPATH``.
"""

import logging
import os
import signal
import sys
import time

from etl_craft.core.errors import HandlerError
from etl_craft.execution import child
from etl_craft.handlers.registry import HANDLERS, HandlerResult

logger = logging.getLogger("etl_craft.handlers.fake")


def handler(context, engine):
    behaviour = context.task_params.get("BEHAVIOUR", "succeed")
    logger.info("fake handler doing %s%s", behaviour, " again" if context.rerun else "")
    logger.info(
        "as of %s%s", context.run_date.isoformat(), " (backfill)" if context.backfill else ""
    )
    print(f"stdout from {context.task_code}", flush=True)
    print(f"stderr from {context.task_code}", file=sys.stderr, flush=True)
    if behaviour == "succeed":
        return HandlerResult(source_count=10, target_count=9, insert_count=7)
    if behaviour == "variables":
        return HandlerResult(target_count=5, variables={"INGESTION_COUNT": 5, "OFFSET": "42"})
    if behaviour == "fail":
        raise HandlerError("the source file is missing")
    if behaviour == "raise":
        raise ValueError("an unexpected value")
    if behaviour == "exit":
        os._exit(3)
    if behaviour == "kill":
        os.kill(os.getpid(), signal.SIGKILL)
    if behaviour == "sleep":
        time.sleep(60)
    if behaviour == "brief":
        time.sleep(2)
        return HandlerResult(target_count=1)
    raise AssertionError(behaviour)  # pragma: no cover


if __name__ == "__main__":
    HANDLERS.update(
        dict.fromkeys(
            ("PYTHON", "SQL", "BUSINESS_RULES", "EMAIL_ALERT"), "fixtures.task_child:handler"
        )
    )
    raise SystemExit(child.main())
