"""An isolated local-pool owner that can die without killing the pytest process."""

import json
import os
import sys
import threading
from pathlib import Path
from time import monotonic

from etl_craft.config import load_config
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import RunStatus
from etl_craft.engine.connection import engine_db
from etl_craft.execution.pools import AttemptSpec, HandleState
from etl_craft.execution.pools.local import LocalPool

if __name__ == "__main__":
    config = load_config(os.environ["ETL_CRAFT_CONFIG"])
    spec = AttemptSpec(**json.loads(Path(sys.argv[1]).read_text(encoding="utf-8")))
    with acting_as(SYSTEM_ACTOR):
        engine = engine_db(config)
        pool = LocalPool(engine, config)
        try:
            handle = pool.submit(spec)
            deadline = monotonic() + 120
            while True:
                result = pool.status(handle)
                if result.state == HandleState.ENDED:
                    if result.error is not None:
                        raise result.error
                    assert result.outcome.status == RunStatus.SUCCESS
                    break
                assert monotonic() < deadline, "local pool worker exceeded its test deadline"
                threading.Event().wait(0.02)
        finally:
            pool.close()
            engine.dispose()
