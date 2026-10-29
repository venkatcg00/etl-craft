"""Wait ``INPUT_PARAMS.seconds`` seconds, standing in for a slow source."""

import time

from etl_craft.scripting import ScriptResult, ScriptTask


def run(task: ScriptTask) -> ScriptResult:
    """Wait without measuring rows or reporting a source offset."""
    seconds = float(task.input_params.get("seconds", 0))
    task.logger.info("waiting %.0f second(s)", seconds)
    time.sleep(seconds)
    return ScriptResult()
