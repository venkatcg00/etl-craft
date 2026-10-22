"""Inspect the saved payloads selected by a successful keyed event wait."""

import json

from etl_craft.scripting import ScriptResult, ScriptTask


def run(task: ScriptTask) -> ScriptResult:
    """Decode each signal's coordinates without rereading the live signal table."""
    for row in task.event_rows:
        json.loads(row["payload"])
    return ScriptResult(row_count=len(task.event_keys))
