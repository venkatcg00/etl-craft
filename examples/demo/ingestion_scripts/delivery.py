"""Read the files the successful event wait recorded under this run."""

import csv
from pathlib import Path

from etl_craft.scripting import ScriptResult, ScriptTask


def run(task: ScriptTask) -> ScriptResult:
    """Count delivered CSV records and report which files were read."""
    rows = 0
    for name in task.input_params["files"]:
        with Path(name).open(newline="", encoding="utf-8") as source:
            rows += sum(1 for _ in csv.DictReader(source))
        task.logger.info("read delivery %s", name)
    return ScriptResult(row_count=rows)
