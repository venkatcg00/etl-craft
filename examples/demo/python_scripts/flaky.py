"""Fail the first time it runs in this project, then succeed: a retry that resumes.

The first attempt leaves a marker file beside the script and fails; every later attempt finds
it and succeeds.
"""

from pathlib import Path

from etl_craft.scripting import ScriptResult

MARKER = Path(__file__).with_name(".flaky-has-failed")


def run() -> ScriptResult:
    """Fail on the first attempt in this project, then succeed."""
    if not MARKER.exists():
        MARKER.write_text("failed once\n", encoding="utf-8")
        raise RuntimeError("the source was not ready yet; the next attempt will find it")
    return ScriptResult(row_count=0)
