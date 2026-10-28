"""Fail the first time it runs in this project, then succeed: a retry that resumes.

The first attempt leaves a marker file in the project log directory and fails; later attempts find
it and succeeds.
"""

from etl_craft.scripting import ScriptResult, ScriptTask


def run(task: ScriptTask) -> ScriptResult:
    """Fail on the first attempt in this project, then succeed."""
    marker = task.config.log_dir / ".flaky-has-failed"
    marker.parent.mkdir(parents=True, exist_ok=True)
    if not marker.exists():
        marker.write_text("failed once\n", encoding="utf-8")
        raise RuntimeError("the source was not ready yet; the next attempt will find it")
    return ScriptResult(row_count=0)
