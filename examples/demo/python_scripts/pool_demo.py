"""A warehouse-independent task for the ten-project pool demonstration."""

from etl_craft.scripting import ScriptResult, ScriptTask


def run(task: ScriptTask) -> ScriptResult:
    """Report one completed task under its own project and run identity."""
    print(f"{task.pipeline_code}: pipeline_run_id={task.pipeline_run_id}")
    return ScriptResult(1)
