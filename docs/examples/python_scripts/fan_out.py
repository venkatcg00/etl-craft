"""Replace each landing batch by task run and stable item key before a managed SQL task."""

from sqlalchemy import text

from etl_craft.scripting import ScriptResult, ScriptTask


def run(task: ScriptTask) -> ScriptResult:
    """Load configured keys into an existing landing table, safely repeatable on retry."""
    keys = task.input_params["keys"]

    def load(key: str) -> ScriptResult:
        with task.warehouse() as engine, engine.begin() as conn:
            identity = {"task_run": task.task_run_id, "item_key": key}
            conn.execute(
                text("DELETE FROM item_landing WHERE task_run_id=:task_run AND item_key=:item_key"),
                identity,
            )
            conn.execute(
                text(
                    "INSERT INTO item_landing (task_run_id,item_key) VALUES (:task_run,:item_key)"
                ),
                identity,
            )
        return ScriptResult(1)

    return task.fan_out(keys, load, max_items=1000, max_parallel=16, retries=1)
