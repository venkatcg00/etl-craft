"""UTILITY: publish a ready marker without inventing an ingestion position or count."""

from etl_craft.scripting import ScriptResult, ScriptTask


def run(task: ScriptTask) -> ScriptResult:
    """Tell the receiving system that this data-movement run is ready."""
    assert task.offset is None
    directory = task.config.host_dir / "outbox"
    directory.mkdir(exist_ok=True)
    (directory / "ready.signal").write_text(str(task.pipeline_run_id), encoding="utf-8")
    return ScriptResult()
