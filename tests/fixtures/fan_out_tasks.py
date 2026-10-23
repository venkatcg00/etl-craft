"""Importable item functions used by the spawned-worker contract tests."""

from etl_craft.scripting import ScriptResult


def one_row(key):
    """Return a known independent count without inheriting task-process connections."""
    return ScriptResult(1)
