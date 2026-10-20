"""Linked pipeline trigger edges and parent/child run navigation."""

from etl_craft.core.enums import Handler
from etl_craft.handlers.run_pipeline import read_trigger
from etl_craft.services.catalog.collect import Catalog
from etl_craft.services.catalog.html import escape, pipeline_url, task_url


def trigger_edges(catalog: Catalog, code: str | None = None, root: str = "") -> str:
    """Draw each trigger edge with its mode and an accessible source task link."""
    edges = []
    for label, task in catalog.tasks.items():
        if task.row.handler != Handler.RUN_PIPELINE:
            continue
        trigger = read_trigger(task.params)
        parent = task.row.pipeline_code
        target = trigger.pipeline_code
        if code is not None and code not in {parent, target}:
            continue
        edges.append(
            '<li class="trigger-edge">'
            f'<a href="{root}{escape(pipeline_url(parent))}">{escape(parent)}</a> '
            f'<span aria-label="triggers in {trigger.mode} mode">→ {trigger.mode} →</span> '
            f'<a href="{root}{escape(pipeline_url(target))}">{escape(target)}</a> '
            f'(task <a href="{root}{escape(task_url(label))}">{escape(label)}</a>)</li>'
        )
    return "<h2>Pipeline triggers</h2><ul>" + "".join(edges) + "</ul>" if edges else ""
