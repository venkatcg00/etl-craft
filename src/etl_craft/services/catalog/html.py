"""Shared escaping and table markup for catalog pages."""

from __future__ import annotations

import hashlib
import html
import re
from collections.abc import Iterable, Sequence


def escape(value: object) -> str:
    """Escape display text and attribute values, including optional database fields."""
    return html.escape("" if value is None else str(value), quote=True)


def render_table(
    headers: Sequence[str],
    rows: Iterable[Sequence[str]],
    empty: str,
    row_classes: Sequence[str] | None = None,
) -> str:
    """Render already escaped cells with the catalog table markup."""
    body = "".join(
        (f'<tr class="{escape(row_classes[index])}">' if row_classes else "<tr>")
        + "".join(f"<td>{c}</td>" for c in row)
        + "</tr>"
        for index, row in enumerate(rows)
    )
    if not body:
        return f'<p class="note">{escape(empty)}</p>'
    head = "".join(f"<th>{escape(h)}</th>" for h in headers)
    return f'<div class="scroll"><table class="list"><tr>{head}</tr>{body}</table></div>'


def slug(key: str) -> str:
    """Return a file name for ``key``: itself when safe, else made safe with a short hash."""
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", key)
    if safe == key and not key.startswith("."):
        return key
    return f"{safe.lstrip('.')}-{hashlib.sha256(key.encode()).hexdigest()[:8]}"


def table_url(name: str) -> str:
    """Return the URL of a table's page, from the site's root."""
    return f"tables/{slug(name)}.html"


def task_url(label: str) -> str:
    """Return the URL of a task's page, from the site's root."""
    return f"tasks/{slug(label)}.html"


def pipeline_url(code: str) -> str:
    """Return the URL of a pipeline's page, from the site's root."""
    return f"pipelines/{slug(code)}.html"


def rule_url(rule_id: int) -> str:
    """Return the URL of a business rule's page, from the site's root."""
    return f"rules/{rule_id}.html"


def script_url(name: str) -> str:
    """Return the URL of an ingestion script's page, from the site's root."""
    return f"scripts/{slug(name)}.html"


def run_url(pipeline_run_id: int) -> str:
    """Return the URL of a pipeline run's page, from the site's root."""
    return f"runs/{pipeline_run_id}.html"


def tag_chips(tags: tuple[str, ...], root: str = "") -> str:
    """Link each exact label to a shareable filtered pipeline overview."""
    from urllib.parse import urlencode

    return " ".join(
        f'<a class="tag-chip" href="{escape(root + "dags.html?" + urlencode({"tag": tag}))}">'
        f"{escape(tag)}</a>"
        for tag in tags
    )
