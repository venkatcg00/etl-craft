"""Shared escaping and table markup for catalog pages."""

from __future__ import annotations

import html
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
