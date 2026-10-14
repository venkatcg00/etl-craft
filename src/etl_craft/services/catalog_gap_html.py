"""Accessible gap indicators and comparison grids for the generated snapshot."""

from collections import Counter
from collections.abc import Callable

from etl_craft.core.enums import GapState
from etl_craft.services.catalog import Catalog, TableAsset
from etl_craft.services.catalog_gaps import (
    GapRecord,
    affected_tasks,
    gap_snapshot,
    lineage_records,
    table_records,
)
from etl_craft.services.catalog_html import escape, render_table

STYLES = {
    GapState.DEPLOYED_ONLY: "amber",
    GapState.CONFIG_ONLY: "red",
    GapState.TYPE_MISMATCH: "purple",
    GapState.NULLABILITY_MISMATCH: "purple",
    GapState.KIND_MISMATCH: "purple",
    GapState.LINEAGE_GAP: "amber",
    GapState.MATCH: "neutral",
}


def state_class(state: GapState) -> str:
    """Colour supplements the visible outcome label."""
    return "gap-" + STYLES.get(state, "grey")


def indicator(record: GapRecord) -> str:
    """Show the outcome in text, with a supplementary warning symbol."""
    symbol = (
        '<span aria-hidden="true">⚠ </span>'
        if record.state in STYLES and record.state != GapState.MATCH
        else ""
    )
    return (
        f'<span class="badge {state_class(record.state)}" title="{escape(record.reason)}">'
        f"{symbol}{escape(record.state)}</span>"
    )


def comparison_grid(
    catalog: Catalog,
    table: TableAsset,
    task_link: Callable[[str], str],
    table_link: Callable[[str], str],
) -> str:
    """Keep inferred names, missing columns and engine columns visible side by side."""
    records = table_records(catalog, table)
    objects = [record for record in records if record.column is None]
    findings = "".join(f"<p>{indicator(r)} {escape(r.reason)}</p>" for r in objects)
    rows, classes = [], []
    lineage = lineage_records(catalog, table)
    for column in table.columns.values():
        compared = [record for record in records if record.column == column.name]
        coverage = compared or objects
        warnings = [record for record in coverage if record.state != GapState.MATCH]
        selected = warnings[0] if warnings else coverage[0]
        classes.append(state_class(selected.state))
        rows.append(
            [
                f'<a href="#col={escape(column.name)}">{escape(column.name)}</a>',
                "Engine managed" if column.engine_managed else "Business column",
                escape(column.declared_type) or "Not declared",
                escape(column.warehouse_type) or "Unavailable"
                if not table.warehouse_checked
                else escape(column.warehouse_type) or "Missing",
                escape(column.comment),
                "<br>".join(
                    f"{escape(r.attribute)}: {indicator(r)} {escape(r.reason)} "
                    f"(declared: {escape(r.declared) or 'Not declared'}; "
                    f"deployed: {escape(r.deployed) or 'Unavailable'})"
                    for r in coverage
                ),
                "<br>".join(
                    f"{indicator(r)} {escape(r.reason)} " + ", ".join(task_link(t) for t in r.tasks)
                    for r in lineage
                ),
                _sources(catalog, table.name, column.name, table_link),
            ]
        )
    compared_at = catalog.compared_at.isoformat() if catalog.compared_at else "Not checked"
    links = ", ".join(task_link(name) for name in affected_tasks(catalog, table))
    return (
        '<h2 id="comparison">Contract and deployment</h2>'
        + f"<p>Compared: {escape(compared_at)}. "
        + f"Warehouse: {escape(catalog.warehouse_identity) or 'Not configured'}. "
        + '<a href="../gaps.html">Project comparison and input version</a></p>'
        + findings
        + render_table(
            [
                "Column",
                "Ownership",
                "Declared type",
                "Deployed type",
                "Comment",
                "Coverage",
                "Lineage",
                "Made from",
            ],
            rows,
            "No column contract is available.",
            classes,
        )
        + f"<p>Affected tasks: {links or 'None'}</p>"
    )


def _sources(catalog: Catalog, table: str, column: str, link: Callable[[str], str]) -> str:
    return "<br>".join(
        (
            f"{link(edge.source_object)}.{escape(edge.source_column)}"
            if edge.source_object
            else "No source column"
        )
        + f" <code>{escape(edge.transformation)}</code> ({escape(edge.task)})"
        for edge in catalog.column_edges
        if edge.target_object == table and edge.target_column == column
    )


def gaps_page(
    catalog: Catalog, table_link: Callable[[str], str], task_link: Callable[[str], str]
) -> str:
    """Expose all coverage states rather than counting an unchecked project as healthy."""
    snapshot = gap_snapshot(catalog)
    counts = Counter(record.state for record in snapshot.records)
    cards = " ".join(
        f'<span class="badge {state_class(state)}">{escape(state)}: {count}</span>'
        for state, count in sorted(counts.items())
    )
    rows = [
        [
            table_link(r.table),
            escape(r.column) or "Object",
            escape(r.attribute),
            indicator(r),
            escape(r.declared) or "Not declared",
            escape(r.deployed) or "Unavailable",
            escape(r.reason),
            ", ".join(task_link(t) for t in r.tasks),
        ]
        for r in snapshot.records
    ]
    filters = (
        '<label>Filter outcomes <input type="search" class="gap-filter" '
        'aria-label="Filter gaps by state, object, task or pipeline"></label>'
    )
    return (
        f"<p>Generated: {escape(snapshot.generated_at.isoformat())}. Compared: "
        f"{escape(snapshot.compared_at.isoformat()) if snapshot.compared_at else 'Not checked'}. "
        f"Warehouse: {escape(snapshot.warehouse) or 'Not configured'}.</p>"
        f"<p>Config/SQL version: <code>{escape(snapshot.contract_version)}</code></p>"
        "<p>This is a dated snapshot; regenerate to compare current metadata. "
        "Counts are attribute outcomes, not distinct tables.</p>"
        f'<p>{cards}</p>{filters}<div class="gap-records">'
        + render_table(
            [
                "Object",
                "Column",
                "Attribute",
                "State",
                "Declared",
                "Deployed",
                "Reason",
                "Affected tasks",
            ],
            rows,
            "No referenced warehouse objects.",
            [state_class(r.state) for r in snapshot.records],
        )
        + "</div>"
    )
