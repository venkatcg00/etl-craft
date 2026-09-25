"""A table's lineage graph, laid out and drawn as SVG for its catalog page.

From one table the graph walks table edges upstream to the first sources and downstream to the
last consumers, across tasks and pipelines. Each table sits in a column by its distance from
the focus: upstream to the left (level -1, -2, ...), downstream to the right. Within a column,
tables are ordered by where their neighbours are, a few sweeps each way, so edges cross less.

Each table box lists the columns that lineage connects to the other tables shown (every column,
for the focus table). Column edges run from a source column's row to a target column's row:
solid for a copy, dashed for a derived value, whose expression is the edge's tooltip. A column
made from no source column, such as ``COUNT(*)`` or a constant, is marked. A task whose columns
cannot be traced still links its tables, header to header, dotted.

Every edge ends in an arrow at the table made from its source. Each box has a header that shows
or hides its columns and a button that opens the table's page. The drawing carries what the
page script needs: each table's level and position, and each edge's two tables and columns, so
the script can collapse tables and stack the boxes again, filter by direction and depth, and
highlight every path through a column.
"""

from __future__ import annotations

import html
from collections.abc import Callable
from dataclasses import dataclass, field

from etl_craft.services.catalog import Catalog
from etl_craft.services.lineage import COPY, Edge

NODE_WIDTH = 260
HEADER = 30
ROW = 22
LEVEL_GAP = 120
NODE_GAP = 28
MARGIN = 20
MAX_ROWS = 30
"""Columns listed per table before the rest are summed up; the focus table lists all."""
MAX_TABLES = 200
"""Tables drawn at most; the farthest levels are left out beyond it, and the page says so."""
SWEEPS = 4
LABEL_CHARS = 30
ARROW = 'marker-end="url(#arrow)"'


@dataclass
class Node:
    """A table in the drawing."""

    name: str
    label: str
    level: int
    external: bool
    columns: list[str] = field(default_factory=list)
    derived: dict[str, str] = field(default_factory=dict)
    hidden_columns: int = 0
    x: float = 0
    y: float = 0

    @property
    def height(self) -> float:
        """The box's height: header, one row per column, one for the hidden ones."""
        rows = len(self.columns) + (1 if self.hidden_columns else 0)
        return HEADER + ROW * max(rows, 0) + (6 if rows else 0)

    def row_y(self, column: str) -> float:
        """Return the vertical middle of a column's row, from the top of the drawing."""
        return self.y + HEADER + ROW * self.columns.index(column) + ROW / 2


@dataclass
class LineageDrawing:
    """A laid-out graph: its tables, column edges, table-only edges and size."""

    focus: str
    nodes: dict[str, Node]
    column_edges: list[Edge]
    table_edges: list[tuple[str, str, str]]
    width: float
    height: float
    truncated_at: int | None

    @property
    def levels(self) -> tuple[int, int]:
        """The furthest upstream and downstream levels drawn."""
        levels = [node.level for node in self.nodes.values()]
        return min(levels), max(levels)


def lineage_drawing(catalog: Catalog, focus: str) -> LineageDrawing:
    """Lay out the lineage of table ``focus``, as far up and down as it goes."""
    upstream: dict[str, set[str]] = {}
    downstream: dict[str, set[str]] = {}
    for edge in catalog.table_edges:
        upstream.setdefault(edge.target, set()).add(edge.source)
        downstream.setdefault(edge.source, set()).add(edge.target)
    levels, truncated_at = _levels(focus, upstream, downstream)
    nodes = {
        name: Node(
            name,
            catalog.tables[name].label if name in catalog.tables else name,
            level,
            catalog.tables[name].external if name in catalog.tables else False,
        )
        for name, level in levels.items()
    }
    column_edges = [
        e
        for e in catalog.column_edges
        if e.target_object in nodes and (e.source_object is None or e.source_object in nodes)
    ]
    _choose_columns(catalog, focus, nodes, column_edges)
    column_edges = [
        e
        for e in column_edges
        if e.source_object is not None
        and e.source_column in nodes[e.source_object].columns
        and e.target_column in nodes[e.target_object].columns
        and e.source_object != e.target_object
    ]
    linked = {(e.source_object, e.target_object) for e in column_edges}
    table_edges = sorted(
        {
            (edge.source, edge.target, edge.task)
            for edge in catalog.table_edges
            if edge.source in nodes
            and edge.target in nodes
            and (edge.source, edge.target) not in linked
        }
    )
    width, height = _place(nodes, column_edges, table_edges)
    return LineageDrawing(focus, nodes, column_edges, table_edges, width, height, truncated_at)


def _levels(
    focus: str, upstream: dict[str, set[str]], downstream: dict[str, set[str]]
) -> tuple[dict[str, int], int | None]:
    """Give each table its signed distance from ``focus``, one level each way at a time.

    A level that would take the drawing past ``MAX_TABLES`` is left out, in both directions,
    and the depth reached is returned with the levels.
    """
    levels = {focus: 0}
    fronts = {-1: {focus}, 1: {focus}}
    depth = 0
    while fronts[-1] or fronts[1]:
        depth += 1
        ring: dict[str, int] = {}
        for sign, graph in ((-1, upstream), (1, downstream)):
            found = {o for name in fronts[sign] for o in graph.get(name, ())}
            fronts[sign] = {o for o in found if o not in levels and o not in ring}
            ring.update(dict.fromkeys(sorted(fronts[sign]), sign * depth))
        if len(levels) + len(ring) > MAX_TABLES:
            return levels, depth - 1
        levels.update(ring)
    return levels, None


def _choose_columns(
    catalog: Catalog, focus: str, nodes: dict[str, Node], edges: list[Edge]
) -> None:
    """List, in each box, the columns lineage connects within the drawing."""
    wanted: dict[str, set[str]] = {name: set() for name in nodes}
    for e in edges:
        if e.source_object is None:
            wanted[e.target_object].add(e.target_column)
            continue
        if e.source_object == e.target_object:
            continue
        wanted[e.target_object].add(e.target_column)
        wanted[e.source_object].add(e.source_column or "")
    for e in edges:
        if e.source_object is None:
            nodes[e.target_object].derived[e.target_column] = e.transformation
    for name, node in nodes.items():
        known = list(catalog.tables[name].columns) if name in catalog.tables else []
        chosen = set(known) if name == focus else wanted[name]
        ordered = [c for c in known if c in chosen] + sorted(chosen - set(known) - {""})
        limit = len(ordered) if name == focus else MAX_ROWS
        node.columns = ordered[:limit]
        node.hidden_columns = len(ordered) - len(node.columns)


def _place(
    nodes: dict[str, Node], column_edges: list[Edge], table_edges: list[tuple[str, str, str]]
) -> tuple[float, float]:
    """Order each level to cross fewer edges, then give every box its position."""
    neighbours: dict[str, set[str]] = {name: set() for name in nodes}
    pairs = {(e.source_object or "", e.target_object) for e in column_edges}
    pairs |= {(source, target) for source, target, _ in table_edges}
    for source, target in pairs:
        if source in nodes and target in nodes:
            neighbours[source].add(target)
            neighbours[target].add(source)
    levels = sorted({node.level for node in nodes.values()})
    order = {
        level: sorted((n for n in nodes.values() if n.level == level), key=lambda n: n.label)
        for level in levels
    }
    for sweep in range(SWEEPS):
        sequence = levels if sweep % 2 == 0 else list(reversed(levels))
        for level in sequence:
            position = {n.name: i for lv in levels for i, n in enumerate(order[lv])}
            weights = {n.name: _barycenter(n, position, neighbours, nodes) for n in order[level]}
            order[level].sort(key=lambda node: weights[node.name])
    heights = {
        level: sum(n.height for n in order[level]) + NODE_GAP * (len(order[level]) - 1)
        for level in levels
    }
    tallest = max(heights.values())
    for column, level in enumerate(levels):
        y = MARGIN + (tallest - heights[level]) / 2
        for node in order[level]:
            node.x = MARGIN + column * (NODE_WIDTH + LEVEL_GAP)
            node.y = y
            y += node.height + NODE_GAP
    width = 2 * MARGIN + len(levels) * NODE_WIDTH + (len(levels) - 1) * LEVEL_GAP
    return width, tallest + 2 * MARGIN


def _barycenter(
    node: Node, position: dict[str, int], neighbours: dict[str, set[str]], nodes: dict[str, Node]
) -> float:
    near = [position[o] for o in neighbours[node.name] if nodes[o].level != node.level]
    return sum(near) / len(near) if near else position[node.name]


def render_svg(drawing: LineageDrawing, link: Callable[[str], str]) -> str:
    """Return the drawing as SVG; ``link`` gives the page URL of a table by name."""
    parts = [
        f'<svg class="lineage" xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {drawing.width:.0f} {drawing.height:.0f}" '
        f'width="{drawing.width:.0f}" height="{drawing.height:.0f}" role="img" '
        f'aria-label="Lineage of {_e(drawing.focus)}" data-node-width="{NODE_WIDTH}" '
        f'data-header="{HEADER}" data-row="{ROW}" data-gap="{NODE_GAP}" data-margin="{MARGIN}">',
        "<defs>",
        *(
            f'<marker id="{name}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="9" '
            f'markerHeight="9" markerUnits="userSpaceOnUse" orient="auto-start-reverse">'
            f'<path class="{css}" d="M0,0 L10,5 L0,10 z"/></marker>'
            for name, css in (("arrow", "arrowhead"), ("arrow-on", "arrowhead on"))
        ),
        "</defs>",
        '<g class="edges">',
    ]
    nodes = drawing.nodes
    for e in drawing.column_edges:
        assert e.source_object is not None and e.source_column is not None
        source, target = nodes[e.source_object], nodes[e.target_object]
        kind = "copy" if e.transformation == COPY else "derived"
        path = _curve(
            source.x + NODE_WIDTH,
            source.row_y(e.source_column),
            target.x,
            target.row_y(e.target_column),
        )
        tip = f"{e.source_object}.{e.source_column} → {e.target_object}.{e.target_column}"
        tip += f"\n{e.transformation}" if kind == "derived" else " (copy)"
        tip += f"\nby {e.task}" if e.task else ""
        parts.append(
            f'<path class="edge {kind}" d="{path}" {ARROW} '
            f'data-from="{_e(_col(e.source_object, e.source_column))}" '
            f'data-to="{_e(_col(e.target_object, e.target_column))}" '
            f'data-source="{_e(e.source_object)}" data-target="{_e(e.target_object)}">'
            f"<title>{_e(tip)}</title></path>"
        )
    for source_name, target_name, task in drawing.table_edges:
        source, target = nodes[source_name], nodes[target_name]
        path = _curve(source.x + NODE_WIDTH, source.y + HEADER / 2, target.x, target.y + HEADER / 2)
        parts.append(
            f'<path class="edge table" d="{path}" {ARROW} '
            f'data-source="{_e(source_name)}" data-target="{_e(target_name)}">'
            f"<title>{_e(f'{source.label} → {target.label}, by {task} (table level)')}</title>"
            "</path>"
        )
    parts.append("</g>")
    for node in sorted(nodes.values(), key=lambda n: (n.level, n.y)):
        parts.append(_node_svg(node, focus=node.name == drawing.focus, link=link))
    parts.append("</svg>")
    return "".join(parts)


def _node_svg(node: Node, *, focus: bool, link: Callable[[str], str]) -> str:
    classes = " ".join(
        c for c in ("node", "focus" if focus else "", "external" if node.external else "") if c
    )
    label = node.label if len(node.label) <= LABEL_CHARS else node.label[: LABEL_CHARS - 1] + "…"
    has_columns = bool(node.columns or node.hidden_columns)
    toggle = ": click to show or hide its columns" if has_columns else ""
    head = (
        f'<g class="head" tabindex="0">'
        f'<rect class="header" width="{NODE_WIDTH}" height="{HEADER}" rx="6"/>'
        + ('<text class="chevron" x="10" y="20">▾</text>' if has_columns else "")
        + f'<text class="title" x="{26 if has_columns else 10}" y="20">{_e(label)}</text>'
        f"<title>{_e(node.label)}{toggle}</title></g>"
    )
    opener = ""
    if not node.external:
        opener = (
            f'<a class="open" href="{_e(link(node.name))}">'
            f'<rect x="{NODE_WIDTH - 28}" y="4" width="22" height="22" rx="4"/>'
            f'<text x="{NODE_WIDTH - 17}" y="20" text-anchor="middle">↗</text>'
            f"<title>Open {_e(node.label)}</title></a>"
        )
    parts = [
        f'<g class="{classes}" data-table="{_e(node.name)}" data-level="{node.level}" '
        f'data-x="{node.x:.0f}" data-y="{node.y:.0f}" '
        f'transform="translate({node.x:.0f},{node.y:.0f})">',
        f'<rect class="box" width="{NODE_WIDTH}" height="{node.height:.0f}" rx="6"/>',
        head,
        opener,
    ]
    for index, column in enumerate(node.columns):
        top = HEADER + ROW * index
        badge = ""
        tip = column
        if column in node.derived:
            badge = f'<text class="badge" x="{NODE_WIDTH - 10}" y="15" text-anchor="end">ƒ</text>'
            tip = f"{column} = {node.derived[column]} (no source column)"
        parts.append(
            f'<g class="col" data-col="{_e(_col(node.name, column))}" '
            f'transform="translate(0,{top})" tabindex="0">'
            f'<rect class="row" width="{NODE_WIDTH}" height="{ROW}"/>'
            f'<text x="12" y="15">{_e(column)}</text>{badge}<title>{_e(tip)}</title></g>'
        )
    if node.hidden_columns:
        top = HEADER + ROW * len(node.columns)
        parts.append(
            f'<text class="more" x="12" y="{top + 15}">+{node.hidden_columns} more column(s)</text>'
        )
    parts.append("</g>")
    return "".join(parts)


def _curve(x1: float, y1: float, x2: float, y2: float) -> str:
    bend = max(40.0, abs(x2 - x1) / 2)
    return (
        f"M{x1:.1f},{y1:.1f} C{x1 + bend:.1f},{y1:.1f} {x2 - bend:.1f},{y2:.1f} {x2:.1f},{y2:.1f}"
    )


def _col(table: str, column: str) -> str:
    return f"{table}|{column}"


def _e(value: str) -> str:
    return html.escape(value, quote=True)
