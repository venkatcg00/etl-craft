"""Lay out a pipeline's waves as a run graph the run page draws in the catalog's DAG style."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from etl_craft.core.enums import RunStatus
from etl_craft.services.operations.models import GraphView

NODE_WIDTH = 210
NODE_HEIGHT = 46
COLUMN_GAP = 70
ROW_GAP = 18
MARGIN = 12


@dataclass(frozen=True)
class GraphNode:
    """One task box: its place, its status under the run and the link that selects it."""

    task_code: str
    x: int
    y: int
    status: RunStatus | None
    selected: bool
    href: str


@dataclass(frozen=True)
class GraphEdge:
    """One dependency line, drawn from the upstream box to the task that waits for it."""

    path: str
    kind: str
    title: str


@dataclass(frozen=True)
class GraphLayout:
    """Boxes and lines sized for an SVG viewBox."""

    nodes: tuple[GraphNode, ...]
    edges: tuple[GraphEdge, ...]
    width: int
    height: int
    node_width: int = NODE_WIDTH
    node_height: int = NODE_HEIGHT


def run_graph(
    graph: GraphView,
    statuses: Mapping[str, RunStatus | None],
    selected: str | None,
    link: Callable[[str], str],
) -> GraphLayout:
    """Place each wave in a column, left to right, and connect tasks to their upstream tasks."""
    places: dict[str, tuple[int, int]] = {}
    for column, wave in enumerate(graph.waves):
        for row, code in enumerate(wave):
            places[code] = (
                MARGIN + column * (NODE_WIDTH + COLUMN_GAP),
                MARGIN + row * (NODE_HEIGHT + ROW_GAP),
            )
    nodes = tuple(
        GraphNode(code, x, y, statuses.get(code), code == selected, link(code))
        for code, (x, y) in places.items()
    )
    edges = tuple(
        _edge(places[upstream], places[code], kind, f"{code} waits for {upstream}: {kind}")
        for code, upstreams in graph.depends_on.items()
        if code in places
        for upstream, kind in upstreams
        if upstream in places
    )
    width = max((x for x, _ in places.values()), default=0) + NODE_WIDTH + MARGIN
    height = max((y for _, y in places.values()), default=0) + NODE_HEIGHT + MARGIN
    return GraphLayout(nodes, edges, width, height)


def _edge(start: tuple[int, int], end: tuple[int, int], kind: str, title: str) -> GraphEdge:
    x1, y1 = start[0] + NODE_WIDTH, start[1] + NODE_HEIGHT // 2
    x2, y2 = end[0], end[1] + NODE_HEIGHT // 2
    bend = max(30, (x2 - x1) // 2)
    path = f"M{x1},{y1} C{x1 + bend},{y1} {x2 - bend},{y2} {x2},{y2}"
    return GraphEdge(path, kind.lower().replace("_", "-"), title)
