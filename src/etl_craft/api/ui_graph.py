"""Lay out a run's graph (in the catalog's DAG style) and its attempt timeline for the run page."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from etl_craft.core.enums import RunStatus
from etl_craft.core.time import as_utc
from etl_craft.services.operations.models import AttemptView, GraphView, TaskRunView

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


TIMELINE_WIDTH = 720
LANE_HEIGHT = 30
LABEL_WIDTH = 170


@dataclass(frozen=True)
class TimelineBar:
    """One attempt's waiting or running stretch on the time axis."""

    kind: str
    x: int
    width: int
    title: str


@dataclass(frozen=True)
class TimelineLane:
    """One task's row: its label, link and attempt bars."""

    task_code: str
    y: int
    href: str
    selected: bool
    bars: tuple[TimelineBar, ...]


@dataclass(frozen=True)
class TimelineLayout:
    """Lanes on a shared axis from the run's first queue time to its last end (or now)."""

    lanes: tuple[TimelineLane, ...]
    start: datetime | None
    end: datetime | None
    width: int = TIMELINE_WIDTH
    label_width: int = LABEL_WIDTH
    lane_height: int = LANE_HEIGHT

    @property
    def height(self) -> int:
        """Return the drawing height for every lane."""
        return max(1, len(self.lanes)) * LANE_HEIGHT + 24


def run_timeline(
    task_runs: Sequence[TaskRunView],
    now: datetime,
    selected: str | None,
    link: Callable[[str], str],
) -> TimelineLayout:
    """Place each attempt's queue wait and execution on one time axis, a lane per task."""
    moments = [
        as_utc(moment)
        for task in task_runs
        for attempt in task.attempts
        for moment in (attempt.queued_at, attempt.started_at, attempt.ended_at)
        if moment is not None
    ]
    if not moments:
        return TimelineLayout((), None, None)
    start, end = min(moments), max([*moments, now] if _open(task_runs) else moments)
    span = max((end - start).total_seconds(), 1.0)

    def x(moment: datetime) -> int:
        return LABEL_WIDTH + round((as_utc(moment) - start).total_seconds() / span * TIMELINE_WIDTH)

    lanes = []
    for index, task in enumerate(task_runs):
        bars = []
        for attempt in task.attempts:
            began = attempt.started_at or now
            if attempt.queued_at is not None:
                bars.append(_bar("queued", x(attempt.queued_at), x(began), attempt, "waiting"))
            if attempt.started_at is not None:
                finish = attempt.ended_at or now
                bars.append(
                    _bar(str(attempt.status), x(attempt.started_at), x(finish), attempt, "ran")
                )
        lanes.append(
            TimelineLane(
                task.task_code,
                index * LANE_HEIGHT + 20,
                link(task.task_code),
                task.task_code == selected,
                tuple(bars),
            )
        )
    return TimelineLayout(tuple(lanes), start, end)


def _open(task_runs: Sequence[TaskRunView]) -> bool:
    return any(a.ended_at is None for task in task_runs for a in task.attempts)


def _bar(kind: str, left: int, right: int, attempt: AttemptView, verb: str) -> TimelineBar:
    return TimelineBar(
        kind,
        left,
        max(2, right - left),
        f"Attempt {attempt.attempt_number} {verb} · {attempt.status}",
    )
