"""The same-pipeline task dependency graph: validation, waves, readiness and dead ends.

The graph holds a pipeline's tasks and the ``CFG_TASK_DEPENDENCY`` edges between them. Edges to
tasks in other pipelines stay out of it, because they have no place in the pipeline's own
order; each task records only how many it has, since its ``RUN_CONDITION`` counts them too.
The cross-pipeline gate settles them at run time.

Given each task's status under the active run, the graph answers which tasks can start now
(``ready``) and which never-run tasks can never start (``unsatisfiable``). A retry resumes: a
task already ``SUCCESS`` or ``SKIPPED`` is never ready again.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass

from etl_craft.core.enums import (
    NOT_RETRYABLE_STATUSES,
    SETTLED_STATUSES,
    TERMINAL_STATUSES,
    DependencyType,
    RunCondition,
    RunStatus,
)
from etl_craft.core.errors import GraphError


class SelfDependencyError(GraphError):
    """A task depends on itself."""


class CycleError(GraphError):
    """The dependency edges form a cycle, so no order can run them."""

    def __init__(self, cycle: tuple[int, ...]) -> None:
        """Keep the cycle's task ids in the order they were followed."""
        self.cycle = cycle
        super().__init__(f"dependency cycle: {' -> '.join(map(str, cycle))}")


class UnknownTaskError(GraphError):
    """An edge names a task id that is not among the graph's tasks."""


@dataclass(frozen=True)
class TaskNode:
    """A ``CFG_TASKS`` row, reduced to what the graph needs.

    ``run_condition`` is ``None`` for ``ALL``, the default; ``run_condition_count`` is read only
    under ``N``. ``cross_pipeline_edge_count`` is how many of the task's dependencies are on
    tasks in other pipelines.
    """

    task_id: int
    run_condition: str | None = None
    run_condition_count: int | None = None
    cross_pipeline_edge_count: int = 0


@dataclass(frozen=True)
class TaskEdge:
    """A same-pipeline ``CFG_TASK_DEPENDENCY`` row: ``task_id`` waits on ``depends_on_task_id``."""

    task_id: int
    depends_on_task_id: int
    dependency_type: str


@dataclass(frozen=True)
class TaskRunState:
    """What one dependency edge needs from its upstream's ``AUD_TASK_RUN_LOG`` row.

    ``status`` is ``None`` while the task has no row under the active run.
    """

    status: str | None = None
    target_count: int | None = None


RunState = Mapping[int, TaskRunState]
"""Each task's state under the active run, by task id; a missing task has not run."""


class DependencyGraph:
    """A validated, immutable same-pipeline task graph; build it with ``build_graph``."""

    def __init__(self, nodes: tuple[TaskNode, ...], edges: tuple[TaskEdge, ...]) -> None:
        """Index the edges by the task that waits on them."""
        self._task_ids = tuple(node.task_id for node in nodes)
        self._nodes_by_id = {node.task_id: node for node in nodes}
        self._dependencies_of: dict[int, list[TaskEdge]] = {t: [] for t in self._task_ids}
        for edge in edges:
            self._dependencies_of[edge.task_id].append(edge)

    @property
    def task_ids(self) -> tuple[int, ...]:
        """The task ids, in the order they were supplied."""
        return self._task_ids

    def dependencies_of(self, task_id: int) -> list[TaskEdge]:
        """Return the edges ``task_id`` waits on."""
        return list(self._dependencies_of[task_id])

    def waves(self) -> list[list[int]]:
        """Return the tasks in static waves: each wave depends only on earlier waves.

        This is the guaranteed-safe order, not the earliest one: a task comes after every
        upstream, even when its ``ANY`` or ``N`` condition could let it start sooner. ``graph``
        prints it, generated DAGs chain it, and a ``--force`` run follows it because it skips
        the status checks ``ready`` relies on. Task ids within a wave are sorted.
        """
        remaining = set(self._task_ids)
        resolved: set[int] = set()
        result: list[list[int]] = []
        while remaining:
            wave = sorted(
                t
                for t in remaining
                if all(e.depends_on_task_id in resolved for e in self._dependencies_of[t])
            )
            result.append(wave)
            resolved.update(wave)
            remaining.difference_update(wave)
        return result

    def total_edge_count(self, task_id: int) -> int:
        """Return how many dependencies ``task_id`` has, cross-pipeline ones included."""
        return (
            len(self._dependencies_of[task_id])
            + self._nodes_by_id[task_id].cross_pipeline_edge_count
        )

    def required_edge_count(self, task_id: int) -> int:
        """Return how many of ``task_id``'s dependencies must be satisfied before it runs.

        Every dependency under ``ALL``, one under ``ANY`` (none when there are none), and
        ``run_condition_count`` under ``N``.
        """
        total = self.total_edge_count(task_id)
        node = self._nodes_by_id[task_id]
        condition = node.run_condition or RunCondition.ALL
        if condition == RunCondition.ANY:
            return 1 if total else 0
        if condition == RunCondition.N:
            return node.run_condition_count or 1
        return total

    def satisfied_edge_count(
        self,
        task_id: int,
        run_state: RunState,
        cross_pipeline_satisfied: int | None = None,
    ) -> int:
        """Return how many of ``task_id``'s dependencies are satisfied now.

        ``cross_pipeline_satisfied`` is how many of its cross-pipeline dependencies the
        cross-pipeline gate has confirmed. ``None`` means they have not been evaluated, and
        they all count as satisfied: the wave scheduler has to start the task for its gate to
        evaluate them, so counting them as unsatisfied would never start it.
        """
        node = self._nodes_by_id[task_id]
        cross = (
            node.cross_pipeline_edge_count
            if cross_pipeline_satisfied is None
            else cross_pipeline_satisfied
        )
        same = sum(
            1
            for edge in self._dependencies_of[task_id]
            if self._edge_satisfied(edge, run_state.get(edge.depends_on_task_id, TaskRunState()))
        )
        return same + cross

    def ready(
        self,
        run_state: RunState,
        cross_pipeline_satisfied: Mapping[int, int] | None = None,
    ) -> list[int]:
        """Return the sorted ids of the tasks that can start now.

        A task is ready when its own status allows another attempt (it has not run, or it
        ``FAILED``) and at least ``required_edge_count`` of its dependencies are satisfied.
        An edge is satisfied when its upstream is:

        - ``SUCCESS`` for a ``SUCCESS`` edge;
        - ``FAILED`` for a ``FAILURE`` edge;
        - terminal (``SUCCESS``, ``FAILED`` or ``SKIPPED``) for an ``ALWAYS`` edge;
        - ``SUCCESS`` with a positive target count for a ``HAS_DATA`` edge.

        ``cross_pipeline_satisfied`` maps a task id to its confirmed cross-pipeline count; a task
        missing from it counts its cross-pipeline dependencies as satisfied (see
        ``satisfied_edge_count``). That assumption alone never starts a task while one of its
        same-pipeline upstreams has not finished: otherwise an ``ANY`` task would start next to
        the upstream it waits on, find the cross-pipeline edge unsatisfied, and settle as
        ``SKIPPED`` just before the upstream would have satisfied it.
        """
        cross = cross_pipeline_satisfied or {}
        result = []
        for task_id in self._task_ids:
            if run_state.get(task_id, TaskRunState()).status in NOT_RETRYABLE_STATUSES:
                continue
            required = self.required_edge_count(task_id)
            supplied = cross.get(task_id)
            if self.satisfied_edge_count(task_id, run_state, supplied) < required:
                continue
            if supplied is None and self._waits_on_unfinished_upstream(
                task_id, run_state, required
            ):
                continue
            result.append(task_id)
        return sorted(result)

    def _waits_on_unfinished_upstream(
        self, task_id: int, run_state: RunState, required: int
    ) -> bool:
        """Whether only assumed cross-pipeline edges carry ``task_id`` while an upstream runs.

        A ``FAILED`` upstream counts as finished, so an ``ANY`` task can still start on its
        cross-pipeline dependency once a same-pipeline upstream has failed.
        """
        if self.satisfied_edge_count(task_id, run_state, 0) >= required:
            return False
        return any(
            run_state.get(edge.depends_on_task_id, TaskRunState()).status not in TERMINAL_STATUSES
            for edge in self._dependencies_of[task_id]
        )

    def unsatisfiable(self, run_state: RunState) -> list[int]:
        """Return the sorted ids of the never-run tasks that can never become ready in this run.

        The caller records them ``SKIPPED``. A dependency is lost for good only when its
        upstream is settled (``SUCCESS`` or ``SKIPPED``) and does not satisfy it; a ``FAILED``
        upstream may still succeed on a retry, and an ``IN-PROGRESS`` one has not finished.
        Skipping a task settles it, which can doom its own dependents, so the search repeats
        until nothing changes. Cross-pipeline dependencies always count as still possible.

        Only tasks with no status are reported; a task that already ran keeps its status.
        """
        known = dict(run_state)
        result: set[int] = set()
        while True:
            newly = [
                task_id
                for task_id in self._task_ids
                if task_id not in result
                and known.get(task_id, TaskRunState()).status is None
                and self._is_unsatisfiable(task_id, known)
            ]
            if not newly:
                return sorted(result)
            for task_id in newly:
                result.add(task_id)
                known[task_id] = TaskRunState(status=RunStatus.SKIPPED)

    def _is_unsatisfiable(self, task_id: int, run_state: RunState) -> bool:
        if not self.total_edge_count(task_id):
            return False
        still_possible = self._nodes_by_id[task_id].cross_pipeline_edge_count + sum(
            1
            for edge in self._dependencies_of[task_id]
            if not self._edge_permanently_unsatisfiable(
                edge, run_state.get(edge.depends_on_task_id, TaskRunState())
            )
        )
        return still_possible < self.required_edge_count(task_id)

    @classmethod
    def _edge_permanently_unsatisfiable(cls, edge: TaskEdge, upstream: TaskRunState) -> bool:
        if upstream.status not in SETTLED_STATUSES:
            return False
        return not cls._edge_satisfied(edge, upstream)

    @staticmethod
    def _edge_satisfied(edge: TaskEdge, upstream: TaskRunState) -> bool:
        if edge.dependency_type == DependencyType.SUCCESS:
            return upstream.status == RunStatus.SUCCESS
        if edge.dependency_type == DependencyType.FAILURE:
            return upstream.status == RunStatus.FAILED
        if edge.dependency_type == DependencyType.ALWAYS:
            return upstream.status in TERMINAL_STATUSES
        if edge.dependency_type == DependencyType.HAS_DATA:
            count = upstream.target_count
            return upstream.status == RunStatus.SUCCESS and count is not None and count > 0
        raise GraphError(f"unknown dependency_type: {edge.dependency_type!r}")


def build_graph(tasks: Sequence[TaskNode], edges: Sequence[TaskEdge]) -> DependencyGraph:
    """Validate ``CFG_`` rows and build their ``DependencyGraph``.

    Raises ``UnknownTaskError`` or ``SelfDependencyError`` for a bad edge, ``GraphError`` for a
    duplicate task, an unknown dependency type or an invalid run condition, and ``CycleError``
    when the edges loop. The Engine DB's CHECK constraints reject most of these on insert; the
    graph checks them again so it is safe to use without one. An ``N`` count larger than the
    task's number of dependencies spans two tables, so only the graph can reject it.
    """
    task_ids = tuple(t.task_id for t in tasks)
    task_id_set = set(task_ids)
    if len(task_id_set) != len(task_ids):
        raise GraphError("duplicate task_id in tasks list")

    dependency_types = {member.value for member in DependencyType}
    edge_count: dict[int, int] = dict.fromkeys(task_ids, 0)
    for edge in edges:
        if edge.dependency_type not in dependency_types:
            raise GraphError(f"unknown dependency_type: {edge.dependency_type!r}")
        if edge.task_id not in task_id_set:
            raise UnknownTaskError(f"edge references unknown task_id={edge.task_id}")
        if edge.depends_on_task_id not in task_id_set:
            raise UnknownTaskError(
                f"edge references unknown depends_on_task_id={edge.depends_on_task_id}"
            )
        if edge.task_id == edge.depends_on_task_id:
            raise SelfDependencyError(f"task_id={edge.task_id} depends on itself")
        edge_count[edge.task_id] += 1

    for task in tasks:
        _check_run_condition(task, edge_count[task.task_id] + task.cross_pipeline_edge_count)

    graph = DependencyGraph(tuple(tasks), tuple(edges))
    _check_acyclic(graph)
    return graph


def _check_run_condition(task: TaskNode, total_edges: int) -> None:
    """Raise ``GraphError`` unless the task's run condition and count fit together."""
    if task.run_condition is None:
        if task.run_condition_count is not None:
            raise GraphError(
                f"task_id={task.task_id} sets run_condition_count with no run_condition"
            )
        return
    if task.run_condition not in {member.value for member in RunCondition}:
        raise GraphError(
            f"task_id={task.task_id} has unknown run_condition: {task.run_condition!r}"
        )
    if task.run_condition != RunCondition.N:
        if task.run_condition_count is not None:
            raise GraphError(
                f"task_id={task.task_id} sets run_condition_count with "
                f"run_condition={task.run_condition!r}, which ignores it"
            )
        return
    if task.run_condition_count is None or task.run_condition_count < 1:
        raise GraphError(
            f"task_id={task.task_id} has run_condition='N' but "
            f"run_condition_count={task.run_condition_count!r}"
        )
    if task.run_condition_count > total_edges:
        raise GraphError(
            f"task_id={task.task_id} requires {task.run_condition_count} satisfied "
            f"dependencies but only has {total_edges} — it could never run"
        )


def _check_acyclic(graph: DependencyGraph) -> None:
    """Raise ``CycleError`` naming a cycle, if the graph has one.

    A depth-first search with an explicit stack, so a dependency chain deeper than Python's
    recursion limit is still checked.
    """
    white, gray, black = 0, 1, 2
    color: dict[int, int] = dict.fromkeys(graph.task_ids, white)

    def upstreams_of(task_id: int) -> Iterator[int]:
        return iter([edge.depends_on_task_id for edge in graph.dependencies_of(task_id)])

    for root in graph.task_ids:
        if color[root] != white:
            continue
        path: list[int] = [root]
        color[root] = gray
        stack: list[tuple[int, Iterator[int]]] = [(root, upstreams_of(root))]
        while stack:
            task_id, pending = stack[-1]
            descended = False
            for upstream in pending:
                if color[upstream] == gray:
                    raise CycleError((*path[path.index(upstream) :], upstream))
                if color[upstream] == white:
                    color[upstream] = gray
                    path.append(upstream)
                    # The frame keeps its iterator, so returning to it resumes the loop here.
                    stack.append((upstream, upstreams_of(upstream)))
                    descended = True
                    break
            if not descended:
                stack.pop()
                path.pop()
                color[task_id] = black
