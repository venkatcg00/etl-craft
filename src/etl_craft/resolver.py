"""Pure dependency-graph resolver for CFG_TASK_DEPENDENCY rows."""

# No Airflow awareness, no database access — every function here takes plain
# data in and returns plain data out, so it can be exercised without a live
# Engine DB. The CLI (`run`, `graph`, `generate-yml`) is the only layer that
# talks to Postgres; it hands this module rows already fetched, and consumes
# its output.
#
# Scope: this module resolves ordering *within a single pipeline* only, from
# CFG_TASK_DEPENDENCY rows where DEPENDS_ON_PIPELINE_ID == PIPELINE_ID. Cross-
# pipeline edges have no DAG-native structure to resolve into waves — per
# CLAUDE.md, those are always resolved by a runtime self-check/poll step
# against AUD_PIPELINE_DEPENDENCY_TRACKER / AUD_TASK_DEPENDENCY_TRACKER
# instead. Callers must filter cross-pipeline edges out before calling
# `build_graph` — passing one in is treated as caller error (ValueError),
# not something this module resolves itself.

from __future__ import annotations

from dataclasses import dataclass

DEPENDENCY_TYPES = frozenset({"SUCCESS", "FAILURE", "ALWAYS", "HAS_DATA"})
TERMINAL_STATUSES = frozenset({"SUCCESS", "FAILED", "SKIPPED"})
# A task's own status blocks it from ready() only for these — FAILED and
# None ("never logged") remain retry-eligible. See DependencyGraph.ready().
NOT_RETRYABLE = frozenset({"SUCCESS", "SKIPPED", "IN-PROGRESS"})


class ResolverError(Exception):
    """Base class for all resolver-raised errors."""


class SelfDependencyError(ResolverError):
    """Raised when a task's dependency edge points at itself."""

    # The DB schema already blocks this at insert time
    # (ck_taskdep_no_self_dep in sql/schema.sql) — this check exists so the
    # resolver is safe to use standalone/in tests without a live Engine DB
    # enforcing that constraint.


class CycleError(ResolverError):
    """Raised when the dependency edges form a cycle with no valid wave ordering."""

    def __init__(self, cycle: tuple[int, ...]) -> None:
        """Store the offending cycle, task ids in the order they were visited."""
        self.cycle = cycle
        super().__init__(f"dependency cycle: {' -> '.join(map(str, cycle))}")


class UnknownTaskError(ResolverError):
    """Raised when an edge references a task_id not present in the supplied task set."""


@dataclass(frozen=True)
class TaskNode:
    """A single CFG_TASKS row, reduced to what the resolver needs."""

    task_id: int


@dataclass(frozen=True)
class TaskEdge:
    """A single CFG_TASK_DEPENDENCY row, reduced to what the resolver needs."""

    # Only same-pipeline edges (depends_on_pipeline_id == pipeline_id, as
    # resolved by the DB's trg_default_taskdep_pipeline trigger) belong here.

    task_id: int
    depends_on_task_id: int
    dependency_type: str


@dataclass(frozen=True)
class TaskRunState:
    """Enough of an AUD_TASK_RUN_LOG row to evaluate one dependency edge."""

    # status is None when the task has no log row yet for the active run,
    # i.e. it hasn't started.

    status: str | None = None
    target_count: int | None = None


class DependencyGraph:
    """An immutable same-pipeline task dependency graph."""

    # Construct via `build_graph`, not directly.

    def __init__(self, tasks: tuple[int, ...], edges: tuple[TaskEdge, ...]) -> None:
        """Index `edges` by their owning task_id for fast dependency lookups."""
        self._task_ids = tasks
        self._edges = edges
        # dependents[t] = edges whose task_id == t (what t depends on)
        self._dependencies_of: dict[int, list[TaskEdge]] = {t: [] for t in tasks}
        for edge in edges:
            self._dependencies_of[edge.task_id].append(edge)

    @property
    def task_ids(self) -> tuple[int, ...]:
        """The task ids in this graph, in the order they were supplied."""
        return self._task_ids

    def dependencies_of(self, task_id: int) -> list[TaskEdge]:
        """Return the edges describing what `task_id` depends on."""
        return list(self._dependencies_of[task_id])

    def waves(self) -> list[list[int]]:
        """Compute static topological generations, ignoring any run-time status."""
        # Wave 0 has no dependencies; wave N depends only on tasks in waves
        # < N. Used by `graph`/`generate-yml` to render structure — same-rank
        # tasks compile into parallel Airflow tasks, later ranks chain after
        # via `>>`. Cycle detection already happened in `build_graph`, so
        # this never raises.
        remaining = set(self._task_ids)
        resolved: set[int] = set()
        result: list[list[int]] = []
        while remaining:
            wave = [
                t
                for t in remaining
                if all(e.depends_on_task_id in resolved for e in self._dependencies_of[t])
            ]
            # build_graph already proved this graph is acyclic, so a
            # non-empty `remaining` always yields a non-empty `wave` here.
            wave.sort()
            result.append(wave)
            resolved.update(wave)
            remaining.difference_update(wave)
        return result

    def ready(self, run_state: dict[int, TaskRunState]) -> list[int]:
        """Return the tasks that can run right now, given each task's current run state."""
        # A task is ready when it is itself retry-eligible and every one of
        # its dependency edges is satisfied. Per CLAUDE.md "Idempotent by
        # construction, retry resumes": a retry skips what's already SUCCESS
        # or SKIPPED and only re-attempts what actually failed or never ran.
        # So a task's own status blocks it from `ready()` only when it's
        # SUCCESS, SKIPPED, or IN-PROGRESS (already running — not to be
        # dispatched a second time concurrently); FAILED and "never logged"
        # (status=None) both remain eligible.
        #
        # Edge satisfaction:
        #   SUCCESS  -> upstream status == SUCCESS
        #   FAILURE  -> upstream status == FAILED
        #   ALWAYS   -> upstream status is terminal (any of SUCCESS/FAILED/SKIPPED)
        #   HAS_DATA -> upstream status == SUCCESS and target_count > 0
        #
        # An upstream with no logged run_state entry is treated as not yet
        # run (status=None), which never satisfies any edge type.
        result = []
        for task_id in self._task_ids:
            state = run_state.get(task_id, TaskRunState())
            if state.status in NOT_RETRYABLE:
                continue
            if all(
                self._edge_satisfied(edge, run_state.get(edge.depends_on_task_id, TaskRunState()))
                for edge in self._dependencies_of[task_id]
            ):
                result.append(task_id)
        return sorted(result)

    @staticmethod
    def _edge_satisfied(edge: TaskEdge, upstream: TaskRunState) -> bool:
        if edge.dependency_type == "SUCCESS":
            return upstream.status == "SUCCESS"
        if edge.dependency_type == "FAILURE":
            return upstream.status == "FAILED"
        if edge.dependency_type == "ALWAYS":
            return upstream.status in TERMINAL_STATUSES
        if edge.dependency_type == "HAS_DATA":
            return (
                upstream.status == "SUCCESS"
                and bool(upstream.target_count)
                and upstream.target_count > 0
            )
        raise ResolverError(f"unknown dependency_type: {edge.dependency_type!r}")


def build_graph(tasks: list[TaskNode], edges: list[TaskEdge]) -> DependencyGraph:
    """Validate and assemble a DependencyGraph from CFG_ rows."""
    # Raises UnknownTaskError, SelfDependencyError, or CycleError on invalid
    # input. Validation order matches how a caller would want to report
    # problems: structural reference errors before the more global cycle
    # check.
    task_ids = tuple(t.task_id for t in tasks)
    task_id_set = set(task_ids)
    if len(task_id_set) != len(task_ids):
        raise ResolverError("duplicate task_id in tasks list")

    for edge in edges:
        if edge.dependency_type not in DEPENDENCY_TYPES:
            raise ResolverError(f"unknown dependency_type: {edge.dependency_type!r}")
        if edge.task_id not in task_id_set:
            raise UnknownTaskError(f"edge references unknown task_id={edge.task_id}")
        if edge.depends_on_task_id not in task_id_set:
            raise UnknownTaskError(
                f"edge references unknown depends_on_task_id={edge.depends_on_task_id}"
            )
        if edge.task_id == edge.depends_on_task_id:
            raise SelfDependencyError(f"task_id={edge.task_id} depends on itself")

    graph = DependencyGraph(task_ids, tuple(edges))
    _check_acyclic(graph)
    return graph


def _check_acyclic(graph: DependencyGraph) -> None:
    """Raise CycleError with the offending cycle if one exists in `graph`."""
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[int, int] = {t: WHITE for t in graph.task_ids}
    path: list[int] = []

    def visit(task_id: int) -> None:
        color[task_id] = GRAY
        path.append(task_id)
        for edge in graph.dependencies_of(task_id):
            upstream = edge.depends_on_task_id
            if color[upstream] == WHITE:
                visit(upstream)
            elif color[upstream] == GRAY:
                cycle_start = path.index(upstream)
                raise CycleError(tuple(path[cycle_start:] + [upstream]))
        path.pop()
        color[task_id] = BLACK

    for task_id in graph.task_ids:
        if color[task_id] == WHITE:
            visit(task_id)
