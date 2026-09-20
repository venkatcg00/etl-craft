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

from collections.abc import Iterator
from dataclasses import dataclass

DEPENDENCY_TYPES = frozenset({"SUCCESS", "FAILURE", "ALWAYS", "HAS_DATA"})
TERMINAL_STATUSES = frozenset({"SUCCESS", "FAILED", "SKIPPED"})
# How many of a task's own edges must be satisfied for it to become ready.
# [ADDITION, 2026-09-20, E2-41] ALL is the historical behaviour and what a
# NULL CFG_TASKS.RUN_CONDITION means; ANY and N are new.
RUN_CONDITIONS = frozenset({"ALL", "ANY", "N"})
# A task's own status blocks it from ready() only for these — FAILED and
# None ("never logged") remain retry-eligible. See DependencyGraph.ready().
NOT_RETRYABLE = frozenset({"SUCCESS", "SKIPPED", "IN-PROGRESS"})
# Statuses a task can never leave. Narrower than NOT_RETRYABLE (IN-PROGRESS
# will transition) and than TERMINAL_STATUSES (FAILED stays retry-eligible,
# which is the whole point of "retry resumes"). This is the set `unsatisfiable`
# reasons about: once an upstream is SUCCESS or SKIPPED, any edge it does not
# already satisfy it never will. orchestrator.py imports it from here as the
# set of tasks needing no further action, rather than keeping its own copy.
SETTLED_STATUSES = frozenset({"SUCCESS", "SKIPPED"})


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

    # run_condition is None for the overwhelming majority of tasks, meaning
    # "ALL" — CFG_TASKS.RUN_CONDITION is nullable and every row predating
    # E2-41 has it unset. run_condition_count is only ever read for "N".

    task_id: int
    run_condition: str | None = None
    run_condition_count: int | None = None


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

    def __init__(self, nodes: tuple[TaskNode, ...], edges: tuple[TaskEdge, ...]) -> None:
        """Index `edges` by their owning task_id for fast dependency lookups."""
        self._nodes = nodes
        self._task_ids = tuple(node.task_id for node in nodes)
        self._nodes_by_id = {node.task_id: node for node in nodes}
        self._edges = edges
        # dependents[t] = edges whose task_id == t (what t depends on)
        self._dependencies_of: dict[int, list[TaskEdge]] = {t: [] for t in self._task_ids}
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

    def required_edge_count(self, task_id: int) -> int:
        """How many of `task_id`'s edges must be satisfied, per its RUN_CONDITION."""
        # [ADDITION, 2026-09-20, E2-41] Before this, the answer was always
        # "all of them" — the literal `all(...)` this replaced. NULL/ALL keeps
        # exactly that behaviour, so every pre-E2-41 CFG_TASKS row resolves
        # identically. build_graph has already rejected an unknown mode, a
        # missing count for 'N', and a count larger than the task's own edge
        # count, so nothing here has to defend against those again.
        edges = self._dependencies_of[task_id]
        node = self._nodes_by_id[task_id]
        condition = node.run_condition or "ALL"
        if condition == "ANY":
            return 1 if edges else 0
        if condition == "N":
            # Validated non-None by build_graph; assert-free narrowing for mypy.
            return node.run_condition_count or 1
        return len(edges)

    def ready(self, run_state: dict[int, TaskRunState]) -> list[int]:
        """Return the tasks that can run right now, given each task's current run state."""
        # A task is ready when it is itself retry-eligible and enough of its
        # dependency edges are satisfied. Per CLAUDE.md "Idempotent by
        # construction, retry resumes": a retry skips what's already SUCCESS
        # or SKIPPED and only re-attempts what actually failed or never ran.
        # So a task's own status blocks it from `ready()` only when it's
        # SUCCESS, SKIPPED, or IN-PROGRESS (already running — not to be
        # dispatched a second time concurrently); FAILED and "never logged"
        # (status=None) both remain eligible.
        #
        # "Enough" is `required_edge_count` — every edge under the default
        # ALL, one under ANY, RUN_CONDITION_COUNT under N.
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
            if self._satisfied_edge_count(task_id, run_state) >= self.required_edge_count(task_id):
                result.append(task_id)
        return sorted(result)

    def unsatisfiable(self, run_state: dict[int, TaskRunState]) -> list[int]:
        """Return the never-run tasks that can never become ready under this run.

        [ADDITION, 2026-09-20, E2-01] The counterpart to `ready()`. Without
        it, a task gated only on something that will never happen — the
        `EMAIL_ALERT`-on-a-`FAILURE`-edge pattern CLAUDE.md's Handlers section
        recommends, when the watched task succeeds — simply never gets an
        AUD_TASK_RUN_LOG row, and orchestrator.py counts a task with no row as
        unsettled, so every successful pipeline using that pattern reported
        FAILED. Callers record these SKIPPED, which SETTLED_STATUSES accepts.
        """
        # An edge is *permanently* unsatisfiable only when its upstream can
        # never change again (SETTLED_STATUSES — deliberately not FAILED,
        # which a later `run` invocation may still retry into SUCCESS, and
        # deliberately not IN-PROGRESS, which is about to transition).
        #
        # Computed to a fixpoint rather than in one pass, because recording a
        # task SKIPPED settles it, which cascades: a downstream ALWAYS edge
        # then becomes satisfiable, while a downstream SUCCESS edge becomes
        # permanently unsatisfiable in turn.
        #
        # Only tasks with no log row at all (status None) are reported. A task
        # that already has a status either ran or was already settled — in
        # particular a FAILED task must stay FAILED and make the pipeline
        # report FAILED, not be quietly converted into a skip.
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
                known[task_id] = TaskRunState(status="SKIPPED")

    def _is_unsatisfiable(self, task_id: int, run_state: dict[int, TaskRunState]) -> bool:
        edges = self._dependencies_of[task_id]
        if not edges:
            return False
        still_possible = sum(
            1
            for edge in edges
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

    def _satisfied_edge_count(self, task_id: int, run_state: dict[int, TaskRunState]) -> int:
        return sum(
            1
            for edge in self._dependencies_of[task_id]
            if self._edge_satisfied(edge, run_state.get(edge.depends_on_task_id, TaskRunState()))
        )

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

    edge_count: dict[int, int] = dict.fromkeys(task_ids, 0)
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
        edge_count[edge.task_id] += 1

    # [ADDITION, 2026-09-20, E2-41] CFG_TASKS' own CHECK constraints already
    # reject a bad mode or a missing/negative count at insert time; these
    # repeat that so the resolver stays safe to use standalone and in tests
    # without a live Engine DB, exactly like the self-dependency check above.
    # The last one is genuinely beyond what any CHECK can express, since it
    # compares a CFG_TASKS column against a count of CFG_TASK_DEPENDENCY rows
    # — `validate` surfaces it via validate_graphs, which is where a config
    # error spanning two tables belongs.
    for task in tasks:
        if task.run_condition is None:
            if task.run_condition_count is not None:
                raise ResolverError(
                    f"task_id={task.task_id} sets run_condition_count with no run_condition"
                )
            continue
        if task.run_condition not in RUN_CONDITIONS:
            raise ResolverError(
                f"task_id={task.task_id} has unknown run_condition: {task.run_condition!r}"
            )
        if task.run_condition != "N":
            if task.run_condition_count is not None:
                raise ResolverError(
                    f"task_id={task.task_id} sets run_condition_count with "
                    f"run_condition={task.run_condition!r}, which ignores it"
                )
            continue
        if task.run_condition_count is None or task.run_condition_count < 1:
            raise ResolverError(
                f"task_id={task.task_id} has run_condition='N' but "
                f"run_condition_count={task.run_condition_count!r}"
            )
        if task.run_condition_count > edge_count[task.task_id]:
            raise ResolverError(
                f"task_id={task.task_id} requires {task.run_condition_count} satisfied "
                f"dependencies but only has {edge_count[task.task_id]} — it could never run"
            )

    graph = DependencyGraph(tuple(tasks), tuple(edges))
    _check_acyclic(graph)
    return graph


def _check_acyclic(graph: DependencyGraph) -> None:
    """Raise CycleError with the offending cycle if one exists in `graph`."""
    # [DEVIATION, 2026-09-20, E2-37] An explicit stack, not recursion. The
    # recursive version raised RecursionError — not ResolverError — on a
    # dependency chain deeper than Python's own limit, turning a config
    # problem into what reads like an engine bug. Same colouring, same
    # traversal order, same cycle tuple; only the bookkeeping moved onto the
    # heap.
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[int, int] = dict.fromkeys(graph.task_ids, WHITE)

    def upstreams_of(task_id: int) -> Iterator[int]:
        return iter([edge.depends_on_task_id for edge in graph.dependencies_of(task_id)])

    for root in graph.task_ids:
        if color[root] != WHITE:
            continue
        path: list[int] = [root]
        color[root] = GRAY
        stack: list[tuple[int, Iterator[int]]] = [(root, upstreams_of(root))]
        while stack:
            task_id, pending = stack[-1]
            descended = False
            for upstream in pending:
                if color[upstream] == GRAY:
                    cycle_start = path.index(upstream)
                    raise CycleError(tuple(path[cycle_start:] + [upstream]))
                if color[upstream] == WHITE:
                    color[upstream] = GRAY
                    path.append(upstream)
                    # `pending` keeps its position, so popping back to this
                    # frame resumes where this loop left off.
                    stack.append((upstream, upstreams_of(upstream)))
                    descended = True
                    break
            if not descended:
                stack.pop()
                path.pop()
                color[task_id] = BLACK
