"""Cross-pipeline dependencies: waiting for the upstream, judging its last run, and trackers.

A pipeline's dependencies on other pipelines (``CFG_PIPELINE_DEPENDENCY``) are checked before a
new run of it starts; a task's dependencies on tasks in other pipelines (``CFG_TASK_DEPENDENCY``
rows naming another pipeline) are checked before the task runs. Each check:

1. Waits while the upstream's latest run is ``IN-PROGRESS``. The first look is at 70% of the
   upstream's average run length, then 80%, 90% and so on; one check waits at most
   ``Orchestration.Gate_wait_minutes`` (an hour unless set) and looks at most 30 times, across all
   of its dependencies.
2. Judges the upstream's latest finished run. The dependency is satisfied only when that run
   is newer than the one it last consumed and its status satisfies the dependency type. An
   older run that would have satisfied it does not count: the last run decides.
3. Once the downstream succeeds, logs that run as consumed in ``AUD_DEPENDENCY_CONSUMPTION``,
   one row per downstream run (or task), dependency and upstream run; the dependency's latest row
   is what it last consumed. A downstream that fails or is skipped consumes nothing, so its
   retry sees the same upstream run.

``Orchestration.Dependency_gates`` relaxes this in local mode: with ``warn`` a dependency that is
not satisfied is reported as bypassed and the run or task goes ahead; with ``off`` nothing is
checked or waited for, and every dependency is reported as bypassed. Only upstream runs that
satisfied their dependency are consumed.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Protocol, TypeVar

from sqlalchemy.engine import Engine

from etl_craft.core.enums import TERMINAL_STATUSES, DependencyType, GatePolicy, RunStatus
from etl_craft.core.errors import GraphError
from etl_craft.engine.repository import trackers
from etl_craft.engine.repository.dependencies import (
    PipelineDependencyEdge,
    fetch_cross_pipeline_task_edges,
    fetch_pipeline_dependency_edges,
)
from etl_craft.engine.runlog import fetch_run_sla

logger = logging.getLogger(__name__)

T = TypeVar("T")

FIRST_LOOK_FRACTION = 0.70
"""The first look at a running upstream is at this fraction of its average run length."""

LOOK_FRACTION_STEP = 0.10
"""Each later look is this much more of the average run length."""

MAX_LOOKS = 30
"""How many times one check looks at running upstreams, across all its dependencies."""

WAIT_LIMIT_SECONDS = 3600.0
"""How long one check waits for running upstreams, across all its dependencies, unless
``Orchestration.Gate_wait_minutes`` says otherwise."""

MIN_LOOK_INTERVAL_SECONDS = 1.0
"""The shortest pause between two looks, for an upstream already past its expected length."""

DEFAULT_RUN_SECONDS = 300.0
"""The run length assumed for an upstream with no finished run to average."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class Clock:
    """How a check sleeps and reads the time; tests replace both."""

    sleep: Callable[[float], None] = time.sleep
    now: Callable[[], datetime] = _utc_now


@dataclass
class WaitBudget:
    """What is left of one check's waiting: a deadline and a number of looks."""

    deadline: datetime
    looks_left: int = MAX_LOOKS

    @classmethod
    def start(cls, clock: Clock, seconds: float = WAIT_LIMIT_SECONDS) -> WaitBudget:
        """Open a full budget from now, of ``seconds``."""
        return cls(deadline=clock.now() + timedelta(seconds=seconds))

    def exhausted(self, clock: Clock) -> bool:
        """Whether the looks or the time have run out."""
        return self.looks_left <= 0 or clock.now() >= self.deadline


def next_look_delay(
    average_seconds: float, elapsed_seconds: float, fraction: float, remaining_seconds: float
) -> float:
    """Return how long to wait before looking at a running upstream again.

    The look is due when the upstream has run ``fraction`` of its average length; never sooner
    than ``MIN_LOOK_INTERVAL_SECONDS`` from now, and never past the check's deadline.
    """
    delay = max(MIN_LOOK_INTERVAL_SECONDS, average_seconds * fraction - elapsed_seconds)
    return max(0.0, min(delay, remaining_seconds))


def satisfies(dependency_type: str, run: trackers.FinishedRun) -> bool:
    """Whether a finished upstream run satisfies a dependency of ``dependency_type``."""
    if dependency_type == DependencyType.SUCCESS:
        return run.status == RunStatus.SUCCESS
    if dependency_type == DependencyType.FAILURE:
        return run.status == RunStatus.FAILED
    if dependency_type == DependencyType.ALWAYS:
        return run.status in TERMINAL_STATUSES
    if dependency_type == DependencyType.HAS_DATA:
        return run.status == RunStatus.SUCCESS and run.has_data
    raise GraphError(f"unknown dependency_type: {dependency_type!r}")


def judge(
    dependency_type: str, run: trackers.FinishedRun | None, last_consumed: int | None
) -> tuple[int | None, str]:
    """Return the upstream run that satisfies a dependency, or ``None`` and why none does.

    Only the latest finished run is judged. It must be newer than ``last_consumed``, the run the
    dependency last consumed, and satisfy ``dependency_type``.
    """
    if run is None:
        return None, "has no finished run"
    if last_consumed is not None and run.run_id <= last_consumed:
        return None, f"has no run since run {last_consumed}, which was already consumed"
    if not satisfies(dependency_type, run):
        detail = " with no rows written" if run.status == RunStatus.SUCCESS else ""
        return None, (
            f"last finished run {run.run_id} ended {run.status}{detail}, which does not satisfy "
            f"a {dependency_type} dependency"
        )
    return run.run_id, f"last finished run {run.run_id} ended {run.status}"


def _wait_while_running(
    fetch_latest: Callable[[], trackers.LatestRun | None],
    fetch_average: Callable[[], float | None],
    label: str,
    budget: WaitBudget,
    clock: Clock,
) -> None:
    """Wait while the upstream's latest run is ``IN-PROGRESS``, within ``budget``.

    Each look opens its own short connection, so nothing is held while sleeping.
    """
    latest = fetch_latest()
    if latest is None or latest.status != RunStatus.IN_PROGRESS:
        return
    average = fetch_average() or DEFAULT_RUN_SECONDS
    fraction = FIRST_LOOK_FRACTION
    logger.info("%s is running (run %d); waiting for it to finish", label, latest.run_id)
    while not budget.exhausted(clock):
        now = clock.now()
        start = latest.start_date
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        delay = next_look_delay(
            average,
            (now - start).total_seconds(),
            fraction,
            (budget.deadline - now).total_seconds(),
        )
        if delay > 0:
            clock.sleep(delay)
        budget.looks_left -= 1
        fraction += LOOK_FRACTION_STEP
        latest = fetch_latest()
        if latest is None or latest.status != RunStatus.IN_PROGRESS:
            return
    logger.warning("%s is still running; stopped waiting for it", label)


@dataclass(frozen=True)
class CrossPipelineCheck:
    """What a gate found for a task's dependencies on tasks in other pipelines.

    ``consumed`` maps each satisfied dependency to the upstream task run that satisfied it, for
    the gate to record once the task succeeds. ``definitive`` is false when the gate did not
    really check, so its reasons never record the task ``SKIPPED``. ``bypassed`` holds the
    dependencies ``Dependency_gates`` let through without being satisfied.
    """

    satisfied_count: int
    reasons: tuple[str, ...] = ()
    consumed: dict[int, int] = field(default_factory=dict)
    definitive: bool = True
    bypassed: tuple[str, ...] = ()


class CrossPipelineGate(Protocol):
    """Checks a task's dependencies on tasks in other pipelines."""

    def check(self, engine: Engine, task_id: int, needed: int) -> CrossPipelineCheck:
        """Return how many of the task's cross-pipeline dependencies are satisfied now."""

    def consume(
        self, engine: Engine, task_id: int, pipeline_run_id: int, consumed: dict[int, int]
    ) -> None:
        """Record the upstream runs a task that succeeded consumed."""


class UncheckedGate:
    """A gate that satisfies no cross-pipeline dependency, for use where none is checked."""

    def check(self, engine: Engine, task_id: int, needed: int) -> CrossPipelineCheck:
        """Report every cross-pipeline dependency as unsatisfied."""
        return CrossPipelineCheck(
            0, ("its dependencies on other pipelines are not checked",), definitive=False
        )

    def consume(
        self, engine: Engine, task_id: int, pipeline_run_id: int, consumed: dict[int, int]
    ) -> None:
        """Record nothing."""
        return None


class TrackedGate:
    """The cross-pipeline gate for tasks, backed by ``AUD_DEPENDENCY_CONSUMPTION``."""

    def __init__(
        self,
        clock: Clock | None = None,
        policy: GatePolicy = GatePolicy.ENFORCE,
        wait_seconds: float = WAIT_LIMIT_SECONDS,
    ) -> None:
        """Wait and read the time with ``clock``, and treat what is not satisfied by ``policy``.

        A running upstream is waited for at most ``wait_seconds``.
        """
        self.clock = clock or Clock()
        self.policy = policy
        self.wait_seconds = wait_seconds

    def check(self, engine: Engine, task_id: int, needed: int) -> CrossPipelineCheck:
        """Check the task's cross-pipeline dependencies until ``needed`` are satisfied.

        Dependencies after that are neither checked nor waited for. Under ``warn`` the ones not
        satisfied are bypassed; under ``off`` none is checked and all are bypassed.
        """
        with engine.connect() as conn:
            edges = fetch_cross_pipeline_task_edges(conn, task_id)
        if self.policy == GatePolicy.OFF:
            return CrossPipelineCheck(
                needed,
                bypassed=tuple(
                    f"upstream task {edge.depends_on_label} ({edge.dependency_type}) not checked"
                    for edge in edges
                ),
            )
        budget = WaitBudget.start(self.clock, self.wait_seconds)
        satisfied = 0
        reasons: list[str] = []
        consumed: dict[int, int] = {}
        for edge in edges:
            if satisfied >= needed:
                break
            label = f"upstream task {edge.depends_on_label}"
            _wait_while_running(
                partial(_read, engine, trackers.fetch_latest_task_run, edge.depends_on_task_id),
                partial(
                    _read, engine, trackers.fetch_average_task_seconds, edge.depends_on_task_id
                ),
                label,
                budget,
                self.clock,
            )
            with engine.connect() as conn:
                run = trackers.fetch_latest_finished_task_run(conn, edge.depends_on_task_id)
                last = trackers.fetch_task_last_consumed(conn, edge.task_dependency_id)
            run_id, why = judge(edge.dependency_type, run, last)
            if run_id is None:
                reasons.append(f"{label} ({edge.dependency_type}) {why}")
                continue
            logger.info("%s (%s) is satisfied: %s", label, edge.dependency_type, why)
            satisfied += 1
            consumed[edge.task_dependency_id] = run_id
        if self.policy == GatePolicy.WARN and satisfied < needed:
            return CrossPipelineCheck(needed, consumed=consumed, bypassed=tuple(reasons))
        return CrossPipelineCheck(satisfied, tuple(reasons), consumed)

    def consume(
        self, engine: Engine, task_id: int, pipeline_run_id: int, consumed: dict[int, int]
    ) -> None:
        """Record the upstream task runs a task that succeeded consumed."""
        with engine.connect() as conn:
            edges = fetch_cross_pipeline_task_edges(conn, task_id)
        with engine.begin() as conn:
            for edge in edges:
                run_id = consumed.get(edge.task_dependency_id)
                if run_id is None:
                    continue
                trackers.record_task_consumed(
                    conn,
                    edge.task_dependency_id,
                    task_id,
                    edge.pipeline_id,
                    pipeline_run_id,
                    edge.depends_on_pipeline_id,
                    run_id,
                )
                logger.info("consumed run %d of upstream task %s", run_id, edge.depends_on_label)


def _read(engine: Engine, fetch: Callable[..., T], *args: object) -> T:
    with engine.connect() as conn:
        return fetch(conn, *args)


@dataclass(frozen=True)
class PipelineGateResult:
    """A pipeline's dependencies on other pipelines: the reasons any are unsatisfied."""

    reasons: tuple[str, ...] = ()
    consumed: dict[int, int] = field(default_factory=dict)
    bypassed: tuple[str, ...] = ()

    @property
    def satisfied(self) -> bool:
        """Whether every dependency is satisfied."""
        return not self.reasons


def check_pipeline_dependencies(
    engine: Engine,
    pipeline_id: int,
    clock: Clock | None = None,
    policy: GatePolicy = GatePolicy.ENFORCE,
    wait_seconds: float = WAIT_LIMIT_SECONDS,
) -> PipelineGateResult:
    """Check every dependency of ``pipeline_id`` on other pipelines; all must be satisfied.

    The check stops at the first unsatisfied dependency, without waiting on the rest. Under
    ``warn`` it checks them all and bypasses the ones not satisfied; under ``off`` it checks
    none and bypasses them all.
    """
    clock = clock or Clock()
    with engine.connect() as conn:
        edges = fetch_pipeline_dependency_edges(conn, pipeline_id)
    if policy == GatePolicy.OFF:
        return PipelineGateResult(
            bypassed=tuple(
                f"upstream pipeline {edge.depends_on_pipeline_code} ({edge.dependency_type}) "
                "not checked"
                for edge in edges
            )
        )
    budget = WaitBudget.start(clock, wait_seconds)
    consumed: dict[int, int] = {}
    bypassed: list[str] = []
    for edge in edges:
        label = f"upstream pipeline {edge.depends_on_pipeline_code}"
        _wait_while_running(
            partial(_read, engine, trackers.fetch_latest_pipeline_run, edge.depends_on_pipeline_id),
            partial(
                _read, engine, trackers.fetch_average_pipeline_seconds, edge.depends_on_pipeline_id
            ),
            label,
            budget,
            clock,
        )
        run_id, why = _judge_pipeline_edge(engine, edge, clock.now())
        if run_id is None:
            reason = f"{label} ({edge.dependency_type}) {why}"
            if policy == GatePolicy.WARN:
                bypassed.append(reason)
                continue
            return PipelineGateResult((reason,))
        logger.info("%s (%s) is satisfied: %s", label, edge.dependency_type, why)
        consumed[edge.pipeline_dependency_id] = run_id
    return PipelineGateResult(consumed=consumed, bypassed=tuple(bypassed))


def consume_pipeline_dependencies(engine: Engine, pipeline_id: int, pipeline_run_id: int) -> None:
    """Record the upstream runs a successful run of ``pipeline_id`` consumed.

    They are judged again as they stood when ``pipeline_run_id`` started, which is when its gate
    passed; an upstream run that finished later is left for the next run. This works whether the
    gate ran in this process or in an earlier ``run --init-only``.
    """
    with engine.connect() as conn:
        edges = fetch_pipeline_dependency_edges(conn, pipeline_id)
        started = fetch_run_sla(conn, pipeline_run_id).start_date
    for edge in edges:
        run_id, _ = _judge_pipeline_edge(engine, edge, started)
        if run_id is None:
            continue
        with engine.begin() as conn:
            trackers.record_pipeline_consumed(
                conn,
                edge.pipeline_dependency_id,
                pipeline_id,
                pipeline_run_id,
                edge.depends_on_pipeline_id,
                run_id,
            )
        logger.info(
            "consumed run %d of upstream pipeline %s", run_id, edge.depends_on_pipeline_code
        )


def _judge_pipeline_edge(
    engine: Engine, edge: PipelineDependencyEdge, ended_by: datetime
) -> tuple[int | None, str]:
    with engine.connect() as conn:
        run = trackers.fetch_latest_finished_pipeline_run(
            conn, edge.depends_on_pipeline_id, ended_by
        )
        last = trackers.fetch_pipeline_last_consumed(conn, edge.pipeline_dependency_id)
    return judge(edge.dependency_type, run, last)
