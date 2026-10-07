"""Cross-pipeline dependencies: waiting for the upstream, judging its last run, and trackers.

A pipeline's dependencies on other pipelines (``CFG_PIPELINE_DEPENDENCY``) are checked before a
new run of it starts; a task's dependencies on tasks in other pipelines (``CFG_TASK_DEPENDENCY``
rows naming another pipeline) are checked before the task runs. Each check:

1. Waits while the upstream's latest run is ``IN-PROGRESS``. The first look is at 70% of the
   upstream's average run length, then 80%, 90% and so on; one check waits at most
   ``Orchestration.Gate_wait_minutes`` (an hour unless set) and looks at most 30 times, across all
   of its dependencies.
2. Judges the upstream's latest finished run. The dependency is satisfied only when that run
   has a newer run id or an allowed newer published revision, and its status satisfies the
   dependency type. An
   older run that would have satisfied it does not count: the last run decides.
3. Once the downstream succeeds, logs that run as consumed in ``AUD_DEPENDENCY_CONSUMPTION``,
   one row per downstream run (or task), dependency, upstream run and revision;
   the dependency's latest row
   is what it last consumed. A downstream that fails or is skipped consumes nothing, so its
   retry sees the same upstream run.

Runs that are part of a backfill are invisible to all of this: they never satisfy a dependency,
never fail one, and are never waited for.

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
from etl_craft.engine.queries import statement
from etl_craft.engine.repository import trackers
from etl_craft.engine.repository.dependencies import (
    fetch_cross_pipeline_task_edges,
    fetch_pipeline_dependency_edges,
)
from etl_craft.execution.leases import as_utc

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
    dependency_type: str,
    run: trackers.FinishedRun | None,
    last_consumed: trackers.ConsumedRun | int | None,
    *,
    consume_repairs: bool = True,
) -> tuple[int | None, str]:
    """Return the upstream run that satisfies a dependency, or ``None`` and why none does.

    Only the latest finished run is judged. A higher run id is new; the same id is new only
    with a higher revision and ``consume_repairs``. Its status must satisfy ``dependency_type``.
    """
    if run is None:
        return None, "has no finished run"
    last = (
        trackers.ConsumedRun(last_consumed, 1) if isinstance(last_consumed, int) else last_consumed
    )
    if (
        last is not None
        and run.run_id == last.run_id
        and run.revision > last.revision
        and not consume_repairs
    ):
        return None, (
            f"run {run.run_id} revision {run.revision} is newer than consumed revision "
            f"{last.revision}, but CONSUME_REPAIRS='N'; enable CONSUME_REPAIRS "
            "to accept repairs"
        )
    if last is not None and (
        run.run_id < last.run_id
        or (run.run_id == last.run_id and (not consume_repairs or run.revision <= last.revision))
    ):
        return None, f"has no run since run {last.run_id}, which was already consumed"
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
    decisions: tuple[trackers.GateDecision, ...] = ()
    next_check_at: datetime | None = None


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

    def check(
        self,
        engine: Engine,
        task_id: int,
        needed: int,
        *,
        budget: WaitBudget | None = None,
        waiter: Callable[..., datetime | None] | None = None,
    ) -> CrossPipelineCheck:
        """Check the task's cross-pipeline dependencies until ``needed`` are satisfied.

        Dependencies after that are neither checked nor waited for. Under ``warn`` the ones not
        satisfied are bypassed; under ``off`` none is checked and all are bypassed.
        """
        with engine.connect() as conn:
            edges = fetch_cross_pipeline_task_edges(conn, task_id, include_repairs=True)
        if self.policy == GatePolicy.OFF:
            return CrossPipelineCheck(
                needed,
                decisions=tuple(
                    trackers.GateDecision(
                        edge.task_dependency_id, True, None, "BYPASSED", "dependency gates are off"
                    )
                    for edge in edges
                ),
                bypassed=tuple(
                    f"upstream task {edge.depends_on_label} ({edge.dependency_type}) not checked"
                    for edge in edges
                ),
            )
        budget = budget or WaitBudget.start(self.clock, self.wait_seconds)
        satisfied = 0
        reasons: list[str] = []
        consumed: dict[int, int] = {}
        decisions: list[trackers.GateDecision] = []
        for edge in edges:
            if satisfied >= needed:
                break
            label = f"upstream task {edge.depends_on_label}"
            next_check = (waiter or _wait_while_running)(
                partial(_read, engine, trackers.fetch_latest_task_run, edge.depends_on_task_id),
                partial(
                    _read, engine, trackers.fetch_average_task_seconds, edge.depends_on_task_id
                ),
                label,
                budget,
                self.clock,
            )
            if next_check is not None:
                return CrossPipelineCheck(0, definitive=False, next_check_at=next_check)
            with engine.connect() as conn:
                run = trackers.fetch_latest_finished_task_run(conn, edge.depends_on_task_id)
                last = trackers.fetch_task_last_consumed(conn, edge.task_dependency_id)
            run_id, why = judge(
                edge.dependency_type, run, last, consume_repairs=edge.consume_repairs
            )
            decisions.append(
                trackers.GateDecision(
                    edge.task_dependency_id,
                    True,
                    run,
                    "SATISFIED"
                    if run_id is not None
                    else "BYPASSED"
                    if self.policy == GatePolicy.WARN
                    else "UNSATISFIED",
                    why,
                )
            )
            if run_id is None:
                reasons.append(f"{label} ({edge.dependency_type}) {why}")
                continue
            logger.info("%s (%s) is satisfied: %s", label, edge.dependency_type, why)
            satisfied += 1
            consumed[edge.task_dependency_id] = run_id
        if self.policy == GatePolicy.WARN and satisfied < needed:
            return CrossPipelineCheck(
                needed, consumed=consumed, bypassed=tuple(reasons), decisions=tuple(decisions)
            )
        return CrossPipelineCheck(satisfied, tuple(reasons), consumed, decisions=tuple(decisions))

    def consume(
        self, engine: Engine, task_id: int, pipeline_run_id: int, consumed: dict[int, int]
    ) -> None:
        """Record the upstream task runs a task that succeeded consumed."""
        with engine.begin() as conn:
            trackers.consume_task_decisions(conn, task_id, pipeline_run_id)


def _read(engine: Engine, fetch: Callable[..., T], *args: object) -> T:
    with engine.connect() as conn:
        return fetch(conn, *args)


@dataclass(frozen=True)
class PipelineGateResult:
    """A pipeline's dependencies on other pipelines: the reasons any are unsatisfied."""

    reasons: tuple[str, ...] = ()
    consumed: dict[int, int] = field(default_factory=dict)
    bypassed: tuple[str, ...] = ()
    decisions: tuple[trackers.GateDecision, ...] = ()
    next_check_at: datetime | None = None

    @property
    def satisfied(self) -> bool:
        """Whether every dependency is satisfied."""
        return not self.reasons and self.next_check_at is None


def check_pipeline_dependencies(
    engine: Engine,
    pipeline_id: int,
    clock: Clock | None = None,
    policy: GatePolicy = GatePolicy.ENFORCE,
    wait_seconds: float = WAIT_LIMIT_SECONDS,
    *,
    budget: WaitBudget | None = None,
    waiter: Callable[..., datetime | None] | None = None,
) -> PipelineGateResult:
    """Check every dependency of ``pipeline_id`` on other pipelines; all must be satisfied.

    The check stops at the first unsatisfied dependency, without waiting on the rest. Under
    ``warn`` it checks them all and bypasses the ones not satisfied; under ``off`` it checks
    none and bypasses them all.
    """
    clock = clock or Clock()
    with engine.connect() as conn:
        edges = fetch_pipeline_dependency_edges(conn, pipeline_id, include_repairs=True)
    if policy == GatePolicy.OFF:
        return PipelineGateResult(
            decisions=tuple(
                trackers.GateDecision(
                    edge.pipeline_dependency_id, False, None, "BYPASSED", "dependency gates are off"
                )
                for edge in edges
            ),
            bypassed=tuple(
                f"upstream pipeline {edge.depends_on_pipeline_code} ({edge.dependency_type}) "
                "not checked"
                for edge in edges
            ),
        )
    budget = budget or WaitBudget.start(clock, wait_seconds)
    consumed: dict[int, int] = {}
    bypassed: list[str] = []
    decisions: list[trackers.GateDecision] = []
    for edge in edges:
        label = f"upstream pipeline {edge.depends_on_pipeline_code}"
        next_check = (waiter or _wait_while_running)(
            partial(_read, engine, trackers.fetch_latest_pipeline_run, edge.depends_on_pipeline_id),
            partial(
                _read, engine, trackers.fetch_average_pipeline_seconds, edge.depends_on_pipeline_id
            ),
            label,
            budget,
            clock,
        )
        if next_check is not None:
            return PipelineGateResult(next_check_at=next_check)
        with engine.connect() as conn:
            run = trackers.fetch_latest_finished_pipeline_run(
                conn, edge.depends_on_pipeline_id, clock.now()
            )
            last = trackers.fetch_pipeline_last_consumed(conn, edge.pipeline_dependency_id)
        run_id, why = judge(edge.dependency_type, run, last, consume_repairs=edge.consume_repairs)
        decisions.append(
            trackers.GateDecision(
                edge.pipeline_dependency_id,
                False,
                run,
                "SATISFIED"
                if run_id is not None
                else "BYPASSED"
                if policy == GatePolicy.WARN
                else "UNSATISFIED",
                why,
            )
        )
        if run_id is None:
            reason = f"{label} ({edge.dependency_type}) {why}"
            if policy == GatePolicy.WARN:
                bypassed.append(reason)
                continue
            return PipelineGateResult((reason,), decisions=tuple(decisions))
        logger.info("%s (%s) is satisfied: %s", label, edge.dependency_type, why)
        consumed[edge.pipeline_dependency_id] = run_id
    return PipelineGateResult(
        consumed=consumed, bypassed=tuple(bypassed), decisions=tuple(decisions)
    )


def consume_pipeline_dependencies(engine: Engine, pipeline_id: int, pipeline_run_id: int) -> None:
    """Consume a successful run's recorded satisfied decisions; never judge its history again."""
    with engine.begin() as conn:
        trackers.consume_pipeline_decisions(conn, pipeline_run_id)


@dataclass(frozen=True)
class GateResult:
    """One nonblocking check: final judgement or the next persisted look."""

    state: str
    check: CrossPipelineCheck | PipelineGateResult
    next_check_at: datetime | None = None


def check_gate(
    engine: Engine,
    pipeline_run_id: int,
    pipeline_id: int,
    *,
    task_id: int | None = None,
    needed: int = 0,
    clock: Clock | None = None,
    policy: GatePolicy = GatePolicy.ENFORCE,
    wait_seconds: float = WAIT_LIMIT_SECONDS,
) -> GateResult:
    """Check without sleeping or holding a worker; resume the run's durable wait budget."""
    clock = clock or Clock()
    now = clock.now()
    params = {"pipeline_run_id": pipeline_run_id, "task_id": task_id}
    with engine.connect() as conn:
        row = conn.execute(statement(conn, "gate_wait"), params).one_or_none()
    waiting = row is not None and row.next_check_at is not None
    first = as_utc(row.first_check_at) if row is not None and waiting else now
    deadline = (
        as_utc(row.wait_until)
        if row is not None and waiting
        else now + timedelta(seconds=wait_seconds)
    )
    looks = row.looks if row is not None and waiting else 0
    due = row is not None and waiting and now >= as_utc(row.next_check_at)
    if due:
        looks += 1
    budget = WaitBudget(deadline, MAX_LOOKS - looks)

    def poll(
        latest_fetch: Callable[[], trackers.LatestRun | None],
        average_fetch: Callable[[], float | None],
        label: str,
        budget: WaitBudget,
        clock: Clock,
    ) -> datetime | None:
        latest = latest_fetch()
        if latest is None or latest.status != RunStatus.IN_PROGRESS or budget.exhausted(clock):
            return None
        if row is not None and waiting and not due:
            return as_utc(row.next_check_at)
        delay = next_look_delay(
            average_fetch() or DEFAULT_RUN_SECONDS,
            (now - as_utc(latest.start_date)).total_seconds(),
            FIRST_LOOK_FRACTION + looks * LOOK_FRACTION_STEP,
            (deadline - now).total_seconds(),
        )
        logger.info("%s: next gate look in %.1fs", label, delay)
        return now + timedelta(seconds=delay)

    check = (
        TrackedGate(clock, policy, wait_seconds).check(
            engine, task_id, needed, budget=budget, waiter=poll
        )
        if task_id is not None
        else check_pipeline_dependencies(
            engine, pipeline_id, clock, policy, wait_seconds, budget=budget, waiter=poll
        )
    )
    with engine.begin() as conn:
        if check.next_check_at is not None and (
            row is None
            or row.next_check_at is None
            or looks != row.looks
            or check.next_check_at != as_utc(row.next_check_at)
        ):
            conn.execute(
                statement(conn, "save_gate_wait"),
                {
                    **params,
                    "first_check_at": first,
                    "wait_until": deadline,
                    "looks": looks,
                    "next_check_at": check.next_check_at,
                },
            )
        elif waiting and check.next_check_at is None:
            conn.execute(
                statement(conn, "finish_gate_wait"), {**params, "looks": min(looks, MAX_LOOKS)}
            )
    satisfied = (
        check.satisfied_count >= needed
        if isinstance(check, CrossPipelineCheck)
        else check.satisfied
    )
    return GateResult(
        "WAIT" if check.next_check_at is not None else "SATISFIED" if satisfied else "UNSATISFIED",
        check,
        check.next_check_at,
    )
