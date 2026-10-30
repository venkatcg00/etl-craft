"""Pipeline Insights: operational metrics over a bounded window, from Engine DB aggregates.

The live Insights page, its API and CLI read, and the generated catalog's snapshot all call
:func:`measure`, so the numbers agree wherever they are shown. Counts a handler did not report
stay ``None`` ("not measured"), never 0, and rates are computed from summed counts, never by
averaging percentages.
"""

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from typing import ClassVar

from sqlalchemy.engine import Connection

from etl_craft.core.enums import TERMINAL_STATUSES, GapAttribute, GapState, RunStatus, SlaStatus
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement

DEFAULT_DAYS = 7
LATEST_RUNS = 10
OBSERVATIONS_PER_RULE = 10


@dataclass(frozen=True)
class PipelineInsight:
    """Run, duration, SLA, attempt and throughput figures for one pipeline, or all selected."""

    pipeline_code: str | None
    runs: int = 0
    succeeded: int = 0
    failed: int = 0
    finished: int = 0
    in_progress: int = 0
    success_rate: float | None = None
    average_seconds: float | None = None
    longest_seconds: float | None = None
    sla_met: int = 0
    sla_breached: int = 0
    attempts: int = 0
    retries: int = 0
    failed_attempts: int = 0
    lost_attempts: int = 0
    average_wait_seconds: float | None = None
    average_run_seconds: float | None = None
    rows_written: int | None = None
    rows_per_second: float | None = None


@dataclass(frozen=True)
class DayInsight:
    """Runs started on one UTC day and the average duration of those that ended."""

    day: date
    runs: int
    succeeded: int
    failed: int
    average_seconds: float | None


@dataclass(frozen=True)
class TaskInsight:
    """Handler-reported counts of one task's successful runs; ``None`` when never reported."""

    pipeline_code: str
    task_code: str
    runs: int
    source_rows: int | None
    target_rows: int | None
    inserted: int | None
    updated: int | None
    deleted: int | None
    rejected: int | None


@dataclass(frozen=True)
class FailureInsight:
    """Failed or lost attempts of one task, by failure class."""

    pipeline_code: str
    task_code: str
    status: str
    failure_class: str | None
    attempts: int
    latest_attempt_id: int


@dataclass(frozen=True)
class Observation:
    """One value a table rule observed, with whether it passed."""

    pipeline_run_id: int
    observed_at: datetime | None
    passed: str | None
    observed: str


@dataclass(frozen=True)
class RuleInsight:
    """A business rule's runs, failures and flagged rows, with its newest observed values."""

    pipeline_code: str
    rule_name: str
    runs: int
    failed: int
    passed: int
    not_passed: int
    flagged_rows: int
    observations: tuple[Observation, ...] = ()


@dataclass(frozen=True)
class LatestRun:
    """One of the newest runs in the window, for drill-down."""

    pipeline_code: str
    pipeline_run_id: int
    status: str
    sla_status: str | None
    start_date: datetime
    end_date: datetime | None
    trigger_kind: str


@dataclass(frozen=True)
class GapCount:
    """How many schema or lineage checks of one attribute ended in one state."""

    attribute: GapAttribute
    state: GapState
    checks: int


def gap_counts(checks: Iterable[tuple[GapAttribute, GapState]]) -> tuple[GapCount, ...]:
    """Count checks by attribute and state; unchecked and unavailable stay separate states."""
    counted: dict[tuple[GapAttribute, GapState], int] = defaultdict(int)
    for check in checks:
        counted[check] += 1
    return tuple(GapCount(a, s, n) for (a, s), n in sorted(counted.items()))


@dataclass(frozen=True)
class Insights:
    """Every built-in panel for the selected pipelines over ``[window_start, window_end)``."""

    SCHEMA: ClassVar[str] = "etl-craft/insights/1"
    window_start: datetime
    window_end: datetime
    pipelines: tuple[str, ...]
    total: PipelineInsight
    per_pipeline: tuple[PipelineInsight, ...]
    days: tuple[DayInsight, ...]
    tasks: tuple[TaskInsight, ...]
    failures: tuple[FailureInsight, ...]
    rules: tuple[RuleInsight, ...]
    latest: tuple[LatestRun, ...]
    gaps: tuple[GapCount, ...] = ()


@dataclass
class _Runs:
    runs: int = 0
    succeeded: int = 0
    failed: int = 0
    finished: int = 0
    in_progress: int = 0
    ended: int = 0
    seconds: float = 0.0
    longest: float | None = None
    sla_met: int = 0
    sla_breached: int = 0

    def add(self, row: Mapping[str, object]) -> None:
        count = int(str(row["runs"]))
        status = RunStatus(str(row["status"]))
        self.runs += count
        self.succeeded += count if status == RunStatus.SUCCESS else 0
        self.failed += count if status == RunStatus.FAILED else 0
        self.finished += count if status in TERMINAL_STATUSES else 0
        self.in_progress += count if status == RunStatus.IN_PROGRESS else 0
        if status in TERMINAL_STATUSES and row["seconds"] is not None:
            self.ended += int(str(row["ended"]))
            self.seconds += float(str(row["seconds"]))
            longest = float(str(row["longest"]))
            self.longest = longest if self.longest is None else max(self.longest, longest)
        self.sla_met += count if row["sla_status"] == SlaStatus.MET else 0
        self.sla_breached += count if row["sla_status"] == SlaStatus.BREACHED else 0


@dataclass
class _Attempts:
    values: dict[str, float] = field(default_factory=lambda: defaultdict(float))

    def add(self, row: Mapping[str, object]) -> None:
        for key in ("attempts", "retries", "failed", "lost", "started", "ended", "measured"):
            self.values[key] += float(str(row[key] or 0))
        for key in ("wait_seconds", "run_seconds", "rows_written", "measured_seconds"):
            self.values[key] += float(str(row[key] or 0))


def window(first: date | None = None, last: date | None = None) -> tuple[datetime, datetime]:
    """Return UTC midnight of ``first`` to the midnight after ``last`` (default the last 7 days)."""
    last = last or datetime.now(UTC).date()
    first = first or last - timedelta(days=DEFAULT_DAYS - 1)
    return datetime.combine(first, time(), UTC), datetime.combine(
        last + timedelta(days=1), time(), UTC
    )


def measure(
    conn: Connection, pipelines: Mapping[int, str], window_start: datetime, window_end: datetime
) -> Insights:
    """Read every panel for ``pipelines`` (id to code) over the half-open window."""
    params = {"window_start": window_start, "window_end": window_end}
    runs: dict[int | None, _Runs] = defaultdict(_Runs)
    days: dict[date, _Runs] = defaultdict(_Runs)
    for row in _rows(conn, "insights_runs", params, pipelines):
        for key in (_id(row), None):
            runs[key].add(row)
        days[_day(row["day"])].add(row)
    attempts: dict[int | None, _Attempts] = defaultdict(_Attempts)
    for row in _rows(conn, "insights_attempts", params, pipelines):
        for key in (_id(row), None):
            attempts[key].add(row)
    per_pipeline = tuple(
        _pipeline(code, runs[pid], attempts[pid])
        for pid, code in sorted(pipelines.items(), key=lambda item: item[1])
    )
    return Insights(
        as_utc(window_start),
        as_utc(window_end),
        tuple(sorted(pipelines.values())),
        _pipeline(None, runs[None], attempts[None]),
        per_pipeline,
        tuple(_day_insight(day, values) for day, values in sorted(days.items())),
        _tasks(conn, params, pipelines),
        _failures(conn, params, pipelines),
        _rules(conn, params, pipelines),
        _latest(conn, params, pipelines),
    )


def _rows(
    conn: Connection, name: str, params: Mapping[str, object], pipelines: Mapping[int, str]
) -> Iterable[Mapping[str, object]]:
    for row in conn.execute(statement(conn, name), dict(params)):
        if row.pipeline_id in pipelines:
            yield dict(row._mapping)


def _id(row: Mapping[str, object]) -> int:
    return int(str(row["pipeline_id"]))


def _count(value: object) -> int:
    return int(str(value or 0))


def _day(value: object) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def _ratio(numerator: float, denominator: float) -> float | None:
    return None if denominator == 0 else numerator / denominator


def _pipeline(code: str | None, runs: _Runs, attempts: _Attempts) -> PipelineInsight:
    values = attempts.values
    measured = int(values["measured"])
    return PipelineInsight(
        code,
        runs.runs,
        runs.succeeded,
        runs.failed,
        runs.finished,
        runs.in_progress,
        _ratio(runs.succeeded, runs.finished),
        _ratio(runs.seconds, runs.ended),
        runs.longest,
        runs.sla_met,
        runs.sla_breached,
        int(values["attempts"]),
        int(values["retries"]),
        int(values["failed"]),
        int(values["lost"]),
        _ratio(values["wait_seconds"], values["started"]),
        _ratio(values["run_seconds"], values["ended"]),
        int(values["rows_written"]) if measured else None,
        _ratio(values["rows_written"], values["measured_seconds"]) if measured else None,
    )


def _day_insight(day: date, runs: _Runs) -> DayInsight:
    return DayInsight(day, runs.runs, runs.succeeded, runs.failed, _ratio(runs.seconds, runs.ended))


def _measured(row: Mapping[str, object], total: str, reported: str) -> int | None:
    return _count(row[total]) if _count(row[reported]) else None


def _tasks(
    conn: Connection, params: Mapping[str, object], pipelines: Mapping[int, str]
) -> tuple[TaskInsight, ...]:
    return tuple(
        sorted(
            (
                TaskInsight(
                    pipelines[_id(row)],
                    str(row["task_code"]),
                    _count(row["runs"]),
                    _measured(row, "source_rows", "source_measured"),
                    _measured(row, "target_rows", "target_measured"),
                    _measured(row, "inserted", "insert_measured"),
                    _measured(row, "updated", "update_measured"),
                    _measured(row, "deleted", "delete_measured"),
                    _measured(row, "rejected", "reject_measured"),
                )
                for row in _rows(conn, "insights_tasks", params, pipelines)
            ),
            key=lambda task: (task.pipeline_code, task.task_code),
        )
    )


def _failures(
    conn: Connection, params: Mapping[str, object], pipelines: Mapping[int, str]
) -> tuple[FailureInsight, ...]:
    return tuple(
        sorted(
            (
                FailureInsight(
                    pipelines[_id(row)],
                    str(row["task_code"]),
                    str(row["status"]),
                    None if row["failure_class"] is None else str(row["failure_class"]),
                    _count(row["attempts"]),
                    _count(row["latest_attempt"]),
                )
                for row in _rows(conn, "insights_failures", params, pipelines)
            ),
            key=lambda failure: (-failure.attempts, failure.pipeline_code, failure.task_code),
        )
    )


def _rules(
    conn: Connection, params: Mapping[str, object], pipelines: Mapping[int, str]
) -> tuple[RuleInsight, ...]:
    observed: dict[int, list[Observation]] = defaultdict(list)
    rows = [
        row
        for row in _rows(conn, "insights_observations", params, pipelines)
        if row["observed"] is not None
    ]
    for row in sorted(rows, key=lambda row: _count(row["rule_run_id"]), reverse=True):
        values = observed[_count(row["rule_id"])]
        if len(values) < OBSERVATIONS_PER_RULE:
            values.append(
                Observation(
                    _count(row["pipeline_run_id"]),
                    None if row["observed_at"] is None else as_utc(row["observed_at"]),
                    None if row["passed"] is None else str(row["passed"]),
                    str(row["observed"]),
                )
            )
    return tuple(
        sorted(
            (
                RuleInsight(
                    pipelines[_id(row)],
                    str(row["rule_name"]),
                    _count(row["runs"]),
                    _count(row["failed"]),
                    _count(row["passed"]),
                    _count(row["not_passed"]),
                    _count(row["flagged_rows"]),
                    tuple(observed[_count(row["rule_id"])]),
                )
                for row in _rows(conn, "insights_rules", params, pipelines)
            ),
            key=lambda rule: (rule.pipeline_code, rule.rule_name),
        )
    )


def _latest(
    conn: Connection, params: Mapping[str, object], pipelines: Mapping[int, str]
) -> tuple[LatestRun, ...]:
    runs = [
        LatestRun(
            pipelines[_id(row)],
            _count(row["pipeline_run_id"]),
            str(row["status"]),
            None if row["sla_status"] is None else str(row["sla_status"]),
            as_utc(row["start_date"]),
            None if row["end_date"] is None else as_utc(row["end_date"]),
            str(row["trigger_kind"]),
        )
        for row in _rows(conn, "insights_latest", {**params, "limit": LATEST_RUNS}, pipelines)
    ]
    runs.sort(key=lambda run: (run.start_date, run.pipeline_run_id), reverse=True)
    return tuple(runs[:LATEST_RUNS])
