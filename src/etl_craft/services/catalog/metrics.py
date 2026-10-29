"""Run metric groups and shared catalog display formatting."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime

from etl_craft.core.enums import CatalogRunKind, RunStatus, SlaStatus
from etl_craft.engine.repository.catalog import LastRun, RunSummary, TaskRunSummary
from etl_craft.services.catalog.html import escape as _e
from etl_craft.services.catalog.html import render_table


def kpi_groups(
    runs: Sequence[RunSummary | TaskRunSummary],
) -> dict[CatalogRunKind, RunKpis]:
    """Group first endings by original trigger, retaining repairs as a separate count."""
    return {
        kind: run_kpis(group)
        for kind in CatalogRunKind
        if (group := [run for run in runs if run.run_kind == kind])
    }


def kpi_summary(runs: Sequence[RunSummary | TaskRunSummary]) -> str:
    """Render comparable metrics per kind within the shown logical-run history."""
    return render_table(
        [
            "Kind",
            "Runs",
            "Succeeded",
            "Failed or cancelled",
            "Repaired",
            "Average run",
            "Longest run",
            "SLA breached",
            "Average rows written",
        ],
        (
            [
                kind.value.title(),
                str(kpis.runs),
                rate(kpis),
                str(kpis.failed),
                str(kpis.repaired),
                duration(kpis.average_seconds),
                duration(kpis.longest_seconds),
                str(kpis.sla_breached)
                if any(isinstance(r, RunSummary) and r.sla_status is not None for r in runs)
                else "",
                f"{kpis.average_rows:,.0f}" if kpis.average_rows is not None else "",
            ]
            for kind, kpis in kpi_groups(runs).items()
        ),
        "",
    )


def kpi_values(runs: Sequence[RunSummary | TaskRunSummary]) -> tuple[str, str]:
    """Compact, labelled group rates and durations for the pipeline overview."""
    groups = kpi_groups(runs)
    return (
        "; ".join(
            f"{kind.value.title()}: {rate(kpis)}; repaired {kpis.repaired}"
            for kind, kpis in groups.items()
        ),
        "; ".join(
            f"{kind.value.title()}: {duration(kpis.average_seconds)}"
            for kind, kpis in groups.items()
        ),
    )


def duration(seconds: float | None) -> str:
    """Format elapsed seconds, leaving an unfinished duration empty."""
    if seconds is None:
        return ""
    whole = round(seconds)
    hours, rest = divmod(whole, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def bars(entries: Sequence[tuple[float | None, str, str, str | None]], unit: str) -> str:
    """Draw ``entries`` (value, status, label, link), oldest first, as a bar per run.

    Each bar's height is its value against the largest, its colour its status; a run with no
    value (not ended) is a short grey bar. Each bar links to its run and says what it holds.
    """
    if not entries:
        return ""
    width, height, gap = 12, 60, 3
    largest = max((value for value, *_ in entries if value), default=0.0) or 1.0
    bars = []
    for index, (value, status, label, url) in enumerate(entries):
        size = max(2.0, (value or 0.0) / largest * height) if value is not None else 2.0
        x = index * (width + gap)
        bar = (
            f'<rect class="bar {_e(status)}" x="{x}" y="{height - size:.1f}" width="{width}" '
            f'height="{size:.1f}"><title>{_e(label)}</title></rect>'
        )
        bars.append(f'<a href="{_e(url)}">{bar}</a>' if url else bar)
    total = len(entries) * (width + gap)
    return (
        f'<figure class="bars"><svg viewBox="0 0 {total} {height}" width="{total}" '
        f'height="{height}" role="img" aria-label="{_e(unit)} of each run, oldest first">'
        f"{''.join(bars)}</svg><figcaption>{_e(unit)} of each run, oldest first; the colour "
        "is the run's status</figcaption></figure>"
    )


def rate(kpis: RunKpis) -> str:
    """Show the success percentage together with its finished-run denominator."""
    rate = kpis.success_rate
    return "" if rate is None else f"{rate:.0%} of {kpis.finished}"


def when(value: datetime | None) -> str:
    """Format a recorded timestamp including its time zone."""
    return value.strftime("%Y-%m-%d %H:%M:%S %Z").strip() if value is not None else ""


def status(value: str | None) -> str:
    """Render an escaped status badge."""
    return f'<span class="status {_e(value)}">{_e(value)}</span>' if value else ""


def facts(pairs: Iterable[tuple[str, str]]) -> str:
    """Render nonempty labelled facts; values may contain generated markup."""
    rows = "".join(f"<dt>{_e(k)}</dt><dd>{v}</dd>" for k, v in pairs if v)
    return f'<dl class="facts">{rows}</dl>' if rows else ""


def counts(run: LastRun | TaskRunSummary) -> str:
    """Show each available row count, including zero."""
    counts = [
        ("source", run.source_count),
        ("target", run.target_count),
        ("inserted", run.insert_count),
        ("updated", run.update_count),
        ("deleted", run.delete_count),
        ("rejected", run.reject_count),
    ]
    return ", ".join(f"{name} {value:,}" for name, value in counts if value is not None)


@dataclass(frozen=True)
class RunKpis:
    """What a pipeline's or task's recent runs add up to."""

    runs: int
    finished: int
    succeeded: int
    failed: int
    sla_breached: int
    average_seconds: float | None
    longest_seconds: float | None
    average_rows: float | None = None
    repaired: int = 0

    @property
    def success_rate(self) -> float | None:
        """The share of finished runs that succeeded, or ``None`` before any finished."""
        return None if not self.finished else self.succeeded / self.finished


def run_kpis(runs: Sequence[RunSummary | TaskRunSummary]) -> RunKpis:
    """Return the KPIs of ``runs``: successes, failures, SLA misses, durations and rows.

    A run counts as finished once it is ``SUCCESS``, ``FAILED`` or ``CANCELLED``; ``SKIPPED``
    runs did no work and ``IN-PROGRESS`` ones have not ended, so neither counts either way.
    """
    finished = [
        r
        for r in runs
        if (r.first_status or r.status)
        in (RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.CANCELLED)
    ]
    seconds = [s for r in finished if (s := r.seconds) is not None]
    rows = [
        r.target_count
        for r in runs
        if isinstance(r, TaskRunSummary)
        and r.status == RunStatus.SUCCESS
        and r.target_count is not None
    ]
    return RunKpis(
        runs=len(runs),
        finished=len(finished),
        succeeded=sum(1 for r in finished if (r.first_status or r.status) == RunStatus.SUCCESS),
        failed=sum(1 for r in finished if (r.first_status or r.status) != RunStatus.SUCCESS),
        sla_breached=sum(
            1 for r in runs if isinstance(r, RunSummary) and r.sla_status == SlaStatus.BREACHED
        ),
        average_seconds=sum(seconds) / len(seconds) if seconds else None,
        longest_seconds=max(seconds) if seconds else None,
        average_rows=sum(rows) / len(rows) if rows else None,
        repaired=sum(
            r.first_status in (RunStatus.FAILED, RunStatus.CANCELLED)
            and r.status == RunStatus.SUCCESS
            for r in runs
        ),
    )
