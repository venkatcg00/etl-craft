"""The catalog's Insights snapshot page, from the same calculation as the live Insights page."""

from __future__ import annotations

from collections.abc import Callable

from etl_craft.services.catalog.html import escape, render_table
from etl_craft.services.insights import GapCount, Insights

NOT_MEASURED = '<span class="note">Not measured</span>'


def _seconds(value: float | None) -> str:
    return "None" if value is None else f"{value:.1f} s"


def _rate(value: float | None) -> str:
    return "No finished runs" if value is None else f"{value:.0%}"


def _count(value: int | None) -> str:
    return NOT_MEASURED if value is None else str(value)


def _linked(href: str | None, text: str) -> str:
    return text if href is None else f'<a href="{escape(href)}">{escape(text)}</a>'


def insights_page(
    insights: Insights, gaps: tuple[GapCount, ...], run_link: Callable[[int], str | None]
) -> str:
    """Render the snapshot window's panels as static tables."""
    head = (
        f'<p class="note">Snapshot of runs started from {insights.window_start:%Y-%m-%d %H:%M} '
        f"UTC up to {insights.window_end:%Y-%m-%d %H:%M} UTC, taken when this catalog was "
        "generated. The live Insights page and <code>etl-craft insights</code> show any window."
        "</p>"
    )
    sections = (
        ("Totals", _totals(insights)),
        ("Pipelines", _pipelines(insights)),
        ("Rows by task", _tasks(insights)),
        ("Failures", _failures(insights)),
        ("Data quality rules", _rules(insights)),
        ("Schema and lineage checks", _checks(gaps)),
        ("Latest runs", _latest(insights, run_link)),
    )
    return head + "".join(f"<h2>{escape(title)}</h2>{body}" for title, body in sections)


def _totals(insights: Insights) -> str:
    t = insights.total
    return render_table(
        ("Runs", "Success rate", "Average duration", "SLA breached", "Retries", "Rows written"),
        [
            (
                str(t.runs),
                _rate(t.success_rate),
                _seconds(t.average_seconds),
                str(t.sla_breached),
                str(t.retries),
                _count(t.rows_written),
            )
        ],
        "No runs in this window.",
    )


def _pipelines(insights: Insights) -> str:
    return render_table(
        (
            "Pipeline",
            "Runs",
            "Success rate",
            "Average",
            "Longest",
            "SLA breached",
            "Retries",
            "Average wait",
            "Average execution",
            "Rows written",
        ),
        [
            (
                escape(p.pipeline_code),
                str(p.runs),
                _rate(p.success_rate),
                _seconds(p.average_seconds),
                _seconds(p.longest_seconds),
                str(p.sla_breached),
                str(p.retries),
                _seconds(p.average_wait_seconds),
                _seconds(p.average_run_seconds),
                _count(p.rows_written),
            )
            for p in insights.per_pipeline
        ],
        "No pipelines.",
    )


def _tasks(insights: Insights) -> str:
    return render_table(
        (
            "Pipeline",
            "Task",
            "Runs",
            "Source rows",
            "Target rows",
            "Inserted",
            "Updated",
            "Deleted",
            "Rejected",
        ),
        [
            (
                escape(k.pipeline_code),
                escape(k.task_code),
                str(k.runs),
                _count(k.source_rows),
                _count(k.target_rows),
                _count(k.inserted),
                _count(k.updated),
                _count(k.deleted),
                _count(k.rejected),
            )
            for k in insights.tasks
        ],
        "No successful task runs in this window.",
    )


def _failures(insights: Insights) -> str:
    return render_table(
        ("Pipeline", "Task", "Status", "Failure class", "Attempts"),
        [
            (
                escape(f.pipeline_code),
                escape(f.task_code),
                escape(f.status),
                escape(f.failure_class or "Unknown"),
                str(f.attempts),
            )
            for f in insights.failures
        ],
        "No failed or lost attempts in this window.",
    )


def _rules(insights: Insights) -> str:
    return render_table(
        (
            "Pipeline",
            "Rule",
            "Runs",
            "Failed runs",
            "Passed",
            "Not passed",
            "Rows flagged",
            "Newest observed",
        ),
        [
            (
                escape(r.pipeline_code),
                escape(r.rule_name),
                str(r.runs),
                str(r.failed),
                str(r.passed),
                str(r.not_passed),
                str(r.flagged_rows),
                escape(", ".join(o.observed for o in r.observations)) or "None",
            )
            for r in insights.rules
        ],
        "No business rules ran in this window.",
    )


def _checks(gaps: tuple[GapCount, ...]) -> str:
    return render_table(
        ("Attribute", "State", "Checks"),
        [(escape(g.attribute), escape(g.state), str(g.checks)) for g in gaps],
        "No declared tables to check.",
    )


def _latest(insights: Insights, run_link: Callable[[int], str | None]) -> str:
    return render_table(
        ("Run", "Pipeline", "Status", "Started (UTC)"),
        [
            (
                _linked(run_link(r.pipeline_run_id), str(r.pipeline_run_id)),
                escape(r.pipeline_code),
                escape(r.status),
                f"{r.start_date:%Y-%m-%d %H:%M:%S}",
            )
            for r in insights.latest
        ],
        "No runs in this window.",
    )
