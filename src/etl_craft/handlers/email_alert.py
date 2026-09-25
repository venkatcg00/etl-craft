"""``HANDLER=EMAIL_ALERT``: one email about how a pipeline run went.

An alert task sits at the end of its pipeline, after the tasks it reports on. It works out the
run's outcome from every other task's status under the run:

- ``FAILED`` when any task failed;
- ``COMPLETED_WITH_ERRORS`` when none failed but the run was not clean: a task was skipped,
  needed a retry, succeeded with an error message, or has not finished; or when ``Enforce_sla``
  is on and the run is already past its pipeline's SLA;
- ``SUCCESS`` otherwise.

Other alert tasks are left out, so two alerts (say, one to operations on ``FAILED`` and one to
stakeholders on ``SUCCESS``) agree. The task's parameters:

- ``EMAIL_TO``: the recipients, separated by ``|``; required.
- ``EMAIL_SUBJECT`` and ``EMAIL_BODY``: the subject and an opening paragraph, with
  ``EMAIL_SUBJECT_<OUTCOME>`` / ``EMAIL_BODY_<OUTCOME>`` for one outcome only. A subject is
  required; a body is required unless ``EMAIL_PIPELINES`` is set.
- ``EMAIL_ON_STATUS``: the outcomes to send on, separated by ``|``; all when unset. On another
  outcome the task succeeds without sending, and its task log says why.
- ``EMAIL_PIPELINES``: ``ALL``, or pipeline codes separated by ``|``, adds a table of each
  one's latest run with a section per pipeline listing its tasks.

Subjects and bodies may use ``$$status`` (the outcome), ``$$pipeline_id`` (the run's id),
``$$pipeline_code``, ``$$task_code`` and ``$$error_message`` (the error messages of the tasks
this one watches through a ``FAILURE`` dependency). Any other ``$$`` token fails the task.
"""

from __future__ import annotations

import html
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.engine import Connection, Engine

from etl_craft.core.enums import EmailFlavour, Handler, RunStatus, SlaStatus
from etl_craft.core.errors import HandlerError, MetadataError
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import fetch_pipeline_detail, resolve_pipeline_id
from etl_craft.engine.repository.runs import (
    TaskStatus,
    fetch_latest_pipeline_run,
    fetch_task_statuses_for_run,
)
from etl_craft.engine.repository.tasks import fetch_failure_watch_messages
from etl_craft.engine.runlog import SlaResult, elapsed_hours, fetch_run_sla
from etl_craft.handlers.mail import page, paragraph, parse_recipients, send_email
from etl_craft.handlers.registry import HandlerResult, TaskContext

OUTCOMES = tuple(member.value for member in EmailFlavour)

OUTCOME_COLORS = {
    EmailFlavour.SUCCESS: "#1a7f37",
    EmailFlavour.COMPLETED_WITH_ERRORS: "#9a6700",
    EmailFlavour.FAILED: "#cf222e",
}

STATUS_COLORS = {
    "SUCCESS": "#1a7f37",
    "FAILED": "#cf222e",
    "SKIPPED": "#6e7781",
    "IN-PROGRESS": "#0969da",
    "PENDING": "#9a6700",
    "NEVER_RUN": "#6e7781",
    "UNKNOWN": "#6e7781",
}

TOKENS = ("status", "pipeline_id", "pipeline_code", "task_code", "error_message")
PARAMETERS = frozenset(
    {
        "EMAIL_TO",
        "EMAIL_SUBJECT",
        "EMAIL_BODY",
        "EMAIL_ON_STATUS",
        "EMAIL_PIPELINES",
        *(f"EMAIL_{part}_{outcome}" for part in ("SUBJECT", "BODY") for outcome in OUTCOMES),
    }
)
"""The task parameters an alert task reads."""
_TOKEN = re.compile(r"\$\$([A-Za-z_][A-Za-z0-9_]*)")


def run(context: TaskContext, engine_db: Engine) -> HandlerResult:
    """Work out the run's outcome and send the alert, or record why none was sent."""
    params = context.task_params
    recipients = parse_recipients(params.get("EMAIL_TO"), "EMAIL_TO")
    wanted = _wanted_outcomes(params.get("EMAIL_ON_STATUS"))
    with engine_db.connect() as conn:
        statuses = fetch_task_statuses_for_run(conn, context.pipeline_id, context.pipeline_run_id)
        sla = sla_so_far(conn, context)
        watched = fetch_failure_watch_messages(conn, context.task_id)
        outcome = run_outcome(statuses, exclude_task_id=context.task_id, sla_missed=sla is not None)
        if wanted is not None and outcome not in wanted:
            return HandlerResult(
                variables={
                    "RUN_STATUS": outcome,
                    "EMAIL_SENT": "false",
                    "REASON": f"{outcome} is not in EMAIL_ON_STATUS",
                }
            )
        subject_template = _template(params, "SUBJECT", outcome)
        body_template = _template(params, "BODY", outcome)
        pipelines = params.get("EMAIL_PIPELINES")
        if not subject_template:
            raise HandlerError(f"EMAIL_SUBJECT (or EMAIL_SUBJECT_{outcome}) is required")
        if not body_template and not pipelines:
            raise HandlerError(
                f"EMAIL_BODY (or EMAIL_BODY_{outcome}) is required when EMAIL_PIPELINES is not set"
            )
        values = {
            "status": outcome,
            "pipeline_id": str(context.pipeline_run_id),
            "pipeline_code": context.pipeline_code,
            "task_code": context.task_code,
            "error_message": "; ".join(m.error_message for m in watched if m.error_message),
        }
        subject = substitute(subject_template, values, "EMAIL_SUBJECT")
        parts = [
            f'<p style="color:{OUTCOME_COLORS[EmailFlavour(outcome)]};font-weight:600">'
            f"{html.escape(context.pipeline_code)}: {html.escape(outcome)}</p>"
        ]
        if sla is not None:
            parts.append(f"<p><strong>{html.escape(sla.describe())}</strong></p>")
        if body_template:
            parts.append(paragraph(substitute(body_template, values, "EMAIL_BODY")))
        if pipelines:
            parts.append(
                digest_html([latest_run_of(conn, code) for code in _codes(conn, pipelines)])
            )
    send_email(context.config, recipients, subject, page("".join(parts)))
    variables: dict[str, object] = {
        "RUN_STATUS": outcome,
        "EMAIL_SENT": "true",
        "EMAIL_TO": "|".join(recipients),
        "EMAIL_SUBJECT": subject,
    }
    if sla is not None:
        variables["SLA"] = sla.describe()
    return HandlerResult(variables=variables)


def alert_parameter_problems(params: Mapping[str, str]) -> list[str]:
    """Return what is wrong with an alert task's parameters, for every outcome it can send on.

    A run sends on one outcome, so it notices a missing subject or body only when that outcome
    happens; this checks each outcome ``EMAIL_ON_STATUS`` allows.
    """
    problems: list[str] = []
    try:
        parse_recipients(params.get("EMAIL_TO"), "EMAIL_TO")
    except HandlerError as error:
        problems.append(str(error))
    try:
        wanted = _wanted_outcomes(params.get("EMAIL_ON_STATUS"))
    except HandlerError as error:
        problems.append(str(error))
        wanted = None
    outcomes = [outcome for outcome in OUTCOMES if wanted is None or outcome in wanted]
    parts = ["SUBJECT"] if params.get("EMAIL_PIPELINES") else ["SUBJECT", "BODY"]
    for part in parts:
        missing = [outcome for outcome in outcomes if not _template(params, part, outcome)]
        if missing:
            problems.append(
                f"no EMAIL_{part} for outcome(s) {', '.join(missing)}: set EMAIL_{part}, or "
                f"EMAIL_{part}_<OUTCOME> for each, or leave them out of EMAIL_ON_STATUS"
            )
    blank = dict.fromkeys(TOKENS, "")
    for name in sorted(params):
        if name in PARAMETERS and name.startswith(("EMAIL_SUBJECT", "EMAIL_BODY")):
            try:
                substitute(params[name], blank, name)
            except HandlerError as error:
                problems.append(str(error))
    return problems


def run_outcome(statuses: Sequence[TaskStatus], *, exclude_task_id: int, sla_missed: bool) -> str:
    """Return the run's outcome from its tasks' statuses; alert tasks are left out."""
    relevant = [
        s for s in statuses if s.task_id != exclude_task_id and s.handler != Handler.EMAIL_ALERT
    ]
    if any(s.status == RunStatus.FAILED for s in relevant):
        return EmailFlavour.FAILED
    clean = all(
        s.status == RunStatus.SUCCESS and not s.error_message and s.attempt_count <= 1
        for s in relevant
    )
    if clean and not sla_missed:
        return EmailFlavour.SUCCESS
    return EmailFlavour.COMPLETED_WITH_ERRORS


def substitute(text: str, values: Mapping[str, str], setting: str) -> str:
    """Replace the ``$$`` tokens in ``text``; ``HandlerError`` naming an unknown one."""
    unknown = sorted({m.group(1) for m in _TOKEN.finditer(text)} - set(values))
    if unknown:
        raise HandlerError(
            f"{setting} uses unknown token(s) {', '.join('$$' + t for t in unknown)}; the "
            f"tokens are {', '.join('$$' + t for t in TOKENS)}"
        )
    return _TOKEN.sub(lambda m: values[m.group(1)], text)


def sla_so_far(conn: Connection, context: TaskContext) -> SlaResult | None:
    """Return the SLA the run has already missed when ``Enforce_sla`` is on, else ``None``."""
    if not context.config.limits.enforce_sla:
        return None
    hours = fetch_pipeline_detail(conn, context.pipeline_id).sla_in_hours
    if hours is None:
        return None
    elapsed = elapsed_hours(
        fetch_run_sla(conn, context.pipeline_run_id).start_date, datetime.now(UTC)
    )
    if elapsed <= hours:
        return None
    return SlaResult(SlaStatus.BREACHED, hours, elapsed)


def _wanted_outcomes(value: str | None) -> set[str] | None:
    if value is None or not value.strip():
        return None
    wanted = {part.strip().upper() for part in value.split("|") if part.strip()}
    unknown = sorted(wanted - set(OUTCOMES))
    if unknown:
        raise HandlerError(
            f"EMAIL_ON_STATUS names unknown outcome(s) {', '.join(unknown)}; the outcomes are "
            f"{', '.join(OUTCOMES)}"
        )
    return wanted


def _template(params: Mapping[str, str], part: str, outcome: str) -> str | None:
    return params.get(f"EMAIL_{part}_{outcome}") or params.get(f"EMAIL_{part}")


def _codes(conn: Connection, value: str) -> list[str]:
    if value.strip().upper() == "ALL":
        return [
            str(code) for code in conn.execute(statement(conn, "active_pipeline_codes")).scalars()
        ]
    return [code.strip() for code in value.split("|") if code.strip()]


@dataclass(frozen=True)
class PipelineDigest:
    """A pipeline's latest run, and its tasks, for the status table."""

    pipeline_code: str
    status: str
    pipeline_run_id: int | None
    start_date: datetime | None
    end_date: datetime | None
    tasks: list[TaskStatus]


def latest_run_of(conn: Connection, pipeline_code: str) -> PipelineDigest:
    """Return a pipeline's latest run; an unknown code is ``UNKNOWN``, one never run ``NEVER_RUN``.

    One wrong code in ``EMAIL_PIPELINES`` shows as its own row rather than losing the report on
    every other pipeline.
    """
    try:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
    except MetadataError:
        return PipelineDigest(pipeline_code, "UNKNOWN", None, None, None, [])
    latest = fetch_latest_pipeline_run(conn, pipeline_id)
    if latest is None:
        return PipelineDigest(pipeline_code, "NEVER_RUN", None, None, None, [])
    tasks = fetch_task_statuses_for_run(conn, pipeline_id, latest.pipeline_run_id)
    return PipelineDigest(
        pipeline_code,
        latest.status,
        latest.pipeline_run_id,
        latest.start_date,
        latest.end_date,
        tasks,
    )


def _status(status: str) -> str:
    color = STATUS_COLORS.get(status, "#57606a")
    return f'<span style="color:{color};font-weight:600">{html.escape(status)}</span>'


def digest_html(entries: Sequence[PipelineDigest]) -> str:
    """Render the pipelines' latest runs: a summary table, then a section per pipeline."""
    rows = "".join(
        f"<tr><td>{html.escape(e.pipeline_code)}</td><td>{_status(e.status)}</td>"
        f"<td>{'' if e.pipeline_run_id is None else e.pipeline_run_id}</td>"
        f"<td>{e.start_date or ''}</td><td>{e.end_date or ''}</td></tr>"
        for e in entries
    )
    sections = "".join(
        f"<details><summary>{html.escape(e.pipeline_code)}: {_status(e.status)}</summary>"
        "<table><tr><th>Task</th><th>Status</th><th>Error</th></tr>"
        + (
            "".join(
                f"<tr><td>{html.escape(t.task_code)}</td><td>{_status(t.status)}</td>"
                f"<td>{html.escape(t.error_message or '')}</td></tr>"
                for t in e.tasks
            )
            or "<tr><td colspan='3'>no active tasks</td></tr>"
        )
        + "</table></details>"
        for e in entries
    )
    return (
        "<h2>Pipeline status</h2><table><tr><th>Pipeline</th><th>Status</th><th>Run</th>"
        f"<th>Started</th><th>Ended</th></tr>{rows}</table>{sections}"
    )
