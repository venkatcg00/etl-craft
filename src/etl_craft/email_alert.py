"""HANDLER=EMAIL_ALERT execution -- sends an SMTP email, per CLAUDE.md's Handlers section.

[ADDITION] Closes the last open item in CLAUDE.md's Handlers section: "the
actual send transport (SMTP creds vs. an API like SES/SendGrid) ... and
whether $$-style substitution applies inside alert bodies." Both resolved by
explicit instruction: the transport is SMTP; substitution does apply, via a
small, closed set of tokens (see _substitute below) -- the same plain-text,
pre-driver substitution spirit as sql_actions.substitute_pipeline_id, not a
general templating engine.

Gating an EMAIL_ALERT task ("a FAILURE edge fires only when what it watches
fails, ALWAYS fires regardless") is already handled generically by the same
CFG_TASK_DEPENDENCY/CFG_PIPELINE_DEPENDENCY machinery every other task uses
(runner.py/orchestrator.py/crosspipe.py) -- by the time this module's
execute() runs, the task has already been judged eligible to run. This
module's only job is to actually send the email.

[ADDITION] Every email this module sends is HTML (with an inline <style>
block -- email clients don't reliably fetch external stylesheets), per
explicit instruction. Two distinguishable statuses matter visually above
all others -- SUCCESS (green) and FAILED (red) -- with the remaining real
AUD_ statuses (SKIPPED/IN-PROGRESS) and this module's own synthetic ones
(PENDING/NEVER_RUN/UNKNOWN, see below) styled more neutrally.

[ADDITION] CFG_TASK_PARAMETERS.PARAMETER_NAME vocabulary this module reads,
mirroring sql_actions.py's own module docstring as the authoritative
reference:
  EMAIL_TO       pipe-separated recipient address list. Required.
  EMAIL_SUBJECT  the subject line, substitution tokens allowed. Required.
  EMAIL_BODY     an intro paragraph, substitution tokens allowed. Required
                 unless EMAIL_PIPELINES is set (a pure status-digest alert
                 doesn't need one).
  EMAIL_PIPELINES
                 optional. Per explicit instruction: "if it is all, send
                 the status of all pipelines for its latest [run]. if the
                 pipeline name is mentioned, then that alone. can contain
                 1, some, all. some = pipeline1|pipeline2." Three shapes,
                 all handled the same way this module reads every other
                 multi-value parameter (pipe-separated):
                   ALL                    every active pipeline
                   PIPELINE_A             exactly that one
                   PIPELINE_A|PIPELINE_B  exactly those ("some")
                 [CHOICE] EMAIL_-prefixed, not a bare PIPELINES, for
                 consistency with this handler's other three parameters —
                 the literal instruction didn't specify a prefix.
                 [CHOICE] "if pipeline should support any other type that
                 too" is read as: never hard-fail this task over one bad
                 name in the list -- an unresolvable pipeline code renders
                 as its own UNKNOWN row in the digest instead of raising,
                 so one typo doesn't lose the report for every other
                 pipeline named alongside it.

When EMAIL_PIPELINES is set, the email gets an appended status digest:
one summary table (pipeline, latest status, run id, start/end) plus one
collapsible <details> section per pipeline (per explicit instruction:
"each pipeline should have collapsible arrow") showing that pipeline's own
latest run's per-task status breakdown (cfg.fetch_task_statuses_for_run).
[CHOICE] Plain <details>/<summary> -- no JavaScript. Most desktop/webmail
clients render it as a real native disclosure triangle; the handful that
don't just show the content always-expanded, which is a safe, readable
fallback, not a broken one.

Substitution tokens (case-sensitive, literal $$ prefix like $$pipeline_id),
usable in EMAIL_SUBJECT/EMAIL_BODY:
  $$pipeline_id     ctx.pipeline_run_id
  $$pipeline_code   ctx.pipeline_code
  $$task_code       ctx.task_code
  $$error_message   every ERROR_MESSAGE from a task this one watches via an
                     active, same-or-cross-pipeline FAILURE-typed
                     CFG_TASK_DEPENDENCY edge (cfg.fetch_failure_watch_messages),
                     joined "; " -- empty string if this task has no FAILURE
                     edges (e.g. it's gated ALWAYS) or the watched task
                     hasn't logged one. [CHOICE] Derived from the task's own
                     dependency edges rather than a new WATCH_TASK_CODE
                     parameter: the edges already say what this task
                     watches, so a second, parallel declaration of the same
                     fact would just be something to keep in sync by hand.
An unrecognized token is left in the text untouched, same as
substitute_pipeline_id's own "token absent -> leave it alone" rule -- no
attempt to guess at a typo.

[CHOICE] "any other template, team should use airflow" -- per explicit
instruction, this module's own reporting shape stops here: one intro
paragraph plus an optional multi-pipeline status digest. A team wanting
richer, more customized reporting is expected to reach for Airflow's own
notification/callback mechanisms instead of this module growing a general
templating engine.
"""

from __future__ import annotations

import html as html_lib
import smtplib
from dataclasses import dataclass
from email.mime.text import MIMEText

from sqlalchemy.engine import Connection

from etl_craft.cfg import (
    CfgError,
    TaskStatusEntry,
    fetch_all_pipelines,
    fetch_failure_watch_messages,
    fetch_pipeline_run_history,
    fetch_task_statuses_for_run,
    resolve_pipeline_id,
)
from etl_craft.config import ConfigError, resolve_secret
from etl_craft.execution import HandlerError, HandlerResult, TaskExecutionContext

_TOKEN_PIPELINE_ID = "$$pipeline_id"
_TOKEN_PIPELINE_CODE = "$$pipeline_code"
_TOKEN_TASK_CODE = "$$task_code"
_TOKEN_ERROR_MESSAGE = "$$error_message"

_STATUS_COLORS = {
    "SUCCESS": "#1a7f37",
    "FAILED": "#cf222e",
    "SKIPPED": "#6e7781",
    "IN-PROGRESS": "#0969da",
    "PENDING": "#9a6700",
    "NEVER_RUN": "#6e7781",
    "UNKNOWN": "#6e7781",
}

_STYLE = (
    "body{font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;color:#1f2328}"
    "table{border-collapse:collapse;margin:0.5em 0}"
    "th,td{border:1px solid #d0d7de;padding:4px 10px;text-align:left}"
    "details{margin:0.4em 0}"
    "summary{cursor:pointer;font-weight:600}"
)


def _resolve_error_message(cfg_conn: Connection, ctx: TaskExecutionContext) -> str:
    """Join every ERROR_MESSAGE this task watches via a FAILURE-typed dependency edge."""
    messages = fetch_failure_watch_messages(cfg_conn, ctx.task_id)
    return "; ".join(m.error_message for m in messages if m.error_message)


def _substitute(text_: str, ctx: TaskExecutionContext, error_message: str) -> str:
    """Plain-text token substitution -- see this module's own docstring for the token list."""
    return (
        text_.replace(_TOKEN_PIPELINE_ID, str(ctx.pipeline_run_id))
        .replace(_TOKEN_PIPELINE_CODE, ctx.pipeline_code)
        .replace(_TOKEN_TASK_CODE, ctx.task_code)
        .replace(_TOKEN_ERROR_MESSAGE, error_message)
    )


@dataclass(frozen=True)
class _PipelineDigestEntry:
    pipeline_code: str
    status: str
    pipeline_run_id: int | None
    start_date: object
    end_date: object
    tasks: list[TaskStatusEntry]


def _resolve_target_pipeline_codes(cfg_conn: Connection, pipelines_param: str) -> list[str]:
    """Parse EMAIL_PIPELINES ("ALL" | one code | pipe-separated codes) into a code list."""
    if pipelines_param.strip().upper() == "ALL":
        return [summary.pipeline_code for summary in fetch_all_pipelines(cfg_conn)]
    return [code.strip() for code in pipelines_param.split("|") if code.strip()]


def _collect_pipeline_digest(cfg_conn: Connection, pipeline_code: str) -> _PipelineDigestEntry:
    try:
        pipeline_id = resolve_pipeline_id(cfg_conn, pipeline_code)
    except CfgError:
        return _PipelineDigestEntry(pipeline_code, "UNKNOWN", None, None, None, [])
    history = fetch_pipeline_run_history(cfg_conn, pipeline_id, limit=1)
    if not history:
        return _PipelineDigestEntry(pipeline_code, "NEVER_RUN", None, None, None, [])
    latest = history[0]
    tasks = fetch_task_statuses_for_run(cfg_conn, pipeline_id, latest.pipeline_run_id)
    return _PipelineDigestEntry(
        pipeline_code,
        latest.status,
        latest.pipeline_run_id,
        latest.start_date,
        latest.end_date,
        tasks,
    )


def _status_span(status: str) -> str:
    color = _STATUS_COLORS.get(status, "#57606a")
    return f'<span style="color:{color};font-weight:600">{html_lib.escape(status)}</span>'


def _render_digest_html(entries: list[_PipelineDigestEntry]) -> str:
    summary_rows = "".join(
        f"<tr><td>{html_lib.escape(e.pipeline_code)}</td><td>{_status_span(e.status)}</td>"
        f"<td>{e.pipeline_run_id if e.pipeline_run_id is not None else ''}</td>"
        f"<td>{e.start_date or ''}</td><td>{e.end_date or ''}</td></tr>"
        for e in entries
    )
    details_blocks = "".join(
        f"<details><summary>{html_lib.escape(e.pipeline_code)} -- "
        f"{_status_span(e.status)}</summary>"
        "<table><tr><th>Task</th><th>Status</th><th>Error</th></tr>"
        + (
            "".join(
                f"<tr><td>{html_lib.escape(t.task_code)}</td><td>{_status_span(t.status)}</td>"
                f"<td>{html_lib.escape(t.error_message or '')}</td></tr>"
                for t in e.tasks
            )
            or "<tr><td colspan='3'>(no active tasks)</td></tr>"
        )
        + "</table></details>"
        for e in entries
    )
    return (
        "<h2>Pipeline status summary</h2>"
        "<table><tr><th>Pipeline</th><th>Status</th><th>Run</th><th>Started</th><th>Ended</th></tr>"
        f"{summary_rows}</table>{details_blocks}"
    )


def _send(ctx: TaskExecutionContext, recipients: list[str], subject: str, body_html: str) -> None:
    if ctx.config.email is None:
        raise HandlerError(
            "no [Email] section configured in craft-connector.yml -- "
            "required for HANDLER=EMAIL_ALERT"
        )
    profile = ctx.config.email.active
    message = MIMEText(body_html, "html")
    message["Subject"] = subject
    message["From"] = profile.from_address
    message["To"] = ", ".join(recipients)

    try:
        with smtplib.SMTP(profile.host, profile.port) as server:
            if profile.use_tls:
                server.starttls()
            if profile.auth_mode == "password":
                secret = resolve_secret(ctx.config, profile)
                server.login(profile.user, secret)
            server.sendmail(profile.from_address, recipients, message.as_string())
    except (smtplib.SMTPException, OSError, ConfigError) as exc:
        raise HandlerError(
            f"EMAIL_ALERT: failed to send via {profile.host}:{profile.port}: {exc}"
        ) from exc


def execute(cfg_conn: Connection, ctx: TaskExecutionContext) -> HandlerResult:
    """Render this task's HTML email (intro + optional status digest) and send it."""
    to_raw = ctx.task_params.get("EMAIL_TO")
    if not to_raw:
        raise HandlerError("CFG_TASK_PARAMETERS.EMAIL_TO is required for HANDLER=EMAIL_ALERT")
    recipients = [addr.strip() for addr in to_raw.split("|") if addr.strip()]

    subject_template = ctx.task_params.get("EMAIL_SUBJECT")
    if not subject_template:
        raise HandlerError("CFG_TASK_PARAMETERS.EMAIL_SUBJECT is required for HANDLER=EMAIL_ALERT")

    pipelines_param = ctx.task_params.get("EMAIL_PIPELINES")
    body_template = ctx.task_params.get("EMAIL_BODY")
    if not body_template and not pipelines_param:
        raise HandlerError(
            "CFG_TASK_PARAMETERS.EMAIL_BODY is required for HANDLER=EMAIL_ALERT "
            "when EMAIL_PIPELINES is not set"
        )

    error_message = _resolve_error_message(cfg_conn, ctx)
    subject = _substitute(subject_template, ctx, error_message)
    intro_html = (
        f"<p>{html_lib.escape(_substitute(body_template, ctx, error_message))}</p>"
        if body_template
        else ""
    )

    digest_html = ""
    if pipelines_param:
        codes = _resolve_target_pipeline_codes(cfg_conn, pipelines_param)
        entries = [_collect_pipeline_digest(cfg_conn, code) for code in codes]
        digest_html = _render_digest_html(entries)

    body_html = (
        f"<html><head><style>{_STYLE}</style></head><body>{intro_html}{digest_html}</body></html>"
    )
    _send(ctx, recipients, subject, body_html)

    return HandlerResult(
        variables={"EMAIL_TO": to_raw, "EMAIL_SUBJECT": subject, "RECIPIENT_COUNT": len(recipients)}
    )
