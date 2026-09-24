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

[DEVIATION, 2026-09-20, E2-43] This handler is a **pipeline-level**
completion alert, not a task-level one. Per explicit interview decision:
"task level emails are noise", and "once you exhaust retries and all of the
tasks that can be run are ran and failed, then send one email considering
all". One email per run, whose flavour is computed by run_flavour() from
every active task's own status under this pipeline_run_id -- read from
AUD_TASK_RUN_LOG, deliberately *not* AUD_PIPELINES_RUN_LOG, which may not be
finalized yet at the moment the alert runs. Three flavours, green/amber/red:
SUCCESS, COMPLETED_WITH_ERRORS ("if the pipeline is marked success with
failure then a neutral status like pipeline is COMPLETED with errors"), and
FAILED. See run_flavour's own docstring for the exact rules.

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
  EMAIL_SUBJECT  the subject line, substitution tokens allowed. Required,
                 unless a per-flavour EMAIL_SUBJECT_<STATUS> covers every
                 flavour this task can reach.
  EMAIL_BODY     an intro paragraph, substitution tokens allowed. Required
                 unless EMAIL_PIPELINES is set (a pure status-digest alert
                 doesn't need one) or a per-flavour EMAIL_BODY_<STATUS>
                 covers it.
  EMAIL_SUBJECT_<STATUS> / EMAIL_BODY_<STATUS>
                 [ADDITION, E2-43] optional per-flavour overrides, where
                 <STATUS> is SUCCESS, COMPLETED_WITH_ERRORS or FAILED — per
                 explicit instruction, "have three templates as said in
                 flavour answer and choose 1 as needed". Each falls back to
                 the plain EMAIL_SUBJECT/EMAIL_BODY when not declared, so a
                 task predating this keeps working unchanged.
  EMAIL_ON_STATUS
                 [ADDITION, E2-43] optional, pipe-separated flavour list.
                 Per explicit instruction: "if there are parameters saying
                 which status to send, send only on that condition, else
                 send on all statuses". When the computed flavour is not in
                 the list, the task records SUCCESS with "no email sent" in
                 its own TASK_LOG rather than SKIPPED — it ran and correctly
                 decided not to act, and SUCCESS also keeps it out of the
                 unsettled set so it can never re-create E2-01.
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
usable in EMAIL_SUBJECT/EMAIL_BODY and their per-flavour variants:
  $$status          [ADDITION, E2-43] this run's computed flavour
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
from datetime import UTC, datetime
from email.mime.text import MIMEText

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft import credentials
from etl_craft.cfg import (
    CfgError,
    TaskStatusEntry,
    fetch_all_pipelines,
    fetch_failure_watch_messages,
    fetch_pipeline_detail,
    fetch_pipeline_run_history,
    fetch_task_statuses_for_run,
    resolve_pipeline_id,
)
from etl_craft.config import ConfigError, resolve_secret
from etl_craft.db import ConnectionError_
from etl_craft.execution import HandlerError, HandlerResult, TaskExecutionContext
from etl_craft.runlog import SLA_BREACHED, SlaResult, elapsed_hours

# [ADDITION, 2026-09-20, E2-43] The three flavours a completed run resolves
# to, per explicit interview decision. Ordered worst-first — _run_flavour
# returns the first that applies.
FLAVOUR_FAILED = "FAILED"
FLAVOUR_COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS"
FLAVOUR_SUCCESS = "SUCCESS"
FLAVOURS = (FLAVOUR_FAILED, FLAVOUR_COMPLETED_WITH_ERRORS, FLAVOUR_SUCCESS)

_FLAVOUR_COLORS = {
    FLAVOUR_SUCCESS: "#1a7f37",
    FLAVOUR_COMPLETED_WITH_ERRORS: "#9a6700",
    FLAVOUR_FAILED: "#cf222e",
}

SMTP_TIMEOUT_SECONDS = 30.0

_TOKEN_PIPELINE_ID = "$$pipeline_id"
_TOKEN_STATUS = "$$status"
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


def _substitute(
    text_: str, ctx: TaskExecutionContext, error_message: str, flavour: str = ""
) -> str:
    """Plain-text token substitution -- see this module's own docstring for the token list."""
    return (
        text_.replace(_TOKEN_PIPELINE_ID, str(ctx.pipeline_run_id))
        .replace(_TOKEN_PIPELINE_CODE, ctx.pipeline_code)
        .replace(_TOKEN_TASK_CODE, ctx.task_code)
        .replace(_TOKEN_ERROR_MESSAGE, error_message)
        .replace(_TOKEN_STATUS, flavour)
    )


def run_flavour(statuses: list[TaskStatusEntry], *, exclude_task_id: int) -> str:
    """Resolve this pipeline run's own flavour from every active task's status.

    [ADDITION, 2026-09-20, E2-43] Per explicit interview decision: an
    EMAIL_ALERT is a *pipeline-level* completion alert, not a task-level one
    ("task level emails are noise"), sent "once you exhaust retries and all of
    the tasks that can be run are ran". The flavour is computed from
    AUD_TASK_RUN_LOG rather than AUD_PIPELINES_RUN_LOG, which may not be
    finalized yet at the moment the alert task runs.

    `exclude_task_id` is the alerting task itself: it is necessarily
    IN-PROGRESS while it runs, so counting it would make every run look
    unfinished.

    [DEVIATION, 2026-09-22, E2-77] *Every* EMAIL_ALERT task is excluded, not
    just this one. Excluding only self is correct for one alert and breaks for
    two, and two is a supported configuration: validate._alert_ordering_issues
    treats alerts as a set and excludes all of them from the leaf requirement,
    and EMAIL_ON_STATUS exists precisely so one alert can go to ops on FAILED
    while another goes to stakeholders on SUCCESS. Both then depend on the
    same leaves and land in the same wave, so whichever ran first saw the
    other as PENDING/IN-PROGRESS -- which lands in the neutral middle -- and
    the team got two contradictory emails about the same run, the amber one
    false, on a perfectly clean run. exclude_task_id is kept alongside the
    handler filter so self-exclusion does not depend on the handler lookup.

    The rules, worst-first:
      FAILED                 any task FAILED.
      COMPLETED_WITH_ERRORS  no outright failure, but something short of
                             clean -- a SKIPPED task, a task that succeeded
                             only after a retry (ATTEMPT_COUNT > 1), a task
                             that succeeded while still carrying an
                             ERROR_MESSAGE, or a task not settled yet. Per explicit instruction: "if
                             the pipeline is marked success with failure then
                             a neutral status like pipeline is COMPLETED with
                             errors".
      SUCCESS                every task SUCCESS, none carrying an error.

    [CHOICE] Unsettled tasks (PENDING/IN-PROGRESS) land in the neutral middle
    flavour rather than SUCCESS. The alert is designed to run at the end of a
    run, but nothing forces that -- and reporting a plain SUCCESS for a run
    that has not finished would be the one genuinely misleading answer of the
    three.
    """
    relevant = [
        entry
        for entry in statuses
        if entry.task_id != exclude_task_id and entry.handler != "EMAIL_ALERT"
    ]
    if not relevant:
        return FLAVOUR_SUCCESS
    if any(entry.status == "FAILED" for entry in relevant):
        return FLAVOUR_FAILED
    if all(
        entry.status == "SUCCESS" and not entry.error_message and entry.attempt_count <= 1
        for entry in relevant
    ):
        return FLAVOUR_SUCCESS
    return FLAVOUR_COMPLETED_WITH_ERRORS


def _template_for(ctx: TaskExecutionContext, base_name: str, flavour: str) -> str | None:
    """Pick EMAIL_<BASE>_<FLAVOUR> if declared, else fall back to plain EMAIL_<BASE>.

    Per explicit instruction ("have three templates as said in flavour answer
    and choose 1 as needed"). The fallback keeps every pre-E2-43 task working
    unchanged: a task declaring only EMAIL_SUBJECT/EMAIL_BODY gets those for
    all three flavours.
    """
    return ctx.task_params.get(f"EMAIL_{base_name}_{flavour}") or ctx.task_params.get(
        f"EMAIL_{base_name}"
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
        # [ADDITION, 2026-09-20, E2-17] timeout. An unreachable-but-accepting
        # relay otherwise blocks on the default socket timeout, which is None.
        with smtplib.SMTP(profile.host, profile.port, timeout=SMTP_TIMEOUT_SECONDS) as server:
            if profile.use_tls:
                server.starttls()
            if profile.auth_mode in {"password", "oauth"} and not profile.user:
                raise HandlerError(
                    f"[Email] profile {profile.name!r} has auth_mode={profile.auth_mode} but no "
                    "user to log in as"
                )
            if profile.auth_mode == "password":
                secret = resolve_secret(ctx.config, profile)
                server.login(profile.user or "", secret)
            elif profile.auth_mode == "oauth":
                _login_xoauth2(server, ctx, profile.user or "")
            server.sendmail(profile.from_address, recipients, message.as_string())
    except (smtplib.SMTPException, OSError, ConfigError, ConnectionError_) as exc:
        raise HandlerError(
            f"EMAIL_ALERT: failed to send via {profile.host}:{profile.port}: {exc}"
        ) from exc


def _login_xoauth2(server: smtplib.SMTP, ctx: TaskExecutionContext, user: str) -> None:
    """Authenticate with SMTP XOAUTH2, using a client-credentials access token.

    [ADDITION, 2026-09-24] auth_mode oauth, for relays that no longer accept
    passwords (Microsoft 365, Google Workspace). Untested against a live relay:
    it follows the documented XOAUTH2 exchange, and success is not guaranteed.
    """
    assert ctx.config.email is not None
    profile = ctx.config.email.active
    token = credentials.client_credentials_token(
        str(profile.extra["token_url"]),
        str(profile.extra["client_id"]),
        resolve_secret(ctx.config, profile),
        profile.extra.get("scope"),
    )
    server.ehlo_or_helo_if_needed()
    server.auth(
        "XOAUTH2",
        lambda challenge=None: f"user={user}\x01auth=Bearer {token}\x01\x01",
        initial_response_ok=True,
    )


def execute(cfg_conn: Connection, ctx: TaskExecutionContext) -> HandlerResult:
    """Resolve this run's flavour, pick the matching template, and send -- or record why not."""
    to_raw = ctx.task_params.get("EMAIL_TO")
    if not to_raw:
        raise HandlerError("CFG_TASK_PARAMETERS.EMAIL_TO is required for HANDLER=EMAIL_ALERT")
    recipients = [addr.strip() for addr in to_raw.split("|") if addr.strip()]

    statuses = fetch_task_statuses_for_run(cfg_conn, ctx.pipeline_id, ctx.pipeline_run_id)
    flavour = run_flavour(statuses, exclude_task_id=ctx.task_id)
    # [ADDITION, 2026-09-24] Orchestration.Enforce_sla: a run already past its
    # SLA_IN_HOURS is not a clean run, whatever its tasks did. Checked here,
    # before EMAIL_ON_STATUS, so an alert can be configured to fire on exactly
    # this. The run is not finalized yet, so the clock is read now.
    sla_note = _sla_breach_so_far(cfg_conn, ctx)
    if sla_note and flavour == "SUCCESS":
        flavour = "COMPLETED_WITH_ERRORS"

    # [ADDITION, 2026-09-20, E2-43] EMAIL_ON_STATUS, per explicit instruction:
    # "if there are parameters saying which status to send, send only on that
    # condition, else send on all statuses". Absent means always send.
    on_status_raw = ctx.task_params.get("EMAIL_ON_STATUS")
    if on_status_raw:
        wanted = {part.strip().upper() for part in on_status_raw.split("|") if part.strip()}
        unknown = wanted - set(FLAVOURS)
        if unknown:
            raise HandlerError(
                f"CFG_TASK_PARAMETERS.EMAIL_ON_STATUS names unknown status(es) "
                f"{sorted(unknown)} -- valid values are {list(FLAVOURS)}"
            )
        if flavour not in wanted:
            # SUCCESS, deliberately not SKIPPED: the task ran and correctly
            # decided not to send. SUCCESS also keeps it out of the unsettled
            # set, so it can never re-create E2-01.
            return HandlerResult(
                variables={
                    "RUN_STATUS": flavour,
                    "EMAIL_SENT": "false",
                    "REASON": f"no email sent: {flavour} not in EMAIL_ON_STATUS",
                }
            )

    subject_template = _template_for(ctx, "SUBJECT", flavour)
    if not subject_template:
        raise HandlerError(
            "CFG_TASK_PARAMETERS.EMAIL_SUBJECT (or EMAIL_SUBJECT_<STATUS>) is required "
            "for HANDLER=EMAIL_ALERT"
        )

    pipelines_param = ctx.task_params.get("EMAIL_PIPELINES")
    body_template = _template_for(ctx, "BODY", flavour)
    if not body_template and not pipelines_param:
        raise HandlerError(
            "CFG_TASK_PARAMETERS.EMAIL_BODY (or EMAIL_BODY_<STATUS>) is required for "
            "HANDLER=EMAIL_ALERT when EMAIL_PIPELINES is not set"
        )

    error_message = _resolve_error_message(cfg_conn, ctx)
    subject = _substitute(subject_template, ctx, error_message, flavour)
    intro_html = (
        f"<p>{html_lib.escape(_substitute(body_template, ctx, error_message, flavour))}</p>"
        if body_template
        else ""
    )

    digest_html = ""
    if pipelines_param:
        codes = _resolve_target_pipeline_codes(cfg_conn, pipelines_param)
        entries = [_collect_pipeline_digest(cfg_conn, code) for code in codes]
        digest_html = _render_digest_html(entries)

    banner = (
        f'<p style="color:{_FLAVOUR_COLORS[flavour]};font-weight:600">'
        f"{html_lib.escape(ctx.pipeline_code)}: {html_lib.escape(flavour)}</p>"
    )
    sla_html = f"<p><strong>{html_lib.escape(sla_note)}</strong></p>" if sla_note else ""
    body_html = (
        f"<html><head><style>{_STYLE}</style></head><body>"
        f"{banner}{sla_html}{intro_html}{digest_html}</body></html>"
    )
    _send(ctx, recipients, subject, body_html)

    variables: dict[str, object] = {
        "RUN_STATUS": flavour,
        "EMAIL_SENT": "true",
        "EMAIL_TO": to_raw,
        "EMAIL_SUBJECT": subject,
        "RECIPIENT_COUNT": len(recipients),
    }
    if sla_note:
        variables["SLA"] = sla_note
    return HandlerResult(variables=variables)


def _sla_breach_so_far(cfg_conn: Connection, ctx: TaskExecutionContext) -> str | None:
    """Describe the SLA this run has already overrun, when Enforce_sla is on; else None."""
    if not ctx.config.limits.enforce_sla:
        return None
    sla_hours = fetch_pipeline_detail(cfg_conn, ctx.pipeline_id).sla_in_hours
    if sla_hours is None:
        return None
    start = cfg_conn.execute(
        text("SELECT START_DATE FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID = :id"),
        {"id": ctx.pipeline_run_id},
    ).scalar_one()
    hours = elapsed_hours(start, datetime.now(UTC))
    if hours <= sla_hours:
        return None
    return SlaResult(status=SLA_BREACHED, sla_hours=sla_hours, elapsed_hours=hours).describe()
