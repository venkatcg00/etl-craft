# Browser UI

Run `etl-craft ui` with the `ui` extra, or use the controller's HTTP server. Put an HTTPS
reverse proxy in front of it, preserving the public Host header (for example nginx
`proxy_set_header Host $http_host`): browser cookies are always Secure, including local sign-in.
Open `/ui`. The minimum width is 320 CSS pixels; 960 pixels or wider gives the desktop layout.
The layout remains usable at 200% zoom.

## Sign-in and sessions

Local accounts and configured SSO use the existing login actions. Wrong account names and
passwords give the same message. Recovery means contacting the administrator shown on the
sign-in page; there are no emailed resets. SSO accounts use their identity provider.

Browser sessions have one fixed lifetime of `Auth.Session_hours` (default 12). Credentials
stay in HTTP-only, Secure, SameSite=Lax host-only cookies, never browser storage. The page
warns two minutes before expiry. Signing in again retains the requested same-site page;
external return addresses are refused. Signing out revokes the session in every tab, and
open tabs return to sign-in. Every browser change needs its CSRF challenge. Browser pages
refuse framing by other sites and ship a content security policy.

After `Auth.Lockout_after` consecutive wrong passwords (default 5; 0 disables lockout), a
local user locks until an admin runs `etl-craft account unlock --name ACCOUNT`. A locked
message appears only after the correct password. Successful sign-in resets the count.
SSO and service identities do not accumulate password lockouts. The last usable admin
cannot be locked, disabled or demoted. Admin password resets create temporary passwords;
the user must change one before opening other pages. Password changes revoke all sessions.

## Profile, settings and themes

The profile menu shows the account, deployment role, current project permission and groups,
the CLI sign-in line for this server, last sign-in in settings, keyboard shortcuts, help,
About and sign-out. Local users have Change password; SSO users do not.

Personal settings belong to the account: theme (`system`, `light`, `dark`), IANA timezone,
default project, page size (1–100) and relative or absolute times. The top-bar theme switch
saves to the same account setting. Both themes use the catalog's shared assets and data-flow
mark. Pins and saved filters will arrive with the list pages that use them.

Use Tab and Shift+Tab to move, the skip link to reach content, `?` for shortcuts, and Escape
to close the shortcut dialog or profile menu. Every project page names its environment and
governance; production has a stronger bordered badge as well as the word PRODUCTION.

```sh
etl-craft account settings --theme dark --timezone Asia/Kolkata --page-size 40
etl-craft account change-password
etl-craft account unlock --name alice
```

## Home, About and catalog

Home shows only authorized projects and pipeline work. Running runs and queued tasks are
current; failed runs and SLA misses use a rolling **last 24 h**, including the start and
excluding the end. The JSON supplies both UTC window boundaries. Counts have no links until
the filtered list pages exist. All counts come from the shared `PROJECT_HEALTH` read:

```sh
etl-craft project-health --json
etl-craft --project SALES project-health --json
etl-craft about --json
etl-craft --project SALES catalog page --path index.html --output catalog-page.html
```

About gives package, applied Engine DB schema, server and live controller versions, the
license and version-specific docs. It omits hosts, paths and connection details.

Catalog pages stream the project's existing `generate-docs` output through the same session;
a read never builds a catalog. Only whole-project viewers may open this static catalog:
it lists every pipeline, so pipeline-limited users must request whole-project access or use
authorized pipeline views. A filtered catalog is later work. The generated site's graphs,
search, tables, gaps and theme control remain one implementation, including offline mode.
`publish-docs` remains unchanged as the interim one-project server.

The corresponding API reads are `GET /api/v1/project-health`, `GET /api/v1/about`, and
`GET /api/v1/projects/{project}/catalog/page?path=index.html`. Catalog paths must be literal
relative generated HTML, CSS, JavaScript or SVG files, without traversal, encodings or symlinks.
CLI and API stream the same bytes.

## Pipelines, runs and controls

**Pipelines** in the sidebar lists the project's pipelines you may view: tags, schedule, the
next scheduled run (as the controller computes it, business calendars included; a schedule that
cannot run, such as one naming an inactive calendar, shows why), the five newest runs as status
marks, the latest run's SLA result, whether automatic runs are paused, and paused tasks. Select
pipelines with their check boxes to pause or resume them together; each succeeds or fails on its
own and the result lists each one. **Export CSV** saves the listed rows. The tag list on the left filters by tag;
the filter is the same `tag` parameter as `etl-craft pipelines --tag` and
`GET /api/v1/projects/{project}/pipelines?tag=…`, so the page, the CLI and the API return the
same pipelines, with the same `next_run`, `schedule_problem` and `recent_runs` fields in JSON.

A pipeline's page has a **Runs** tab (run durations as bars, then one page of runs, newest
first, with **Older runs** to page back), a **Matrix** tab (tasks as rows and the same runs as
columns, each cell a task run's status and duration that opens that task in its run) and a
**Tasks** tab (each task, its wave and what it waits for). **Run now** admits one manual run of
the whole pipeline for the controller, optionally for a run date. The details panel shows the schedule, refresh type, SLA, tags, upstream pipelines
and paused tasks. **Pause** and **Resume** act on automatic runs of this pipeline only.

A run's page draws its tasks as a graph coloured by status, with the same tasks in a table;
the **Timeline** view puts each attempt on one time axis, its wait to start apart from its run.
Select a task in the graph or the table to open its panel (status, rows, rejects, attempts and
what it waits for); the page address keeps the selection, so a link or Back returns to it.
Actions act on the selection only: the whole run, or the selected task in it. Each action comes
from the `capabilities` read: an unavailable one is disabled and says why. Choosing one opens a
confirmation with the exact scope, its data effect and whether successful work is kept; the
request goes to `POST /api/v1/projects/{project}/controls`, as `etl-craft control` does, and the
page shows the recorded status. A request is not completion: the page shows the run's state
when it changes.

A selected task's panel also has **Task actions**, each with the same confirmation: **Rerun
task** and **Rerun from here** (with the tasks that depend on it) in this run, **Run task only**
(a standalone execution outside any pipeline run), **Mark as** SUCCESS, FAILED or SKIPPED,
**Pause task** in every run (optionally giving downstream tasks a standing outcome and an end
time) and **Resume task**, and, for a fan-out task, **Items**: each item's status, rows and error,
with Rerun, Cancel and Mark as per item and **Rerun failed items**. A run that is still going has
**Reconcile**, which records attempts whose owner stopped renewing their lease as LOST so their
tasks can be retried. The pipeline header has **Refresh**, which validates and pins the current
definition. Each posts the same `/api/v1` request as the CLI (`run`, `mark`, `pause`, `items`,
`reconcile`, `pipelines refresh`).

The list, pipeline and run pages update live. Every 10 seconds the server re-reads what the
page shows, as you, and the page fetches itself again when that changed; "Updated N s ago" says
when it last checked. While a dialog is open or you are working inside the page, a notice says
the page has changed and offers **Refresh now** instead, so nothing moves under you. A lost
connection shows "Live updates lost, reconnecting"; signing out or losing access stops live
updates with the reason.

Each attempt in a task's panel opens the attempt's page: status, owner, host and process, exit
code, times, rows, the error with its failure class, and the log. The log shows its last
256 KiB (the download has all of it) and, while the attempt is queued or running, follows new
lines every 2 seconds; clear **Follow the end** to stop scrolling. **Download log** saves the
same bytes as `etl-craft logs --attempt-id ID` and
`GET /api/v1/projects/{project}/attempts/{attempt_id}/log`.

A run's **Run log** view is the pipeline run log: one line per recorded event of the run, in
time order, each with its time in UTC, a stable code (`RUN_STARTED`, `TASK_QUEUED`,
`TASK_RETRY_DUE`, `TASK_ADMITTED`, `TASK_STARTED`, `TASK_ENDED`, `TASK_LOST`, `TASK_SETTLED`,
`WAIT_OPENED`, `GATE_DECIDED`, `CONTROL_REQUESTED`, `CONTROL_ACKNOWLEDGED`, `CONTROL_COMPLETED`,
`INTERVENTION`, `SLA_BREACHED`, `SLA_MET`, `RUN_ENDED`, and the cancel codes), the run, the
task, the actor who caused it where one did, and the event's values. It is rendered from the
audit tables, not written separately, so it matches `status`, `explain` and the audit, wherever
the tasks ran. **Download run log** saves the same text as
`etl-craft logs --pipeline-code CODE --run-id ID` and `GET /api/v1/projects/{project}/run-log`.
**Download run zip** saves the whole run in one file: `run.log`, `status.json`,
`history.json` (with the run's interventions), and per task `explain.json` and each attempt's
log (`TASK/attempt-N.log`). `manifest.json` lists every file with its size; an attempt log that
cannot be read here (removed by retention, not uploaded yet, or moved) is listed with that
reason instead of being left out. `etl-craft logs --pipeline-code CODE --run-id ID --zip PATH`
writes the same zip, and `GET /api/v1/projects/{project}/run-zip` serves it.

## Pipeline Insights

**Insights** in the sidebar shows the operational panels for the pipelines you can view, over a
window of UTC days (the last 7 by default, at most 92), optionally for selected pipelines:

- totals: runs, success rate (of finished runs), average duration, SLA breaches, retries, the
  average wait to start and the average execution time, and rows written;
- runs per day, succeeded and failed, as a chart and a table;
- per pipeline: runs, success rate, average and longest duration, SLA breaches, attempts,
  retries, lost attempts, rows written and rows per second of measured execution;
- rows by task: source, target, inserted, updated, deleted and rejected counts of successful
  task runs as each handler reported them (stages are never added together);
- failures by task and failure class; data quality rules with their failed runs, passed and not
  passed table checks, flagged rows and newest observed values; schema and lineage checks by
  attribute and state; and the latest runs, each linking to its run.

A count a task did not report is shown as **Not measured**, never 0, and rates are computed
from summed counts. `etl-craft insights [--from DAY] [--to DAY] [--pipeline-code CODE]
[--tag TAG] --format json` and `GET /api/v1/projects/{project}/insights` return the same
document, and the generated catalog has an **Insights** tab with the last 7 days at generation.

The schema and lineage panel says when the warehouse was last compared with the declared tables
and on what schedule. **Check now** runs the comparison and rebuilds the catalog (it needs the
developer role, and is disabled when the project turns on-demand checks off). Until a check
has run, the states come from the declared metadata only and most read Unchecked.

## Command console

**Console** in the sidebar is the etl-craft CLI in the browser. Type a command line as you
would in a terminal (`pipelines`, `status --pipeline-code ORDERS`, `pause --pipeline-code
ORDERS --reason maintenance`, `--format json` for JSON): it runs in the selected project as
you, through the same actions and checks as the CLI and the API, and prints the CLI's own text,
errors and exit code. Tab completes commands and flags, ↑ and ↓ recall earlier lines, and
`help` or `COMMAND --help` prints the usage.

The console lists the commands your account role or pipeline grants can run; each command is
still checked when it runs. It never runs host commands (`controller`, `migrate`, `ui`), never
edits config files (`config`, `rename`), never signs in, never reads or writes a file on the
server (options such as `--zip` or `--output`), and never prompts for a password: use the CLI
for those. **Your recent requests** lists your audited requests (`audit --mine`); reads are not
recorded. The console is available in servers started with `etl-craft ui` or
`etl-craft controller`.

## Search

The search box in the top bar (press `/` or Ctrl+K) finds pipelines and tasks by any part of
their code or name, a run by its exact run id or run key, and tables when you can view the
whole project. Results open the object's page; a table opens its catalog page. The search page
also lists the pipelines and runs you opened recently in this browser; they are kept in the
browser only. `etl-craft search TEXT` and `GET /api/v1/projects/{project}/search?q=TEXT` return
the same hits.

## Administration

Administrators see **Administration** in the sidebar. Its tabs list accounts, groups, API
tokens, pipeline access (per project), projects, pools and announcements, and each form or row
action posts the same request as the matching CLI command (`account`, `group`, `token`,
`access`, `project`, `pool`, `announce`), so the same checks and audit records apply. The
Accounts tab shows whether an account is locked, its failed sign-ins and last sign-in, and
offers **Unlock**; you cannot disable your own account from the page. A created token's value
appears once, with **Copy token**: store it then, because it is never shown again.

Announcements post a banner on every page for everyone signed in, from their start until their
end or until an administrator ends them; critical announcements are read out by screen
readers. From the CLI:

```bash
etl-craft announce add --message "Upgrade tonight at 22:00 UTC" --level WARNING \
  --ends-at 2026-11-01T23:00:00+00:00 --reason "planned upgrade"
etl-craft announce list --all
etl-craft announce end --announcement-id 3 --reason "upgrade finished"
```

## Upgrade

Run `etl-craft migrate` before starting the new server. Normal login and UI startup refuse
pending Engine migrations, naming the Engine DB key and applied/required migration. If no
admin session is available, the local `migrate` command prompts for the admin account and
password. Its upgrade-only proof reads the original account fields, issues no session and
never applies lockout counters; wrong passwords and non-admins are refused and audited. Engine migration
`0075_browser_accounts.sql` follows business calendars (0074) in both dialects. It adds
`FAILED_SIGNINS` (0), `LOCKED_DATE` (NULL), `MUST_CHANGE_PASSWORD` (N) and validated
`UI_SETTINGS` ({}) to existing `CFG_ACCOUNTS` rows. Existing passwords and accounts remain;
a later admin password reset sets the temporary-password requirement. The migration also
adds partial indexes for recent health counts and successful sign-in/catalog audit reads.

Migration `0078_announcements.sql` adds the `AUD_ANNOUNCEMENTS` audit table; existing data is
unchanged.
