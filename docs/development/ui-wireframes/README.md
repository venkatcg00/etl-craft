# etl-craft console wireframes — S7.A.0

Owner decisions, 25 October 2026. No S7.A page code starts before the owner approves these
wireframes; merging this `docs(plan)` PR is that approval. They are design documents, not
shipped assets, and they add no route or action.

Open [index.html](index.html) (the sign-in page) and click through. Everything is static HTML
and one stylesheet: no JavaScript, fonts, CDN or server. The folder is excluded from MkDocs.

## Design direction

- **Databricks is the template.** Top bar with the project switcher and global search, a left
  sidebar, Jobs-style pipeline pages (Runs, Matrix, Tasks, Permissions tabs and a details
  panel), job-run-style run pages (Graph, Timeline, List and a run or task panel), and a
  Catalog Explorer for tables (Columns, Lineage, History, Rules, Details).
- **PowerCenter decides how you act.** Select an object, then act on it from the right-click
  menu, the side panel or the header buttons; nothing acts without a selection.
- **Console, not forms.** The command center is a terminal: type an `etl-craft` command line,
  get the CLI's output. Host commands and metadata edits are refused with the CLI's message.
- **Themes.** A shaded light theme (never pure white) and a dark theme. The system setting
  chooses; the half-circle button in the top bar switches to the other theme. In the
  wireframes the switch lasts for the current page only (no script); the real UI remembers it.
- **Design notes stay out of the way.** Each page's read actions, routes and gaps are in the
  closed "Design notes" drawer at the bottom right, not in the console itself.

## Walk the operator flow

1. [Sign in](index.html) → [Home](home.html): health tiles, recent runs, projects.
2. [Pipelines](pipelines.html): the tag tree is the navigator; [filter by tag](pipelines-ingestion.html).
3. [SI_CLIENT_BETA](pipeline-beta.html): run-duration bars and runs; [matrix](pipeline-beta-matrix.html),
   [tasks](pipeline-beta-tasks.html), [permissions](pipeline-beta-access.html), [run now](pipeline-beta-run-now.html).
4. [Run 419](run-419.html): graph; [timeline](run-419-timeline.html); select
   [beta_source_to_lnd](run-419-task.html) for its panel; [right-click menu](run-419-menu.html);
   [recover](run-419-recover.html) → [acknowledged](run-419-ack.html); [mark](run-419-mark.html).
5. [Attempt 812 log](attempt-812.html), [fan-out items](items-419.html), [pipeline run log](run-419-log.html).
6. [Gamma waiting for signal rows](run-417.html) and [SI_DM's window](run-420.html).
7. [Catalog](catalog.html) → [ods](schema-ods.html) → [interactions](table-ods-interactions.html):
   [lineage](table-ods-interactions-lineage.html), [history](table-ods-interactions-history.html),
   [rules](table-ods-interactions-rules.html), [details](table-ods-interactions-details.html);
   [schema gaps](catalog-gaps.html).
8. [Console](console.html) → [after a command](console-ran.html).
9. [Insights](insights.html), [Agents and pools](agents.html), [Backfills](backfills.html),
   [Interventions](interventions.html), [Uploads](uploads.html), [Metadata](metadata.html),
   [Search](search.html).

### Link and action rules shown

- The sidebar, breadcrumbs, tabs, search results, list rows, graph nodes and matrix cells
  select or drill in the same tab, so Back works.
- A link from one object's page to another object (a table in lineage, a downstream pipeline,
  an attempt log, a parent upload) opens a new tab, with the ↗ icon and hidden
  "(opens in a new tab)" text.
- Actions are buttons, never links. Dialogs and toasts are static pages drawn over the page
  they belong to; Cancel returns to it. Receipt is not completion: the toast says REQUESTED.

## Coverage

| Element or page (metadata-and-ui.md, S7.A) | Wireframe |
| --- | --- |
| Sign-in | [index](index.html) |
| Project overview and health | [Home](home.html) |
| Navigator: tags, pipelines, recent runs | [Pipelines tag tree](pipelines.html), sidebar Recents |
| Global search | top bar → [search](search.html) |
| Breadcrumbs | every page below Home |
| Pipeline list (tags, schedule, next run, recent runs, SLA, pause) | [Pipelines](pipelines.html) |
| Pipeline side panel | [details panel](pipeline-beta.html) |
| Run matrix | [Matrix](pipeline-beta-matrix.html) |
| Run-duration chart | [Runs tab](pipeline-beta.html) |
| Runs list, filters, parent and child links | [Runs](runs.html), [failed](runs-failed.html), [run 410 parent](run-410.html) |
| Run graph | [Run 419](run-419.html), [Alpha 418](run-418.html), [Gamma 417](run-417.html), [DM 420](run-420.html) |
| Task timeline (queue, gate, event wait, execution) | [Timeline](run-419-timeline.html), [Gamma](run-417-timeline.html) |
| Task side panel | [Task panel](run-419-task.html) |
| Right-click menu | [Menu](run-419-menu.html) |
| Repair and rerun | [Recover](run-419-recover.html), [rerun a finished run](run-416-recover.html) |
| Run now with parameters | [Run dialog](pipeline-beta-run-now.html) (override gap noted) |
| Live refresh | "Live · paused" in run headers; stream contract is a gap |
| Attempt: live tail, exit code, owner, host, download | [Attempt 812](attempt-812.html), [811](attempt-811.html) |
| Pipeline run log and run zip | [Run log](run-419-log.html) |
| Backfills | [Backfills](backfills.html), [start](backfills-start.html) |
| Interventions | [Interventions](interventions.html) |
| Agents and pools | [Agents and pools](agents.html) |
| Catalog: tables, lineage, rules | [Catalog](catalog.html), [table](table-ods-interactions.html), [lineage](table-dm-fact-lineage.html), [rules](table-ods-interactions-rules.html) |
| Schema gaps and states | [Gaps](catalog-gaps.html), [columns](table-ods-interactions.html) |
| Pipeline Insights | [Insights](insights.html) |
| Metadata (read-only, source file, actor) | [Metadata](metadata.html) |
| Uploads | [Uploads](uploads.html) |
| Command center | [Console](console.html) |

## Scenario

Illustrative review data, not measurements. 2026-10-25 UTC. Beta run 419 retries
partition-2 while partitions 0, 1 and 3 succeeded. Alpha run 418 holds an ODS merge open
(attempt 9031). Gamma run 417 waits for MinIO signal rows and holds no slot. SI_DM run 420
read the window 00:00 ≤ UPDATE_DATE < 05:59, fenced by Alpha's running attempt. SI_REFERENCE
run 410 came from stewardship upload 74.

## Status marks

Every status is an icon plus its word; colour is never the only signal.

| Status | Mark |
| --- | --- |
| SUCCESS, Matched, ACCEPTED, HEALTHY | green check |
| FAILED, LOST, Config only, REFUSED | red cross |
| IN-PROGRESS, RUNNING | blue spinner |
| WAITING, REQUESTED, Deployed only, STALE, DRAINING | amber clock |
| QUEUED, Engine managed, Unchecked | grey dotted ring |
| SKIPPED, CANCELLED | grey square |
| Type mismatch | purple diamond |

## Read actions, POST actions and gaps

Every page lists its S5.AA reads in its design-notes drawer. Prefix below is
`/api/v1/projects/{project}` (except `/api/v1/health` and `/api/v1/whoami`). Current action-table
sources: `services/operations/actions.py`, request types and control/inspect services, read
at the plan's origin/main baseline. A route existing does not imply every desired filter,
aggregate or child document is available; those gaps stay explicit.

| Existing read action | Current route | Used by |
| --- | --- | --- |
| HEALTH, WHOAMI | `/api/v1/health`, `/api/v1/whoami` | shell/identity; HEALTH is not an agent/project-capacity list |
| PIPELINES, PIPELINE | `/pipelines`, `/pipelines/{pipeline_code}` | lists, pipeline panels, schedules |
| GRAPH, STEPS | `/graph`, `/steps` | dependency graph, selected task/run details |
| STATUS, RUN_DETAIL, HISTORY | `/status`, `/runs/{run_id}`, `/history` | runs, statuses, durations, historical selection |
| EXPLAIN, CAPABILITIES, CONTROL_REQUEST | `/explanations`, `/capabilities`, `/control-requests/{control_id}` | reasons, permitted controls, acknowledgement |
| ATTEMPT, LOGS | `/attempts/{attempt_id}`, `/attempts/{attempt_id}/log` | attempt detail and byte-offset tail stream |
| ITEMS | `/pipelines/{pipeline_code}/runs/{run_id}/tasks/{task_code}/items` | bounded Beta item rows |
| AUDIT | `/audit` | actor/intervention records; not a synthesized pipeline log |
| CATALOG_GAPS, LINEAGE, VALIDATE, DOCS_VERSION_CHECK | `/catalog/gaps`, `/lineage`, `/validation`, `/docs/versions` | structural gaps, lineage, validation, generated-doc versions |

| Buttons | Current POST action route / wireframe state |
| --- | --- |
| Run now, run task only, run from here, rerun, backfill | RUN `/runs` → [run dialog](pipeline-beta-run-now.html), [backfill dialog](backfills-start.html), [acknowledgement](run-419-ack.html) |
| Pause, hold, stop, abort, recover | CONTROL `/controls` → [recover dialog](run-419-recover.html) → [acknowledgement toast](run-419-ack.html) |
| Mark, watermark or offset reset | MARK `/marks` → [mark dialog](run-419-mark.html) with typed reason and confirmation |
| Item retry | ITEM_CONTROL `/pipelines/{pipeline_code}/runs/{run_id}/tasks/{task_code}/items/controls` → [items](items-419.html) |
| Console commands | the same routes; the console parses a command line and calls them ([console](console.html)) |
| Force | RUN route exists; remote force is host-only today, so the console refuses it with the CLI's message until S7.A.2 adds the capability |
| Upload | no route yet; S7.M gap → [uploads](uploads.html) |

### Required read gaps before implementation

No action is added by this PR. The following must be planned/implemented through the shared
operations layer before the relevant live view can claim parity:

- **Project list/health and global search:** authorized bounded project/pipeline/task/table/run
  aggregates, exact lookup, cursors and filters. Do not fetch all records into a browser.
- **Matrix and filtered history:** batched paged runs × tasks, explicit terminal/live duration,
  attempt-status filter, sort/tag/date/run-key predicates. Existing HISTORY/STEPS are building
  blocks, not evidence that the matrix aggregate/filter combination exists.
- **Agents/pools:** an operator-facing read for current slots, resource limits, usage, queued
  work, heartbeats and stale states. Agent token/protocol routes are not user console reads.
- **Pipeline Insights:** bounded shared KPI/trend operations with stage/denominator provenance,
  source counts vs successive-stage rows, retries vs logical runs, unavailable measurements.
- **Metadata/table/script/access:** full readonly rows and actor/as-of/config-source documents,
  table lineage version differences and source text; S7.Y access list is planned, not present.
- **Backfill inventory:** aggregate progress/list read beyond individual history/run details.
- **Logs and download:** S7.A.3 synthesized audit run log, HTTP attachment disposition,
  streaming whole-run zip/manifest, missing-log reasons; do not invent `/runs/.../log` today.
- **S7.M uploads:** assigned steward pipeline list, validation result/history and upload action.
- **One-off run parameter overrides:** current RunRequest has no arbitrary override mapping.
  The command form displays a readonly per-run preview and marks the S7.A.2 request-contract
  gap; it does not expose definition editing or pretend a new field already works.
- **Live refresh and remote force:** event stream subscription/read contract and the shared
  capability/role rules for controls currently host-only. No local shell or metadata actions.

## Contracts to settle during S6

| Item | Views | Required inputs / semantics |
| --- | --- | --- |
| S6.B Logs outside Engine DB | attempt, run log, task panel, uploads | tail offsets/byte bounds; ordered chunks; full upload acknowledged vs pending; retention removed vs inaccessible; download streaming; exact task/attempt/run identity and manifest reasons |
| S6.F Metrics | projects, project, runs/chart/matrix, timeline, items, agents, Insights, DM freshness | bounded date windows, units, comparisons; logical-run vs attempt identity; queue/gate/event/execution phases; slot vs query usage; missing measurements/denominators; shared CLI/API calculation provenance |
| S7.X Bounded windows | SI_DM pipeline/run and lineage | committed low, frozen high, task-run owner, source attempt fence, retry/empty-window outcome and common-clock evidence |

## Accessibility and validation

- Landmarks, a skip link, labelled inputs, `aria-current` on the current view, real buttons for
  actions, and accessible names on every graph node, chart bar, matrix cell and timeline bar.
- Text contrast meets WCAG AA in both themes.
- A one-time link walk found every local link, form target and fragment resolves, no duplicate
  ids, and every page reachable from the sign-in page. It is not added to CI.
- Screenshots at 1440 px and 390 px, light and dark, were checked in Firefox.
