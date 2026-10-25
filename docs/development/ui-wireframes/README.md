# ETL Craft console wireframes — S7.A.0

Owner decisions: **25 October 2026**. This is one combined `docs(plan)` review with the
Support Insights schedule/window/access plan. **No S7.A page code may start before owner
approval; merging this PR is that approval.** These files are design documents, not shipped
assets or an implementation of the routes described below.

Open [index.html](index.html) locally. No JavaScript, font downloads, CDN, server or build is
needed. The shell stays dense: a narrow navigator, search/breadcrumb bar, content and selection
panel. On a narrow screen the navigator and panel stack; wide tables/timelines have labelled,
keyboard-focusable horizontal scroll regions. The source folder is excluded from MkDocs to
prevent `README.md` generating an index page over the static `index.html` and to keep review
assets out of the published product documentation. The roadmap links this source guide.

## Walk the operator flow

1. [Projects](index.html) → [support_insights](project.html) → [pipelines](pipelines.html).
2. Apply the [ingestion filter](pipelines-filtered.html), select the
   [pipeline side panel](pipeline-panel.html), then open [Beta](pipeline-beta.html).
3. Inspect [runs and duration bars](runs.html), the [matrix](matrix.html),
   [run 419 graph/timeline](run.html), the [task panel](task.html) and its
   [action menu](task.html#actions). Historical cells open their actual task/run states.
4. Inspect [Beta partition items](items.html), the [attempt log](attempt.html),
   [failed attempt 811](attempt-811.html) and the [pipeline log/manifest](run-log.html).
5. [Command center](commands.html) → type a reason → Review → confirm the selected scope →
   static acknowledgement. Mark, force, abort, rerun and offset reset each have their own
   addressable form/confirmation. A running selection's reset is refused by the real service;
   the prototype shows the preconditions rather than implying it is currently allowed.
6. Compare [Gamma waiting](run-gamma.html) with [DM's independent window](run-dm.html).
   Gamma holds no agent slot; Alpha's in-progress merge fences the first DM window, not its
   old pipeline ID or Gamma's idle run. The next window includes Alpha once.
7. Review [uploads](uploads.html), [backfills](backfills.html), [interventions](interventions.html),
   [agents/pools](agents.html), [Insights](insights.html), [gaps](gaps.html),
   [lineage/catalog](lineage.html) and [read-only metadata](metadata.html).

### What the static interactions mean

- Every navigation target is a real link. Cross-object links have a visible ↗ icon,
  `target="_blank" rel="noopener"` and hidden “(opens in a new tab)” text. Same-run task
  selection, filters and page tabs stay in the tab. The sidebar (console views and the
  pipeline tree), breadcrumbs, search results and the first column of a list are navigation:
  they select the object and stay in the tab, so Back works. New tabs are for links from one
  object's page to another object.
- Buttons are native form controls, never anchors disguised as actions. Prototype forms use
  **GET to an acknowledgement HTML file** so they work from disk. Each displays the **actual
  POST action route intended by the real UI**. They send no API request, execute no command,
  confer no permission and never navigate to another object's page as a side effect.
- Review buttons lead to a confirmation state, not an action. Confirmation buttons lead to
  acknowledgement pages. Because there is no JavaScript/server, the first form's reason is
  not dynamically echoed: confirmation labels the static sample and requires the reason again.
  The real UI must preserve the exact typed value and selection through review and submit.
- Confirmations are ordinary pages, not modal dialogs; there is no fake `aria-modal`, focus
  trap or missing Escape handler. Browser Back or Cancel leaves the confirmation.
- The task menu is an addressable `#actions` section: it is the visible right-click/menu design,
  with a keyboard link equivalent. No custom context-menu behavior is claimed.
- Downloads are labelled design-sample `.txt`/JSON files. Whole-run ZIP is disabled with its
  missing route explained; there is no fabricated ZIP or invented full-log download endpoint.
- Receipt is not completion: acknowledgement text distinguishes REQUESTED, acknowledged,
  completed and refused outcomes. Remote force currently remains host-only; its static
  acknowledgement explicitly shows refusal pending the S7.A.2 capability.

## Scenario and visual evidence

The names, pools and topology are the Support Insights design, with **illustrative review
fixtures**, not measurements from a running deployment. Do not use the sample counts as release
evidence. The date is 2026-10-25 UTC; Beta run 419 retries partition 2 while 0/1/3 succeed;
Gamma run 417 waits for an unconsumed object signal row; Alpha run 418 holds an ODS merge open from 05:59;
SI_DM run 420 freezes 00:00 → 05:59 and succeeds at 06:01:22. Alpha commits at 06:02 and
appears in the next DM window. Beta historical runs 415 and 416 have distinct global Engine DB IDs from Alpha run 418.
Historical links retain their exact run identity. No run-ID ordering is the DM offset. ODS UPDATE_DATE is projected as `ods_changed_at` to
avoid returning a reserved engine audit name from the managed SELECT.
SI_REFERENCE run 410 is detached from stewardship upload 74. Three one-slot ingestion agents
and nine warehouse agents retain twelve × 256 MiB; Beta's two bounded readers share one slot.

Status text/colour mapping is fixed in the single `wireframes.css`:

| State | Colour | Non-colour evidence |
| --- | --- | --- |
| SUCCESS | dark green #23623b | text SUCCESS and dot |
| FAILED / LOST | dark red #a0292b | text FAILED/LOST and dot |
| RUNNING | blue #1556a1 | text RUNNING and dot |
| WAITING / RETRYING | ochre #805612 | explicit text and dot |
| QUEUED / SKIPPED / unavailable / stale | grey #525860 | distinct explicit labels |
| Type mismatch | purple #723b8c | explicit mismatch, declared/deployed values |
| Config only | dark red | Config only, reason; never inferred from colour |
| Deployed only / lineage gap | ochre | labelled reason and affected object |
| Matched / Not declared | neutral grey | distinct explicit labels and coverage |

Charts are static HTML/CSS, never decorative. Bars/cells expose run/task/status/value in an
accessible name, axes have time units, and KPI tiles show value/unit/period/comparison. Counts
identify their denominator/provenance; “Not measured” is not zero. Queue/gate/event phases
are separate from execution. A dependency-gate segment without an attempt selects its waiting
task state rather than inventing an attempt; execution bars select the exact attempt.

## Coverage: workflow navigation

| Element from metadata-and-ui.md | Wireframe / addressable state |
| --- | --- |
| Navigator: tags, pipelines, recent runs | Shared shell in [index](index.html); ingestion and mart/reference trees |
| Global search | Shared labelled search form → [search results](search.html) |
| Breadcrumbs | Every file; project → selected pipeline → exact run → task as applicable |
| Pipeline list, name/tags/schedule/next tick/last runs/SLA/pause | [Pipelines](pipelines.html), [applied tag/sort/page](pipelines-filtered.html) |
| Pipeline side panel: schedule/timezone/catch-up/overlap/tags/email/SLA/dependencies/triggers/access/source | [Pipeline panel](pipeline-panel.html) |
| Run matrix / cell selection | [Matrix](matrix.html), separate historical/current task files |
| Run-duration chart | [Runs](runs.html), labelled duration/status/run links |
| Runs list, kind/date/key/status filters, parent/child links | [Runs](runs.html), [filtered](runs-filtered.html), [reference child](run-reference.html) |
| Run graph / selected task | [Beta run](run.html), [task](task.html), [Alpha](run-alpha.html) |
| Timeline with queue/gate/event/execution phases | [Run timeline](run.html), [Gamma wait](run-gamma.html), [fan-out items](items.html) |
| Task side panel: status/attempts/classes/parameters/rows/rejects/explain/tail/owner/host | [Task panel](task.html), [attempts](task.html#attempts), [wait explanation](task.html#waits) |
| Repair/rerun / affected selection preview | [Task menu](task.html#actions), [rerun form](commands.html#rerun), [confirmation](confirm-rerun.html) |
| Run now with parameters / equivalent CLI | [Command center](commands.html#force), [force review](confirm-force.html); host-only refusal shown |
| Live refresh / paused stream | Shared footer; [explicit paused state](run.html#stream); no real SSE in this prototype |

## Coverage: S7.A pages and gap/metadata design

| Required page / element | Wireframe |
| --- | --- |
| App shell / Project Overview / health | [Projects](index.html), [project](project.html), shared sidebar/search/breadcrumb/panel |
| Pipelines / DAG | [Alpha](pipeline-alpha.html), [Beta](pipeline-beta.html), [Gamma](pipeline-gamma.html), [DM](pipeline-dm.html), [Reference](pipeline-reference.html) |
| Runs grid / filter | [Matrix](matrix.html), [runs](runs.html), [filter](runs-filtered.html) |
| Run detail / attempt counts / errors / explain / scoped controls | [Run 419](run.html), [task panel](task.html), [items](items.html) |
| Attempt live tail / exit / owner / host / download | [Attempt 812](attempt.html), [attempt 811](attempt-811.html), sample download |
| Pipeline run log / zip availability | [Run log](run-log.html), manifest sample and disabled ZIP gap |
| Backfill start/progress/stop | [Backfills](backfills.html#progress) |
| Interventions who/when/why | [Interventions](interventions.html) |
| Agents and pools | [Agents](agents.html#ingestion), warehouse/capacity/heartbeat sections |
| Catalog lineage / tables / rules / scripts | [Lineage](lineage.html), [table comparison](table.html), [rules](rules.html), [scripts](scripts.html) |
| Pipeline Insights / KPI/trends/task breakdown/latest runs | [Insights](insights.html), linked runs/tasks |
| Schema gaps / coverage / table history | [Gaps](gaps.html), [stale state](gaps.html#stale), [table history](table.html#history) |
| Metadata readonly / config source / actor / history / validation | [Metadata](metadata.html), [rules](metadata.html#rules), [script](metadata.html#scripts), [access record](metadata.html#access) |
| Uploads assigned pipeline/single file/validation/reject/history | [Uploads](uploads.html), [accepted](uploads.html#accepted), [refused](uploads.html#refused) |
| Command center / reason / confirmation / CLI | [Commands](commands.html), [mark](confirm-mark.html), [force](confirm-force.html), [abort](confirm-abort.html), [rerun](confirm-rerun.html), [offset](confirm-offset.html) |
| Selection then act / no editing / new-tab affordance | Task menu/panel + all object-reference links; no definition editor anywhere |

## Read actions, POST actions and gaps

Every page has a visible S5.AA feed annotation. Prefix below is
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

| Buttons | Current POST action route / acknowledgement |
| --- | --- |
| Run, task-only, rerun, run-from, backfill | RUN `/runs` → [ack](ack-run.html); request flags/capability govern scope |
| Pause/resume/hold/stop/abort/recover | CONTROL `/controls` → [ack](ack-control.html); definition pause omits run ID, controls require reason |
| Mark / watermark / scalar or partition offset reset | MARK `/marks` → [ack](ack-mark.html); active-owner/reset checks still apply |
| Item retry | ITEM_CONTROL `/pipelines/{pipeline_code}/runs/{run_id}/tasks/{task_code}/items/controls` → [ack](ack-items.html) |
| Force | RUN route exists, remote force is currently host-only → [refusal ack](ack-force.html); S7.A.2 gap, not a new route |
| Upload | No existing route; S7.M gap → [prototype-only ack](ack-upload.html) |

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

## Accessibility review (fixing-accessibility)

Applied the repository skill to all HTML/CSS. Review fixes include:

| Finding risk | Resolution |
| --- | --- |
| Icons/status colours could carry the only meaning | Fixed text per state; new-tab icon hidden from screen readers with explicit hidden announcement |
| Narrow grids could clip fields or trap horizontal layout | Responsive shell stacks; only labelled table/timeline regions scroll; no whole-page min-width |
| Custom right-click menu/modal would lack keyboard/focus handling | Addressable ordinary section and confirmation pages; native buttons/details; visible focus outline; no fake modal/menu role |
| Form controls could lack names or reasons | Wrapping labels/IDs, required reason/confirmation fields, helper associations, readonly scope fields |
| Charts could be inaccessible pictures | Text bars/cells with accessible values, labelled axes/time units; KPI unit/period/comparison and table alternatives |
| Live errors/availability might look healthy | Explicit paused/unavailable/removed/refused text; status region on acknowledgements; no automatic polling |
| External assets might leak or fail offline | Shared local CSS only; no script/font/CDN requests |

Body text #20252b on white has high contrast; link/focus #1556a1 and all status text colours
on white exceed 4.5:1 (verified by the one-time check). Tables have captions/column headers;
landmarks and a skip link are present. No positive tabindex. Native details controls supply
keyboard semantics. The checks are source/contrast/link checks, not a claim of browser or
screen-reader certification.

## Review validation and limitations

A one-time standard-library HTML/link walker checks every local href/form target and fragment,
reachability from index, duplicate IDs, labels, new-tab announcements, heading structure,
missing captions/column headers, absence of JavaScript/external styles, action acknowledgements
and contrast. It is run for this PR only, **not added to CI**. Results/counts and durations are
in the PR. Strict `make docs` passes with the review-only folder excluded. Runtime suites are
not needed for this no-src plan change; the normal static gate is still run.

No Playwright MCP is exposed in this session; no browser tooling was installed. Desktop
1440px/narrow 390px screenshots and a visual/browser accessibility review are therefore
unavailable and must be completed during owner review or the eventual UI implementation.
The responsive design and source checks do not substitute for those screenshots.
