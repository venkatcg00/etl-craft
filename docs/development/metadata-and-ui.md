# Metadata integrity and built-in pipeline insights

This is the corrective design following source definitions, before worker-agent work resumes.
S5.O.1 implements the SCD lifecycle and legacy upgrades. S5.O.2a enforces common config fields
and safe token history. Stable pipeline, task, parameter, dependency and rule identities now
include inactive rows. Scoped table/column source contracts and task/rule target FKs are
implemented, and merge column lists refuse repeated names. Versioned lineage with endpoint
components and the published target state finish S5.O.2b; coloured gaps and dashboards remain
planned. The frozen 0.4 soak deployment remains unchanged.

## Evidence and current gaps

| Area | Current implementation | Required correction |
| --- | --- | --- |
| Source metadata | `CFG_TABLES` holds separate components; `CFG_COLUMNS.TABLE_ID` is the parent FK, with all-row name identity and active ordinal uniqueness | Source, rule target and task target conversion complete; recorded endpoint conversion remains S5.O.2b |
| Config audit fields | Core config fields are mandatory; project config DDL requires a single PK, typed non-null base fields and valid active values | Common base contract implemented in S5.O.2a; scoped identity/FK normalization remains S5.O.2b |
| API tokens | Common actor/date/active fields and safe typed history, retaining expiry and revocation; hashes remain excluded | S5.O.2a implemented; upgrade the Engine DB before using it |
| Object references | Rule and task targets use `CFG_TABLES` FKs; lineage endpoints, rule results and target state still use combined names | Replace persisted object identity with relational references and separate identifier components |
| SCD targets | Both merge actions have creation/update actors and dates and active/deleted flags; soft deletes preserve closed history | S5.O.1 complete; upgrade legacy targets before using the new lifecycle |
| Generated documentation | Optional warehouse comparison reports textual column/type gaps; untraced tasks have reasons | Add structured gap states, row colours, warning symbols and project-wide gap navigation |
| Dashboard | Catalog has recent-run KPIs; no built-in pipeline metrics dashboard | Provide shared services and built-in panels in generated docs and the live UI |

The reference is the Support Insights repository's
`superset/exports/dashboard_export.zip`, dashboard `Support Insights Dashboard`, and
`infra/sql/15_vw_schema_views_creation.sql`. Its pipeline charts cover number of runs, average
duration, processed data, valid/invalid counts, quality percentage, runs/validity per source,
duration trends, performance and the latest ten runs. Use these operational meanings in
etl-craft; its customer interaction charts remain business-specific example dashboards.

## Relational metadata contract

### Legacy identity inventory and readiness

The deployment-wide `validate` check reports duplicate natural keys across active and inactive
rows in every CSV-backed config table, resolved source-column/position collisions and malformed
task/rule targets. It is read-only: existing metadata and immutable history remain intact.
Object findings are warnings while the legacy object schema remains supported. Engine migration
0027 refuses duplicate pipeline/task/parameter/dependency/rule keys transactionally and enforces
all-row uniqueness for those identities. It preserves the rows and existing audit versions.
Engine migration 0028 converts source contracts and refuses ambiguity before any pending migration.
It preserves column IDs/stamps, untouched legacy typed history and an immutable conversion
baseline for historical reconstruction. Ordinal uniqueness applies to active columns because
ordinal is an attribute that retirement releases, while column identity remains reserved.
Source tracing retains its existing unquoted-name boundary until the remaining object consumers
are converted. Task and rule target FKs are implemented. The repeated merge column check and the
recorded audit endpoints remain, as the three slices of
[S5.O.2b](road-to-1.0.0.md#s5o2b-remaining-object-identities). Subsequent object normalization must refuse
unresolved ambiguity before writing any converted rows; this diagnostic alone is
not the relational migration or its completion gate.

| Persisted value | Classification | Conversion and affected readers/writers |
| --- | --- | --- |
| `CFG_SOURCE_COLUMNS.TABLE_NAME` | Object identity repeated per column | `CFG_TABLES` parent and `CFG_COLUMNS.TABLE_ID`; source repository, CSV round trips, seed export, validation and catalog/lineage schema cache |
| Task parameter `TARGET_OBJECT` | Task's target object identity | FK on the existing task; config import/export, task parameter consumers, same-target/conflicting-writer queries, SQL and script handlers |
| `CFG_BUSINESS_RULES.TARGET_TABLE_ID` | Rule's target object identity | Implemented FK on the existing rule; component CSV references and handler-boundary rendering. Recorded rule-result endpoint conversion remains pending |
| Task `MERGE_KEY` / `MERGE_COMPARE_COLUMNS` | Ordered column-name lists of the task's own output, not object identities | Stay ordered task parameter values with no table of their own; the SQL task reader refuses a name given twice. Hashing and config round trips are unchanged |
| Task `MERGE_DEDUPE_ORDER` | Ordered terms with direction/null ordering | Preserve each term's identifier, direction and null ordering; do not split it as an object name |
| `AUD_COLUMN_LINEAGE.SOURCE_OBJECT` / `TARGET_OBJECT` | Per-task cache of the latest computation, rewritten whenever its inputs change; not history | Separate endpoint components. History moves to lineage versions per target table, which start at the upgrade; cached rows are not converted |
| `AUD_BUSINESS_RULES_RESULTS.TARGET_TABLE` | Historical rule-result attribution | Separate recorded target components; current rule edits must not rewrite past results |
| `AUD_TARGET_HASH_VERSION.TARGET_OBJECT` / `AUD_TARGET_VIEW_STATEMENT.TARGET_OBJECT` | Published state for a warehouse target | Separate full target identity, preserving published versions/hashes and timestamps; rehash, view replacement and publication readers/writers |
| Config typed histories and `AUD_METADATA_CHANGES` projection | Immutable old configuration versions | Preserve every version and validity interval; historical exports must rebuild matching table/column parents, including old names and inactive contracts |
| Target advisory/file lock names | Derived opaque lock keys | Derive from the canonical resolved identity; never treat the generated lock name as catalog/schema/table metadata |
| SQL text, script/SQL file paths, offsets, event file URIs, JSON dictionaries, migration names and content hashes | SQL expressions or opaque values | Keep their current semantics; dots and delimiters alone do not identify a relational field |

The parser recognizes two or three SQL identifier components, including quoted dots, and uses
the configured warehouse dialect's case rules. Missing catalog remains `NULL` without a
configured catalog. With one, relative and explicit references to that catalog compare as one
identity. Readiness does not widen the current handlers' accepted target syntax. Source-column
comparisons use original stored spelling rather than the legacy CSV reader's lowercase copy.

### Required normalized schema

Use ordinary relational table/column contracts, without mirroring the warehouse's entire
information schema. `information_schema` is authoritative for deployed structure; configuration
is authoritative for declared expectations, ownership and engine behaviour. The intentional
overlap is the contract needed to validate drift and trace external sources offline. Store only
objects referenced by this project, not every warehouse object. SQL text and display labels may
contain qualified names; persisted identity has separate components and foreign keys.

| Entity | Identity and relationship | Attributes |
| --- | --- | --- |
| `CFG_TABLES` | `TABLE_ID` PK; unique resolved object identity within deployment/project | Separate catalog name (where applicable), schema name, table name; declared table/view kind |
| `CFG_COLUMNS` | `COLUMN_ID` PK; `TABLE_ID` FK; unique parent/name and parent/ordinal | Column name, positive ordinal, declared type, nullability where declared |

These two entities have the common config fields below. Do not introduce catalog/schema tables
unless they acquire independently managed attributes or relationships. Group table records by
their name components in the UI; grouping does not require extra database entities. Store target
table FKs on existing tasks and rules. Derive SQL read relationships and managed output columns
from SQL through the existing lineage service/cache; do not add another editable relationship
table or persist inferred outputs as user config. Explicit script/external source contracts use
column rows; inferred managed outputs remain derived and carry their SQL provenance. Existing
typed history and lineage evidence have distinct audit purposes and remain necessary.

For catalog-less warehouses, catalog absence is explicit; enforce natural-key uniqueness even
when that component is NULL. Do not invent a catalog SQL identifier or use empty strings. Where
catalogs exist, resolve an omitted catalog from the configured warehouse once. Never lower-case quoted identifiers or
guess an ambiguous two-part/three-part name. Preserve dialect identifier semantics and reject
ambiguous imports with the original value and correction instructions.

Task target references and rule target references use `TABLE_ID`; parameter dictionaries retain
JSON values, but object identity is not hidden in JSON. Dependency relationships are rows.
Merge column lists name columns of the task's own output rather than objects, so they remain
ordered task parameters. Inventory all task parameters
and audit/state tables before migration, classifying SQL expressions, paths and opaque tokens
separately from composite relational values; dots inside those strings do not make them object
references. Render qualified SQL identifiers only at the warehouse execution boundary.

Read deployed columns, types, constraints and ordinals through existing warehouse metadata
queries when comparison is requested. Do not maintain editable deployed-schema copies. Reuse
existing audit/cache mechanisms for a dated comparison result only if needed to show the last
check; identify its timestamp/version and replace stale observations without changing the
declared contract. Each proposed new table must have independently owned data, a concrete
writer/reader and a requirement that an existing entity or derived query cannot meet.

Lineage history is versioned per target table. A version holds every column edge into the
table, with the writing task, the source and target catalog, schema, table and column names as
they were computed, and its validity interval. A table gets a new version only when its edges
change. Live joins may additionally reference metadata IDs. Later renames or retirement must
not rewrite a version or remove it through cascading deletes. The per-task lineage cache is
separate from this history, and its keys use the complete ordered contract.

## Common config and audit rules

Every `CFG_` table, including tokens and future users/pools/projects, has:

- A stable primary key, scoped natural-key uniqueness and FKs for relationships.
- `CREATED_BY`, `CREATE_DATE`, `UPDATED_BY`, `UPDATED_DATE`: all NOT NULL, actors resolved by
  etl-craft and timestamps in UTC. Keep these existing config names consistently.
- `ACTIVE_FLAG`: NOT NULL, constrained to `Y`/`N`; retirement is attributed and audited.
- Typed configuration history preserving the original creation fields. Updates stamp the
  update fields; outside writes remain refused. Token active state does not override expiry
  or revocation, and hashes/secrets are excluded from general metadata exports and UI.

Natural identities cannot be duplicated through inactive copies; reactivate the existing row.
History belongs in the existing typed history tables. Migration must identify active/inactive
duplicates and produce an actionable conflict report before choosing a canonical identity;
never discard them automatically. Apply a complete config change and its history in one Engine
DB transaction, with foreign-key enforcement and rollback on any invalid row. Avoid hard
deletes of parents with children. ACID here describes Engine DB writes; warehouse and Engine DB
writes do not become one distributed transaction, and each warehouse retains its actual
transaction/DDL guarantees.

Audit tables follow their purpose rather than taking a mutable config template indiscriminately:
immutable events have an ID, actor, occurrence timestamp and related run/task/object keys;
lifecycle rows have creation/update attribution and guarded state transitions; configuration
history has validity bounds and `CURRENT_FLAG`. Deletion/active flags apply where a lifecycle
requires them. Preserve the existing engine-only write guards and immutable evidence.

For SCD1 and SCD2 targets keep `CREATE_DATE`, `UPDATE_DATE`, `ACTIVE_FLAG` and `DELETE_FLAG`,
plus existing run/hash/row identity fields. Protected SCD tables retain `CREATED_BY` and `UPDATED_BY`
with accurate creation/update attribution; ordinary project business tables may omit actor columns. SCD1 insert/update
leaves a live row active and not deleted; soft delete sets inactive/deleted; revival restores
active/not-deleted. SCD2 active means the current live version: closing a version makes it
inactive without labelling a normal historical version deleted; deleting a current row makes
it inactive/deleted; revival opens a new active/not-deleted version. Preserve creation stamps,
stamp mutations, and leave unchanged rows untouched. Existing SCD1 targets need a controlled
upgrade deriving active state from delete state, under the existing target lock. Do not assume
every historical inactive SCD2 row represents a deletion.

## Project business tables and human overrides

Creation/update actor columns are optional on ordinary project-owned warehouse business tables.
SCD tables using human overwrite protection retain `CREATED_BY` and `UPDATED_BY`. These fields
identify the actual writer, including an external system or an authenticated uploader; they must not automatically attribute every write to etl-craft. A pipeline started
by a person remains an ETL write, distinct from that person's deliberate data edit. Accurate
write provenance is required independently of whether the business table exposes actor columns.
Engine-owned `CFG_` tables retain their mandatory config audit contract.

S5.O.3 adds overwrite protection only to SCD1/SCD2 tables:

- A human insert automatically protects the inserted business row. A human update automatically
  locks the changed business column values; whole-row protection can also be selected. Automated
  writes, including ERP, ETL and scheduled file ingestion, create no locks regardless of how many
  systems write the table. Every participating system still respects existing human locks. A lock follows the stable business key across SCD2 versions, rather than a generated row ID.
  Persist only override state and write/lock evidence; reuse table contracts and existing audit
  mechanisms, without mirroring business data or information schema.
- ETL respects active locks. A row lock prevents ETL updates, deletion, closure and replacement;
  a column lock preserves that value while allowing unlocked columns to change. Any locked
  value prevents ETL from deleting its row. Compare/hash the effective values after protection,
  so an attempted change only to locked values produces no spurious SCD2 version.
- A user with permission to edit that row/column can update a protected value. Keep protection
  on the new value unless explicitly released. Unlock is a separate authorized operation;
  releasing it permits the next ETL merge to apply source values. Record the previous and next
  lock state, actual actor/system, time, scope and upload/request identity.
- Manual SCD2 changes create a new version and preserve closed history. Carry active business-key
  locks forward and preserve creation attribution according to the SCD version lifecycle.
  Row-level update actors cannot substitute for column-level provenance when another system
  later changes an unlocked column.
- Upload/edit/unlock and ETL merge paths use the same target ownership and concurrency rules.
  Enforce protection at the write boundary, with atomic operations or version-checked conditional
  writes supported by the warehouse. Define recovery where data and provenance cannot commit
  together; do not claim distributed ACID. Refuse unsupported protection rather than overwrite
  on an unchecked/stale lock observation.
- External writers need an authenticated integration or supported database change/audit mechanism
  that records their actual identity and honors the same policy. Never infer a human owner from
  an arbitrary actor-column string, a shared service account, or an unexplained changed value.
  Unknown/unobserved writes are surfaced explicitly rather than attributed to a guessed user.

The UI shows a row/column lock symbol, protected scope, writer/owner, time and reason, with
permission-controlled Edit and Unlock actions. Pipeline Insights reports protected values/rows
and prevented overwrites separately from failures and normal unchanged rows. Ordinary non-SCD
tables keep their existing write behavior; they gain no automatic overwrite locks.

Acceptance covers optional ordinary-table actor columns, required protected-SCD actor fields,
ETL and external-system attribution, human inserts/updates versus automated uploads,
row and column locks, authorized edits/unlocks, denied edits, concurrent ETL/upload/unlock,
SCD1 updates/deletes/revival, SCD2 version continuity, hash/count correctness, rollback/recovery
and clear unsupported/unobserved-writer outcomes on each supported warehouse.

## PowerCenter-style operator controls

The live UI uses a workflow monitor model: select a pipeline, run, task or attempt and act on
that scope. Starting an individual task must not require rerunning the whole pipeline or an
Airflow-style clear operation. etl-craft does not support Airflow (S5.Y); no outside scheduler
limits native controls. PowerCenter supports task-level stop and independent session
recovery; use its [StopTask command](https://docs.informatica.com/data-integration/powercenter/10-4-0/command-reference/pmcmd-command-reference/stoptask.html)
and [session recovery strategies](https://docs.informatica.com/data-integration/powercenter/10-5-7/advanced-workflow-guide/workflow-recovery/configuring-task-recovery/task-recovery-strategies/session-task-strategies.html)
as concrete references for control granularity and recovery semantics.

| Scope | Planned UI actions | Current gap or restriction to review |
| --- | --- | --- |
| Pipeline definition | Run now; pause/resume automatic execution; schedule controls; one-off parameter overrides | Pipeline pause currently blocks manual starts too; add an authorized one-off action that leaves automatic execution paused |
| Selected pipeline run | Suspend/resume dispatch; graceful stop; abort; recover unfinished work; restart; start from a selected task | Pipeline cancel exists, but stop/abort and run-scoped suspension need distinct semantics |
| Task definition | Run this task only in an explicit standalone execution; run in a selected run | CLI/service `run_task` exists; no dedicated HTTP task-run route; avoid forcing an unrelated active pipeline context |
| Selected task run | Hold/release dispatch; run now; stop/abort just this task; recover/restart; rerun only; rerun with selected downstream scope | No task-only pause or stop/abort API; cancellation is currently whole-run; current flag combinations need intent-based review |
| Selected task result | Skip/mark with reason; reset a supported checkpoint/watermark; repair output using an explicit new revision/run | Preserve successful evidence and consumer decisions; offer a repair path instead of an unexplained dead end |
| Attempt | Logs, progress, owner, query/process identity and control-request status | Show request acceptance separately from worker/query acknowledgement and actual completion |
| Selected pipelines/tasks | Bulk actions restricted to the selected objects, with per-object outcomes | An action on one pipeline/task must not pause or cancel unrelated work |

Differentiate schedule pause, run suspension and task hold. Admission pause allows already
running work to finish; mid-task suspension requires a handler-supported durable checkpoint.
Where that checkpoint does not exist, expose graceful stop or restart explicitly instead of
pretending a process can safely freeze. A normal manual task run respects dependencies by
default, with a permitted, reasoned Run with current inputs action when operators need a bypass.
Run only and Run from here are distinct: the former starts no downstream work; the latter
previews and runs the selected continuation scope. Recovery preserves successful work and uses
actual durable checkpoints; restart is a separate choice with its data effects shown.

S5.Q reviews each refusal in request validation, interventions, run admission and task execution.
Classify it as an implementation limitation, an authorization requirement, or a data/ownership
invariant. Replace unnecessary whole-pipeline, paused-pipeline, remote-mode and flag-combination
restrictions with scoped, audited operations. Review the one-active-run rule for independent
manual executions and named instances, including target, checkpoint and consumption isolation,
rather than keeping a blanket restriction merely because the scheduler was built that way.
The local provider and agents use the same command contract.

Retain exact run/attempt identities, permission checks, live ownership/fencing, target locks,
immutable history, committed consumption decisions and SCD human-value protection. A force or
recovery action does not implicitly release a human data lock. State transitions must gain the
new supported operations rather than silently relabel live work or erase attempts. Every
remaining refusal names its reason and an available recovery action.

Shared services publish available actions and their reasons for the selected state. UI menus and
DAG-node actions use those capabilities; CLI/API/UI share the same semantics and auditing.
Commands return a durable request/run/attempt identity promptly, remain observable after browser
disconnection, and are idempotent on repeated clicks. Stop/abort follows the owning worker and
warehouse query until acknowledgement; a request alone is not displayed as a finished stop.
Show actor, reason, scope, parameters, affected tasks and whether successful work is preserved.
Use confirmation where a restart/abort/override changes data, not for routine navigation or
read-only inspection.

Acceptance is an action/state/capability matrix on SQLite/PostgreSQL and local/remote owners:
individual pipeline pause, isolated task execution while other work runs, task-only stop/abort,
suspend/resume, run-from-task, recovery/restart, dependency overrides, selected bulk operations,
permissions, repeated clicks, stale UI state, lost acknowledgements, restart durability,
checkpoint/data effects and preserved audit/consumer/human-lock evidence. Browser tests in
S7.A.2 verify those flows rather than only displaying buttons.

## UI elements and gap states

Reuse the catalog's CSS, SVG lineage and server-rendered UI design. Pages render the documents
of S5.AA read actions; every change goes through the same `/api/v1` route as the CLI. The generated
project documentation exposes the same states as a dated snapshot; the live UI exposes a
read-only warehouse comparison action. A check records time, warehouse identity, config/SQL
version and per-object outcome. A failed or inaccessible check never means matched.

| State | Appearance | Row explanation |
| --- | --- | --- |
| Deployed only | Amber background/badge and ⚠ | Present in warehouse; missing from contract |
| Config only | Red background/badge and ⚠ | Declared; missing from deployed table |
| Type mismatch | Purple background/badge and ⚠ | Declared and deployed types side by side |
| Lineage gap | Amber badge and ⚠ on affected column/node | Unresolved source/expression and the reason |
| Match | Neutral row and explicit Matched label | Compared successfully |
| Unchecked/unavailable/stale | Grey badge with explicit state | Never represent this as zero gaps or success |

Colour is supplementary: symbols have accessible text, and every state has a label and reason.
State the comparison coverage: a managed SQL output with inferred names but no declared types
can match column names while its types remain Not declared. Never label an unchecked attribute
matched or count it as a type mismatch merely because its contract is absent.
Keep engine-added columns visible, labelled Engine managed; compare against the action's engine
column contract rather than reporting expected audit fields as missing configuration. Compare
missing tables/views explicitly. A missing table can be Expected before first deployment;
show that state with its reason, rather than concealing it or claiming a match. Metadata queries
use the configured warehouse connection and read schema metadata only, not business rows.

| UI element | Contents and navigation |
| --- | --- |
| Project Overview | Run health, SLA misses, waiting/queued work, schema-gap and lineage-gap cards; check time |
| Pipeline Insights tab | Date/project/pipeline filters, built-in KPI cards, trend charts, task breakdown and latest runs |
| Schema Gaps tab | Filters by gap kind, object and pipeline; table/column, declared/deployed values, reason and affected tasks |
| Table detail | Side-by-side column contract/deployment grid, warning row styles, audit fields, upstream/downstream links; current lineage version and its history, with the column sources each version added and removed |
| Lineage tab | Warning nodes and affected edges; selected-column trace, unresolved reason, link to table comparison |
| Run detail | Attempts, row counts, rejects, wait duration and errors; links back to pipeline trends and table gaps |
| Metadata view | Table contracts grouped by catalog/schema, with child column contracts; read-only, with creation fields, actor/history, `validate` findings, the source config file, a drift badge for direct edits and standing-control badges; changes go through the config files and `etl-craft config apply`, or direct SQL edits in a `RELAXED` project ([Direct config edits](#direct-config-edits)) |

Project documentation needs both a summary gaps page and the per-column warnings, not only
console output. Static pages say when generated and when compared, and link each gap to its
table/task/lineage view. Export the structured gap records through the same service/API used
by these pages. Distinguish no source lineage for a constant expression from failed tracing;
script lineage is unknown unless explicitly declared, never fabricated from its target name.

## Wireframe approval before UI implementation

Owner decision, 2026-10-25: S7.A.0 is reviewed while S6 is in progress. The
[static console wireframes](https://github.com/venkatcg00/etl-craft/blob/main/docs/development/ui-wireframes/README.md)
cover the navigation/page inventory below, S6.B log availability and S6.F metric needs.
No S7.A page code precedes owner approval; merging that docs(plan) PR is the approval.
Missing action-table reads are explicit gaps, not invented endpoints. Access is managed through
S7.Y's admin access commands before S7.A; S7.B extends them to role bindings. Metadata views
remain read-only and never link access to a grants.csv definition.

## Workflow navigation: Databricks and PowerCenter

PowerCenter's Workflow Monitor sets the interaction: a navigator of folders and workflows, a
Gantt chart and a task view of the same runs, and right-click actions on the selected object.
Databricks Workflows sets the navigation: searchable job lists with tags, a run matrix, graph
and timeline views of a run, and a task side panel. They agree on selection-then-act, so they
combine: Databricks' views are more ways to select, and PowerCenter's scoped actions are the
only way to act.

| Element | From | etl-craft view and data |
| --- | --- | --- |
| Navigator | PowerCenter navigator; Databricks sidebar | Left tree: tags (as PowerCenter folders), then pipelines, then recent runs; a pipeline with several tags appears under each; only pipelines the account may view (S5.AD) |
| Global search | Databricks search | Pipelines, tasks, tables and run ids or keys from one box (`/` or Ctrl+K); results open the object's page |
| Breadcrumbs | Databricks | Project › pipeline › run › task run › attempt on every page; each part is a link |
| Pipeline list | Databricks jobs list | Name, tags, schedule (with `Allow_schedule` and pause state), next tick, last N runs as status dots, SLA state; filters by tag, status, schedule state and name; sortable |
| Pipeline side panel | Databricks job details | Schedule, timezone, catch-up, overlap, tags, `EMAIL_RECIPIENTS`, SLA, upstream and downstream pipelines, trigger tasks in and out (S5.AC), principal grants and effective CAN_VIEW/CAN_RUN/CAN_MANAGE levels with their sources (S7.Y), definition source file |
| Run matrix | Databricks matrix view | Tasks as rows, the latest runs as columns, a status cell per task run with duration; a cell selects that task run |
| Run-duration chart | Databricks runs chart | Bars of recent run durations coloured by status, above the run list; a bar selects its run |
| Runs list | Databricks job runs; PowerCenter task view | All runs or one pipeline's, filtered by status, trigger kind, date range, tag and run key; parent and child runs linked (S5.AC) |
| Run graph | Databricks graph view; PowerCenter workflow view | The DAG coloured by task status, waits and holds marked; a node selects its task run |
| Task timeline | Databricks timeline; PowerCenter Gantt chart | One bar per attempt on a time axis, with queue, gate and event wait segments apart from execution; a bar selects its attempt |
| Task side panel | Databricks task run panel; PowerCenter properties | Status, attempts with failure classes, parameters as run, rows and rejects, `explain`, log tail and download (S7.A.3), owner and host; opens beside the graph, matrix or timeline |
| Repair and rerun | Databricks repair run | Offered as S7.A.2's Recover, Run from here and Run task only, with the affected tasks shown before confirmation; no separate repair semantics |
| Run now with parameters | Databricks | S7.A.2's one-off parameter overrides, audited with the run |
| Live refresh | Databricks; PowerCenter monitor | Server-sent events update the open view; a paused stream says so |

Pipeline permissions follow Databricks job permissions (owner decision, 2026-10-26).
`CAN_VIEW`, `CAN_RUN` and `CAN_MANAGE` apply to one pipeline or tag in one project. A principal
is exactly one group, account or deployment role (`viewer` or `operator`, including holders of
higher deployment roles). Effective permission is the highest level from the account, active
groups and applicable role grants; projects default-deny without a project grant, and admins always have access. A pipeline
without an active pipeline/tag grant uses the project level; such grants elevate a pipeline only
when the account has at least CAN_VIEW on the project. Project CAN_MANAGE delegates any level
on that project or its pipelines/tags, never deployment admin actions. Missing and inaccessible
projects have the same reply across interfaces, including served catalog. Existing projects
upgrade with viewer/operator role grants preserving access; new projects have no grants. CAN_MANAGE includes running/controls and delegation
of any permission on that pipeline; managing individual pipelines does not authorize a tag
grant unless the account manages that tag. The S7.A access panel shows effective levels and
their sources, offering grant/revoke only to admins or the relevant manager. Revocation is
checked on the next request. Admitted runs retain group identities and recheck current account,
group and role permissions, including project access, before admitting or starting children; running children keep their
admitted work. Left out: Databricks' job/task editors, "Run as" and compute settings.
Definitions still change through developer config (or permitted RELAXED edits).


## Environments and governance

Owner decisions, 2026-10-25. Several environments may share one Engine DB (S5.G), one project
each: `support_insights_dev`, `support_insights_uat` and `support_insights_prod` can run on the
same engine with different roles and access. A project's environment and its rules are admin
settings, never developer configuration: a merged config change cannot make production easier
to change.

`CFG_PROJECTS` gains three columns:

| Column | Values | Meaning |
| --- | --- | --- |
| `ENVIRONMENT_TYPE` | `DEV`, `SIT`, `UAT`, `PROD`, `SANDBOX`, `CUSTOM` (an enum in `core.enums`) | What the project is for; shown as a badge on every page |
| `ENVIRONMENT_NAME` | Optional label, for example `perf` or `dr` | Names a `CUSTOM` environment, or refines another type |
| `GOVERNANCE` | `STRICT` (default) or `RELAXED` | Which interactive rules apply (below) |

A check constraint refuses `ENVIRONMENT_TYPE = 'PROD'` with `GOVERNANCE = 'RELAXED'`. Existing
projects upgrade as `CUSTOM` and `STRICT`: the upgrade does not guess what an environment is.

**What `RELAXED` relaxes.** This is the one switch for rapid development; nothing else changes it.

- *Direct config edits.* People may edit the package `CFG_` tables of that project with SQL in
  the Engine DB (next section).
- *Reasons.* Interventions accept an empty reason: marks, standing overrides, task pauses,
  holds and releases, stops and aborts, recover and restart, run with current inputs, offset
  and watermark resets, item controls, pipeline pause and resume, data edits and unlocks. The
  audit row records the actor, time and scope as always, with no reason.
- *Confirmations.* A typed confirmation (typing the run id or the table name) becomes a plain
  confirm. The scope preview is still shown before a data-changing action.

**What no setting relaxes.** Role and grant checks; the audit record of every action, with its
actor; Engine DB constraints; fail rather than guess; leases, fencing and target locks;
recorded consumption decisions; human data locks; secret handling; retention. Admin actions
on projects, environments, pools and access always need a reason, in every environment.

**Where it is checked.** In the `services/operations` action, which reads the selected
project's `GOVERNANCE`; the CLI's `--reason` becomes optional at the parser so the service
decides, and the API and the console use the same check. A refusal in a `STRICT` project names
the project, its environment and the remedy (`--reason`), never "argument required".

**Admin, not config.** Projects and pools leave the config files. `projects.csv` and
`pools.csv` are removed from `config plan`, `apply` and `export`, as S7.Y removes `grants.csv`;
leftover files are ignored as unknown inputs, never applied as team-owned tables. Admin
commands manage them (`etl-craft project add|set|list|disable`, `etl-craft pool
add|set|list|disable`), each an audited `services/operations` action with an API route and an
admin page. Developer config files left: `pipelines`, `tasks`, `task_parameters`,
`task_dependencies` always; `pipeline_dependencies`, `tables`, `columns`, `business_rules` when
used.

**Shared warehouses are allowed.** Environments may share one warehouse. Doctor and the admin
project page show an informational note when two projects' warehouse connections resolve to
the same database and schema; nothing blocks it.

**Access per environment.** S7.Y's access commands and S7.B's role bindings take a project
scope, so a developer can be an operator in DEV and a viewer in PROD.

## Direct config edits

Owner decision, 2026-10-25, for rapid testing. In a `RELAXED` project, a developer may change
package `CFG_` rows with SQL in the Engine DB, as some people find editing rows faster than
editing files. This is the one exception to "every write to etl-craft's Engine DB tables goes
through etl-craft"; the AGENTS.md rule changes with the implementation.

- *Triggers.* The actor-guard triggers allow an outside insert, update or soft retirement
  (`ACTIVE_FLAG = 'N'`) of a package `CFG_` row only when the row's project is `RELAXED`.
  They fill `CREATED_BY` and `UPDATED_BY` with `sql:<database user>` (`sql:direct` on SQLite,
  which has no users), stamp `UPDATED_DATE`, and write the typed `CFG_` history row exactly as
  `config apply` does. Hard deletes, `AUD_` tables, history tables and admin tables (projects,
  pools, accounts, groups, grants, tokens) stay refused everywhere.
- *Constraints.* Keys, foreign keys, allowed values and NOT NULL still refuse a bad row at once.
- *Candidate, not live.* A direct edit changes the definition the next pipeline refresh reads
  (below); runs keep using the last good version until refresh validates the change.
- *Drift.* Rows changed directly since the last apply from files carry a "changed in the Engine
  DB" badge on the metadata view; `config plan` lists the drift per row, and `config export`
  writes the current rows back to the CSV files. Drift is an accepted cost of rapid
  development (owner).
- *UI editing* is optional and, if built, uses the same switch and the same history.

## Pipeline refresh

Owner decision, 2026-10-25: a refresh like Airflow's DAG parser, except a broken pipeline never
disappears.

- One **Refresh** action per pipeline and per project re-reads its metadata rows, SQL files and
  scripts, then runs `validate`, the lineage check and the bundle check (S5.F). Config apply
  ends with a refresh of what it changed; the controller refreshes a project whose `CFG_` rows
  changed directly since its last refresh, so a SQL edit shows up without another step.
- A pipeline that fails refresh stays listed with an error badge. Its graph shows the broken
  tasks and edges in red, each finding with its file, line, value found and remedy, and the
  findings are listed on the pipeline page.
- The last good version stays active and runnable, labelled with its time, until a refresh
  passes. A per-pipeline setting, `BLOCK_ON_REFRESH_ERROR` (`N` by default), blocks new runs
  instead; this column lives in `pipelines.csv` because it is pipeline behaviour.
- Refresh results are recorded with the actor, the version checked and the findings, so the UI
  shows "last refreshed" and the history of refreshes.

## Standing task controls

Owner decision, 2026-10-25: operators change a task's behaviour without a config change. These
are controls, not metadata: they live in an audited control store, never in `CFG_` tables or
the config files, and they take precedence over the definition wherever the definition is shown.

- **Task pause.** A standing pause of one task across runs, like pipeline pause: runs reach the
  task and wait there, held, until it is resumed; other branches continue.
- **Standing outcome.** "Treat this task as `SKIPPED`, `SUCCESS` or `FAILED`" for every new run
  until removed. The task does not run; its downstream sees the chosen outcome. An overridden
  `SUCCESS` produces no data: `HAS_DATA` conditions see no data and consumers see no fresh output.
- Each has an actor, a reason (unless the project is `RELAXED`), a start and an optional expiry,
  and is removed by its opposite action. Runs record the control they applied.
- Views show the effective behaviour with an override badge: the metadata view, the pipeline
  graph, `explain` and the run that applied it. Home shows an "active overrides" count.
  `config plan` and `apply` note tasks with a standing control, so a definition change does not
  hide one.

## UI capability coverage

Owner, 2026-10-25: the UI is Databricks in layout and PowerCenter in capability, and must not
miss a button. Every action in the S5.AA action table has a place in the UI, or is listed as
console-only with its reason. The wireframes show the layout; this matrix is the record of
coverage, and an S7.A test generated from the action table fails when a non-host, non-agent
action has neither.

| Action | Object | UI place |
| --- | --- | --- |
| `RUN`, `TRIGGER_PIPELINE` | Pipeline, task | Pipeline header Run now and Run with parameters; task menu Run task only and Run from here |
| `CONTROL`: `HOLD`, `RELEASE`, `STOP`, `ABORT`, `RECOVER`, `RESTART`, `RUN_PAUSED`, `PAUSE`, `RESUME` | Pipeline, run, task run | Run header and task menu, each with its opposite next to it; pipeline header Pause and Resume, with the catch-up count; `RUN_PAUSED` as Run once while paused |
| `CONTROL_BULK` | Selected pipelines, runs or tasks | Bulk bar over multi-select in lists, matrix and graph, with per-object results |
| `MARK` | Task run | Task menu Mark as, with a scope preview |
| `ITEM_CONTROL` | Fan-out item | Items view row menu |
| `RECONCILE` | Run, attempt | Run header Reconcile when an attempt's lease expired |
| Standing task controls | Task | Task page and task menu: Pause task, Set standing outcome, Remove |
| Offset and watermark reset | `INGESTION` Python task, watermark SQL task (S7.AD) | Task page Offsets panel, with the stored value and the new one; absent on other tasks |
| Pipeline refresh | Pipeline, project | Pipeline header Refresh; project page Refresh all |
| `DATA_PROVENANCE`, `DATA_EDIT`, `DATA_UNLOCK` | Table row and column | Catalog table Data tab, lock symbols, Edit and Unlock |
| `LINEAGE_REFRESH`, `GENERATE_DOCS`, `DOCS_VERSION`, `VALIDATE` | Project, pipeline | Catalog header and pipeline page |
| `AGENT_CONTROL` | Agent | Agents page row menu |
| Groups, accounts, tokens | Admin identities | Admin pages with list, add, change and disable; account unlock/password reset (S7.A.7) |
| Access | Project pipelines and tags | Effective levels and group/account/role sources; admins or relevant CAN_MANAGE holders grant/revoke; bulk is admin-only (S7.Y) |
| Projects and pools | Admin objects | Audited, reasoned add/set/list/disable commands and pages (S7.Y); S7.Z adds environment/governance fields |
| Announcements | Deployment | Admin announcement page; the banner on every page (S7.A.7) |
| `PURGE`, `REHASH`, upgrade targets, `CLONE` | Project, table | Admin maintenance page, each with a dry-run preview first |
| `WHOAMI`, `LOGOUT`, `REFRESH`, password change, personal settings | Session, own account | Profile menu in the top bar and the settings page (S7.A.7) |
| Read actions (`STATUS`, `EXPLAIN`, `HISTORY`, `AUDIT`, `ATTEMPT`, `LOGS`, `ITEMS`, `HEALTH`, `CAPABILITIES`, …) | Their object | The view that renders them |

Views the wireframes lack and S7.A builds: task-run statistics (rows in, inserted, updated,
deleted and rejected, throughput, `SQL_SCRIPT` statements); scheduled runs (what runs next and
when); system health (controller lease, Engine DB, warehouses, agents); held and paused work;
runs filtered by agent, pool and failure class; admin and maintenance pages.

## Usability rules

Drawn from the problems Airflow users report most (log failures, the Clear dialog, the grid
losing selection, slow pages, missing overviews), as rules for every S7.A page:

- **Logs.** A failure summary first, with jump to the error; never a bare "not found": say
  pending, removed by retention or unreadable, with the reason (S6.B).
- **Actions.** Every action previews its exact scope before it runs, in words and counts, with
  no flag checkboxes to combine. Actions the role lacks are hidden; actions the state forbids
  are disabled with the reason. Each action sits next to its opposite. Requests show
  requested, acknowledged, completed or refused. A "changed since you opened this" warning
  stops acting on stale state. Every action shows its CLI line.
- **Selection.** A click selects and the selection stays through refresh and scrolling; sticky
  headers; full names on hover and in the panel, never only truncated.
- **Speed.** Reads are bounded and paged, with a performance budget per view.
- **Plain words.** Why a run or task is waiting, in one sentence; a one-line run summary;
  pause and resume show how many runs will catch up; every time shows its timezone, and the
  run date is labelled apart from the start time; fan-out items appear by key.
- **Findings on the object.** Validation and refresh findings appear on the pipeline, task or
  table they concern.
- **Finding things.** Global search with `/` and Ctrl+K, pins, saved filters.
- **Status.** System health and maintenance banners; inline help on each page.
- **Personal settings** are limited to theme, timezone, default project, pins and filters.
- **Accessible.** WCAG AA contrast and keyboard-only operation.

## Built-in dashboard calculations

Reuse `run_kpis` where its semantics apply. Query bounded date ranges through repository
aggregates; paginate run details rather than loading every audit row into the UI. Generated
docs show their selected snapshot interval; the live dashboard uses filters and explicit refresh.

| Panel | Calculation/constraint |
| --- | --- |
| Runs and success rate | One logical pipeline run, not one retry attempt; show all status categories; rate uses terminal runs |
| Duration and SLA | End minus start for terminal runs; live elapsed time separate; average duration and trend, SLA misses |
| Processed rows | Successful task counts by selected task/stage; never sum successive stages as unique pipeline input |
| Inserts/updates/deletes/rejects | Label handler-reported counts by action; SCD2 inserts include newly opened update versions |
| Valid/invalid and quality | Explicit measured stage and denominator; aggregate valid/eligible counts, not average percentages |
| Retries and failures | Attempts separate from logical runs; error drill-down; latest ten runs link to details |
| Waiting and throughput | Wait/queue duration separate from execution duration; processed rows per measured execution interval |
| Schema and lineage health | Separate counts by gap kind and check state; never count an unavailable comparison as healthy |

The Support Insights source-level grouping becomes an optional declared source/task grouping.
Absent valid/eligible or duplicate evidence displays Not measured, not zero or an invented
`source_count - target_count`. A rejected row, a failed business rule and an SCD2 historical
version are different measurements. Drill-down retains run/task/stage and count provenance.

## Delivery order and acceptance

1. **S5.O Normalized metadata and lifecycle invariants**, ahead of S5.C: inventory every CFG,
   audit/state object reference and composite parameter; implement table/column contracts, common fields,
   uniqueness, histories, source/target references and SCD1 lifecycle upgrade. Reach both Engine
   DB schemas and new migrations, repositories, actor/history triggers, services, SQL handlers,
   config CSVs/import/export/as-of, CLI/API, demo seeds, release upgrade fixtures and documentation.
2. **S5.P Project documentation gap UI**: structured comparison/lineage gaps, project summary,
   table row colours/icons, graph warnings and links. Reuse current comparison and rendering;
   retain offline generation and explicit unchecked/unavailable states.
3. **S7.A.1 Pipeline Insights UI**: shared metric services and generated snapshot panels, then
   the live server tab, filters, refresh and drill-down under existing authentication/roles.
   This is mandatory built-in operational UI, not a dashboard each adopting team must create.
4. Resume S5.C worker work after S5.O/P. S7.A.1 depends on the server UI shell; it must precede
   the Support Insights dashboard acceptance, which also verifies parity of operational metrics.

Migration uses new numbered migrations; never rewrite released or already merged migrations.
Preflight parsing, identity conflicts and audit NULLs before any change; backfill known actors
and timestamps from existing evidence. Where unavailable, record explicit migration provenance
instead of claiming the original author/date is known. Preserve old IDs through an audited
mapping, history/as-of exports and cache invalidation. Convert existing config inputs with an
explicit report; canonical exports contain separate entities/components. Any temporary legacy
import is confined to that conversion boundary and has a documented removal version. Do not
maintain two writable sources of truth. Prove fresh installs and upgraded databases equivalent.

Acceptance must cover SQLite/PostgreSQL PK/FK/uniqueness/NOT NULL enforcement, atomic rollback,
actor attribution, inactive-row reactivation, historical exports, ambiguous/quoted names and
released-schema upgrades. Warehouse acceptance covers added/missing/retyped columns, absent
objects, engine columns and inaccessible checks. SCD acceptance covers unchanged/update/delete/
revival and SCD2 version closure across supported warehouses. UI tests assert symbols, text,
row state, graph warnings, filters and links; browser tests check keyboard access and colour
contrast. Metric fixtures include retries, zero denominators, unknown counts and multi-stage
pipelines; compare every built-in operational panel to an independent audit query.

S5.O.1 is complete. Remaining items are complete only when their corresponding migrations,
exports, services and UI acceptance pass; recording the design does not close them.
