# Metadata integrity and built-in pipeline insights

This is the corrective design following source definitions, before worker-agent work resumes.
S5.O.1 implements the SCD lifecycle and legacy upgrades. S5.O.2a enforces common config fields and safe token history. The normalized object schema,
coloured gap indicators and dashboard remain planned. The frozen 0.4 soak deployment remains unchanged.

## Evidence and current gaps

| Area | Current implementation | Required correction |
| --- | --- | --- |
| Source metadata | `CFG_SOURCE_COLUMNS` repeats a dotted `TABLE_NAME` on every column; no parent table FK | Separate table contracts and their column contracts; split object name components |
| Config audit fields | Core config fields are mandatory; project config DDL requires a single PK, typed non-null base fields and valid active values | Common base contract implemented in S5.O.2a; scoped identity/FK normalization remains S5.O.2b |
| API tokens | Common actor/date/active fields and safe typed history, retaining expiry and revocation; hashes remain excluded | S5.O.2a implemented; upgrade the Engine DB before using it |
| Object references | Rule targets, lineage endpoints and target state use combined names; task target is a parameter string | Replace persisted object identity with relational references and separate identifier components |
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
JSON values, but object identity is not hidden in JSON. Key lists and dependency relationships
use ordered child rows rather than delimiter-separated scalars. Inventory all task parameters
and audit/state tables before migration, classifying SQL expressions, paths and opaque tokens
separately from composite relational values; dots inside those strings do not make them object
references. Render qualified SQL identifiers only at the warehouse execution boundary.

Read deployed columns, types, constraints and ordinals through existing warehouse metadata
queries when comparison is requested. Do not maintain editable deployed-schema copies. Reuse
existing audit/cache mechanisms for a dated comparison result only if needed to show the last
check; identify its timestamp/version and replace stale observations without changing the
declared contract. Each proposed new table must have independently owned data, a concrete
writer/reader and a requirement that an existing entity or derived query cannot meet.

Historical lineage evidence retains separate source/target catalog, schema, table and column
names as they were computed, with task, computation time and contract/SQL version. Live joins
may additionally reference metadata IDs. Later renames or retirement must not rewrite old
evidence or remove it through cascading deletes. Cache keys use the complete ordered contract.

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
Airflow-style clear operation. Airflow remains an integration adapter, rather than the model
that limits native controls. PowerCenter supports task-level stop and independent session
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
Local and owned remote workers use the same command contract; legacy external-orchestrator
mode needs an adapter/capability outcome rather than an unconditional native-UI dead end.

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

Reuse the catalog's CSS, SVG lineage, service layer and server-rendered UI design. The generated
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
| Table detail | Side-by-side column contract/deployment grid, warning row styles, audit fields, upstream/downstream links |
| Lineage tab | Warning nodes and affected edges; selected-column trace, unresolved reason, link to table comparison |
| Run detail | Attempts, row counts, rejects, wait duration and errors; links back to pipeline trends and table gaps |
| Metadata editor | Table contracts grouped by catalog/schema, with child column contracts; immutable creation fields, actor/history and validate-on-save |

Project documentation needs both a summary gaps page and the per-column warnings, not only
console output. Static pages say when generated and when compared, and link each gap to its
table/task/lineage view. Export the structured gap records through the same service/API used
by these pages. Distinguish no source lineage for a constant expression from failed tracing;
script lineage is unknown unless explicitly declared, never fabricated from its target name.

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
