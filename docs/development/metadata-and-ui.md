# Metadata integrity and built-in pipeline insights

This is the corrective design following source definitions, before worker-agent work resumes.
S5.O.1 implements the SCD lifecycle and legacy upgrades. The normalized metadata schema,
coloured gap indicators and dashboard remain planned. The frozen 0.4 soak deployment remains unchanged.

## Evidence and current gaps

| Area | Current implementation | Required correction |
| --- | --- | --- |
| Source metadata | `CFG_SOURCE_COLUMNS` repeats a dotted `TABLE_NAME` on every column; no parent table FK | Separate table contracts and their column contracts; split object name components |
| Config audit fields | Pipelines, tasks, parameters, dependencies, rules and source columns have keys and actor/date fields, but audit fields allow NULL | Enforce one mandatory base contract across every `CFG_` table |
| API tokens | PK and creation actor/time, expiry and revocation; no common update fields or active flag | Add the common base fields while retaining token lifecycle fields |
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

For SCD1 and SCD2 targets keep `CREATE_DATE`, `CREATED_BY`, `UPDATE_DATE`, `UPDATED_BY`,
`ACTIVE_FLAG` and `DELETE_FLAG`, plus existing run/hash/row identity fields. SCD1 insert/update
leaves a live row active and not deleted; soft delete sets inactive/deleted; revival restores
active/not-deleted. SCD2 active means the current live version: closing a version makes it
inactive without labelling a normal historical version deleted; deleting a current row makes
it inactive/deleted; revival opens a new active/not-deleted version. Preserve creation stamps,
stamp mutations, and leave unchanged rows untouched. Existing SCD1 targets need a controlled
upgrade deriving active state from delete state, under the existing target lock. Do not assume
every historical inactive SCD2 row represents a deletion.

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
