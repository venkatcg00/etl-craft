# Changelog

All notable changes are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed

- Snowflake renames use the fully qualified destination, keeping Iceberg replacements and their
  recovery copies in the target schema even when the connection uses a different default schema.

- Databricks identity replacements accept string column collations inherited from the preserved
  table default, including current `SHOW CREATE TABLE` output. Explicit overrides, changed
  candidate collations and other business column constraints remain refused before publication.

- `Docs_site.Schedule` uses the same cron validation as pipeline schedules, rejecting invalid
  ranges and impossible calendar dates; both accept `@yearly` and `@annually`.

- CLI initialization accepts its exact queued run when the server admitted it concurrently,
  without rewriting gate decisions or taking over a different run.

### Changed

- Run commands exit 22 when work remains unfinished and 23 when a single task recorded nothing
  because its dependencies are not met. A paused pipeline that starts nothing and an orchestrator
  `--init-only` step retain exit 0; a stopped backfill exits 22 unless it failed or was cancelled.

- Task completion passes one frozen count record through the handler and guarded transitions.
  Run selection and pipeline graph loading share their existing rules; dependency readers always
  return the stored repair policy. Direct query-to-record mappings preserve Boolean JSON values.
  Authentication fields are checked once by configuration parsing before connections are built.
  Python lifecycle callers use `resolve_run` and `counts=Counts(...)` for task completion.

- Shared UTC conversion, enum parsing, credential masking and warehouse column discovery replace
  duplicated helpers. Email TLS modes accept case-insensitive values. Warehouse session setup
  closes cursors on failure and retains PostgreSQL's commit and Iceberg extension setup.
  Engine dialects own metadata trigger refresh; migration policy markers replace filename checks
  without changing existing SQL bytes or checksums.

- Service operations take pipeline codes directly and build inspection documents from stored rows
  without intermediate summary models or duplicate reads. Run and mark calls share their audit
  dispatch; pause and resume share one service and CLI implementation. JSON schemas and CLI
  text remain unchanged. Python callers use `set_pause(..., verb="pause")` or `verb="resume"`.

- Removed unused internal helpers and lifecycle shortcuts. Tests now use the production
  attempt queue/claim/finish path and atomic dependency consumption; test scene setup stays
  in fixtures. No configuration, schema or command behavior changes.

- CI validates pull requests without repeating after merge. Twenty consecutive chaos passes per
  Engine DB run in release validation after the evidence check, rather than on every PR.

- Execution identity names have one meaning across SQL, ingestion, business rules and email:
  `pipeline_id` is the definition, `pipeline_run_id` the pipeline execution, and `task_run_id` the
  task execution. SQL uses matching substitution switches. Migrate SQL and email templates that
  used `$$pipeline_id` for a run to `$$pipeline_run_id`; rename `PIPELINE_ID_SUBSTITUTION` to
  `PIPELINE_RUN_ID_SUBSTITUTION` for those SQL tasks. Rename `$$pipeline_id_filter` and
  `PIPELINE_ID_FILTER` to `$$pipeline_run_id_filter` and `PIPELINE_RUN_ID_FILTER`.

### Added

- Generated DAG YAML carries version 1 and validates against its packaged JSON Schema. A separate
  `etl-craft-airflow` distribution loads pipeline, global and docs exports as real Airflow DAGs
  on Airflow 2.11.x and 3.3.x, preserving their commands, actor identity, graph and trigger rules.

- Optional authenticated HTTP API and OpenAPI contract, served with the local overseer;
  print-once bearer tokens with viewer, operator and admin roles and immediate revocation.

- Read-only `status` and `explain` commands with exact run selection, task summaries, dependency
  reasons, persisted waits and retry schedules. JSON output now covers inspection, `validate`,
  `doctor` and `lineage` through the shared service serializer.

- Local automatic task retries with persisted due times and budgets, capped exponential backoff,
  retryability recorded by the child, and fenced claims. Delayed attempts consume no worker slots;
  cancelled attempts and SQL guards never retry. Remote retries remain owned by the orchestrator.
  Upgrade the Engine DB with migration `0017_retries`.

- Ready-task scheduling shared by the local CLI and server: each completion releases ready
  downstream work immediately. Gate waits persist deadlines and look counts in `AUD_GATE_WAITS`
  without holding task workers, and survive restarts. Upgrade with migration `0016_gate_waits`.
  The server uses one bounded task pool and cooperative pipeline supervision. Gate averages
  exclude stand-in runs as well as backfills.

- Local server schedules support five-field cron, timezone-aware ticks, bounded catch-up,
  overlap policies and durable queued runs with exact UTC tick keys. Upgrade the Engine DB
  for migration `0015_schedules`; validate schedule metadata before starting the server.
  Manual run dates default to `Orchestration.Timezone` (UTC unless configured), while audit
  timestamps stay UTC. Generated DAGs preserve schedule timezone and logical-date semantics.

- `etl-craft server` supervises active local runs under one deployment leader, with PostgreSQL
  session locking and committed execution notifications, SQLite file locking, bounded active
  graph caching, exact-run crash recovery and configurable graceful shutdown. Upgrade the
  Engine DB for the new `AUD_OVERSEERS` process history.

- Actor-scoped Python operations for run lifecycle, tasks, pipeline controls, backfills and
  inspection, with frozen views and canonical execution identities. The corresponding CLI
  commands use `--format json` to return the same schema-versioned documents. Requests remain
  append-only and CLI delegation records each request once; related views share a stable
  database snapshot.

- A chaos suite for concurrent admission, process loss, fenced recovery, operator races and
  remote deliveries, including a five-second PostgreSQL outage. Release validation requires twenty
  consecutive passes per Engine DB and uploads each iteration's results.

- Consistent `PIPELINE_ID`, `PIPELINE_RUN_ID` and `TASK_RUN_ID` audit columns on every SQL target,
  with those identities also exposed to ingestion scripts and business-rule SQL. Append retries
  replace only their task run's batch under the target lock; legacy append targets warn on every
  write. `etl-craft upgrade-targets [--action APPEND_TABLE] [--target S.T] [--dry-run]` adds missing
  nullable identity columns to configured SQL and ingestion targets without changing history.

- SQL validation refuses active writers that resolve different table formats for the same
  qualified target, including writers in other pipelines. Creation, setup, overwrite and merges
  check the existing Databricks Delta/UniForm or Snowflake native/Iceberg format before staging
  or changing the target; format changes require an explicit migration or another target.

- Native cloud SQL targets use generated `ROW_ID` keys: Databricks Delta and UniForm identity
  columns and ordered Snowflake autoincrement. Atomic clone publication keeps identity-backed
  `CREATE_TABLE` replacement safe; overwrites and schema evolution retain the generator.
  Older computed-key targets remain writable. Concurrent Trino append processes share the
  qualified target lock through warehouse commit, with SQLite and PostgreSQL Engine DB coverage.

- SQL schema evolution adds nullable columns through `ALTER TABLE`, retaining existing rows,
  keys, comments, dependencies, storage locations and supported table properties. Complete
  warehouse types preserve precision, scale and supported lengths and nested types. Opted-in
  evolution refuses existing-column type changes before DDL; DuckDB Iceberg nested additions
  are refused with a catalog-engine remedy. Partial non-transactional additions can be retried.

- Safe SQL table replacement: native PostgreSQL and DuckDB roll back failed replacements;
  Snowflake, Databricks and Trino use atomic publication where supported. DuckDB and Snowflake
  Iceberg retain recovery tables and restore failed writes, leaving backups available if
  restoration also fails. Table comments and supported properties survive; unsupported layouts
  are refused before publication.

- Joined SCD1 updates and SCD2 version closing use warehouse-specific set-based statements.
  PostgreSQL indexes and analyzes merge stages; a 100,000-row SCD1 regression checks the
  30-second performance budget. Composite keys, NULL preservation and unchanged-row audit
  fields retain their behavior across native and Iceberg warehouses.

- Canonical version-2 merge hashes distinguish NULL, empty text and embedded separators, and
  normalize UTC timestamps, decimal scales and booleans across warehouses. Floating-point
  compare columns are refused. `etl-craft rehash --target S.T [--dry-run]` upgrades all stored
  hashes, including SCD2 history, and migration `0011_target_hash_version.sql` tracks the contract
  published after warehouse commits. Target locks coordinate SQL mutations and hash upgrades.

- Atomic Engine DB endings: successful attempts commit their outcome, summary, returned Python
  offset and recorded task-dependency consumption together. Pipeline endings commit status, SLA
  and recorded pipeline-dependency consumption together, before finalization hooks run.

- Recorded gate decisions at pipeline and task-attempt admission. Successful downstream work
  consumes the exact selected upstream identities and revisions, including after upstream repairs
  or metadata changes. Dependencies accept newer published revisions of the same upstream run
  when `CONSUME_REPAIRS = 'Y'`; `N` accepts only newer run identities. Migration
  `0010_gate_repairs.sql` tracks pending repairs so failed repairs do not publish a revision.

- Run and attempt ownership with 60-second leases and 15-second heartbeats. Every run reconciles
  expired attempts; `etl-craft reconcile` also supports pipeline and task filters. Lost attempts
  preserve uncertain outcomes, verified local child processes are stopped, and expired idle
  runs resume under their original identity. `mark --stale` reconciles before marking and
  refuses live leases. Children receive explicit attempt and owner identities.

- Explicit `--run-id` and `--run-key` selection for run, mark, cancel, history and steps.
  Completed runs require an explicit identity; `history --all` lists runs. Pipeline tasks
  receive their parent's selected run. Remote DAG steps share an orchestrator run key and
  logical date, so clearing an older DAG run reopens that run. Remote generation requires
  an Airflow version floor of 2.2.0 (`generate-yml --airflow-version`).

- Centralized guarded run and attempt transitions, with `StaleTransitionError` (exit 20).
  Execution records immutable attempt outcomes and updates their task summaries atomically;
  concurrent admission, wrong owners and superseded results are refused. Reopening records
  its intervention in the same transaction. Migration `0009_preserve_request_actors.sql`
  keeps unknown historical requesters unknown when their attempts change.

- Actor identities on run starts and endings, attempts, interventions and pauses; immutable
  command requests and metadata before/after history, with `etl-craft audit` filters. Migration
  `0008_actors_and_audit_guards.sql` protects audit and metadata writes on SQLite and PostgreSQL.
  Generated remote DAGs pass their actor, `setup --print-grants` prints deployment role SQL,
  and `doctor` checks extra write grants and SQLite file permissions.

- Run identities and trigger kinds, owner and lease fields, output revisions and configuration
  fingerprints; attempt and gate-decision tables, consumed revisions and repair-consumption flags.
  Migration `0007_identity.sql` preserves historical run identities and copies the latest known
  execution attempts. New runs receive manual, backfill or stand-in keys. Admission records gate decisions and consumes their selected revisions.

### Fixed

- The catalog's search builds links only from the site's own relative pages: a result whose path
  is not one falls back to the home page, and the page's way back to the site root must be made
  of `../` steps.

## [0.2.0] - 2026-10-04

### Added

- Named development fault points, a synchronized admission-race helper and a reusable real-CLI
  process harness. Stabilization defects have an explicit regression mapping checked by CI.

- Secrets files accept UTF-8 byte-order marks and `export KEY=VALUE`; missing secret variables
  include suggestions for similarly named keys.

- SMTP `tls_mode`: verified STARTTLS (the default), implicit TLS (`ssl`), or unauthenticated
  plain SMTP (`none`), with an optional project-relative `ca_file`. `use_tls` stays supported.
- Run history and KPIs in the catalog: each pipeline's and task's latest 30 runs, with success
  rates, failures, SLA misses, average and longest durations, average rows written, and a bar
  per run coloured by status; a page per run with its tasks, interventions, and the upstream
  runs it was built from and the downstream runs that used it, from the consumption log. The
  DAGs tab shows each pipeline's success rate and average run.

- `AUD_DEPENDENCY_CONSUMPTION`, an append-only log of every upstream run consumed through a
  cross-pipeline dependency: one row per downstream run (or task), dependency and upstream run.
  A dependency's latest row is what it last consumed, so gates behave as before, and the log
  says which upstream runs each run was built from. Migration `0003_dependency_consumption.sql`
  carries each tracker's last consumed run into it and drops `AUD_PIPELINE_DEPENDENCY_TRACKER`
  and `AUD_TASK_DEPENDENCY_TRACKER`.
- `Orchestration.Gate_wait_minutes`: how long a cross-pipeline gate waits for a running upstream
  (60 unless set; `0` judges at once).

- `etl-craft pause` and `resume`: while a pipeline is paused, `run` starts nothing of it and
  exits `0`, and a run in progress starts no more tasks, to go on once resumed. `list` and the
  catalog show paused pipelines; `AUD_PIPELINE_PAUSES` keeps every pause.
- `etl-craft run --skip --reason`: records a run `SKIPPED` on purpose, for the pipelines that
  depend on it to see.
- Run dates and backfills: every run runs as of a date (`AUD_PIPELINES_RUN_LOG.RUN_DATE`): the
  day it started, `run --run-date`, or `--init-only --run-date`, which generated DAGs pass the
  orchestrator's date with. SQL tasks read it as `$$run_date` (with `RUN_DATE_SUBSTITUTION`),
  scripts as `task.run_date`. `run --backfill FROM:TO --reason` runs the pipeline once per date,
  as backfill runs that check no cross-pipeline gate, consume nothing, and neither read nor store
  script offsets; `history` shows each run's date. Migration `0002_run_date.sql` adds the columns
  and dates the earlier runs by the day they started.
- The Engine DB's first migration, `0001_pipeline_pauses.sql`: `etl-craft setup` (or `migrate`)
  applies it to an Engine DB made by 0.1.0. A test upgrades each released schema and compares
  it with a new one.

### Fixed

- Released documentation versions use the maintained API page renderer against their own
  tagged source, so API landing and package pages receive documentation corrections. Every
  version and the `latest` alias pass strict builds and rendered API content checks.

- Fresh initialization creates the Engine DB schema and packaged migration ledger atomically
  under the migration lock. Migration `0006_run_backfill_constraint.sql` aligns the named
  backfill constraint with fresh databases on PostgreSQL and SQLite, preserving run history.
- PostgreSQL lock errors distinguish contention from connection failures; remote initialization
  conflicts name `--finalize-only`, and `--force` refuses cancelled runs with a new-run remedy.
- Cancellation watcher logs retain run/task context. Lineage caches include the active catalog
  and sqlglot version. Versioned docs subprocesses find tools beside their Python interpreter.
- SQL substitutions preserve tokens inside strings, quoted identifiers and comments; the
  shared statement scanner also handles nested comments and PostgreSQL escaped strings.

- Clearing business-rule flags sends at most 1,000 keys per Engine DB statement, avoiding
  parameter-limit failures for large sets on SQLite and PostgreSQL. All batches share the
  rule's transaction, so a failure cannot leave partially cleared flags or a false success.

- Release evidence must name the exact required marker and cover every test collected on HEAD,
  with one passing outcome per test and consistent counts. Subsets, extra tests and selection
  overrides cannot satisfy the gate; platform exceptions require explicit suite declarations.

- Pipeline and task codes use one database-enforced rule: an ASCII letter followed by letters,
  digits or underscores, at most 128 characters. Migration reports invalid legacy codes before
  applying anything; generated commands quote arguments and refuse invalid codes and control steps.
- Published catalog GET and HEAD requests reject encoded hidden paths, traversal and symlinks
  outside the site, including directory indexes.
- Python API landing and package pages link to their modules and the scripting contracts.
  Documentation builds check rendered API content as well as strict MkDocs validation.

- Duplicate configuration keys, empty secret variables, credential-bearing JDBC query keys,
  zero task concurrency and invalid TCP ports are refused with setting-specific errors.
- Key and certificate paths, secrets files and SQLite databases resolve beside the config,
  including symlinked configs. Doctor checks configured files for readability.
- DuckDB Iceberg storage secrets are retained as variable names and resolved for each new
  connection; HTTP and SDK INFO logs are suppressed in routine attempt logs.

- Email subjects collapse whitespace, drop control characters and cap their length at 200
  characters; the body keeps the full error. Header failures name the transport and step.
- SMTP verifies certificates and hostnames and refuses password or OAuth login without TLS.
- Partial recipient refusal succeeds with `EMAIL_WARNING` in the task audit and a WARNING in
  the attempt log; total refusal fails the task.
- Ingestion scripts load as registered modules with their own future imports, so dataclasses,
  enums, pickling, type-hint resolution and fork process pools work. Timestamp offsets refuse
  nanoseconds the store cannot keep, and invalid result variables fail before an offset is saved.
- A transaction on a SQLite Engine DB is now all or nothing. The driver no longer manages
  transactions itself; every transaction opens with `BEGIN IMMEDIATE`, so a savepoint that was a
  transaction's first write no longer commits early, DDL rolls back with its transaction, and a
  failure between starting a run and recording that its gate refused it can no longer leave the
  run `IN-PROGRESS` to be resumed without the gate.
- A run is never taken over, skipped or ended by a process that does not own it. Starting a run
  never hands back one another process started; a refused gate skips only the run it started; and
  a run with a task still `IN-PROGRESS` in another process stays `IN-PROGRESS`, naming the task.
  `mark --task_code X --status FAILED --stale --reason ...` releases a task whose process is gone.
- Backfills and scheduled runs are kept apart. A plain `run` does not resume a backfill's run and
  a backfill does not take over a scheduled run; a backfill that meets another run between dates
  stops with the command that takes up from there. Dependency gates never see backfill runs, and
  a backfill counts a `FAILURE` dependency on another pipeline as not met. Run-length averages use
  only `SUCCESS` and `FAILED` runs that are not backfills.
- `run --task_code --rerun` of a task whose failure ended the run leaves the tasks skipped because
  of it to run, instead of ending the run `SUCCESS` without them. Resetting skipped tasks (by
  `mark` or a rerun) keeps a `SKIPPED` row another pipeline's run consumed, naming that run.
- A task's settings are checked before its row is bound. When its process cannot start, or the
  command is interrupted, the attempt is recorded `FAILED` with the cause instead of being left
  `IN-PROGRESS`; recording an attempt's outcome retries a dropped Engine DB connection.
- A task that already ended `FAILED` is never rewritten `SKIPPED`, and a refused `--rerun` no
  longer reopens the run.
- A run skipped on purpose counts as ended: `run --task_code` refuses it like any ended run.
- A run's SLA is decided once. Ending a reopened run again keeps the `MET` or `BREACHED` it had,
  and sends no breach email for it.
- A run cancelled while it was being ended stays `CANCELLED`: nothing is consumed and no
  end-of-run hook runs.
- `SIGTERM` or `SIGHUP` to `etl-craft run` stops the task processes it started and records their
  attempts `FAILED` with the signal, on `run --task_code` and `--rerun` as on whole-pipeline runs
  and backfills.
- A task process exits as soon as its outcome is recorded, instead of holding the task's slot
  until a thread its script left running ends; the threads are named in the attempt's log.
- A task process writes its output unbuffered and flushes it when stopped, so the attempt's log
  keeps what a script printed before it hung or timed out.

- SQL tasks check the SELECT's columns before touching the target: a column the engine writes
  itself (`PIPELINE_RUN_ID`, `ROW_ID`, the audit columns), as a `SELECT *` over an engine-written
  table returns, or a name that needs quoting, fails the task naming it. `validate` reports the
  first when the SELECT lists its columns.
- A NULL in a merge key column fails `SCD1_MERGE`, `SCD2_MERGE` and `DELETE_ROWS` before the
  target changes, instead of inserting the row again on every run.
- A `MERGE_DEDUPE_ORDER` that ties between rows that differ fails the task instead of keeping
  one of them at random.
- A SELECT ending in a `--` comment works with every action, as one ending in `;` did.
- Scratch tables carry a token per attempt and, where the warehouse has no temporary tables, live
  in the target's schema, so a table of the same name elsewhere is never read as one.
- A soft `DELETE_ROWS` leaves rows already flagged deleted as they were, and a merge brings a
  soft-deleted key back when its SELECT returns it again.
- A business rule clears the flag of a key with no row left in its table, and judges only the
  active versions of a table with `ACTIVE_FLAG`.

### Changed

- A failed email preflight is logged and data tasks may still run. Failed `EMAIL_ALERT` tasks
  stay failed and are logged at ERROR, but data tasks determine a data pipeline's final outcome.
  Pipelines containing only alerts still use their alert tasks' outcomes.
- `HAS_DATA` means the upstream wrote rows: inserted, updated or deleted, as the new
  `AUD_TASK_RUN_LOG.ROWS_WRITTEN` records (migration `0004_rows_written.sql`). An append of no
  rows, or a merge that changed nothing, no longer satisfies it because the target holds rows.
  Rows recorded before the migration are judged by `TARGET_COUNT`; `mark --rows` sets both.
- A local run whose tasks were all `SKIPPED` ends `SKIPPED`, as in remote mode, so a pipeline
  that depends on it with `SUCCESS` is skipped too.
- `run --task_code --force` onto an ended run reopens it, records the `REOPEN`, and ends it again
  from its tasks' statuses after the task: a forced task that fails leaves the run `FAILED`.

### Verified

- All 14 required release suites passed from clean commit `63e1c6d`, with 2,118 passing
  test outcomes and no skips. The built wheel was exercised by pip, uv, package checks and
  the complete Databricks and Snowflake suites, including both table formats. The evidence
  and artifact checksums are in `release/evidence/0.2.0/`.

## [0.1.0] - 2026-09-26

The first release: a rewrite of the earlier implementation (kept at the `archive/iteration-2`
tag) with the same design, in a layered package with its own tests, documentation and release
evidence.

### Pipelines as metadata

- Pipelines, tasks, their parameters, dependencies and business rules are rows in the Engine
  DB's `CFG_` tables, which your team writes and etl-craft only reads; history is written to
  the `AUD_` tables. The Engine DB is a SQLite file or a PostgreSQL schema, created by
  `etl-craft setup` (or `init-db`) and carried forward by `migrate`, with your project's own
  migrations beside the packaged ones.
- `craft-connector.yml`, written by your team, holds the connections, per profile (`dev`,
  `prod`, ...). Every setting is a variable or a value; a secret always names a variable that
  must be set. One complete example per choice of Engine DB, warehouse, secrets source, mode
  and authentication is published with the documentation.

### Running

- `etl-craft run --pipeline_code X` runs a pipeline in dependency waves, one process per task,
  with bounded parallelism, time limits, per-attempt log files, and resumption: a stopped or
  retried run never repeats finished tasks. `--task_code` runs one task. The run each task
  belongs to is resolved from the Engine DB, never passed in.
- Dependencies of type `SUCCESS`, `FAILURE`, `ALWAYS` and `HAS_DATA`, run conditions `ALL`,
  `ANY` and `N`, and dependencies on other pipelines and their tasks, judged on the upstream's
  last finished run and consumed once. SLA tracking on every run, with SLA emails.
- Four task handlers: `SQL` (one read-only SELECT, wrapped by the engine in one of eight
  actions: `CREATE_TABLE`, `SETUP_TABLE`, `OVERWRITE_TABLE`, `APPEND_TABLE`, `SCD1_MERGE`,
  `SCD2_MERGE`, `DROP_TABLE`, `DELETE_ROWS`), `PYTHON` ingestion scripts with offsets and
  `INPUT_PARAMS`, `BUSINESS_RULES` that flag and clear failing rows in waves, and
  `EMAIL_ALERT` through SMTP (password or XOAUTH2) or `sendmail`.
- Warehouses: PostgreSQL, DuckDB, DuckDB over an Iceberg REST catalog, Trino over Iceberg,
  Databricks (Delta and UniForm) and Snowflake (native and Iceberg). The third-party drivers
  are extras: `trino`, `databricks`, `snowflake`, `aws`.
- Local mode: etl-craft is the orchestrator. `mark` sets a task or a run's status with a
  reason (a failed task marked `SUCCESS` lets its dependents run when the run resumes),
  `mark --new-run` records a stand-in upstream run, `cancel` stops a run and ends it
  `CANCELLED`, `run --rerun [--with-downstream]` and `--ignore-dependencies` run a task past
  the usual checks, and `Orchestration.Dependency_gates: enforce | warn | off` relaxes
  cross-pipeline gates per profile. Every intervention is recorded in
  `AUD_RUN_INTERVENTIONS`, and shown by `history` and the catalog.
- Remote mode: the orchestrator is the only source of truth. `generate-yml` writes each
  pipeline as a DAG holding every rule (trigger rules, sensors on other pipelines and their
  tasks, `max_active_runs: 1`); tasks run when the orchestrator says. Rules an orchestrator
  cannot express (`N` run conditions, `HAS_DATA`, mixed dependency types) fail in `validate`,
  `generate-yml` and `run --init-only`, naming each one.

### Checking and understanding

- `doctor` reports every check (settings, secrets, connections, schemas, migrations, email,
  cloning, relaxed gates) and `setup` sets up or upgrades in one command. `validate` checks
  every pipeline's metadata with the handlers' own rules without running anything.
- `list`, `graph`, `steps` and `history` inspect pipelines and runs. `lineage` traces columns
  across tasks and pipelines; `docs-version` keeps task documentation in versions.
- `generate-docs` writes a searchable catalog site: pipelines with their DAGs, tables with
  columns, lineage graphs, business rules, last runs and interventions, refreshed on a schedule
  (`generate-yml --docs`). `publish-docs` serves it through an ngrok tunnel, limited to
  `Docs_site.Allowed_ips`.
- Cloning copies the Engine DB tables into the warehouse after every run, and `etl-craft clone`
  on demand.
- Every error class has its own exit status, and every failure names the object, the value
  found, what was expected and the remedy.

### Documentation and examples

- The Support Insights demo (`examples/demo`) and a Quick Start that runs it; guides for every
  feature; deployment, connector and security pages; and references generated from the code:
  the command line, `craft-connector.yml`, task parameters, the Engine DB schema and the Python
  API.

### Verified

- The release was tested from the built wheel, installed with pip and with uv, on Linux, with
  macOS and Python 3.11 to 3.13 in CI: every Engine DB with every local warehouse, the demo end
  to end, locally verifiable authentication modes, and the demo's warehouse work on Databricks
  and Snowflake. The evidence is in `release/evidence/0.1.0/`.

[Unreleased]: https://github.com/venkatcg00/etl-craft/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/venkatcg00/etl-craft/releases/tag/v0.2.0
[0.1.0]: https://github.com/venkatcg00/etl-craft/releases/tag/v0.1.0
