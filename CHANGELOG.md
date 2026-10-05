# Changelog

All notable changes are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Actor identities on run starts and endings, attempts, interventions and pauses; immutable
  command requests and metadata before/after history, with `etl-craft audit` filters. Migration
  `0008_actors_and_audit_guards.sql` protects audit and metadata writes on SQLite and PostgreSQL.
  Generated remote DAGs pass their actor, `setup --print-grants` prints deployment role SQL,
  and `doctor` checks extra write grants and SQLite file permissions.

- Run identities and trigger kinds, owner and lease fields, output revisions and configuration
  fingerprints; attempt and gate-decision tables, consumed revisions and repair-consumption flags.
  Migration `0007_identity.sql` preserves historical run identities and copies the latest known
  execution attempts. New runs receive manual, backfill or stand-in keys. Attempt transitions,
  lease enforcement and recording gate decisions remain subsequent roadmap work.

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
