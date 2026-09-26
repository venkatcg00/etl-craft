# Changelog

All notable changes are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `etl-craft pause` and `resume`: while a pipeline is paused, `run` starts nothing of it and
  exits `0`, and a run in progress starts no more tasks, to go on once resumed. `list` and the
  catalog show paused pipelines; `AUD_PIPELINE_PAUSES` keeps every pause.
- `etl-craft run --skip --reason`: records a run `SKIPPED` on purpose, for the pipelines that
  depend on it to see.
- The Engine DB's first migration, `0001_pipeline_pauses.sql`: `etl-craft setup` (or `migrate`)
  applies it to an Engine DB made by 0.1.0. A test upgrades each released schema and compares
  it with a new one.

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

[Unreleased]: https://github.com/venkatcg00/etl-craft/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/venkatcg00/etl-craft/releases/tag/v0.1.0
