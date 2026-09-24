# Changelog

This file records release-facing changes from the first public release process onward. Earlier
development history remains available in the Git log and `ITERATION_2.md`.

## Unreleased

### Changed

- **`craft-connector.yml` is written by the team and only read by etl-craft.** `setup` no longer
  creates or rewrites it: it validates the file, then creates or migrates the Engine DB. The
  `set-execution-mode` command is gone; `Orchestration.Mode` is edited in the file.
- **One configuration layout**, sections in a fixed order: `Secrets`, `Orchestration`, `Engine`,
  `Warehouse`, then the optional `Cloning`. The DAG defaults and the Email relay now live inside
  `Orchestration`. Every section can hold `dev`/`sit`/`uat`/`prod` (or any) profile blocks,
  selected by `<Section>.Profile`, then `Secrets.Profile`. The earlier layouts
  (`Execution`/`Source`/`Postgres`, and `Variables` blocks) are refused with a pointer to the
  example.
- **Every setting is a variable or a value.** Text that names a variable set in the secrets source
  (the environment or the `.env` file) takes its value; anything else is used as written, so
  `Profile: ETL_CRAFT_PROFILE` selects per environment and `Profile: dev` is simply `dev`. Secrets
  must be variables. `doctor` warns about values used as written that look like variable names.
  The hidden `ETL_CRAFT_PROFILE` / `ETL_CRAFT_<SECTION>_PROFILE` overrides are gone.
- **A secret variable that is not set is a load-time error.** A `secret`, `token` or `s3_secret`
  field in the selected profile must name a variable the secrets source holds, and every command
  refuses the file otherwise, naming the variable. Previously this was found at the first
  connection.
- **Connections are tested at initialize time, and a failure fails.** `setup` runs every `doctor`
  check before creating or migrating anything and exits 1 if one fails. A pipeline run
  (`run --init-only`, or a local `run --pipeline_code`) tests the warehouse and email relay its
  tasks use before it mints a run, and exits 1 without starting one if a test fails.
- **`Orchestration.Allow_schedule`** (default `true`): `false` makes `generate-yml` emit
  `schedule: null` even when a pipeline has a `RUN_SCHEDULE`, so non-production environments hold
  every pipeline without running it on a timetable.
- **Dialects are separated, one file per database.** `dialects/engine_dialects/{postgres,sqlite}/`
  each own their module, `schema.sql` and migrations; `dialects/warehouse_dialects/` has one module
  per database and table format (`postgres`, `duckdb`, `duckdb_iceberg`, `trino_iceberg`,
  `databricks`, `databricks_iceberg`, `snowflake`, `snowflake_iceberg`). The SQL actions are written
  once and ask the dialect only for what differs.
- `Warehouse.Table_format` defaults to `native`; `iceberg` on a Postgres warehouse is refused.
- **SQLite is the default Engine DB; PostgreSQL is the recommended production Engine DB.** A
  `jdbc:sqlite:` Engine DB needs no database server. `doctor` states SQLite's limits (serialized
  writes, one machine).

- Recommended cloud connections use separate fields: Databricks uses JDBC URL, catalog, schema,
  and token; Snowflake uses user, account, database, schema, warehouse, role, and token.
- Databricks `iceberg` targets use Delta with UniForm enabled. Snowflake Iceberg targets default
  to Snowflake-managed storage; customer external volumes still require a base location.
- Made package and project migrations separate, checksummed streams so project migrations cannot
  mask engine migrations or silently change after application.
- Scoped the documented launch path to PostgreSQL Engine DB and PostgreSQL warehouse, with clear
  acceptance requirements for optional warehouse integrations.

### Added

- **Authentication types** `token`, `key_file`, `oauth`, `sso` and `sts` alongside `none` and
  `password`, per target: PostgreSQL (Engine DB and warehouse), Trino, Databricks, Snowflake,
  DuckDB's Iceberg catalog and the SMTP relay. Only the verified ones (see `docs/configuration.md`)
  have run against a live service; the rest can be used, but success is not guaranteed, and
  `doctor` warns about them. `sts` on PostgreSQL needs the new `aws` extra.
- **`Orchestration.Enforce_sla`** now works: each finished run records `SLA_STATUS` `MET` or
  `BREACHED` against `SLA_IN_HOURS` (migration `0005`, both Engine DBs), a breach is reported in
  the run outcome and `history`, and an `EMAIL_ALERT` after the SLA has passed is amber.
- DuckDB over an Iceberg REST catalog as a warehouse (`Name: DuckDB`, `Table_format: iceberg`).
- `docs/examples/`: a complete, tested `craft-connector.yml` for every Engine DB, warehouse
  dialect, secrets source and orchestration mode.
- `jdbc:sqlite:<path>` Engine DB profiles (`auth_mode: none`), a packaged SQLite `schema.sql`,
  and a SQLite ENGINE migration stream. Migration and single-writer-warehouse locks use a file lock
  beside the SQLite database where PostgreSQL uses an advisory lock.

- SCD1 `PRESERVE_TARGET`, defaulting to `false`. When enabled, updates use
  `COALESCE(source_value, target_value)` so source nulls retain existing values. Change detection
  and stored hashes use the resulting values; inserts are unchanged.
- Deployment, backup, upgrade, monitoring, security, and release-rollout documentation.
- A release checklist for package publishing and customer environments.

### Fixed

- `docs/first-pipeline.md` built its target with `CREATE_TABLE` and then ran `SCD1_MERGE` into
  it, which the merge's audit-column check refuses. It now uses `SETUP_TABLE`, portable
  `WITH ... VALUES` inserts that run on SQLite and PostgreSQL, and creates the target schema.
- The README quickstart used pre-release `ETL_CRAFT_POSTGRES_*` setup variable names.

- Removed credential-bearing parameters from preferred Databricks JDBC URLs before setup can
  persist them in a legacy manifest.
- Preserved the original dependency-consumption watermark when execution joins an existing
  pipeline run.
- Rejected unused fields in token connection profiles and aligned validation with supported
  Snowflake-managed Iceberg storage.
- Preserved canonical connection variable mappings and cloning storage settings during setup.
- Corrected cross-pipeline documentation version lookups, email-alert dependency validation,
  and business-rule failure recording when committing results fails.

### Verification and known limits

- Local verification: 619 tests passed, four credential-gated cloud tests skipped, and 95.36%
  coverage. Schema, typing, formatting, lint, documentation-style, and clean-wheel smoke checks
  passed. These results do not establish the supported-Python CI matrix or customer acceptance.
- Earlier live Databricks and Snowflake checks are recorded in `CLAUDE.md`; cloud acceptance was
  not rerun for the latest remediation, including `PRESERVE_TARGET`.
- The release remains Alpha. Structured logging and task-output capture remain deferred
  (E2-18/E2-20); deployment owners must provide scheduler logging and alerting.

No package release has been tagged from this changelog yet.
