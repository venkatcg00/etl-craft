# Changelog

All notable changes are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Package skeleton for the rewrite: layered packages, `etl-craft --version`, tooling, CI and
  package verification with pip and uv.
- Local test services in `docker-compose.yml`: PostgreSQL with password and with
  client-certificate login, MinIO, an Iceberg REST catalog, Trino and Mailpit, with tests that
  prove each one works.
- Release evidence: every test belongs to a suite in `release/required-suites.toml`;
  `scripts/run_suite.py` records a suite's results, and `scripts/release_gate.py` reports whether
  the current commit is releasable.
- Documentation site built with MkDocs Material: overview, section skeleton, exit codes, and a
  Python API reference generated from the code. `make docs` builds it with `--strict`.
- The documentation site is deployed to GitHub Pages by the Docs workflow on every push to
  `main`, with a version selector: `dev` from `main`, and `X.Y` from each release line's
  newest tag, the newest aliased `latest`. `make docs-site` builds the same site locally.
- Core domain: the `EtlCraftError` hierarchy with the exit code each family maps to, the
  Engine DB and configuration value sets as `StrEnum`s with the run-status groups, and
  `etl_craft.core.log`, which writes the `etl_craft` loggers as text or JSON lines.
- Command line framework: a command registry, the output layer, `--config`, `--log-level`
  and `--log-format` before or after the command name, and `error:` lines with the exit code
  of each `EtlCraftError`. The Command line reference page is generated from the parser.
- Task dependency graph (`etl_craft.core.graph`): validation, static waves, the tasks ready
  to start under `ALL`, `ANY` and `N` run conditions, and the never-run tasks that can no
  longer start. A guide describes dependency types and run conditions.
- Text helpers (`etl_craft.core.text`): the generic JDBC URL parser, `.env` parsing, a
  quote- and dollar-quote-aware SQL statement splitter, `$$pipeline_id` substitution, the
  read-only SELECT lint, identifier and `schema.table` checks, and checksums.
- Process supervisor (`etl_craft.execution.supervisor`): runs each child as a freshly
  started interpreter in its own session, appends its output to a log file and keeps the
  tail, stops the whole process group on timeout, and runs children with bounded
  parallelism, starting the next as soon as one ends. `etl_craft.core.filelock` provides the
  cross-process file lock.
- `craft-connector.yml` loader (`etl_craft.config`): sections in their fixed order, one
  block per profile, every setting a variable or a value, secrets that must name a variable
  that is set, and each connection's auth mode and fields checked against what its Engine DB,
  warehouse or mail relay accepts. Warehouse JDBC URLs, including the DuckDB, Databricks and
  Snowflake forms, are parsed at load. The annotated example and one example file per
  connection choice are published with the documentation.
- Engine DB dialects for SQLite and PostgreSQL (`etl_craft.dialects.engine`): the packaged
  schema for each, the query catalog, connections with every PostgreSQL auth mode (tokens
  minted per connection for `oauth` and `sts`), cross-process locks, and schema tests that run
  the same rules against both databases. A task can no longer set `RUN_CONDITION_COUNT`
  without `RUN_CONDITION = 'N'`.
- Warehouse dialects (`etl_craft.dialects.warehouse`) for PostgreSQL, DuckDB, DuckDB over an
  Iceberg REST catalog, Trino over Iceberg, Databricks (Delta and UniForm) and Snowflake
  (native and Iceberg), and `etl_craft.warehouse.connection`: warehouse engines with every auth
  mode, writers of a DuckDB file queued behind an Engine DB lock, and a check that a Trino
  catalog is Iceberg. Databricks tables can be external (`EXTERNAL_LOCATION`), and Snowflake
  Iceberg tables can name an external catalog (`CATALOG`).
