# Rewrite plan: etl-craft 0.1.0

`main` rebuilds etl-craft from the implementation archived at `archive/iteration-2`.
The design stays as it is; the rewrite changes the structure, the tests and the documentation:

- the design: run-id resolution, one `run` verb, the closed SQL action vocabulary, the Engine DB
  and warehouse split, `craft-connector.yml`, the CLI surface and the schema;
- the structure: layered packages, no circular imports, small modules;
- the tests: split by level, plus end-to-end runs from the installed package;
- the documentation: comments describe behaviour only, and a documentation site replaces the
  decision logs.

The release is 0.1.0: a Python package (one `py3-none-any` wheel and an sdist) that installs with
`pip` and `uv pip`. It is built and verified here, not published. The release gate blocks it until
every resource connection and real pipeline runs are tested, the cloud warehouses included.

**Languages:** Python for everything; SQL for the dialects (Engine DB schemas, migrations and
query catalogs; warehouse statement fragments).

## Architecture

### Layers

`core` → `config` → `dialects` → `engine` | `warehouse` → `handlers` → `execution` → `services` →
`cli`, lowest first. `lint-imports` enforces the order in CI.

- `config` depends on `core` only. Dialects declare their authentication capabilities as data,
  and `config` validates against that registry instead of importing connection code.
- Heavy optional imports (sqlglot, boto3, cloud dialects) load only on the paths that use them.

### SQL as the dialect language

- Each Engine DB dialect (`dialects/engine/postgres`, `dialects/engine/sqlite`) owns `schema.sql`,
  `migrations/` and `queries/*.sql`: every Engine DB query, as a shared default overridden per
  dialect where the SQL differs.
- Each warehouse dialect keeps its SQL fragments (CREATE clause, hash expression, ROW_ID strategy)
  in one module.
- There are no released users, so the previous migrations are folded into a fresh `schema.sql`
  per dialect, and `migrations/` starts empty.

### Layout

```
src/etl_craft/
  core/        errors.py enums.py graph.py text.py log.py filelock.py
  config/      model.py discovery.py loader.py resolve.py auth.py targets.py
  dialects/engine/{base.py, queries/, postgres/, sqlite/}   each: schema.sql migrations/ queries/
  dialects/credentials.py
  dialects/warehouse/{base.py, registry.py, postgres.py, duckdb.py, duckdb_iceberg.py,
               trino_iceberg.py, databricks.py, databricks_iceberg.py, snowflake.py,
               snowflake_iceberg.py}
  engine/      connection.py queries.py runlog.py schema.py migrations.py locks.py
               repository/{pipelines,tasks,dependencies,runs,business_rules,offsets,trackers,
                           lineage,docs,history}.py
  warehouse/   connection.py
  handlers/    registry.py sql/{stage,schema_evolution,audit,row_id,dedupe,actions}.py
               business_rules.py python_script.py email_alert/{flavour,render,send}.py scripting.py
  execution/   context.py runner.py child.py supervisor.py scheduler.py lifecycle.py gates.py
               limits.py connection_checks.py
  services/    doctor.py setup.py validate/ cloning.py dag_yaml.py lineage.py doc_versions.py
               docs_site/
  cli/         __init__.py output.py commands/
tests/  unit/ integration/<area>/ e2e/ acceptance/cloud/ fixtures/ plugins/
examples/demo/   docs/   scripts/   release/{required-suites.toml, evidence/}
```

### Where the archived modules go

| Archived module | New home |
|---|---|
| `resolver.py` | `core/graph.py` |
| text helpers in `warehouse.py`, `config.py`, dialects, `sql_actions.py`, `validate.py` | `core/text.py` |
| `config.py` | `config/*` (`ConnectorConfig.postgres` becomes `engine`; mode values are `local` and `remote` everywhere) |
| `db.py`, `runlog.py`, `cfg.py`, `init_db.py`, `migrate.py`, `documentation.py` | `engine/*`, with SQL moved to `queries/*.sql` |
| `warehouse.py` | `warehouse/*` |
| `credentials.py` | `dialects/credentials.py` |
| `runner.py`, `orchestrator.py`, `crosspipe.py`, `limits.py`, `connections.py`, `execution.py` | `execution/*` |
| `sql_actions.py`, `business_rules.py`, `scripts.py`, `email_alert.py` | `handlers/*` |
| `doctor.py`, `setup_command.py`, `validate.py`, `cloning.py`, `generate_yml.py`, `docs_generator.py`, `column_lineage.py` | `services/*` |
| `cli.py` | `cli/commands/*` |

### Standards

- **Tooling:** ruff (lint, format, docstrings), `mypy --strict`, import-linter, pre-commit,
  Conventional Commits, Dependabot.
- **History gate:** `scripts/check_no_history.py` rejects change-history commentary in CI.
- **Errors:** one `EtlCraftError` hierarchy; every error class has its own exit status
  (`core.errors.ExitCode`): 0 success, 1 a failed run or check, 2 usage, one code per error
  class, 16 unexpected.
- **Logging:** logs are how a failure is debugged, so they say what was being done, to what,
  and why it failed.
  - `logging` with `etl_craft.<module>` loggers; `--log-level` and `--log-format text|json`.
  - Every record written during a task run carries its context: pipeline code, task code,
    `pipeline_run_id`, `task_run_id` and attempt number (JSON fields, and a text prefix).
  - A failure logs the step it was in, the object it was working on and the full error, with
    the traceback at `DEBUG` and the cause in the message.
  - SQL actions and business rules log only what the engine produces: each statement they run
    (the SQL at `DEBUG`), its row count and duration, and each step of an action.
  - A Python script may log however it likes; its stdout, stderr and `logging` output are all
    captured, never lost.
  - Each task attempt's output goes to its own log file; the tail is stored in `TASK_LOG`, and
    `ERROR_MESSAGE` holds the one-line cause.
  - Counts are recorded in `AUD_TASK_RUN_LOG`: `SOURCE_COUNT`, `TARGET_COUNT`, `INSERT_COUNT`,
    `UPDATE_COUNT`, `DELETE_COUNT` from SQL actions, and the source, target and insert counts an
    ingestion script reports.
- **Process model:** a task runs in a freshly started interpreter (`subprocess` in a new session),
  never a fork. The parent kills the process group on timeout and records FAILED when the child
  dies without reporting.
- **Engine SQL:** selected columns are aliased in lowercase, and every catalog query is tested on
  both Engine DB dialects.
- **Version:** one version in `pyproject.toml`; `etl_craft.__version__` reads the package metadata.
- **Coverage:** at least 90%.

## Branches

Each branch is cut from `main`, ports one slice from `archive/iteration-2` with its tests, and is
squash-merged by pull request (see `CONTRIBUTING.md` for the definition of done).

| # | Branch | Scope | Archived source | After | Status |
|---|---|---|---|---|---|
| A1 | `chore/bootstrap` | Package skeleton, tooling, CI, history gate, package verification | — | — | done |
| A2 | `chore/test-release-harness` | Test tree and markers; docker-compose (postgres, postgres with TLS client certificates, minio, iceberg-rest, trino, mailpit); evidence plugin; `release/required-suites.toml`; `scripts/release_gate.py`; release-gate CI job | `tests/conftest.py`, `docker-compose.yml` | A1 | done |
| A3 | `docs/site-scaffold` | MkDocs Material, mike, mkdocstrings, gen-files; navigation skeleton; strict build in CI | — | A1 | done |
| A4 | `docs/github-pages` | Deploy the site to GitHub Pages from CI, rebuilt on every push to `main`: `dev` from `main`, `X.Y` from each release line's newest tag, `latest` for the newest | — | A3 | done |
| B1 | `feat/core-domain` | Errors, enums, logging, exit codes | scattered | A1 | done |
| B2 | `feat/cli-framework` | Parser, output layer, exit-code mapping, `--config`, `--log-*` | `cli.py` | B1 | done |
| B3 | `feat/core-graph` | Dependency graph: validation, waves, ready, unsatisfiable, run conditions | `resolver.py` | B1 | done |
| B4 | `feat/core-text` | JDBC parsers, `.env`, statement splitters, `$$pipeline_id` substitution, read-only lint, identifier checks, checksums | several | B1 | done |
| B5 | `feat/process-supervisor` | Spawned child interpreter, timeouts, process-group kill, output capture, bounded parallel batches, file locks | `runner.py`, `orchestrator.py` | B1 | done |
| C1 | `feat/config` | Sections and order, profiles, variable-or-value, secrets must be set, auth validation | `config.py` | B4 | done |
| C2 | `feat/engine-dialects` | PostgreSQL and SQLite dialects, baseline schemas, query catalog, locks, schema tests | `dialects/engine_dialects/` | C1, B5 | done |
| C3 | `feat/warehouse-dialects` | Eight warehouse dialects, registry, credentials, `open_warehouse`, single-writer queue | `dialects/warehouse_dialects/`, `warehouse.py`, `credentials.py` | C1 | done |
| D1 | `feat/engine-schema`, `feat/engine-repository` | Repositories, run log, `init-db`, `migrate` | `db.py`, `cfg.py`, `runlog.py`, `init_db.py`, `migrate.py` | C2 | done |
| E1 | `feat/execution-runner` | Task context, handler registry, `run --task_code`, child entry point, crash detection, timeouts, per-attempt log file with the tail in `TASK_LOG`, run context on every log record | `runner.py`, `handlers.py`, `limits.py`, `execution.py` | D1, B3, B5 | done |
| E2 | `feat/execution-pipeline` | Wave scheduler, init/finalize, cross-pipeline gates and trackers (the upstream's last tracked run must satisfy the dependency; a SKIPPED task stays SKIPPED), SLA tracked on every run with SLA lapse detected while the run is still going, connection tests at start, cloning hook | `orchestrator.py`, `crosspipe.py`, `connections.py` | E1, C3 | done |
| C4 | `feat/engine-schemas` | A `schema` setting in both the Engine and Warehouse profiles. The Engine schema holds every Engine DB table: on PostgreSQL it must already exist, the engine connects with it as the search path, and `init-db`, `migrate` and `doctor` fail naming it when it is missing; SQLite has no schemas, so the Engine DB file is attached under that name and created if missing. The Warehouse schema is where cloning copies the Engine DB tables; it must already exist, and `setup` and `doctor` check it. Task targets keep naming their own `schema.table` | — | C1, D1 | done |
| C5 | `feat/target-databases` | Every warehouse profile names its database, checked when the file loads; the engine connects in it. `TARGET_OBJECT` is `schema.table` (in the active database) or `database.schema.table` (as written); a bare table name fails | — | C4, F1b | done |
| P1 | `feat/project-layout` | The `etl-craft/` project directory holds everything a deployment needs: `craft-connector.yml`, `sql_files/`, `ingestion_scripts/`, `migrations/` and `logs/`. Discovery finds `etl-craft/craft-connector.yml`, and relative paths start from the project directory | `config.py` | C1, E1 | done |
| P2 | `chore/linux-only` | Dropped: the existing Windows and macOS support stays, and new code keeps working on all three | — | B5 | dropped |
| F1 | `feat/handler-sql-actions` | `SOURCE_SQL` as an inline query or a file under `sql_files/` (`SOURCE_SQL_FILE`); `PIPELINE_ID_SUBSTITUTION` and `PIPELINE_ID_FILTER` (true/false) enable `$$pipeline_id` (the run's pipeline id) and `$$pipeline_id_filter` (`pipeline_id = <id>`, `1=1` on FULL refresh) in queries and files; stage, schema evolution, audit columns, ROW_ID strategies, dedupe, the seven actions; each statement logged with its row count and duration; source, target, insert, update and delete counts recorded | `sql_actions.py` | E1, C3 | done |
| F1b | `feat/sql-explicit-targets` | Only `CREATE_TABLE` (drop and create, with the SELECT's rows) and `SETUP_TABLE` (create only when missing, checked through information_schema rather than `IF NOT EXISTS`, with the audit columns of the pipeline's writer of that table) create tables. `OVERWRITE_TABLE`, `SCD1_MERGE`, `SCD2_MERGE`, `DELETE_ROWS` and the new `APPEND_TABLE` fail when their target is missing, naming the remedy (a `SETUP_TABLE` task, or pre-create it). `APPEND_TABLE` inserts the SELECT's rows with `PIPELINE_RUN_ID` and `CREATE_DATE`, without comparing shapes. `DROP_TABLE` drops only if the target exists, still only after this pipeline's `CREATE_TABLE` for it succeeded in the run. A task holds exactly one SELECT; lookups and aggregations live inside it (CTEs, subqueries). `SETUP_TABLE` takes its audit columns only from tasks that write rows to the target, and fails when they disagree | — | F1 | |
| F2 | `feat/handler-business-rules` | Rule waves, flag and deactivate, forced scope; each rule's statements, flagged and cleared counts logged | `business_rules.py` | F1 | done |
| F3 | `feat/handler-python-scripts` | Script contract (a script reads its source from the stored offset and writes to its table; no pipeline-id tokens), `INPUT_PARAMS` task parameter as a JSON object passed to the script as a dictionary, scripts under `ingestion_scripts/`, offset tracker, capture of the script's stdout, stderr and `logging` output, the one row count the script reports, recorded as the source, target and insert counts in `AUD_TASK_RUN_LOG`, `etl_craft.scripting` helper | `scripts.py` | E1 | done |
| F4 | `feat/handler-email-alert` | Flavours, templates, digest, SMTP password and XOAUTH2; SLA lapse emails through the Email settings, sent only when `Enforce_sla` is on; a `sendmail` transport beside SMTP | `email_alert.py` | E1 | done |
| G1a | `feat/services-inspect` | `list`, `graph`, `steps`, `history` | readers in `cfg.py` | E2 | done |
| G1b | `feat/services-generate-yml` | `generate-yml` | `generate_yml.py` | E2 | done |
| G1c | `feat/services-lineage` | The lineage engine the catalog builds on: every active SQL task's SELECT (inline or file, tokens replaced) traced column by column with sqlglot, several sources per column, stored in `AUD_COLUMN_LINEAGE` by a hash of what it depends on; `lineage` summary, `--table`/`--column` with `--upstream`/`--downstream`/`--depth` across tasks and pipelines, `--strict`; `docs-version` | `column_lineage.py`, `documentation.py` | E2, F1 | done |
| G3 | `feat/services-docs-publish` | `etl-craft publish-docs` serves the `generate-docs` site through an ngrok tunnel, configured in a new optional `Docs_site` section: `Authtoken` (a variable name), optional `Domain` (the team's reserved or custom domain; without it, the account's permanent free dev domain), and optional `Allowed_ips` (CIDR ranges, enforced by ngrok's IP restriction). There is no login: the site is reachable only by its link, is never listed or indexed (`X-Robots-Tag: noindex` and `robots.txt` disallow everything), and teams allow the site's domain through their own firewall; the docs name that domain. Records the published URL in the Engine DB and fails, naming both URLs, if a later publish gets a different one. Runs as a long-lived service; the site rebuilds on `generate-docs`. The ngrok SDK is an optional extra | — | G1 | |
| G4 | `feat/services-catalog-lineage` | The `generate-docs` site as a searchable data catalog, in the style of Alation. **Assets:** a page for every table (each `TARGET_OBJECT`, and each source table found in SQL), with its columns, the tasks that write and read it, the business rules on it, its last run and row counts; pages for pipelines, tasks, rules and scripts. **Lineage graphs:** table-level and column-level lineage stitched across tasks and pipelines into one graph. From any table or column, a graph traces every path up to its root sources and down to its last consumers, with columns nested in their tables. Each edge is marked as a direct copy or a derived value (an expression, `CASE` or aggregate, which may have several sources). Clicking a column highlights its whole path. The graph library is vendored (no CDN), and each page shows the closure of its asset, with a depth control for large graphs. **Search:** fuzzy search over pipelines, tasks, tables, columns, rules and documentation text, with filters by asset type. **Coverage:** SQL tasks parse with sqlglot, after `$$pipeline_id` substitution and reading `SOURCE_SQL_FILE`. Ingestion scripts appear at table level, reading from their external source. Without `--strict`, SQL that cannot be parsed keeps its task on the table graph with an "column lineage unavailable" badge and the parser's reason, and the build lists every such task. With `--strict`, the build fails on any. `--with-warehouse` optionally adds column types and comments from the warehouse. The `lineage` command gains `--upstream`, `--downstream` and `--column` for the same full traversal as text | `docs_generator.py`; the lineage from G1c | G1c, F1 | |
| G2a | `feat/services-doctor-setup` | `doctor`: every check reported, not only the first (settings used as written that read like unset variables, secrets, unverified auth modes, the Engine DB's connection, schema, tables and pending migrations, the warehouse's connection and schema, in-memory DuckDB, a Trino catalog that is not Iceberg, the email relay or `sendmail`); exit 1 on a failed check. `setup`: the same checks, then create the Engine DB tables when there are none and apply pending migrations; changes nothing when a check fails; creates nothing outside the Engine DB's tables | `doctor.py`, `setup_command.py` | E2 | done |
| G2b | `feat/services-validate` | `validate`: every active pipeline's metadata checked without running anything, with the handlers' own checks (SQL spec and storage, `SETUP_TABLE` audit columns, business rules, scripts parsed and not imported, alert subjects and bodies for every outcome), graphs and cycles within and between pipelines, dependencies on inactive tasks and pipelines, `HAS_DATA` on tasks that report no rows, alert ordering, `ROW_ID` rule keys on rebuilt tables, codes, typed `PIPELINE_PARAMETERS`, and unread parameters (WARN); `--pipeline_code`; exit 1 on a FAIL | `validate.py` | E2, F1 | done |
| G2c | `feat/services-cloning` | Cloning the Engine DB tables into the warehouse after each run | `cloning.py` | E2 | |
| H1 | `test/connection-matrix` | Every resource with every locally verifiable auth mode | `tests/test_auth.py` | G2 | |
| H2 | `test/e2e-demo` | Demo project run from the installed package, via pip and via uv | — | G1, G2, F2–F4 | |
| H3 | `test/cloud-acceptance` | Databricks and Snowflake suites, `make acceptance-cloud`, evidence | cloud tests | G2 | |
| I1 | `docs/guides` | Quick Start, guides, deployment, connectors | `docs/`, `README.md` | G1, G2 | |
| I2 | `docs/reference-generated` | Generated CLI, configuration, parameter and schema references; the `docs` evidence suite (a test around the strict build) | — | G1, G2 | |
| J1 | `release/0.1.0` | Version, changelog, release notes, wheel and sdist, install matrix, checksums, evidence, gate, tag | — | all | |

In parallel: A2 with A3; F1b, then C4, after F1; B2–B5; C2 with C3; F1–F4 after P1 (F2 after F1); G1 with G2, G1c then G4, then G3; H1–H3 with I1–I2.
Critical path: A1 → B1 → B4 → C1 → C2 → D1 → E1 → E2 → G2 → H2 → J1.

## Checkpoints

Artifacts are built and install-verified at each checkpoint, never published.

| Checkpoint | Branches | Artifact |
|---|---|---|
| CP0 Bootstrap | A1–A4 | wheel installs and prints its version; the gate reports "not releasable" |
| CP1 Core | B1–B5 | graph, text, supervisor, CLI skeleton |
| CP2 Connect | C1–C3, D1 | `setup`, `doctor`, `init-db` and `migrate` work from the wheel against every local resource; `0.1.0a1` |
| CP3 Execute | E1–E2, F1–F4 | full pipeline runs in local and orchestrator mode; `0.1.0a2` |
| CP4 Parity | G1–G2 | every command and feature of the archived implementation; `0.1.0b1` |
| CP5 Verified | H1–H3, I1–I2 | all local suites green, documentation complete; `0.1.0rc1` |
| CP6 Release | J1 | the gate passes, including the locally run cloud evidence; tag `v0.1.0` |

## Testing and the release gate

**Suites.** Each is a pytest marker and produces an evidence file:

- `unit`;
- `engine-sqlite`, `engine-postgres`;
- `warehouse-postgres`, `warehouse-duckdb`, `warehouse-duckdb-iceberg`, `warehouse-trino-iceberg`;
- `connections`, `e2e-pip`, `e2e-uv`, `package`, `docs`;
- `cloud-databricks`, `cloud-snowflake`.

**Connection matrix.** At least one verified path per resource:

| Resource | Auth modes tested |
|---|---|
| SQLite Engine DB | none |
| PostgreSQL Engine DB and warehouse | password, key_file (TLS client-certificate container) |
| DuckDB | none |
| DuckDB over an Iceberg REST catalog | none, oauth (client-credentials grant against the local catalog) |
| Trino over Iceberg | none |
| Databricks | token (local cloud run) |
| Snowflake | password, token (local cloud run) |
| SMTP relay | none, password (Mailpit) |

Every other auth mode is documented as usable without a guarantee, and `doctor` warns about it.

**End-to-end demo.** The `examples/demo` project runs from a clean virtual environment with the
built wheel installed via `pip`, and separately via `uv pip`.

- Pipelines:
  - an ingestion pipeline whose Python script writes dummy rows and reports `INGESTION_COUNT` and
    `LATEST_OFFSET_UPDATE`;
  - a transform pipeline with dummy steps covering the eight SQL actions (incremental
    `$$pipeline_id`, `PRESERVE_TARGET`, SCD2 history, soft and hard `DELETE_ROWS`);
  - business rules in two waves, with the `INCOMPLETE`, `REJECT` and `REPORT` types;
  - an `EMAIL_ALERT` delivered to Mailpit;
  - dependencies of every type, run conditions `ALL`, `ANY` and `N`, and a cross-pipeline gate;
  - a task that fails once and resumes on retry, a killed child recorded FAILED, a timeout, SLA
    enforcement and cloning.
- Both execution modes:
  - local (`run --pipeline_code`);
  - orchestrator, where a simulated orchestrator executes the `generate-yml` output and honours
    its trigger rules.
- Assertions check the warehouse data, the audit rows, the email received, and the output of
  `history`, `graph`, `lineage`, `validate`, `doctor` and `generate-docs`.
- Matrix: Engine DB (SQLite, PostgreSQL) × warehouse (PostgreSQL, DuckDB, DuckDB over Iceberg,
  Trino over Iceberg). The cloud suites run a subset on Databricks and Snowflake, native and
  Iceberg.

**Cloud suites, run locally.**

- `make acceptance-cloud` reads credentials from the gitignored `.env.acceptance`
  (`.env.acceptance.example` lists the variables).
- It runs in a local Claude Code session.
- It writes `release/evidence/<version>/cloud-*.json`, recording the commit, the wheel hash and
  each test's outcome, without secrets.

**Release gate.** `scripts/release_gate.py` runs in CI on `release/*` branches and in the tag
script. It passes only when all of these hold:

- every suite in `release/required-suites.toml` has an evidence file;
- every evidence file shows no failures and no skips;
- each evidence commit is an ancestor of HEAD;
- the changes since that commit touch only `release/evidence/`, the changelog and the release
  notes;
- the wheel hash matches.

Otherwise the repository reports "not releasable" and no tag is created.

## Documentation site

MkDocs Material, laid out like Spark's documentation:

- **Overview and Quick Start.** The Quick Start is executed by the end-to-end suite.
- **Programming guides:**
  - pipelines and tasks;
  - dependencies and run conditions;
  - run lifecycle and retries;
  - SQL actions;
  - business rules;
  - Python ingestion;
  - email alerts;
  - incremental and full refresh;
  - cross-pipeline dependencies;
  - lineage and documentation.
- **Deploying:**
  - local mode;
  - orchestrator mode and converting the YAML;
  - operations;
  - security.
- **Connectors:**
  - Engine DBs;
  - one page per warehouse;
  - the authentication matrix.
- **Reference**, generated from the code so it cannot drift:
  - the CLI;
  - `craft-connector.yml`;
  - task parameters;
  - the Engine DB schema;
  - exit codes.
- **Python API** (mkdocstrings), release notes, contributing.

Published at <https://venkatcg00.github.io/etl-craft/>. The Docs workflow deploys it to
GitHub Pages on every push to `main`, rebuilding every version each time, with no branch
storing the pages: `dev` from `main`, `X.Y` from each release line's newest `vX.Y.Z` tag, and
`latest` for the newest line, which becomes the default. mike assembles the versions and the
version selector. CI builds the site with `--strict`, checks links and tests the snippets.

## Packaging

- `uv build` produces the wheel and the sdist.
- `scripts/verify_package.sh` installs both with `pip` and with `uv pip` into clean environments
  and checks the installed command.
- Later branches extend it with the extras, the wheel contents (dialect SQL, query catalogs,
  templates, vendored JavaScript) and checksums.
- CI runs it on Linux and macOS for Python 3.11–3.13. Windows gets an informational smoke job.

## 0.1.0 release checklist

- [ ] Every branch merged; CI green on Python 3.11–3.13 on Linux and macOS.
- [ ] ruff, `mypy --strict`, import-linter and the history gate clean; coverage at least 90%.
- [ ] Both Engine DBs: `setup`, `init-db`, `migrate`; a second concurrent IN-PROGRESS run is
      rejected.
- [ ] The connection matrix passes, including the Databricks and Snowflake evidence.
- [ ] The eight SQL actions pass on every local warehouse and on the cloud subset, native and
      Iceberg.
- [ ] These pass:
  - [ ] business rules;
  - [ ] Python ingestion, including the offset tracker;
  - [ ] the three email flavours, delivered;
  - [ ] every dependency type and run condition;
  - [ ] cross-pipeline gates and trackers;
  - [ ] retry-resume;
  - [ ] crash detection;
  - [ ] timeouts;
  - [ ] SLA;
  - [ ] cloning.
- [ ] The end-to-end demo passes from the installed package, in local and orchestrator mode, via
      `pip` and `uv pip`.
- [ ] The output of `generate-yml`, `generate-docs`, `lineage`, `history`, `steps`, `graph`,
      `doctor` and `validate` is verified.
- [ ] The wheel and sdist are built and install-verified on every supported Python and OS;
      checksums recorded.
- [ ] The documentation site builds strict, the generated references are current, the Quick
      Start runs in the end-to-end suite, and the supported matrix and known limits are
      published; after the `v0.1.0` tag, the site is redeployed with `0.1` as `latest`.
- [ ] Every required suite has evidence for the release commit with no failures or skips;
      `scripts/release_gate.py --version 0.1.0` exits 0; the tag is created only by the release
      script.
