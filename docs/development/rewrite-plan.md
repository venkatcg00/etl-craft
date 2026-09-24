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
  core/        errors.py enums.py graph.py text.py log.py
  config/      model.py discovery.py loader.py resolve.py auth.py
  dialects/engine/{base.py, postgres/, sqlite/}   each: schema.sql migrations/ queries/
  dialects/warehouse/{base.py, registry.py, postgres.py, duckdb.py, duckdb_iceberg.py,
               trino_iceberg.py, databricks.py, databricks_iceberg.py, snowflake.py,
               snowflake_iceberg.py}
  engine/      connection.py queries.py runlog.py schema.py migrations.py locks.py
               repository/{pipelines,tasks,dependencies,runs,business_rules,offsets,trackers,
                           lineage,docs,history}.py
  warehouse/   connection.py credentials.py
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
| `warehouse.py`, `credentials.py` | `warehouse/*` |
| `runner.py`, `orchestrator.py`, `crosspipe.py`, `limits.py`, `connections.py`, `execution.py` | `execution/*` |
| `sql_actions.py`, `business_rules.py`, `scripts.py`, `email_alert.py` | `handlers/*` |
| `doctor.py`, `setup_command.py`, `validate.py`, `cloning.py`, `generate_yml.py`, `docs_generator.py`, `column_lineage.py` | `services/*` |
| `cli.py` | `cli/commands/*` |

### Standards

- **Tooling:** ruff (lint, format, docstrings), `mypy --strict`, import-linter, pre-commit,
  Conventional Commits, Dependabot.
- **History gate:** `scripts/check_no_history.py` rejects change-history commentary in CI.
- **Errors:** one `EtlCraftError` hierarchy. Exit codes: 0 success, 1 run or validation failure,
  2 configuration or usage error.
- **Logging:** `logging` with `etl_craft.<module>` loggers; `--log-level` and
  `--log-format text|json`. Each task's output is captured to a log file, and its tail is stored
  in `TASK_LOG`.
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
| A2 | `chore/test-release-harness` | Test tree and markers; docker-compose (postgres, postgres with TLS client certificates, minio, iceberg-rest, trino, mailpit); evidence plugin; `release/required-suites.toml`; `scripts/release_gate.py`; release-gate CI job | `tests/conftest.py`, `docker-compose.yml` | A1 | |
| A3 | `docs/site-scaffold` | MkDocs Material, mike, mkdocstrings, gen-files; navigation skeleton; strict build in CI | — | A1 | |
| B1 | `feat/core-domain` | Errors, enums, logging, exit codes | scattered | A1 | |
| B2 | `feat/cli-framework` | Parser, output layer, exit-code mapping, `--config`, `--log-*` | `cli.py` | B1 | |
| B3 | `feat/core-graph` | Dependency graph: validation, waves, ready, unsatisfiable, run conditions | `resolver.py` | B1 | |
| B4 | `feat/core-text` | JDBC parsers, `.env`, statement splitters, `$$pipeline_id` substitution, read-only lint, identifier checks, checksums | several | B1 | |
| B5 | `feat/process-supervisor` | Spawned child interpreter, timeouts, process-group kill, output capture, bounded parallel batches, file locks | `runner.py`, `orchestrator.py` | B1 | |
| C1 | `feat/config` | Sections and order, profiles, variable-or-value, secrets must be set, auth validation | `config.py` | B4 | |
| C2 | `feat/engine-dialects` | PostgreSQL and SQLite dialects, baseline schemas, query catalog, locks, schema tests | `dialects/engine_dialects/` | C1, B5 | |
| C3 | `feat/warehouse-dialects` | Eight warehouse dialects, registry, credentials, `open_warehouse`, single-writer queue | `dialects/warehouse_dialects/`, `warehouse.py`, `credentials.py` | C1 | |
| D1 | `feat/engine-repository` | Repositories, run log, `init-db`, `migrate` | `db.py`, `cfg.py`, `runlog.py`, `init_db.py`, `migrate.py` | C2 | |
| E1 | `feat/execution-runner` | Task context, handler registry, `run --task_code`, child entry point, crash detection, timeouts | `runner.py`, `handlers.py`, `limits.py`, `execution.py` | D1, B3, B5 | |
| E2 | `feat/execution-pipeline` | Wave scheduler, init/finalize, cross-pipeline gates and trackers, SLA, connection tests at start, cloning hook | `orchestrator.py`, `crosspipe.py`, `connections.py` | E1, C3 | |
| F1 | `feat/handler-sql-actions` | Stage, schema evolution, audit columns, ROW_ID strategies, dedupe, the seven actions | `sql_actions.py` | E1, C3 | |
| F2 | `feat/handler-business-rules` | Rule waves, flag and deactivate, forced scope | `business_rules.py` | F1 | |
| F3 | `feat/handler-python-scripts` | Script contract, offset tracker, output capture, `etl_craft.scripting` helper | `scripts.py` | E1 | |
| F4 | `feat/handler-email-alert` | Flavours, templates, digest, SMTP password and XOAUTH2 | `email_alert.py` | E1 | |
| G1 | `feat/services-inspect-generate` | `list`, `graph`, `steps`, `history`, `lineage`, `generate-yml`, `generate-docs`, `docs-version` | readers in `cfg.py`, `generate_yml.py`, `docs_generator.py`, `column_lineage.py` | E2 | |
| G2 | `feat/services-ops` | `doctor`, `setup`, `validate`, cloning | `doctor.py`, `setup_command.py`, `validate.py`, `cloning.py` | E2, F1 | |
| H1 | `test/connection-matrix` | Every resource with every locally verifiable auth mode | `tests/test_auth.py` | G2 | |
| H2 | `test/e2e-demo` | Demo project run from the installed package, via pip and via uv | — | G1, G2, F2–F4 | |
| H3 | `test/cloud-acceptance` | Databricks and Snowflake suites, `make acceptance-cloud`, evidence | cloud tests | G2 | |
| I1 | `docs/guides` | Quick Start, guides, deployment, connectors | `docs/`, `README.md` | G1, G2 | |
| I2 | `docs/reference-generated` | Generated CLI, configuration, parameter and schema references; Python API | — | G1, G2 | |
| J1 | `release/0.1.0` | Version, changelog, release notes, wheel and sdist, install matrix, checksums, evidence, gate, tag | — | all | |

In parallel: A2 with A3; B2–B5; C2 with C3; F1–F4 (F2 after F1); G1 with G2; H1–H3 with I1–I2.
Critical path: A1 → B1 → B4 → C1 → C2 → D1 → E1 → E2 → G2 → H2 → J1.

## Checkpoints

Artifacts are built and install-verified at each checkpoint, never published.

| Checkpoint | Branches | Artifact |
|---|---|---|
| CP0 Bootstrap | A1–A3 | wheel installs and prints its version; the gate reports "not releasable" |
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
  - a transform pipeline with dummy steps covering the seven SQL actions (incremental
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

Versioned with mike. CI builds the site with `--strict`, checks links and tests the snippets.

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
- [ ] The seven SQL actions pass on every local warehouse and on the cloud subset, native and
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
      published.
- [ ] Every required suite has evidence for the release commit with no failures or skips;
      `scripts/release_gate.py --version 0.1.0` exits 0; the tag is created only by the release
      script.
