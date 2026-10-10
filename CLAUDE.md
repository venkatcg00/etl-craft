# CLAUDE.md

etl-craft is a metadata-driven ETL orchestration engine, written in Python with SQL for the
database dialects. Pipelines, tasks and dependencies are rows in an Engine DB (SQLite by default,
PostgreSQL in production); the engine reads them and runs the work against one warehouse.

`main` is a rewrite of the implementation archived at the `archive/iteration-2` tag.
The design is unchanged; the structure, tests and documentation are new. The plan, its branch
list and the release checkpoints are in `docs/development/rewrite-plan.md`. The work from 0.1.0 to
1.0.0, item by item, is in `docs/development/road-to-1.0.0.md`. Read the relevant plan before
starting a branch.

## Commands

```bash
make sync            # .venv with every dependency group
make check           # ruff lint + format check, mypy --strict, lint-imports, history gate, tests + coverage
make test            # tests only
make verify-package  # build wheel + sdist, install each with pip and uv in clean environments
make services-up     # local PostgreSQL (password and TLS), MinIO, Iceberg REST, Trino, Mailpit
make suite SUITE=unit   # run one release suite and record its evidence
make release-gate    # is HEAD releasable? (release/README.md)
make docs            # documentation site, strict build into site/ (make docs-serve to preview)
```

Before opening a pull request, finish the full local `make check docs` run and all relevant
service and live-cloud acceptance tests. Do not open the PR while these checks are still running.
When changing GitHub Actions workflows, run `actionlint` locally before pushing; Python tests
do not validate GitHub expression contexts.
Build the current wheel and source distribution together, then run the installed-wheel demos
across all four local warehouses with
`ETL_CRAFT_TEST_WHEEL` pointing to that wheel and `ETL_CRAFT_REQUIRE_SERVICES=1`. The regular
coverage run skips those demos when no wheel is supplied; `make verify-package` checks installation
but does not run the pipeline demos.

Pull-request CI runs once per revision and does not repeat after merge. Keep repeated chaos and
other expensive stress gates in release validation; ordinary regression cases still run once
with the regular suite. Documentation deployment runs after merge. The Nightly workflow runs the
tests and the wheel demo against the newest release of every dependency; a red nightly run on a
green `main` means an upstream release broke something.

## Layers

Each package imports only the packages below it. `lint-imports` enforces this.

| Package | Responsibility |
|---|---|
| `cli` | argparse commands, output, exit codes |
| `api` | optional FastAPI routes, bearer authorization and HTTP server lifecycle |
| `overseer` | leadership, active-run working set, local supervision and shutdown |
| `services` | actor-scoped operations, doctor, setup, validate, cloning, DAG YAML, lineage, docs site |
| `execution` | task runner, process supervisor, ready-task scheduler, run lifecycle, cross-pipeline gates |
| `handlers` | SQL actions, business rules, Python ingestion scripts, email alerts |
| `engine` / `warehouse` | Engine DB access and warehouse access (siblings, independent) |
| `dialects` | per-database SQL and connection details (engine: `schema.sql`, `migrations/`, `queries/`) |
| `config` | `craft-connector.yml` model and loader |
| `core` | errors, enums, dependency graph, text parsing |

## Design rules that must hold

- `etl-craft run` is the only execution verb. `--task_code` runs one task; without it the engine
  starts ready tasks after each completion, one child process per task. Gate waits use no worker
  slots and retain their budget in AUD_GATE_WAITS.
- A task is given its run by whoever starts it; nothing resolves a run by recency. Commands
  select an explicit `--run-id` or `--run-key`, or the single non-terminal run. A partial unique
  index keeps one IN-PROGRESS run per pipeline.
- Whole-pipeline and task supervisors renew 60-second ownership leases every 15 seconds.
  Reconcile expired attempts as `LOST` before retrying; never adopt an expired lease directly,
  and signal a local process only after verifying its PID and process birth identity.
- Retries resume: tasks already `SUCCESS` or `SKIPPED` under the run are not re-run.
- Cross-pipeline consumption uses recorded admission decisions, never a new judgement at
  completion. A repaired run publishes one new output revision only when it ends SUCCESS;
  the dependency's CONSUME_REPAIRS flag decides whether the same run's newer revision is fresh.
- Successful attempt endings commit the outcome, task summary, returned script offset and
  recorded task-dependency consumption together. Pipeline endings commit status, SLA and
  recorded pipeline-dependency consumption together; finalization hooks run after commit.
- Order comes from `CFG_TASK_DEPENDENCY` and `CFG_PIPELINE_DEPENDENCY` rows, never from code.
- SQL tasks supply exactly one read-only SELECT; the engine wraps it in one of eight actions
  (`CREATE_TABLE`, `SETUP_TABLE`, `OVERWRITE_TABLE`, `APPEND_TABLE`, `SCD1_MERGE`, `SCD2_MERGE`,
  `DROP_TABLE`, `DELETE_ROWS`) and owns every write. Only `CREATE_TABLE` and `SETUP_TABLE`
  create tables; the others fail when their target is missing.
- Merge hashes use typed, NULL-tagged, length-prefixed values and UTC timestamps. Fresh merge
  targets publish hash version 2 after creation; existing targets need `etl-craft rehash` before
  merging. Never publish a hash version before its warehouse update commits. Target mutations
  and rehashing hold the same Engine DB target lock.
- One warehouse per deployment. Third-party SQLAlchemy dialects are optional extras and never
  imported by engine code; DuckDB's, the default local warehouse, is the one core dependency.
- `craft-connector.yml` is written by the team and only read by the engine. Secrets are always
  variable names, never values. Its directory is the project directory (`etl-craft/`), holding
  `sql_files/`, `ingestion_scripts/`, `migrations/` and `logs/`; relative paths start there.
- A failure is not always bad. When metadata, files, connections or data are not what the
  engine expects, fail rather than guess or work around it, and make the failure easy to debug:
  name the exact object, the value found, what was expected and the remedy, and record it in the
  audit tables and the attempt's log.

- Every write to the Engine DB goes through etl-craft and names its actor; the Engine DB
  refuses any other. Metadata edits use project migrations; audit history is append-only.

## Conventions

- Comments and docstrings describe current behaviour. No decision tags, dates, review ids or
  change narratives; `make history` rejects them.
- Raise errors from the `core` error hierarchy. Every error class has its own exit status in
  `core.errors.ExitCode` (0 success, 1 a failed run or check, 2 usage, then one per error class,
  16 unexpected); a new error class gets the next free code after the last one, and a test enforces that no two share one.
- Log through `logging` (`etl_craft.<module>` loggers). Only the `cli` package prints.
- Raw SQL aliases every selected column in lowercase; SQLite and PostgreSQL disagree on the case
  of unquoted identifiers.
- Every test carries its suite's marker from `release/required-suites.toml` (or `harness`);
  collection fails otherwise. Unit tests need no services; others call
  `fixtures.services.require(...)`, which skips when the service is down.

## Porting a slice from the archive

`git show archive/iteration-2:<path>` shows the previous code. Port the code and the tests that
cover it together, keep behaviour, rewrite comments to describe what the code does now, and
follow `CONTRIBUTING.md`'s definition of done.
