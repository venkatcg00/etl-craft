# CLAUDE.md

etl-craft is a metadata-driven ETL orchestration engine, written in Python with SQL for the
database dialects. Pipelines, tasks and dependencies are rows in an Engine DB (SQLite by default,
PostgreSQL in production); the engine reads them and runs the work against one warehouse.

`main` is a rewrite of the implementation archived at the `archive/iteration-2` tag.
The design is unchanged; the structure, tests and documentation are new. The plan, its branch
list and the release checkpoints are in `docs/development/rewrite-plan.md`. Read it before
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

## Layers

Each package imports only the packages below it. `lint-imports` enforces this.

| Package | Responsibility |
|---|---|
| `cli` | argparse commands, output, exit codes |
| `services` | doctor, setup, validate, cloning, DAG YAML, lineage, docs site |
| `execution` | task runner, process supervisor, wave scheduler, run lifecycle, cross-pipeline gates |
| `handlers` | SQL actions, business rules, Python ingestion scripts, email alerts |
| `engine` / `warehouse` | Engine DB access and warehouse access (siblings, independent) |
| `dialects` | per-database SQL and connection details (engine: `schema.sql`, `migrations/`, `queries/`) |
| `config` | `craft-connector.yml` model and loader |
| `core` | errors, enums, dependency graph, text parsing |

## Design rules that must hold

- `etl-craft run` is the only execution verb. `--task_code` runs one task; without it the engine
  runs the pipeline in dependency waves, one child process per task.
- `pipeline_run_id` is never passed to a task. Each task resolves the active run from
  `AUD_PIPELINES_RUN_LOG`; a partial unique index keeps one IN-PROGRESS run per pipeline.
- Retries resume: tasks already `SUCCESS` or `SKIPPED` under the run are not re-run.
- Order comes from `CFG_TASK_DEPENDENCY` and `CFG_PIPELINE_DEPENDENCY` rows, never from code.
- SQL tasks supply exactly one read-only SELECT; the engine wraps it in one of eight actions
  (`CREATE_TABLE`, `SETUP_TABLE`, `OVERWRITE_TABLE`, `APPEND_TABLE`, `SCD1_MERGE`, `SCD2_MERGE`,
  `DROP_TABLE`, `DELETE_ROWS`) and owns every write. Only `CREATE_TABLE` and `SETUP_TABLE`
  create tables; the others fail when their target is missing.
- One warehouse per deployment. Third-party SQLAlchemy dialects are optional extras and never
  imported by engine code.
- `craft-connector.yml` is written by the team and only read by the engine. Secrets are always
  variable names, never values. Its directory is the project directory (`etl-craft/`), holding
  `sql_files/`, `ingestion_scripts/`, `migrations/` and `logs/`; relative paths start there.
- A failure is not always bad. When metadata, files, connections or data are not what the
  engine expects, fail rather than guess or work around it, and make the failure easy to debug:
  name the exact object, the value found, what was expected and the remedy, and record it in the
  audit tables and the attempt's log.

## Conventions

- Comments and docstrings describe current behaviour. No decision tags, dates, review ids or
  change narratives; `make history` rejects them.
- Raise errors from the `core` error hierarchy. Every error class has its own exit status in
  `core.errors.ExitCode` (0 success, 1 a failed run or check, 2 usage, then one per error class,
  16 unexpected); a new error class gets a new code, and a test enforces that no two share one.
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
