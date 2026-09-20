# etl-craft

A standalone, metadata-driven ETL orchestration engine.

Your pipelines, tasks, dependencies and transformations are **rows in Postgres**, not
Python DAG files. `etl-craft` reads that metadata and executes it. It never writes logic
it wasn't explicitly given.

It works with Airflow, with another orchestrator, or with nothing at all — the only hard
runtime dependency is a Postgres connection.

```bash
uv add etl-craft          # or: pip install etl-craft
etl-craft init-db         # create the schema in an empty database
etl-craft doctor          # check config, secrets and every connection
```

## Why you might want it

- **Order comes from data.** `CFG_TASK_DEPENDENCY` rows are the DAG. Nothing enumerates
  order in code.
- **One execution primitive.** `etl-craft run` runs a task or a whole pipeline, locally or
  as the command an Airflow `BashOperator` shells out to. There is no separate "local mode"
  code path to drift.
- **Idempotent by construction.** A retry reads the log, skips what already succeeded, and
  re-attempts only what failed.
- **No orchestrator lock-in.** No REST calls, no XCom, no `dag-factory`. `generate-yml`
  emits a DAG description you can feed to Airflow or ignore entirely.

## Five-minute quickstart

You need Docker and [uv](https://docs.astral.sh/uv/).

**1. Start a Postgres for the engine's own metadata.**

```bash
docker run -d --name etl-craft-pg -p 55432:5432 \
  -e POSTGRES_USER=etl_craft -e POSTGRES_PASSWORD=etl_craft -e POSTGRES_DB=etl_craft \
  postgres:16
```

**2. Install and configure.**

```bash
uv add etl-craft
etl-craft configure          # interactive; or see docs/craft-connector.example.yml
```

`configure` prints the exact environment variable each profile's secret is read from —
for the default `dev` profile that is `ETL_CRAFT_POSTGRES_DEV_SECRET`:

```bash
export ETL_CRAFT_POSTGRES_DEV_SECRET=etl_craft
```

**3. Create the schema and check everything.**

```bash
etl-craft init-db
etl-craft doctor
```

**4. Register a pipeline.** Pipeline creation is deliberately *not* a CLI verb — it is
git-managed SQL, reviewed like any other change. See
[docs/first-pipeline.md](docs/first-pipeline.md) for a complete, runnable walkthrough.

**5. Run it.**

```bash
etl-craft list
etl-craft validate
etl-craft run --pipeline_code MY_PIPELINE
etl-craft history --pipeline_code MY_PIPELINE
```

## Commands

| Command | Purpose |
|---|---|
| `init-db` | Apply the packaged schema to an empty Engine DB |
| `migrate` | Apply pending `sql/migrations/*.sql` to an existing Engine DB |
| `configure [--env FILE]` | Write `craft-connector.yml`, interactively or from an env file |
| `set-execution-mode local\|orchestrator` | One-time-per-environment mode lock |
| `doctor` | Check config, every secret, and every configured connection |
| `validate` | Config integrity checks no database constraint can enforce |
| `run --pipeline_code X [--task_code Y] [--force]` | The one execution verb |
| `list` | List active pipelines |
| `graph --name X` | Print a pipeline's dependency waves |
| `steps --pipeline_code X` | List a pipeline's tasks and their parameters |
| `history --pipeline_code X [--task_code Y] [--limit N]` | Recent run history |
| `lineage --table schema.table` | Every task declaring that table as a source or target |
| `generate-yml [--pipeline_code X \| --global]` | Emit an Airflow-shaped DAG description |
| `generate-docs [--output DIR]` | Emit a static, searchable documentation site |

Every command takes `--config PATH`; it also honours `$ETL_CRAFT_CONFIG`, and otherwise
searches upward from the current directory for `craft-connector.yml`.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Success. A task recorded `SKIPPED` because it was correctly gated off also exits `0`. |
| `1` | The work ran and failed: a failed task or pipeline, a validation issue, a failed migration. |
| `2` | The command could not run: bad configuration, unresolvable secret, unreachable Engine DB. |

## Documentation

- [docs/configuration.md](docs/configuration.md) — every `craft-connector.yml` section
- [docs/parameters.md](docs/parameters.md) — the `CFG_TASK_PARAMETERS` reference, per handler
- [docs/first-pipeline.md](docs/first-pipeline.md) — register and run your first pipeline
- [docs/craft-connector.example.yml](docs/craft-connector.example.yml) — a commented example

`etl-craft generate-docs` produces a searchable site describing *your* pipelines, from the
metadata in your own Engine DB. It complements these docs rather than replacing them.

## Requirements

- Python 3.11+
- Postgres for the Engine DB (its constraint guarantees are load-bearing — a partial unique
  index is what makes run-id creation race-safe)
- Optionally, any SQLAlchemy-supported warehouse for the data itself. Dialects are optional
  extras you install yourself: `uv add "etl-craft[clickhouse]"`

## License

Apache-2.0. See [LICENSE](LICENSE).
