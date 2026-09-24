# etl-craft

A metadata-driven engine for SQL pipelines.

Your pipelines, tasks, dependencies and transformations are **rows in a database** — the
Engine DB — not Python DAG files. `etl-craft` reads that metadata and executes it. It never
writes logic it wasn't explicitly given.

The Engine DB is **SQLite by default** (one file, nothing to install) and **PostgreSQL for
production**.

**Release status: 0.1.0 Alpha.** The supported production path is a PostgreSQL Engine DB and a
PostgreSQL warehouse, operated by a named team. A SQLite Engine DB is for local development
and single-machine deployments. Local execution is built in. For Airflow, `generate-yml`
produces an Airflow-shaped descriptor; your deployment supplies the DAG loader and Airflow
runtime. Other schedulers can invoke `etl-craft run --pipeline_code PIPELINE --task_code TASK`
themselves, but no native adapter is shipped for them.

From this source checkout:

```bash
uv sync
```

The package is not published to a registry yet. For an artifact build, install the produced wheel
with `pip install dist/*.whl` after `uv build`.

## Why you might want it

- **Order comes from data.** `CFG_TASK_DEPENDENCY` rows are the DAG. Nothing enumerates
  order in code.
- **One execution primitive.** `etl-craft run` runs a task or a whole pipeline, locally or
  as the command an Airflow `BashOperator` shells out to. There is no separate "local mode"
  code path to drift.
- **Idempotent by construction.** A retry reads the log, skips what already succeeded, and
  re-attempts only what failed.
- **No orchestrator lock-in.** No REST calls, no XCom, no `dag-factory`. `generate-yml`
  emits a descriptor for an Airflow loader you own; local execution needs no loader.

## Five-minute quickstart

You need [uv](https://docs.astral.sh/uv/). Nothing else — no Docker, no database server.

**1. Write `craft-connector.yml`.** It is yours: etl-craft reads it and never writes it. The
smallest one needs nothing installed and nothing set: a SQLite Engine DB and a DuckDB warehouse,
both files beside it.

```yaml
Secrets:
  Source_type: environment
  Profile: dev

Orchestration:
  Mode: local

Engine:
  dev:
    jdbc_url: jdbc:sqlite:etl-craft-engine.db

Warehouse:
  Name: DuckDB
  dev:
    jdbc_url: jdbc:duckdb:warehouse.duckdb
```

That is [docs/examples/minimal-local.yml](docs/examples/minimal-local.yml).
[docs/examples/](docs/examples/README.md) has a complete file for every Engine DB, every warehouse
(PostgreSQL, DuckDB, DuckDB or Trino over Iceberg, Databricks, Snowflake, each native or Iceberg),
both secrets sources and both orchestration modes.

**2. Run `setup`, then `doctor`.**

```bash
uv sync
uv run etl-craft setup      # creates (or migrates) the Engine DB craft-connector.yml describes
uv run etl-craft doctor     # verifies every connection
```

`setup` is idempotent: run it again after any upgrade to bring the Engine DB forward. There are no
prompts, so the same command works in CI.

**3. Going to production: PostgreSQL for the Engine DB.** SQLite is one file on one machine:
every Engine DB write is serialized, and an orchestrator worker on another host cannot open it.
Give the file a `prod` profile whose values are variable *names*, and set those variables in each
environment, in the process environment or in a `.env`-style file that `Secrets` points at:

```yaml
Engine:
  dev:
    jdbc_url: jdbc:sqlite:etl-craft-engine.db
  prod:
    jdbc_url: ENGINE_JDBC_URL     # e.g. jdbc:postgresql://db:5432/etl_craft
    user: ENGINE_USER
    auth_mode: ENGINE_AUTH_MODE   # password | key_file
    secret: ENGINE_SECRET
```

`ETL_CRAFT_PROFILE=prod` selects that profile in every section, so `Warehouse` needs a `prod`
block too ([docs/examples/warehouse-postgres.yml](docs/examples/warehouse-postgres.yml) shows
both). The file holds no secrets, so it is safe to commit. See [docs/configuration.md](docs/configuration.md) for every setting.

**4. Register a pipeline.** Pipeline creation is deliberately *not* a CLI verb — it is
git-managed SQL, reviewed like any other change. See
[docs/first-pipeline.md](docs/first-pipeline.md) for a complete, runnable walkthrough.

**5. Run it.**

```bash
uv run etl-craft list
uv run etl-craft validate
uv run etl-craft run --pipeline_code MY_PIPELINE
uv run etl-craft history --pipeline_code MY_PIPELINE
```

## Commands

| Command | Purpose |
|---|---|
| `setup` | **Start here.** Validate `craft-connector.yml`, then create or migrate its Engine DB. Idempotent; run it again after any upgrade |
| `init-db` | Apply the packaged schema to an empty Engine DB |
| `migrate` | Apply pending packaged migrations, then an optional project migration stream |
| `doctor` | Check config, every secret, and every configured connection |
| `validate` | Config integrity checks no database constraint can enforce |
| `run --pipeline_code X [--task_code Y] [--force]` | The one execution verb |
| `list` | List active pipelines |
| `graph --name X` | Print a pipeline's dependency waves |
| `steps --pipeline_code X` | List a pipeline's tasks and their parameters |
| `history --pipeline_code X [--task_code Y] [--limit N]` | Recent run history |
| `lineage --table schema.table` | Every task declaring that table as a source or target |
| `lineage --column schema.table.column` | Column-level lineage, parsed from `SOURCE_SQL` |
| `docs-version` | Record a new documentation version for any task whose text changed |
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
- [docs/craft-connector.example.yml](docs/craft-connector.example.yml) — the annotated reference config
- [docs/examples/](docs/examples/README.md) — a complete config for every engine, warehouse, secrets source and mode
- [docs/operations.md](docs/operations.md) — deployment, backup, upgrade and monitoring guidance
- [docs/release-checklist.md](docs/release-checklist.md) — release and rollout verification
- [SECURITY.md](SECURITY.md) — private vulnerability reporting and deployment basics
- [CHANGELOG.md](CHANGELOG.md) — release-facing change history

`etl-craft generate-docs` produces a searchable site describing *your* pipelines, from the
metadata in your own Engine DB — including each task's `DOCUMENTATION` (with its version)
and its column-level lineage. Search is fuzzy, and the site ships everything it needs, so
it works on a network with no outbound access. It complements these docs rather than
replacing them.

### Lineage

`etl-craft lineage --table schema.table` lists the tasks declaring a table as a source or
target. `etl-craft lineage --column schema.table.column` goes further: it parses each SQL
task's `SOURCE_SQL` with [sqlglot](https://sqlglot.com) and resolves which upstream column
actually feeds it — through aliases, joins and CTEs — along with the expression applied.
Results are cached in `AUD_COLUMN_LINEAGE`, keyed by a hash of the SQL they came from, so
editing a query invalidates them automatically.

## Requirements

- Python 3.11+
- For production, PostgreSQL 14+ as the Engine DB (SQLite, the default, needs nothing). Its
  partial unique index makes concurrent run-id creation safe.
- PostgreSQL as the reference warehouse for a launch. DuckDB is useful for local development
  and serializes writers. The repository exercises Trino against a local Iceberg stack. The
  Databricks and Snowflake integrations require a real account and must pass their gated
  acceptance tests before a customer deployment. See [docs/configuration.md](docs/configuration.md)
  for supported URL/authentication combinations and table-format limits.

## License

Apache-2.0. See [LICENSE](LICENSE).
