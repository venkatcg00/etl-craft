# etl-craft

A metadata-driven engine for SQL pipelines.

Your pipelines, tasks, dependencies and transformations are **rows in Postgres**, not
Python DAG files. `etl-craft` reads that metadata and executes it. It never writes logic
it wasn't explicitly given.

**Release status: 0.1.0 Alpha.** The supported launch path is a PostgreSQL Engine DB and a
PostgreSQL warehouse, operated by a named team. Local execution is built in. For Airflow,
`generate-yml` produces an Airflow-shaped descriptor; your deployment supplies the DAG loader
and Airflow runtime. Other schedulers can invoke
`etl-craft run --pipeline_code PIPELINE --task_code TASK` themselves, but no native adapter is
shipped for them.

From this source checkout:

```bash
uv sync
```

The package is not published to a registry yet. For an artifact build, install the produced wheel
with `pip install dist/*.whl` after `uv build`. Continue with the quickstart below to create the
bootstrap file before running `setup`.

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

You need Docker and [uv](https://docs.astral.sh/uv/).

**1. Start Postgres, then create the separate warehouse database used below.**

```bash
docker run -d --name etl-craft-pg -p 55432:5432 \
  -e POSTGRES_USER=etl_craft -e POSTGRES_PASSWORD=etl_craft -e POSTGRES_DB=etl_craft \
  postgres:16

until docker exec etl-craft-pg pg_isready -U etl_craft -d etl_craft >/dev/null 2>&1; do sleep 1; done
docker exec etl-craft-pg psql -v ON_ERROR_STOP=1 -U etl_craft -d postgres \
  -c 'CREATE DATABASE analytics;'
```

**2. Describe your environment in a `.env` file.**

```bash
cat > .env <<'EOF'
ETL_CRAFT_MODE=local
ETL_CRAFT_SOURCE_TYPE=file
ETL_CRAFT_SOURCE_PATH=./.env
ETL_CRAFT_POSTGRES_PROFILE=dev
ETL_CRAFT_POSTGRES_JDBC_URL=jdbc:postgresql://localhost:55432/etl_craft
ETL_CRAFT_POSTGRES_USER=etl_craft
ETL_CRAFT_POSTGRES_AUTH_MODE=password
ETL_CRAFT_POSTGRES_DEV_SECRET=etl_craft

# The warehouse your tables live in. Optional — needed only for SQL and
# BUSINESS_RULES tasks. Postgres:
ETL_CRAFT_WAREHOUSE_JDBC_URL=jdbc:postgresql://localhost:55432/analytics
ETL_CRAFT_WAREHOUSE_USER=etl_craft
ETL_CRAFT_WAREHOUSE_AUTH_MODE=password
ETL_CRAFT_WAREHOUSE_DEV_SECRET=etl_craft
ETL_CRAFT_WAREHOUSE_TABLE_FORMAT=native
# ...or DuckDB, which is a file and has nothing to authenticate to:
# ETL_CRAFT_WAREHOUSE_JDBC_URL=jdbc:duckdb:/data/warehouse.duckdb
# ETL_CRAFT_WAREHOUSE_AUTH_MODE=none
EOF
chmod 600 .env
```

This is a local development secrets file. Keep it out of version control. In CI, export the
same variables and use `uv run etl-craft setup --from-environment` instead.

**3. Install the checkout and run `setup`. Once, and then whenever anything changes.**

```bash
uv sync
uv run etl-craft setup      # writes craft-connector.yml, then creates or migrates the schema
uv run etl-craft doctor     # verifies every secret and connection
```

`setup` is idempotent: the first run creates the canonical `craft-connector.yml`, and later
runs update it and bring the Engine DB forward. There are no prompts, so the same input works in
CI. Already have the settings exported in your environment? `uv run etl-craft setup
--from-environment` uses them directly.

It also prints the exact secret variable each profile expects, so you never discover the
name from a later command failing.

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
| `setup` | **Start here.** Set up or update this deployment: config, then schema/migrations. Idempotent — run it again after any change |
| `init-db` | Apply the packaged schema to an empty Engine DB |
| `migrate` | Apply pending packaged migrations, then an optional project migration stream |
| `set-execution-mode local\|remote` | One-time-per-environment mode lock |
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
- [docs/craft-connector.env-secrets.example.yml](docs/craft-connector.env-secrets.example.yml) — environment-backed canonical config
- [docs/craft-connector.file-secrets.example.yml](docs/craft-connector.file-secrets.example.yml) — file-backed canonical config
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
- PostgreSQL 14+ for the Engine DB. Its partial unique index makes concurrent run-id creation
  safe.
- PostgreSQL as the reference warehouse for a launch. DuckDB is useful for local development
  and serializes writers. The repository exercises Trino against a local Iceberg stack. The
  Databricks and Snowflake integrations require a real account and must pass their gated
  acceptance tests before a customer deployment. See [docs/configuration.md](docs/configuration.md)
  for supported URL/authentication combinations and table-format limits.

## License

Apache-2.0. See [LICENSE](LICENSE).
