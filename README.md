# etl-craft

A standalone, metadata-driven ETL orchestration engine.

Your pipelines, tasks, dependencies and transformations are **rows in Postgres**, not
Python DAG files. `etl-craft` reads that metadata and executes it. It never writes logic
it wasn't explicitly given.

It works with Airflow, with another orchestrator, or with nothing at all — the only hard
runtime dependency is a Postgres connection.

```bash
uv add etl-craft          # or: pip install etl-craft
etl-craft setup           # writes the config, creates or updates the schema
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

**2. Describe your environment in a `.env` file.**

```bash
cat > .env <<'EOF'
ETL_CRAFT_MODE=local
ETL_CRAFT_SOURCE_TYPE=environment
ETL_CRAFT_POSTGRES_PROFILE=dev
ETL_CRAFT_POSTGRES_JDBC_URL=jdbc:postgresql://localhost:55432/etl_craft
ETL_CRAFT_POSTGRES_USER=etl_craft
ETL_CRAFT_POSTGRES_AUTH_MODE=password
EOF
export ETL_CRAFT_POSTGRES_DEV_SECRET=etl_craft
```

**3. Run `setup`. Once, and then whenever anything changes.**

```bash
uv add etl-craft
etl-craft setup      # writes craft-connector.yml, then creates or migrates the schema
etl-craft doctor     # verifies every secret and connection
```

`setup` is idempotent, in the dbt sense: the first run sets everything up, and every run
after that brings it to current. There is no separate first-run path and no prompts, so it
behaves identically on a laptop and in CI. Already have the settings exported in your
environment? `etl-craft setup --from-environment` skips the file entirely.

It also prints the exact secret variable each profile expects, so you never discover the
name from a later command failing.

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
| `setup` | **Start here.** Set up or update this deployment: config, then schema/migrations. Idempotent — run it again after any change |
| `init-db` | Apply the packaged schema to an empty Engine DB |
| `migrate` | Apply pending `sql/migrations/*.sql` to an existing Engine DB |
| `set-execution-mode local\|orchestrator` | One-time-per-environment mode lock |
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
- [docs/craft-connector.example.yml](docs/craft-connector.example.yml) — a commented example

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
- Postgres for the Engine DB (its constraint guarantees are load-bearing — a partial unique
  index is what makes run-id creation race-safe)
- Optionally, any SQLAlchemy-supported warehouse for the data itself. Dialects are optional
  extras you install yourself: `uv add "etl-craft[clickhouse]"`

## License

Apache-2.0. See [LICENSE](LICENSE).
