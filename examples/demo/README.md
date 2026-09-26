# Support Insights: the etl-craft demo

A small support-analytics platform: two clients send their customer-support interactions in their
own shapes, and etl-craft lands them, parses them into one shape, keeps the agents and their
history, builds a support fact with checks on it, and summarises it per area and team.

```text
lnd (landed, as sent) ─► prs (parsed, typed) ─► pre_dm (every client, one shape) ─► dm (fact, summary)
                         ds (agents, support areas) ─┘   cdc (agents' teams over time)
aud: the Engine DB's own tables, cloned after every run
```

| Pipeline | What it does |
|---|---|
| `CLIENT_ALPHA` | lands Client Alpha's interactions (a number offset), parses them, removes its test calls (`DELETE_ROWS`, hard) |
| `CLIENT_BETA` | lands Client Beta's events (a timestamp offset) and parses them into the shared names |
| `SUPPORT_DM` | once both clients have succeeded: agents (`SCD1_MERGE`, keeping an email the feed leaves out), their teams over time (`SCD2_MERGE`), retired agents (`DELETE_ROWS`, soft), the support fact (`SETUP_TABLE`, `OVERWRITE_TABLE`), three business rules in two waves, a summary, a scratch table dropped at the end, an alert when it ends and a watcher that alerts when a feed fails |
| `SUPPORT_EXPORT` | a slow export that overruns its time limit and its SLA |
| `SUPPORT_BACKFILL` | a long backfill, to stop and resume |

The data is synthetic and the same every time, so the results can be checked: some ratings are
out of range, some calls run long, and one agent is unknown, for the business rules to find. The
first `SUPPORT_DM` run fails on purpose: its `flaky_feed` is not ready the first time.

## Run it

Start Mailpit for the alerts (`make services-up` in the etl-craft repository starts one on port
51025), then, from this folder:

```bash
python -c "import duckdb; c = duckdb.connect('warehouse.duckdb'); [c.execute(s) for s in open('warehouse_schemas.sql').read().split(';') if s.strip()]"
etl-craft setup                        # checks every connection, creates the Engine DB
# load the metadata: sqlite3 engine.db < metadata/support_insights.sql
etl-craft validate
etl-craft run --pipeline_code CLIENT_ALPHA
etl-craft run --pipeline_code CLIENT_BETA
etl-craft run --pipeline_code SUPPORT_DM      # fails at flaky_feed; the alerts say so
etl-craft run --pipeline_code SUPPORT_DM      # succeeds on the same client runs
etl-craft generate-docs && open catalog/index.html
```

The emails arrive in Mailpit at <http://localhost:58025>. `etl-craft history`, `graph`, `steps`
and `lineage` show what ran.

## Under an orchestrator

In remote mode (`Orchestration.Mode: remote`) the orchestrator is the only source of truth for
scheduling: `etl-craft generate-yml --pipeline_code <code>` writes each pipeline as a DAG holding
every rule, and etl-craft runs each task when the DAG says. Two of the demo's rules have no
equivalent in an orchestrator (`HAS_DATA` dependencies and a `2 of 3` run condition), so
`etl-craft validate` fails on them in remote mode and names each one. Load
`metadata/remote_mode.sql` after the demo's metadata to turn them into rules an orchestrator
applies.

## Files

| Path | Holds |
|---|---|
| `craft-connector.yml` | SQLite Engine DB, DuckDB warehouse, Mailpit, cloning into `aud` |
| `metadata/support_insights.sql` | the pipelines, tasks, parameters, dependencies and rules, as `CFG_` rows; runs as written on SQLite and PostgreSQL |
| `metadata/remote_mode.sql` | the changes remote mode needs, loaded after `support_insights.sql` |
| `ingestion_scripts/` | the scripts that land each source, plus a flaky feed and a slow task |
| `sql_files/` | the SELECTs the SQL tasks wrap in their actions |
| `warehouse_schemas.sql` | the schemas to create first: etl-craft never creates warehouse schemas |

The end-to-end tests run this demo from the built wheel installed with pip, on SQLite and
PostgreSQL Engine DBs and on PostgreSQL, DuckDB, DuckDB over Iceberg and Trino warehouses, in
local mode and under a simulated orchestrator; and installed with uv, on SQLite and DuckDB
(`tests/e2e/test_demo.py`).
