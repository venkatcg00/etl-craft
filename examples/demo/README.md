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

The [Quick Start](../../docs/quick-start.md) walks through it. In short, with etl-craft installed
and Mailpit running for the alerts (`make services-up` in the etl-craft repository starts one on
port 51025), from this folder:

```bash
python prepare.py warehouse                   # the schemas the demo writes, in warehouse.duckdb
etl-craft setup                               # checks connections and prompts for the first admin
etl-craft login                               # stores your private session on this server
python prepare.py metadata                    # the pipelines, as CFG_ rows, in engine.db
etl-craft validate
etl-craft run --pipeline_code CLIENT_ALPHA
etl-craft run --pipeline_code CLIENT_BETA
etl-craft run --pipeline_code SUPPORT_DM      # fails at flaky_feed; the alerts say so
etl-craft run --pipeline_code SUPPORT_DM      # succeeds on the same client runs
etl-craft generate-docs --with-warehouse      # then open catalog/gaps.html
```

`prepare.py metadata` creates numbered seed migrations under `migrations/` and runs
`etl-craft migrate`. Repeating it leaves applied files unchanged. To edit loaded metadata,
add a new project migration instead of editing an applied seed. The optional remote-mode
seed is added after the main seed.

The emails arrive in Mailpit at <http://localhost:58025>. `etl-craft history`, `graph`, `steps`
and `lineage` show what ran. `etl-craft lineage --strict` also traces every SQL task before
the warehouse tables exist, using the script-landed source shapes in the seed metadata.
Each computation records changed table lineage as a version. After the first tracing, use
`etl-craft lineage --table <schema.table> --version 1` (substitute a target listed by `lineage`)
or `etl-craft lineage --as-of <ISO-time>` to read retained edges; catalog table pages show
version history. The gaps tab compares deployment metadata, shows engine-managed columns and
links warnings to tasks and lineage. Generate without `--with-warehouse` for offline docs with
explicit Unchecked coverage. `catalog/gaps.json` exports the same records. History begins after upgrading the Engine DB and computing lineage.

To see incremental generation, run the offline build twice with no intervening metadata or
run changes:

```bash
etl-craft generate-docs
etl-craft generate-docs
```

Unchanged pipeline/task/table pages retain their modification times and generation timestamps;
search and gaps refresh. The generated `.etl-craft-manifest.json` tracks page inputs and HTML
hashes. On a corrupt-manifest/cached-HTML error, remove only that manifest and regenerate.
Pipeline/task histories and the DAG overview group KPIs by Scheduled, Backfill, Repair and
Other; a repaired logical run is counted once in its current group. History displays the latest
30 runs per pipeline/task, with older consumed run identities shown as text.

## Under an orchestrator

In remote mode (`Orchestration.Mode: remote`) the orchestrator is the only source of truth for
scheduling: `etl-craft generate-yml --pipeline_code <code>` writes each pipeline as a DAG holding
every rule, and etl-craft runs each task when the DAG says. Two of the demo's rules have no
equivalent in an orchestrator (`HAS_DATA` dependencies and a `2 of 3` run condition), so
`etl-craft validate` fails on them in remote mode and names each one. Load
`metadata/remote_mode.sql` after the demo's metadata (`python prepare.py metadata
metadata/remote_mode.sql`) to turn them into rules an orchestrator applies.

## Files

| Path | Holds |
|---|---|
| `craft-connector.yml` | SQLite Engine DB, DuckDB warehouse, Mailpit, cloning into `aud` |
| `metadata/support_insights.sql` | source-column definitions and the pipelines, tasks, parameters, dependencies and rules, as `CFG_` rows; runs as written on SQLite and PostgreSQL |
| `metadata/remote_mode.sql` | the changes remote mode needs, loaded after `support_insights.sql` |
| `ingestion_scripts/` | the scripts that land each source, plus a flaky feed and a slow task |
| `sql_files/` | the SELECTs the SQL tasks wrap in their actions |
| `warehouse_schemas.sql` | the schemas to create first: etl-craft never creates warehouse schemas |
| `prepare.py` | creates those schemas in `warehouse.duckdb`, and loads the metadata into `engine.db` |

The end-to-end tests run this demo from the built wheel installed with pip, on SQLite and
PostgreSQL Engine DBs and on PostgreSQL, DuckDB, DuckDB over Iceberg and Trino warehouses, in
local mode and under a simulated orchestrator; and installed with uv, on SQLite and DuckDB
(`tests/e2e/test_demo.py`).

## SQL sources with watermarks

The client feeds demonstrate Python offsets. A SQL source outside etl-craft can use the same
typed offset store with `WATERMARK_COLUMN`, `WATERMARK_TYPE`, `WATERMARK_START` and
`$$watermark`; [the SQL watermark example](../../docs/examples/sql-watermark.md) shows the
parameters and a merge reload. The demo's existing SQL tasks keep their pipeline-run filters
because their sources are already written by etl-craft.

## File deliveries

Load `metadata/event_waits.sql` separately to try a scheduled local event wait before a CSV
script. See [the file-delivery example](../../docs/examples/event-wait.md) for the commands;
[Event waits](../../docs/guides/event-waits.md) describes settling, consumption and timeouts.

The generated catalog can also sit behind the team's proxy. Set `Docs_site.External_url`
(for example `https://docs.example.com/support/`) and the proxy's `Trusted_proxies` CIDRs, then
run `etl-craft publish-docs --port 8080`. Preserve the prefix at the proxy and enforce team
authentication there. No ngrok credentials are needed; see the catalog hosting guide and
`deploy/catalog/` in the repository for the proxy and persistent-service examples.

The setup/configuration walkthrough needs an admin account because it provisions and migrates
the Engine DB. A deployed run-only service token needs operator; project configuration applies
need developer. Clone, rehash, server startup and publishing documentation need admin. These
permissions are identical through the CLI, Python services and HTTP/UI routes. A role refusal
returns exit 26 (HTTP 403) and records `REFUSED` in the Engine DB audit history.

ETL rows use the run writer `etl-craft` in `CREATED_BY` and `UPDATED_BY`, including runs
started by a person. Action audit rows retain the signed-in account with kind `USER` or
`SERVICE`; automatic engine actions use `SYSTEM`. Starting `etl-craft server` requires an
admin service token in `ETL_CRAFT_TOKEN`.

## Protect a human correction

After the first successful `SUPPORT_DM` run, serve this project and sign in as a USER admin
through its [HTTP sign-in flow](../../docs/deploying/security.md). With that access token in
`ACCESS_TOKEN` and the server URL in `SERVER`, correct Ann's name:

```bash
curl --fail-with-body "$SERVER/api/v1/pipelines/SUPPORT_DM/tasks/agents/values" \
  -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' \
  -d '{"key":{"agent_code":"A01"},"values":{"agent_name":"Anne"},"reason":"Verified spelling"}'
```

Run `SUPPORT_DM` again: `ds.agents` retains `Anne` while unlocked source fields still refresh.
The response and the `/values/provenance` endpoint attribute the protected name to the admin;
ETL retains the engine identity on fields it changes. To release the correction:

```bash
curl --fail-with-body "$SERVER/api/v1/pipelines/SUPPORT_DM/tasks/agents/values/unlock" \
  -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' \
  -d '{"key":{"agent_code":"A01"},"columns":["agent_name"],"reason":"Use source spelling again"}'
```

The next `SUPPORT_DM` run restores `Ann`. This opt-in walkthrough uses native DuckDB or
PostgreSQL; protection refuses the Iceberg/cloud variants. It creates no automatic file-upload
flow. See [the full protection contract](../../docs/guides/human-overrides.md), including
backing up warehouse-side evidence with the business tables.

## Run an individual SQL task

To execute a task independently without reopening a demo pipeline run, choose its task code
from `etl-craft steps --pipeline_code SUPPORT_DM` and run:

```bash
etl-craft run --pipeline_code SUPPORT_DM --task_code area_summary --standalone \
  --ignore-dependencies --reason "demo inputs already verified"
```

Outside dependencies require the
explicit reason above; selected descendants are included only with `--with-downstream`.
Finish any active exclusive pipeline run first. Script offsets, SQL watermarks, business-rule
flags and event checkpoints are shared, so those handlers need an explicit pipeline-run
context rather than standalone execution. Manual success never supplies an upstream pipeline
publication.
