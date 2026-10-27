# Support Insights: the etl-craft demo

A small support-analytics platform: two clients send their customer-support interactions in their
own shapes, and etl-craft lands them, parses them into one shape, keeps the agents and their
history, builds a support fact with checks on it, and summarises it per area and team.

```text
lnd (landed, as sent) ─► prs (parsed, typed) ─► pre_dm (every client, one shape) ─► dm (fact, summary)
                         ds (agents, support areas) ─┘   cdc (agents' teams over time)
aud: the Engine DB's own tables, cloned on its own schedule
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
etl-craft run --pipeline-code CLIENT_ALPHA
etl-craft run --pipeline-code CLIENT_BETA
etl-craft run --pipeline-code SUPPORT_DM      # fails at flaky_feed; the alerts say so
etl-craft run --pipeline-code SUPPORT_DM      # succeeds on the same client runs
etl-craft generate-docs --with-warehouse      # then open catalog/gaps.html
```

Audit JSON and logs redact nested secret fields and connection URL credentials; the demo
configuration still names secret variables rather than storing their values.

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

The mart's end alert waits for the parallel business-rule check as well as its data outputs,
so its outcome describes completed work even when the ANY summary finishes first.

The controller schedules and supervises these pipelines. External schedulers can submit runs
through the authenticated HTTP API. Install `etl-craft[ui]` for all-in-one HTTP, or run
`etl-craft controller --no-ui` for the standard-library `/health` and `/metrics` endpoints. This SQLite demo
does not support separate `etl-craft ui` copies; those require PostgreSQL. Remote commands
use `--api https://etl.example.com` or `ETL_CRAFT_API`, with the usual service token or login.


## Files

| Path | Holds |
|---|---|
| `craft-connector.yml` | SQLite Engine DB, DuckDB warehouse, Mailpit, cloning into `aud` |
| `metadata/support_insights.sql` | source-column definitions and the pipelines, tasks, parameters, dependencies and rules, as `CFG_` rows; runs as written on SQLite and PostgreSQL |
| `python_scripts/` | the scripts that land each source, plus a flaky feed and a slow task |
| `sql_scripts/` | the SELECTs the SQL tasks wrap in their actions |
| `warehouse_schemas.sql` | the schemas to create first: etl-craft never creates warehouse schemas |
| `prepare.py` | creates those schemas in `warehouse.duckdb`, and loads the metadata into `engine.db` |

The end-to-end tests run this demo from the built wheel installed with pip, on SQLite and
PostgreSQL Engine DBs and on PostgreSQL, DuckDB, DuckDB over Iceberg and Trino warehouses;
and installed with uv, on SQLite and DuckDB
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
need developer. Clone, rehash, controller startup and publishing documentation need admin. These
permissions are identical through the CLI, Python services and HTTP/UI routes. A role refusal
returns exit 26 (HTTP 403) and records `REFUSED` in the Engine DB audit history.

ETL rows use the run writer `etl-craft` in `CREATED_BY` and `UPDATED_BY`, including runs
started by a person. Action audit rows retain the signed-in account with kind `USER` or
`SERVICE`; automatic engine actions use `SYSTEM`. Starting `etl-craft controller` requires an
admin service token in `ETL_CRAFT_TOKEN`.

## Protect a human correction

After the first successful `SUPPORT_DM` run, serve this project and sign in as a USER admin
through its [HTTP sign-in flow](../../docs/deploying/security.md). With that access token in
`ACCESS_TOKEN` and the server URL in `SERVER`, correct Ann's name:

```bash
curl --fail-with-body "$SERVER/api/v1/values" \
  -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' \
  -d '{"pipeline_code":"SUPPORT_DM","task_code":"agents","key":{"agent_code":"A01"},"values":{"agent_name":"Anne"},"reason":"Verified spelling"}'
```

Run `SUPPORT_DM` again: `ds.agents` retains `Anne` while unlocked source fields still refresh.
The response and the `/values/provenance` endpoint attribute the protected name to the admin;
ETL retains the engine identity on fields it changes. To release the correction:

```bash
curl --fail-with-body "$SERVER/api/v1/values/unlock" \
  -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' \
  -d '{"pipeline_code":"SUPPORT_DM","task_code":"agents","key":{"agent_code":"A01"},"columns":["agent_name"],"reason":"Use source spelling again"}'
```

The next `SUPPORT_DM` run restores `Ann`. This opt-in walkthrough uses native DuckDB or
PostgreSQL; protection refuses the Iceberg/cloud variants. It creates no automatic file-upload
flow. See [the full protection contract](../../docs/guides/human-overrides.md), including
backing up warehouse-side evidence with the business tables.

## Run an individual SQL task

To execute a task independently without reopening a demo pipeline run, choose its task code
from `etl-craft steps --pipeline-code SUPPORT_DM` and run:

```bash
etl-craft run --pipeline-code SUPPORT_DM --task-code area_summary --standalone \
  --ignore-dependencies --reason "demo inputs already verified"
```

Outside dependencies require the
explicit reason above; selected descendants are included only with `--with-downstream`.
Finish any active exclusive pipeline run first. Script offsets, SQL watermarks, business-rule
flags and event checkpoints are shared, so those handlers need an explicit pipeline-run
context rather than standalone execution. Manual success never supplies an upstream pipeline
publication.

### Scoped holds and maintenance stops

Use the pipeline code and run id printed by the demo run. Pause a selected task without
pausing unrelated work, or drain a run before maintenance:

```bash
etl-craft hold --pipeline-code PIPELINE --run-id 42 --task-code TASK --reason "inspect input"
etl-craft release --pipeline-code PIPELINE --run-id 42 --task-code TASK --reason "ready"
etl-craft stop --pipeline-code PIPELINE --run-id 42 --reason "maintenance"
etl-craft control-request --control-id 17 --format json
```

Replace `PIPELINE`, `TASK`, `42` and `17` with this project's code and returned identities.
A request is finished only at `COMPLETED`; inspect `run_status` and the affected attempt
acknowledgements. Server warehouse aborts without a query acknowledgement adapter refuse;
use `stop` to drain them. A task-only control leaves the rest of the run available.

Scoped recovery and restart are preparatory operations. Replace `RUN_ID` with an exact run from
`etl-craft history`; do not use a successful run for recovery:

```bash
etl-craft run --pipeline-code SUPPORT_DM --run-id RUN_ID --restart --request-key demo-restart --reason "new context" --format json
etl-craft capabilities --pipeline-code SUPPORT_DM --run-id RUN_ID --format json
```

Run the returned `execution_run_id` explicitly. Restart retains current checkpoints and human
locks; it can repeat data writes. Use `run --controls-file selected-controls.json --format json`
for an explicit selection with per-object outcomes; see `docs/examples/operator-controls.md`.

Team-owned config is independent of package metadata. The optional
`metadata/team_config.sql` creates `CFG_TEAM_ROUTING` without package base fields. Apply it
with your team's database scripts or as a project migration. Plain SQL can maintain it;
etl-craft does not guard, capture, export, document or clone its rows. `AUD_` stays reserved.

## Find and operate a tagged group

The client pipelines carry `support` and their client tag; the mart carries `support` and `mart`.

```bash
etl-craft pipelines --tag support --tag client-alpha
etl-craft status --tag support
etl-craft history --tag support --limit 20
etl-craft pause --tag support --reason "demo maintenance" --yes
etl-craft resume --tag support --reason "demo ready" --yes
```

Omit `--yes` from pause or resume to print the selection without changing it. The generated
catalog shows the same labels, with shareable filtered links from its DAGs overview.

### Running a child from a task

A parent ETL task can use `HANDLER=RUN_PIPELINE`, `TRIGGER_PIPELINE_CODE=CLIENT_ALPHA` and
`TRIGGER_MODE=WAIT` (or `DETACH`) instead of relying on independently timed schedules. Keep
ordinary task dependencies for ordering and run the controller. WAIT frees the agent slot;
explicit reruns launch a new child while automatic retries retain its identity. See the
[pipeline-trigger configuration example](../../docs/guides/pipeline-triggers.md#configuration-file-example)
for the CSV rows to apply to a project copy; the default demo scenario remains unchanged.

### Pipeline groups in a shared deployment

The demo starts with open pipelines. An admin can create a group with
`etl-craft group add --name support` and add an account with
`etl-craft group add-member --name support --account alice`. Then run
`etl-craft access grant --project default --group support --pipeline SUPPORT_DM --permission CAN_RUN --reason "Demo"`
to grant access on a pipeline (or use `--tag` for a tag). Deployment viewers can then operate only their granted
pipelines. The [permission guide](../../docs/deploying/security.md#pipeline-permission-groups)
explains SSO group IDs, retained run claims and the whole-project static catalog boundary.

## Unmanaged SQL steps

A `SQL_SCRIPT` task can follow a managed task through an ordinary `SUCCESS` dependency.
Set `SCRIPT_FILE=sql_scripts/maintenance.sql` and `RETRY_ON=NONE`; the optional
[`sql_scripts/maintenance.sql`](sql_scripts/maintenance.sql) example reads the run context.
It is not wired into the demo pipelines. See the [SQL scripts guide](../../docs/guides/sql-scripts.md)
for writes, autocommit, retries and the per-statement attempt log.

For deployment with agents, configure artifact storage and publish the project with
`etl-craft bundle publish --project-dir examples/demo --api URL` before admitting runs. Local
working-tree execution needs no artifact store. See the pinned project bundles guide.

`--project CODE` selects the metadata namespace and must agree with `Orchestration.Project`
when the file sets it. There is no implicit project. Use `config apply --project-dir` for a
project folder, whose own `craft-connector.yml` sets its project code. A shared Engine DB can
hold another project with the same pipeline codes and its own approved Warehouse, Email and
Cloning definitions; see the [project bundle guide](../../docs/deploying/project-bundles.md).

## Ten projects on one Engine DB

The script creates one configured folder per project, including its change CSVs and task script;
project codes come from `Orchestration.Project`, never the folder names.

After setup, with an admin credential and the deployment's `default` pool active, run:

```bash
python examples/demo/ten_projects.py examples/demo/craft-connector.yml
```

The script creates `demo_team_01` through `demo_team_10`, each with a `POOL_DEMO` pipeline and
one `pool_demo.py` task, then runs each pipeline through the normal CLI. All ten use the Engine
DB from that configuration. Existing projects and deployment pools are preserved. The generated
CSV files are temporary; the project definitions and execution history remain in the Engine DB.
Config files carry only changes: reapplying these files is a no-op and leaves other rows
in each demo namespace untouched. Only explicit `ACTIVE_FLAG=N` retires a row.

For another configuration path, copy `python_scripts/pool_demo.py` into that project's
`python_scripts/` first. This example uses local execution without an artifact store; the
[agent deployment guide](../../docs/deploying/agents.md) covers publishing projects for queue
execution. The contract gate is `ETL_CRAFT_REQUIRE_SERVICES=1 uv run pytest -m cluster`; it exercises both providers on
PostgreSQL, with agent-only registration/drain/API-failover cases selected for queue execution.

## History retention

The demo leaves retention disabled. To bound its history, add the commented `Retention`
section from its connector file, review `etl-craft purge --dry-run --format json`, then run
`etl-craft purge` as an administrator. Files expire separately from run history; retained
consumption and recovery evidence can keep old run skeletons. See the
[retention guide](../../docs/deploying/engine-db.md#retaining-project-history).

For a verified history archive, create a separate schema in the configured warehouse, then run
`etl-craft purge --archive-to history --dry-run` and `etl-craft purge --archive-to history`.
Rerun the same destination after interruption; pending archive work blocks plain purge.
See [history retention](../../docs/deploying/engine-db.md#retaining-project-history).

To expire operational rejects, export the configuration and set `REJECT_RETENTION_DAYS=30`
on the intended `tables.csv` rows. Apply the files, review `etl-craft purge --dry-run`, then
purge. Empty periods keep rows forever. Set snapshot/orphan periods only with the supported
Trino Iceberg maintenance dialect; the other demo warehouses must leave those fields empty.
Business rows and active business-rule findings are retained.


Attempt logs remain complete files under `logs/`; the task summary keeps reported values plus
an 8 KiB output tail. Set `Logs.Tail_bytes` in `craft-connector.yml` to change that byte budget.
For agents, the API publishes full logs to `Logs.Store` (default `file://<Log_dir>/agents`),
with an explicit `Logs.Profile` for S3; see the deployment logging guide.

### Business-rule scope

Support Insights rules are complete SELECTs returning `fact_key` from `$$rows_in_scope`.
Normal checks flag and clear only this run's active rows. Corrected rows outside the run and
rows deleted from the target keep their flags until an explicit full check with `run --force`
for the selected BR task and an exact run identity. The engine adds no periodic rechecks;
teams configure ordinary pipelines and schedules themselves. Returned, newly flagged, cleared
and dropped counts appear per rule in the task's reported values and log. Upgrading converts
stored fragment rules to complete SELECTs with EXISTS; export config again after upgrading.
