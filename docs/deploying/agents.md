# Agents and the PostgreSQL queue

Set `Deployment.Profile: CONTROLLER_AGENTS` explicitly on the controller and API copies.
PostgreSQL alone does not select cluster execution. `LAPTOP`, `CONTROLLER` and an unset profile
keep local execution, including with a PostgreSQL Engine DB. Cluster mode refuses SQLite.

The controller admits attempts to the queue and monitors them. An attempt without an eligible
agent stays `QUEUED`, naming its pool and slot kind; it never runs on the controller instead.
Each agent runs one process per task and reaches the deployment only through its API. Agents
open no listening ports and need no Engine DB account, URL or credentials.

## Start an agent

Issue a distinct credential for each agent, as an administrator:

```sh
etl-craft token create --name host-a --role agent --api https://craft.example.com
```

Keep the returned token secret. An agent credential cannot invoke a person's actions, and a
person's credential cannot register, claim, renew or report an agent attempt. Revoking the token
prevents further requests; the agent stops its task processes before their leases expire.

Install the same package version as the API and controller, copy the project's files to the
agent, then start it:

```sh
export ETL_CRAFT_TOKEN="$HOST_A_AGENT_TOKEN"
etl-craft agent --api https://craft.example.com --project /srv/project \
  --pool default --python-slots 4 --warehouse-slots 16
```

`--project` resolves `craft-connector.yml` in that directory. Without it, the normal config
resolution applies, including `--config` and `ETL_CRAFT_CONFIG`. Paths such as
`python_scripts/load.py` and `sql_scripts/orders.sql` remain relative to that project copy.
The agent ignores the Engine section, which may be absent. Warehouse and email settings and
credentials required by its tasks must be available on the agent. Keep project copies aligned
until pinned bundles are available. HTTP is accepted only on loopback; use HTTPS elsewhere.

Claims wait up to 30 seconds on notifications. A single session connection on each API copy
serves all claim polls. A missed notification falls back to another queue check within ten
seconds. Renewals run independently of claim polls and log uploads, every ten seconds. The
Engine DB clock governs the 60-second attempt leases. A child also watches its own monotonic
lease deadline and its parent's process birth identity: it stops before lease expiry if the
agent dies or cannot renew. A retry gets a new attempt row; expired ownership is never adopted.

## Capacity and controls

The shared resource report measures CPU and memory allocation, reserves and pressure at startup
and on renewals. Flags cap the derived Python and warehouse slots; they cannot create capacity.
Unknown CPU or memory capacity is visible and admits no task. Claims account for reservations
on overlapping allocations on the same host, so two agents cannot count that host twice.
A task must fit one agent; capacity on different hosts is never combined to fit a task.

```sh
etl-craft agents --api https://craft.example.com
etl-craft agent drain --agent-id 12 --api https://craft.example.com
etl-craft agent resume --agent-id 12 --api https://craft.example.com
etl-craft agent stop --agent-id 12 --api https://craft.example.com
```

`drain` stops new claims and completes current tasks; `resume` permits claims again; `stop`
drains and exits. Commands and their acknowledgements are durable. A command for a `GONE` agent
fails by name. SIGTERM drains for `--drain-seconds` (default 300), then stops remaining children
with a failed outcome, `agent drained`. The controller withdraws agents after three missed
heartbeats and reconciles expired attempts as `LOST` before retrying.

The agents service supplies registry status, slot usage and free capacity, running attempts,
resource evidence and pending commands to `agents`, `doctor`, `status` and the API.

## Locks and warehouse statements

Writers to one target, and to a single-writer warehouse, share one lock row in the Engine DB. Its
holder renews it with its lease. A warehouse keeps running a statement after its client dies,
and may still commit it, so a lock left by an ended or expired holder passes only after the next
holder has stopped that holder's statements and confirmed that none still run. Until then the
lock stays with the old holder, and the new writer fails after a bounded wait, naming the target,
the holder and what still runs. Failed work confirms its own statements stopped before
unlocking; a failed confirmation retains the row.

Every warehouse session a holder opens is labelled with it: `application_name` on PostgreSQL,
the client `source` on Trino and `QUERY_TAG` on Snowflake. Scripts must not change these
settings. Attempt labels include an owner or credential namespace, so deployments sharing a
warehouse do not reuse labels. The agent taking over stops the old holder's statements with its own warehouse
credentials. Embedded DuckDB needs no stop: a statement ends with its process. The warehouse
login needs the right to stop its own sessions (PostgreSQL `pg_terminate_backend`, Trino
`kill_query`, Snowflake `SYSTEM$CANCEL_QUERY`).

Databricks requires SQL connector 4.5.0 or newer (installed by the `databricks` extra).
Sessions carry an `etl_craft_holder` query tag. The adapter reads every active
Query History page, cancels matching statement IDs through the Statement Execution API and
confirms that no tagged query remains. The SQL connector's existing authentication is reused;
the warehouse identity needs query-history access and permission to cancel those statements.
Missing permissions, incomplete history or an unconfirmed stop leave the control pending.
Agents never open an Engine DB connection for cancellation.

Operator stop and abort requests wake waiting agent claims through pool and agent notifications.
Renewal remains the fallback when a notification is lost. The agent stops the child and confirms
its tagged warehouse statements ended before reporting a cancellation acknowledgement.

## Connection pooling and shared logs

Ordinary API queries can use PgBouncer transaction pooling. `LISTEN` needs a direct connection
or session-mode pool to the **same Engine DB**:

```yaml
Engine:
  prod:
    Jdbc_url: jdbc:postgresql://pgbouncer:6432/etl_craft
    Listen_jdbc_url: jdbc:postgresql://postgres-rw:5432/etl_craft
    User: etl_craft
    Secret: ENGINE_SECRET
    Schema: public
```

`Listen_jdbc_url` is optional, PostgreSQL only, and changes only the address. It reuses the
profile's authentication; unset, it uses `Jdbc_url`. Both the controller's leadership listener
and API claim listener use this address. At startup, and in `doctor`, a bounded NOTIFY round
trip proves delivery. Failure refuses startup with the masked address and a direct/session-pool
remedy; successfully accepting `LISTEN` alone is insufficient with transaction pooling.

Agents upload bounded ordered log chunks through the API. For now their text and hashes are
stored in the Engine DB so any API copy can serve a live log, including after another copy
restarts. Identical retries are acknowledged once; changed or out-of-order chunks fail by
attempt and sequence. Every upload is fenced by live attempt ownership. Logs and their chunk
rows are subject to the retention policy when configured retention is available.

Target writes, target maintenance and the single-writer warehouse queue use one shared durable
resource lock. Its database key is unique; expired or ended holders are replaced atomically.
A wait is bounded and names the target, holder and held duration on timeout. Inspect the holder
before retrying a blocked action.

## Pools, limits and resource placement

Define active pools in `config/pools.csv` (`POOL_NAME,DESCRIPTION,ACTIVE_FLAG`), including
`default` for tasks without `POOL`. Apply the file with `etl-craft config apply`. Agents must
register in an existing active pool. Set task parameters `POOL`, `CPU_REQUEST` and
`MEMORY_REQUEST_MB`; resource requests are positive whole numbers. Defaults are one CPU and
512 MB for Python, and 0.25 CPU and 128 MB for warehouse work. Gates take no task slots.

`MAX_PARALLEL_TASKS` in `pipelines.csv` limits each pipeline, across its runs and owners.
`PRIORITY` defaults to zero; higher numbers are claimed first and equal priorities use the
oldest queued attempt. A pipeline at its limit is skipped so another can run. Set
`Warehouse.Max_warehouse_queries` to cap warehouse-kind attempts across pipelines and pools.
Only claimed and running attempts count. Limited work stays QUEUED with its reason in
status, explain, the API and its attempt record.

Claims reserve CPU and memory atomically and never combine different agents' capacities.
`validate` rejects requests that fit no active agent in the pool; absent agents leave work
queued. The same check uses measured local allocation when no agents are selected. Local
process and per-kind slot ceilings remain independent of the pipeline limits.

Agent measurements are cgroup-aware and refresh on the existing heartbeat. `--cpu N` and
`--memory-mb N` only lower measured capacity. `--declared-capacity`, with both declarations,
fronts an external quota (for example a script's own runtime); the registry labels it DECLARED.
etl-craft itself launches no external jobs. `--python-slots 0` or `--warehouse-slots 0` makes
an agent single-kind. Slots remain upper bounds alongside CPU and memory.

`agents`, `status`, `doctor` and the API share advertised, reserved and free resources. Draining
withdraws free capacity. Endings, LOST reconciliation and fenced lease expiry release active
reservations. Keep measured-host agents' scopes accurate: overlapping host allocations share
one resource budget.
