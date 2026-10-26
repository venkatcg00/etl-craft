# Controller metrics and starting alerts

Scrape `GET /metrics` at `Orchestration.Api_address`. Both the FastAPI listener and
`controller --no-ui` return Prometheus text format 0.0.4 through the same operation.
The endpoint is unsigned, like `/health`, and intended for an internal deployment network.
Restrict access at the network or proxy boundary. No token, SQL text, filesystem path or
execution identity is exported, and a scrape writes no audit request.

The leader exports cluster gauges. A UI copy or controller which has not acquired leadership
exports `etl_craft_controller_leader 0` and its own process measurements, without cluster gauge
samples. Scrape each process directly; a load-balanced address mixes process counters.
Controllers expose agent measurements from recorded registrations and attempt ownership;
agents remain API clients and open no HTTP port.

## Series

All names below include the `etl_craft_` prefix. `project` is the Engine DB project ID, `agent`
is the registered agent ID, and `slot_kind` uses the existing `python` and `warehouse` values.
These are the only identity dimensions; individual pipelines, runs, tasks and attempts are
never labels. `query` is the catalog query name or `unknown`, never SQL or bind values.

| Name after the prefix | Type | Labels and meaning |
| --- | --- | --- |
| `controller_leader` | gauge | 1 while this process holds leadership, otherwise 0 |
| `controller_heartbeat_age_seconds` | gauge | Seconds since the leader committed its heartbeat |
| `runs` | gauge | `project`, `status`: current QUEUED and IN-PROGRESS logical runs |
| `attempts` | gauge | `project`, `status`: QUEUED, CLAIMED and RUNNING attempts |
| `queue_depth` | gauge | `project`, `pool`, `slot_kind`: queued attempt count |
| `queue_oldest_age_seconds` | gauge | Same labels: seconds since the oldest queued attempt was recorded |
| `active_agents` | gauge | `pool`: ACTIVE registrations with a heartbeat less than 30 seconds old |
| `agent_slots` | gauge | `pool`, `agent`, `slot_kind`, `status`: capacity, used or free slots; free uses the admission accountant's CPU, memory and overlapping-host limits |
| `agent_running_children` | gauge | `pool`, `agent`: owned RUNNING attempts |
| `agent_heartbeat_age_seconds` | gauge | `pool`, `agent`: age of the current ACTIVE or DRAINING registration; reconciled GONE registrations leave the exposition |
| `gate_waits` | gauge | `project`: persisted waits with NEXT_CHECK_AT under a nonterminal run |
| `runs_ended_total` | counter | `project`, `status`: terminal logical-run transitions observed by this leader process |
| `attempts_ended_total` | counter | `project`, `status`: terminal attempt outcomes observed by this leader process |
| `lease_expiries_total` | counter | `project`: attempt ownership expiries committed as LOST |
| `lost_attempts_total` | counter | `project`: committed LOST attempts; uncertain side effects require investigation |
| `retries_total` | counter | `project`: additional attempts queued under an existing task run |
| `admission_latency_seconds` | histogram | `project`: recorded run creation to successful admission; already-active creations have near-zero waiting time |
| `dispatch_latency_seconds` | histogram | `project`: queued attempt to successful ownership claim |
| `engine_db_statement_latency_seconds` | histogram | `query`: controller-engine cursor execution duration |

Histograms expose cumulative `_bucket` samples, including `le="+Inf"`, plus `_sum` and `_count`.
Bucket upper bounds in seconds are 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5,
10 and 30. For admission p99, for example:

```promql
histogram_quantile(0.99, sum by (le) (rate(etl_craft_admission_latency_seconds_bucket[5m])))
```

Event counters describe transitions observed and committed through the leader's process;
they reset when that process restarts. They are not durable historical totals. Use existing
run-history services for date-filtered Pipeline Insights calculations. A transaction or
savepoint rollback publishes no lifecycle observation. Work performed in another process
is represented when the controller observes its recorded outcome.

## Cost and failure behavior

A leader scrape performs four reads, regardless of the number of projects or terminal history
rows. SQLite additionally starts its stable read transaction. Current-state indexes avoid
scanning ended runs and historical agents. Only current attempt reservations and current agent
registrations are materialized. The controller's existing probe connection allowance is shared
with gate probes; scrapes use a dedicated session outside the work pool and add no Engine.

An occupied probe session returns 503 with `metrics connection is busy`, the expected free
session, and the retry remedy. Database/schema failures return 503 naming the Engine DB and
the connectivity/migration remedy. A failed scrape never becomes a zero-valued cluster.
Gauges with no matching current rows have no samples; a follower omits cluster samples.

## Starting alert rules

Load [the example Prometheus rules](../examples/prometheus-rules.yaml) into Prometheus and
adjust the durations for the team's operational budget. Start with:

- Queue age above 60 seconds for two minutes: inspect agent freshness, free slots, task
  resource requests and pipeline/project/warehouse admission limits. Queue age alone does
  not prove agents are incorrectly idle.
- A LOST attempt since the controller started: inspect its recorded owner, lease, attempt
  log and side effects before repairing the run. The example intentionally remains firing
  while that process's LOST counter is nonzero.
- A stale or absent leader: inspect the controller process, its Engine DB connection and
  leadership logs. UI followers do not substitute for a missing controller heartbeat.

Prometheus's normal stale-series handling applies when the HTTP listener itself disappears.
The example contains no schedules, actions or automatic run repairs.
