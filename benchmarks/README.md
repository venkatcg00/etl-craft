# Controller load benchmark

This is a manual/release developer tool, excluded from PR CI. Use an isolated PostgreSQL
Engine DB, object stores and warehouse, never a team's live deployment. No extra dependency is
needed beyond the repository's existing environment and PostgreSQL/UI extras.

The reference deployment is one controller (4 vCPU, 12 GiB), ten agents (2 vCPU, 8 GiB each),
and PostgreSQL (4 vCPU, 16 GiB), in one zone. Install the same candidate wheel on every role.
Record its version, commit, wheel hash, CPU/memory allocations, PostgreSQL configuration and
network placement with the report. The 24-hour reference run waits until the S8 Support
Insights demo completes without failures. A small workstation run does not satisfy that gate.

Prepare a normal `craft-connector.yml` with `Deployment.Profile: CONTROLLER_AGENTS`, a disposable
PostgreSQL Engine DB, `Artifacts.Store`, `Orchestration.Api_address`, and the team's existing
Warehouse and Secrets configuration. Use absolute paths for local stores. Back up and restore
procedures are in [Operations](../docs/deploying/operations.md). Initialize with `etl-craft setup`,
issue an administrator SERVICE token for the driver and a distinct agent token per host.
Agent bootstrap files use existing settings and need no Engine DB credentials. Keep tokens in
the credential environment; the manifest must not contain their values.

From the repository root:

```sh
uv run python -m benchmarks.generate /srv/craft-load --config /srv/template/craft-connector.yml \
  --projects 100 --pipelines 100 --tasks 10 --task-seconds 1 --failure-rate 0
uv run python -m benchmarks.run /srv/craft-load --deployment /srv/deployment.json \
  --seconds 86400 --interval 5 --seed-runs 200 --connection-ceiling 80 \
  --report release/benchmarks/1.0.0.md
```

Generation refuses an existing output directory. It writes 100 projects × 100 pipelines ×
10 ordinary Python tasks, complete config CSVs, forward-only pipeline/task dependencies and
daily schedules distributed across 1440 UTC minutes. Each project has two independent root
pipelines; later pipelines depend on the one two positions earlier. `INPUT_PARAMS` configures
fake-task duration and failure rate. There is no benchmark handler or scheduler in the engine.
The runner applies and publishes through `config apply` and `bundle publish`, then starts the
controller, waits for leadership, starts the agents and waits for their registration. Initial
load uses `run --init-only` through the API for up to two roots per project, with at most
eight concurrent CLI submissions so that 200 roots can overlap; subsequent schedules are entirely
the controller's. Choose duration/slots so the reference run actually reaches 200 active runs.
The report records peak active runs; fewer than 200 cannot qualify the reference tick budget.

A deployment manifest gives argv arrays, not interpolated shell commands:

```json
{
  "controller": ["/srv/venv/bin/etl-craft", "--config", "/srv/template/craft-connector.yml", "controller"],
  "agents": [["ssh", "agent-a", "/srv/start-benchmark-agent"]],
  "metrics": "https://controller.internal/metrics",
  "probe": ["/srv/probe-benchmark"]
}
```

Supply ten agent commands for the reference cluster. Team-owned launch scripts inherit the
host's credentials and `exec` the existing controller/agent commands. For remote launchers,
forward TERM and wait for the remote process to exit: killing an SSH client alone is not a
remote stop guarantee. Stage the generated project/config files and the candidate wheel on
the appropriate hosts first. The controller API must be reachable by the agents before they
start; use HTTPS off loopback. The driver stops its launch process groups on exit and saves
controller/agent logs in the generated root. A process exit, unavailable scrape or failed probe
fails the harness rather than reporting a healthy zero.

`probe` must return JSON with `postgres_cpu_percent` (CPU use divided by the allocated PostgreSQL
CPU count), `connections` (deployment-wide live PostgreSQL sessions), `max_connections`, and
`controller_memory_bytes` (resident memory). The included Linux/Docker probe is:

```sh
uv run python -m benchmarks.probe --config /srv/template/craft-connector.yml \
  --postgres-container craft-postgres --postgres-cpus 4
```

The runner supplies `BENCHMARK_CONTROLLER_PID` for a local controller. A remote controller
launcher writes a PID file; run the probe on that host with `--controller-pid-file`. If
PostgreSQL is elsewhere, a team wrapper combines the two host measurements into that same JSON
contract (for example SSH to the database host for Docker CPU and to the controller for RSS).
The wrapper must fail if either measurement is unavailable. Managed services can supply CPU
from their monitoring API and sessions from `pg_stat_activity`; never substitute local host CPU.
The connection ceiling must match the deployment's configured allowance and cannot exceed 80%
of PostgreSQL `max_connections`. The read-only probe itself counts as a session.

Budgets live only in `budgets.py`. Admission p99 and tick p95 use conservative upper buckets
from the existing metrics histograms. At reference scale, tick observations are bucket deltas
only for intervals whose two endpoint scrapes both show at least 200 active runs; idle ticks
cannot hide overloaded ticks. No qualifying observations fails that budget. Small-scale smoke
runs use all observed tick intervals and disclose their lower active-run qualification. Queue-age p99 uses nearest-rank samples of the oldest
queue age only when that pool and slot kind report free slots. CPU, connections and memory use sampled
maxima; this is not continuous spike detection. Missing observations fail. Counters reset if
the controller restarts; the runner refuses an unexpected process exit. Preserve raw metrics
with external monitoring for reference evidence and inspect task failures/retries alongside
the report. Nonzero fake failure rate deliberately exercises the normal retry paths.

For a smoke run, use `--projects 2 --pipelines 5 --tasks 2 --task-seconds .1`, a 120-second run,
and `--report /tmp/craft-load-smoke.md`. Include that report in the PR description, never under
`release/benchmarks/`. Only qualifying release runs write results there. The runner exits
nonzero for any failed measured budget; report qualification still requires the reference
resources, 24-hour duration, 200 active runs and successful restore test.
