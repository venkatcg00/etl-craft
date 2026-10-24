# Deployment profiles and sizing

etl-craft measures the CPU and memory its process may use. With a declared deployment profile,
`run`, `controller` and `ui` size task slots and HTTP server threads from that
measurement; `Orchestration.Db_pool_size` sets their Engine DB pool ceiling. Without a profile,
sizes come from the `Orchestration` settings and the measurement is only logged and shown by
`doctor`.

## Choose a profile

```yaml
Deployment:
  Profile: CONTROLLER        # LAPTOP | CONTROLLER | CONTROLLER_AGENTS
```

The `Deployment` section goes after `Orchestration`. It has no profile blocks: `Profile` names
the deployment shape. For a different shape per environment, write a variable name and set it
in each environment.

| Profile | For | Reserve for the rest of the host | Task slots | Server threads |
|---|---|---|---|---|
| `LAPTOP` | a machine a person also uses | half the CPUs (at least 1) and half the memory (at least 2 GB) | from what is left | 4 per CPU, at most 4 |
| `CONTROLLER` | a dedicated host running every role | a tenth of the CPUs (at least 0.5) and of the memory (at least 1 GB) | from what is left | 4 per CPU, at most 40 |
| `CONTROLLER_AGENTS` | a controller with separate agents | as `CONTROLLER` | none: agents run tasks | as `CONTROLLER` |

`LAPTOP` also requires `Orchestration.Api_address` to be a loopback address. `CONTROLLER_AGENTS`
requires a PostgreSQL Engine DB. The controller queues work for [API-only agents](agents.md);
with no eligible agent it leaves attempts queued and never executes them locally.

etl-craft never picks a profile for you and never writes `craft-connector.yml`. `doctor`
recommends `LAPTOP` when no profile is set, because it cannot tell whether the host is shared;
choose `CONTROLLER` for a dedicated host.

## How sizes are derived

1. **Measure.** The allocation is the smallest of the CPU affinity, the CPU quota of the
   process's cgroup and every ancestor cgroup (v2, else v1), and the host CPU count. Memory is
   the smallest cgroup memory limit, else the host total. Each value is reported with the file
   or call it came from.
2. **Reserve.** The profile's reserve is set aside, and the etl-craft process itself (the
   controller and the HTTP server, counted once) takes 0.5 CPU and 256 MB.
3. **Divide.** What is left becomes task slots at these estimates per task process:

   | Kind | CPU | Memory |
   |---|---|---|
   | SQL, business-rule and alert tasks | 0.25 | 128 MB |
   | Python scripts | 1 | 512 MB |

   The task slots are what SQL tasks fit; Python slots are what Python scripts fit, never
   more than the task slots.
4. **Cap.** `Local_python_slots` and `Local_warehouse_slots`, when written, are ceilings:
   a measured size never exceeds them. `CFG_PIPELINES.MAX_PARALLEL_TASKS` separately caps
   each pipeline; raising it never raises the machine's measured capacity.
5. **Pool.** `Orchestration.Db_pool_size` defaults to 10 connections per process, with no
   overflow. Profile sizing does not independently enlarge that declared ceiling.

`doctor` shows each step's result and the constraint that bound it, for example
`26 (CPU: 6.7 free at 0.25 each)`.

## Fallbacks and refusals

- **CPU unavailable:** sized as one CPU, with at most one server thread.
- **Memory unavailable:** task slots are not limited by memory.
- **No task slot fits:** `CONTROLLER` starts with no slots and logs why; runs stay queued. `run`
  refuses to start, naming the allocation, the need and the remedy.
- **Less memory than the etl-craft process needs (256 MB):** both refuse to start.

## Connection budget

PostgreSQL controller, `ui` and local `run` processes reserve their maximum connection footprint
before starting work. `Orchestration.Db_pool_size` defaults to 10 (minimum 1), with no overflow.
Local task children use `NullPool` and return each connection after use. API-only agents and their
task processes open zero Engine DB connections.

`doctor` reports the registered deployment total, or a proposed controller when none is running.
Before setup creates the reservation table, it checks that proposed footprint; missing tables
and pending migrations remain separate doctor checks.
A startup or doctor check fails above 80% of PostgreSQL `max_connections`, naming the total and
remedy. Admission is serialized, so two replicas cannot spend the same remaining budget.

The total is the sum of each process's pool, dedicated sessions and local task slots. Dedicated
sessions include ownership renewal (one per process), controller leadership/LISTEN (one per
controller), and API claim notifications (one per API copy, including an integrated controller), plus the bounded probes below.
Thus a controller reserves `pool + 4 + local slots`, a standalone `ui` reserves
`pool + 4 + local slots`, and a local `run` reserves `pool + 2 + local slots`. Session allowances are conservative even before the
first claim subscription. UI task slots cover synchronous HTTP task selections. A shared process
ceiling applies across API callers and controller pool instances; excess work stays queued before
claiming an attempt or starting its child. One separately bounded SQL gate-probe connection
is included in each role, without retaining an agent slot during gate waits. A standalone `ui`
also reserves one independent readiness-probe connection, so health checks do not wait behind
a busy request pool or a gate query. Sessions use the same SQLAlchemy engine and independent
unpooled connections; no second engine is created for LISTEN.

Reservations renew every 15 seconds for the configured Engine DB outage budget plus 60 seconds. Clean shutdown closes work before releasing
capacity. An expired reservation stays charged while its owning PostgreSQL session is alive;
a missing heartbeat alone never proves its connections stopped. Tagged child sessions also
retain the reservation if the parent dies. Engine DB sessions carry
`application_name=etl-craft-process-<reservation id>` for this ownership check. Ownership loss stops the process,
and expired owners cannot reopen pooled connections or renew an ended reservation. A crashed
process's capacity becomes available after its sessions end and its reservation expires.

PgBouncer transaction mode may front `ui` request pools. Set `Engine.<profile>.Listen_jdbc_url`
to the same Engine DB's direct/session-mode address for ownership and LISTEN. The controller
connects directly because its leadership and advisory locks require session affinity. Engine DB
credentials remain host-local and use the existing profile authentication settings.

`publish-docs` serves static files and releases its idle Engine DB pool before waiting for HTTP
requests. Short provisioning/inspection commands use the remaining server headroom rather than
holding a long-lived process reservation.
