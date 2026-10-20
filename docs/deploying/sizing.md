# Deployment profiles and sizing

etl-craft measures the CPU and memory its process may use. With a declared deployment profile,
`run` and `controller` size their task slots, HTTP server threads and Engine DB pool from that
measurement. Without one, nothing changes: the sizes come from the `Orchestration` settings, as
before, and the measurement is only logged and shown by `doctor`.

## Choose a profile

```yaml
Deployment:
  Profile: CONTROLLER        # laptop | controller | controller-agents
```

The `Deployment` section goes after `Orchestration`. It has no profile blocks: `Profile` names
the deployment shape. For a different shape per environment, write a variable name and set it
in each environment.

| Profile | For | Reserve for the rest of the host | Task slots | Server threads |
|---|---|---|---|---|
| `laptop` | a machine a person also uses | half the CPUs (at least 1) and half the memory (at least 2 GB) | from what is left | 4 per CPU, at most 4 |
| `controller` | a dedicated host running every role | a tenth of the CPUs (at least 0.5) and of the memory (at least 1 GB) | from what is left | 4 per CPU, at most 40 |
| `controller-agents` | a controller with separate agents | as `controller` | none: agents run tasks | as `controller` |

`laptop` also requires `Orchestration.Api_address` to be a loopback address. `controller-agents`
requires a PostgreSQL Engine DB. Agents arrive in a later release: until then, `run` and
`controller` refuse to start with `controller-agents` instead of running tasks locally.

etl-craft never picks a profile for you and never writes `craft-connector.yml`. `doctor`
recommends `laptop` when no profile is set, because it cannot tell whether the host is shared;
choose `controller` for a dedicated host.

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
   | Python ingestion scripts | 1 | 512 MB |

   The task slots are what SQL tasks fit; ingestion slots are what ingestion tasks fit, never
   more than the task slots.
4. **Cap.** `Max_parallel_tasks`, `Local_ingestion_slots` and `Local_warehouse_slots`, when
   written, are ceilings: a derived size never exceeds them, and they never raise it.
5. **Pool.** The process's Engine DB pool holds 2 connections, one more per 8 task slots, and
   one per server thread. Task processes open one connection each.

`doctor` shows each step's result and the constraint that bound it, for example
`26 (CPU: 6.7 free at 0.25 each)`.

## Fallbacks and refusals

- **CPU unavailable:** sized as one CPU, with at most one server thread.
- **Memory unavailable:** task slots are not limited by memory.
- **No task slot fits:** `controller` starts with no slots and logs why; runs stay queued. `run`
  refuses to start, naming the allocation, the need and the remedy.
- **Less memory than the etl-craft process needs (256 MB):** both refuse to start.

## Connection budget

On a PostgreSQL Engine DB, `doctor` adds the server's pool, the pool's overflow (10) and one
connection per task slot, and warns when that exceeds 80% of `max_connections`.
