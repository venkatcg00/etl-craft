# Running the controller

`etl-craft controller` creates scheduled runs and supervises active local pipeline runs until it receives SIGTERM or Ctrl-C.
It uses the same task handlers, ready-task dispatch, gates, leases, cancellation guards, SLA checks
and finalization hooks as foreground `etl-craft run`.

With the optional `etl-craft[server]` extra, it also serves the [authenticated HTTP API](http-api.md)
at `Orchestration.Api_address` (default `127.0.0.1:8730`).

## Starting and submitting work

Initialize or upgrade the [Engine DB](engine-db.md), configure `Orchestration.Mode: local`,
and set `ETL_CRAFT_TOKEN` to an admin SERVICE account token. The controller and its schedules
record that account; a user session is refused for controller startup. Start it with the same
project configuration used by the CLI:

```bash
etl-craft --config /srv/etl-craft/craft-connector.yml controller
```

From another terminal, create a run with an explicit key:

```bash
etl-craft --config /srv/etl-craft/craft-connector.yml run \
  --pipeline_code SALES --init-only --run-key daily:2026-10-07 --run-date 2026-10-07
etl-craft --config /srv/etl-craft/craft-connector.yml history \
  --pipeline_code SALES --run-key daily:2026-10-07 --format json
```

Initialization performs the usual admission checks. The controller picks up an unowned
`IN-PROGRESS` run by its exact `pipeline_run_id`; it never chooses an ended run by recency.
Scheduled runs begin `QUEUED`. The oldest queued run of each pipeline is admitted through
the pipeline gates when there is no active sibling. Gate checks never sleep or enter the task
agent pool. Ready tasks are dispatched after each completion; automatic retries remain a
subsequent roadmap item. To run under external scheduler or another orchestrator, use
[remote mode](http-api.md#starting-runs-from-another-scheduler); `controller` refuses that mode.

With a [deployment profile](sizing.md), the controller sizes its pool, threads and Engine DB pool
from the measured allocation, and the settings below are ceilings. `Max_parallel_tasks` bounds
the controller's local pool, which every run it supervises shares;
`Local_ingestion_slots` and `Local_warehouse_slots` split it by kind (see
[running pipelines](../guides/running-pipelines.md)). Cooperative pipeline supervision and
pending gate checks do not use those slots. Runs still owned by another foreground process are left with that
process. Paused or inactive pipelines receive no dispatch; resuming a pipeline allows its
active run to continue.

## Schedules and logical dates

Configure schedules through a [project migration](engine-db.md). For example:

```sql
UPDATE CFG_PIPELINES
SET RUN_SCHEDULE = '0 2 * * *',
    SCHEDULE_TIMEZONE = 'America/New_York',
    SCHEDULE_START_DATE = '2026-10-01',
    CATCHUP = 'Y', MAX_CATCHUP_RUNS = 2, OVERLAP_POLICY = 'QUEUE'
WHERE PIPELINE_CODE = 'SALES';
```

Run `etl-craft validate` after editing metadata. `RUN_SCHEDULE` accepts five fields (minute,
hour, day of month, month, weekday), lists, ranges and positive steps; month names `JAN`–`DEC`
and weekdays `SUN`–`SAT` are supported. Sunday is 0 or 7. The macros are `@hourly`, `@daily`,
`@weekly`, `@monthly`, `@yearly` and `@annually`. Restricted day-of-month and weekday fields match either day,
following [cron's day-field rule](https://man7.org/linux/man-pages/man5/crontab.5.html).
A NULL schedule creates no automatic runs.

`SCHEDULE_TIMEZONE` is an IANA name. NULL uses `Orchestration.Timezone`, which defaults to
`UTC`. A missing wall-clock minute during a DST jump fires at the next valid minute; a repeated
minute fires once, at its first occurrence. Audit timestamps remain UTC. A scheduled run's
`RUN_DATE` is the tick's date in its schedule timezone; manual runs and stand-ins without an
explicit date use the project timezone. Generated remote DAGs use the schedule timezone for
both their start date and the `data_interval_end` date passed to CLI tasks.

Each tick gets `RUN_KEY = schedule:<UTC ISO instant>` and `TRIGGER_KIND = SCHEDULE`. The key
prevents duplicate ticks after restart. `SCHEDULE_START_DATE` includes that local calendar date;
when absent, scheduling begins after the pipeline's creation timestamp.

With `CATCHUP = 'N'` (default), only the latest due tick is queued. With `CATCHUP = 'Y'`, up to
`MAX_CATCHUP_RUNS` latest ticks are queued, oldest first. Older missed ticks are recorded
`SKIPPED`, with reason `missed while no controller was running`. Each tick commits separately,
so long catch-up histories release the database writer lock between records.

`OVERLAP_POLICY = 'SKIP'` (default) records a due tick `SKIPPED` with reason
`previous run still active` when the pipeline already has queued or active work. `QUEUE`
preserves the tick for later admission. Only one run per pipeline can be `IN-PROGRESS`.
You can inspect or cancel a queued run by its exact `--run-id` or `--run-key`.
These columns govern the local controller; the existing `PIPELINE_PARAMETERS.CATCHUP` boolean
continues to control external scheduler's generated DAG catch-up setting.

## Gate waits and dispatch

Migration `0016_gate_waits` adds `AUD_GATE_WAITS`. Each run and task has at most one current
wait; NULL `TASK_ID` identifies pipeline admission. `FIRST_CHECK_AT`, `WAIT_UNTIL`, `LOOKS` and
`NEXT_CHECK_AT` preserve the budget across restarts. Completed checks clear `NEXT_CHECK_AT`,
retaining the last budget for inspection. Gate timings remain 70%, 80%, and subsequent fractions
of the upstream's average real run length, at least one second apart, limited by
`Gate_wait_minutes` and 30 looks. Stand-in and backfill runs do not influence that average.

The controller advances pipeline supervision from its main loop. It starts a task agent only after
a final gate judgement, passing that snapshot to the task runner for atomic decision recording.
A waiting task creates no attempt. Pauses, cancellation, ownership leases, process guards, SLA
checks and finalization hooks apply to the same exact execution identities as foreground runs.

## Leadership and recovery

Only one controller owns a deployment. PostgreSQL holds a schema-specific session advisory lock
on a dedicated connection. SQLite holds an OS file lock beside its database file, so all
controllers must reach the same file. A second controller exits with the current controller's id and host.
Leadership lasts until local agents have stopped and the lock is released.

`AUD_CONTROLLERS` records the process id, host, version, start, heartbeat and clean stop times.
The starting caller is recorded; background supervision and automatic shutdown use the system
actor. Heartbeats are refreshed every 15 seconds. An unclosed history row after a crash does
not constitute leadership and is not given an invented stop time.

The loop polls once per second. On PostgreSQL, committed changes to pipeline runs, task
attempts and pipeline pauses also wake it through `LISTEN etl_craft_events`. Rolled-back
changes send no notification. A restarted controller rebuilds its working set from queued and active runs;
it caches only their graphs, invalidating them on metadata edits or deletions.

A crashed controller's run and attempt leases must expire before another controller takes ownership.
Reconciliation records expired attempts as `LOST` and stops a local child only after verifying
its PID and process birth identity. It then releases expired run ownership and resumes the
same pipeline and task run, preserving successful or skipped tasks and earlier attempts.

## Engine DB restarts and failovers

On a PostgreSQL Engine DB, the controller, its runs and their task processes wait out a restart or
managed failover of up to `Orchestration.Engine_db_outage_seconds` (default 120):

- Refused connections are retried once a process has reached the Engine DB. A database that was
  never reached, such as one with a wrong URL or password, fails at once.
- A run's step, a lease heartbeat or the controller's loop that loses its connection mid-statement
  is repeated when the Engine DB answers again. Each repeats from the recorded state.
- The controller's session lock ends with its connection. After reconnecting, the controller takes the
  lock again before supervising anything. If another controller took it meanwhile, this one stops
  and says which controller is active.
- Task ownership leases last 60 seconds and are renewed every 15. An outage that outlasts a
  lease cannot be renewed through. The attempt is reconciled `LOST` and retried under its
  task's retry settings, like any other lost attempt.

Past the budget, the controller fails with the length of the outage and the setting's name. Restart
it once the Engine DB is back, or raise the setting for longer failovers. A SQLite Engine DB is
a local file and has no such outages.

## Refusals at start

`etl-craft controller` binds `Orchestration.Api_address` first. When the address is in use, it
stops with exit code 10 before taking leadership, naming the address. Stop the process
listening there, or choose a free port (port 0 picks one).

A task process starts only when its log folder under `Orchestration.Log_dir` can be written and
its disk has at least 64 MiB free. Otherwise the attempt is recorded `FAILED`, with the folder,
the free space found and the remedy, and the task does no work.

## Shutdown

SIGTERM or Ctrl-C immediately stops new dispatch. Tasks already running may finish within
`Orchestration.Shutdown_grace_seconds`, which defaults to 60 and accepts zero. When the grace
period ends, remaining task processes are stopped using the existing supervisor's process
guards and kill grace. A task waiting on a cross-pipeline gate is interrupted without starting
it. The controller leaves unfinished runs `IN-PROGRESS` with their exact identities for resumption;
it does not mark a partial run successful or create a replacement run.

For example:

```yaml
Orchestration:
  Mode: local
  Max_parallel_tasks: 8
  Shutdown_grace_seconds: 60
```

Use a process manager to restart the controller after a failure. Leave enough time in its stop
policy for the configured shutdown grace and the task supervisor's final process cleanup.

## Updating an existing deployment

Stop the old scheduling process before upgrading. Run `etl-craft migrate` to preserve its
process history under `AUD_CONTROLLERS`, keyed by `CONTROLLER_ID`. Update process-manager
commands to `etl-craft controller`; there is no alias for the previous command. Choose
`laptop`, `controller` or `controller-agents` in `Deployment.Profile` and update any older
profile values. Agents execute tasks; the controller schedules and supervises them. Cluster
agents are introduced separately from this naming change.
