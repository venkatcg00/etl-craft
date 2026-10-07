# Running the local server

`etl-craft server` creates scheduled runs and supervises active local pipeline runs until it receives SIGTERM or Ctrl-C.
It uses the same task handlers, wave execution, gates, leases, cancellation guards, SLA checks
and finalization hooks as foreground `etl-craft run`.

## Starting and submitting work

Initialize or upgrade the [Engine DB](engine-db.md), configure `Orchestration.Mode: local`,
and start the server with the same project configuration used by the CLI:

```bash
etl-craft --config /srv/etl-craft/craft-connector.yml server
```

From another terminal, create a run with an explicit key:

```bash
etl-craft --config /srv/etl-craft/craft-connector.yml run \
  --pipeline_code SALES --init-only --run-key daily:2026-10-07 --run-date 2026-10-07
etl-craft --config /srv/etl-craft/craft-connector.yml history \
  --pipeline_code SALES --run-key daily:2026-10-07 --format json
```

Initialization performs the usual admission checks. The server picks up an unowned
`IN-PROGRESS` run by its exact `pipeline_run_id`; it never chooses an ended run by recency.
Scheduled runs begin `QUEUED`. The oldest queued run of each pipeline is admitted through
the existing pipeline gates when there is no active sibling. Existing wave and blocking gate
behavior remains in force; nonblocking ready-set dispatch and automatic retries are subsequent
roadmap items. To run under Airflow or another orchestrator, use
[remote mode](orchestrator.md); `server` refuses that mode.

`Max_parallel_tasks` limits tasks inside each pipeline and bounds the number of concurrent
pipeline supervisors. Runs still owned by another foreground process are left with that
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
`@weekly` and `@monthly`. Restricted day-of-month and weekday fields match either day,
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
`SKIPPED`, with reason `missed while no overseer was running`. Each tick commits separately,
so long catch-up histories release the database writer lock between records.

`OVERLAP_POLICY = 'SKIP'` (default) records a due tick `SKIPPED` with reason
`previous run still active` when the pipeline already has queued or active work. `QUEUE`
preserves the tick for later admission. Only one run per pipeline can be `IN-PROGRESS`.
You can inspect or cancel a queued run by its exact `--run-id` or `--run-key`.
These columns govern the local server; the existing `PIPELINE_PARAMETERS.CATCHUP` boolean
continues to control Airflow's generated DAG catch-up setting.

## Leadership and recovery

Only one server owns a deployment. PostgreSQL holds a schema-specific session advisory lock
on a dedicated connection. SQLite holds an OS file lock beside its database file, so all
servers must reach the same file. A second server exits with the current overseer's id and host.
Leadership lasts until local workers have stopped and the lock is released.

`AUD_OVERSEERS` records the process id, host, version, start, heartbeat and clean stop times.
The starting caller is recorded; background supervision and automatic shutdown use the system
actor. Heartbeats are refreshed every 15 seconds. An unclosed history row after a crash does
not constitute leadership and is not given an invented stop time.

The loop polls once per second. On PostgreSQL, committed changes to pipeline runs, task
attempts and pipeline pauses also wake it through `LISTEN etl_craft_events`. Rolled-back
changes send no notification. A restarted server rebuilds its working set from active runs;
it caches only their graphs, invalidating them on metadata edits or deletions.

A crashed server's run and attempt leases must expire before another server takes ownership.
Reconciliation records expired attempts as `LOST` and stops a local child only after verifying
its PID and process birth identity. It then releases expired run ownership and resumes the
same pipeline and task run, preserving successful or skipped tasks and earlier attempts.

## Shutdown

SIGTERM or Ctrl-C immediately stops new dispatch. Tasks already running may finish within
`Orchestration.Shutdown_grace_seconds`, which defaults to 60 and accepts zero. When the grace
period ends, remaining task processes are stopped using the existing supervisor's process
guards and kill grace. A task waiting on a cross-pipeline gate is interrupted without starting
it. The server leaves unfinished runs `IN-PROGRESS` with their exact identities for resumption;
it does not mark a partial run successful or create a replacement run.

For example:

```yaml
Orchestration:
  Mode: local
  Max_parallel_tasks: 8
  Shutdown_grace_seconds: 60
```

Use a process manager to restart the server after a failure. Leave enough time in its stop
policy for the configured shutdown grace and the task supervisor's final process cleanup.
