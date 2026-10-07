# Running the local server

`etl-craft server` supervises active local pipeline runs until it receives SIGTERM or Ctrl-C.
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
It does not create schedules or admit `QUEUED` runs yet. Schedule generation, nonblocking
ready-set dispatch and automatic retries are subsequent roadmap items. Existing wave and
gate behavior remains in force. To run under Airflow or another orchestrator, use
[remote mode](orchestrator.md); `server` refuses that mode.

`Max_parallel_tasks` limits tasks inside each pipeline and bounds the number of concurrent
pipeline supervisors. Runs still owned by another foreground process are left with that
process. Paused or inactive pipelines receive no dispatch; resuming a pipeline allows its
active run to continue.

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
