# Operations

## Backups

The **Engine DB** holds your pipelines (the `CFG_` tables) and their history (the `AUD_` tables):
back it up like any database that matters.

- SQLite: copy the file while nothing runs, or use `sqlite3 engine.db ".backup engine-backup.db"`,
  which is safe while it is in use.
- PostgreSQL: `pg_dump --schema=<schema>` of the Engine DB schema, or the backups your platform
  takes of the database.

The `CFG_` rows are best also kept as the SQL that writes them, in version control, so a pipeline
change is reviewed like code. [Cloning](cloning.md) copies the Engine DB tables into the
warehouse on its own schedule, which gives analysts the history, but it is a copy, not a backup.

The **warehouse** holds your data; back it up as your platform does. etl-craft keeps no copy of
it.

## Upgrading etl-craft

```bash
pip install --upgrade etl-craft     # with the extras you use
etl-craft setup                     # checks everything and applies pending migrations
```

`setup` runs every [`doctor`](doctor-and-setup.md) check, then applies the Engine DB migrations
the new version brings, and your project's own, in order; each is recorded and never applied
twice. Read the [release notes](../release-notes.md) first, and upgrade while no run is going.
See [Engine DB setup and upgrades](engine-db.md).

## Logs and history

Local attempts keep complete files under
`<Log_dir>/<PIPELINE>/run-<id>/<TASK>.attempt-<n>.log`. Agent logs are published by the API to
`Logs.Store`, defaulting to `file://<Log_dir>/agents`; S3 locations require an explicit
`Logs.Profile` naming an `Object_stores` profile. Configure the same host-owned store and
credentials on every API and controller copy. Agents require neither credentials nor a mount.
`doctor` probes explicitly configured log stores using the existing safe storage probe.
Back up this store alongside the Engine DB. Keep the original store configuration while its
logs or pending retention cleanup remain; a changed prefix fails rather than guessing a new
location. For S3, configure the bucket's lifecycle to abort incomplete multipart uploads left
by an abruptly terminated API process.

`AUD_TASK_RUN_LOG.TASK_LOG` holds reported values plus at most `Logs.Tail_bytes` captured output
bytes (8192 by default). Upgrades preserve old, larger values in their original rows. Full
files, these summaries and uploaded chunk rows follow `Retention.Attempt_log_days`; an omitted
period keeps them forever. `etl-craft purge --dry-run` previews cleanup. The committed removal
timestamp distinguishes removal by retention from a missing or inaccessible file, and durable
cleanup work retries interrupted file/object deletion, including unacknowledged destination
files and unfinished file staging when retention wins before an upload retry.

An agent ending first commits its outcome, summary, checkpoint and consumption. A store upload
failure cannot change that result. Its message and exit code are recorded separately in
`AUD_ACTIONS` as a failed `agent log` action with the exact attempt/project identity; controller
maintenance also logs the failure. Restore access and repeat the same ending report, or let
controller maintenance retry within a minute, including after controller restart. Chunks remain
the durable source until retention, including when publication was interrupted after writing
the file but before acknowledgment. A killed agent is reconciled as `LOST` before its available
partial chunks are published; bytes never acknowledged by the API cannot be recovered from a
lost agent host.

## Watching it run

- The exit status of every command (see [Exit codes](../reference/exit-codes.md)).
- Alert tasks and SLA emails (see [Email alerts](../guides/email-alerts.md)).
- `etl-craft history --pipeline-code X --all` for runs and interventions, and the
  [catalog site](../guides/catalog.md) for every pipeline's last run, refreshed nightly.
