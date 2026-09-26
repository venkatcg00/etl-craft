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
warehouse after every run, which gives analysts the history, but it is a copy, not a backup.

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

- Each task attempt writes its log to `<Log_dir>/<PIPELINE>/run-<id>/<TASK>.attempt-<n>.log`
  (see [Running a task](../guides/running-tasks.md#logs)). etl-craft never deletes them: remove
  old run folders on your own schedule, for example with `find logs -mindepth 2 -maxdepth 2
  -type d -mtime +30 -exec rm -rf {} +`.
- `AUD_TASK_RUN_LOG.TASK_LOG` keeps the end of each attempt's log, and the `AUD_` tables keep
  every run, attempt, rule result and intervention, so the Engine DB grows with every run.

## Watching it run

- The exit status of every command (see [Exit codes](../reference/exit-codes.md)).
- Alert tasks and SLA emails (see [Email alerts](../guides/email-alerts.md)).
- `etl-craft history --pipeline_code X` for runs and interventions, and the
  [catalog site](../guides/catalog.md) for every pipeline's last run, refreshed nightly.
