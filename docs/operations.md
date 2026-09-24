# Operating etl-craft

This guide describes the operating path for the 0.1.0 Alpha release. Use it for a controlled
deployment with a named owner, a PostgreSQL Engine DB, and a PostgreSQL warehouse. Treat each
cloud warehouse as a separate acceptance target before enabling it for a customer workload.

## Deploying a controlled environment

1. Create a dedicated PostgreSQL database for the Engine DB. It stores pipeline definitions,
   audit history, migration state, dependency trackers, and documentation/lineage caches. Do not
   share it with application tables. A SQLite Engine DB, the default, is for
   local development and single-machine use only: it serializes Engine DB writes, and
   orchestrator workers on other hosts cannot reach it.
2. Create a separate warehouse database or namespace for pipeline targets. The reference launch
   path is PostgreSQL. DuckDB is for local development and permits one writer at a time.
3. Build or install one pinned package artifact for every process that will run tasks. Do not mix
   package versions within a running environment.
4. Write `craft-connector.yml` with variable names only (start from `docs/examples/`). Put their
   values in a protected environment or a file readable only by the service account. etl-craft
   never writes this file, so review changes to it like any other deployment config.
5. Run `etl-craft setup` to create the Engine DB for a new environment, then run both checks:

   ```bash
   etl-craft doctor
   etl-craft validate
   ```

6. Run a non-destructive representative pipeline before enabling its normal schedule. Preserve
   the command's stdout and stderr in the scheduler or workload runner.

`doctor` checks active connection profiles. `validate` checks the configured pipeline metadata and
warehouse capabilities. Both must pass before scheduling a new or changed pipeline.

For Airflow, package and deploy a loader that consumes the output of `generate-yml`. The generated
YAML is a descriptor, not a ready-to-import DAG. In remote mode, every worker must have the same
package version, config path, configuration values, and access to the Engine DB and warehouse.

## Backup and recovery

Back up the Engine DB before an upgrade and at a cadence that matches the recovery point objective
for pipeline definitions and audit history. A PostgreSQL custom-format backup is suitable for that
database:

```bash
export PGPASSFILE=/etc/etl-craft/pgpass
pg_dump --format=custom --no-owner \
  --file="etl-craft-engine-$(date +%F).dump" \
  --dbname='postgresql://etl_craft@engine.internal:5432/etl_craft?sslmode=require'
pg_restore --list "etl-craft-engine-$(date +%F).dump" >/dev/null
```

Use a libpq connection URI or the `PGHOST` / `PGPORT` / `PGUSER` / `PGDATABASE` variables for
PostgreSQL tools. `ENGINE_JDBC_URL` is a JDBC value for etl-craft and cannot be passed directly
to `pg_dump`. Keep the password in a protected `PGPASSFILE` or use your platform's equivalent
credential mechanism.

For a SQLite Engine DB, copy the file with SQLite's online backup rather than `cp`, which can
catch a write half-done: `sqlite3 etl-craft-engine.db ".backup engine-$(date +%F).db"`, or from
Python, `sqlite3.connect(src).backup(sqlite3.connect(dst))`.

Keep the backup encrypted and separate from the credentials that can restore it. Test restoration
into a new, empty PostgreSQL database before relying on a backup. The warehouse has its own data
retention and recovery requirements; an Engine DB backup does not back up warehouse tables,
external Iceberg storage, or secret values.

Keep a copy of the canonical manifest, the variable-name mapping, and the reviewed project
migrations with the deployment source. Store real values in the secret system, not alongside that
copy.

## Upgrading safely

1. Run the full backup procedure and verify the backup can be listed or restored in a nonproduction
   database.
2. Pause new pipeline dispatches. Allow active tasks to finish or deliberately drain the workload
   according to the owning team's runbook.
3. Deploy the new pinned artifact to each execution environment.
4. Apply migrations once against the Engine DB:

   ```bash
   etl-craft migrate
   etl-craft doctor
   etl-craft validate
   ```

5. Run a representative pipeline, inspect its history, then resume normal scheduling.

`migrate` serializes concurrent migration attempts with an advisory lock. It processes the packaged
engine migration stream before any project stream. Project migrations may come from
`--migrations-dir`, `ETL_CRAFT_MIGRATIONS_DIR`, or `./sql/migrations`; they do not replace package
migrations. Applied files are checksummed. Never edit or delete an applied migration: add a new
migration instead and investigate any checksum error before retrying.

## Monitoring and incident response

The Engine DB is the durable execution record. Start with the built-in history commands:

```bash
etl-craft history --pipeline_code MY_PIPELINE
etl-craft history --pipeline_code MY_PIPELINE --task_code MY_TASK
```

With `Orchestration.Enforce_sla: true`, each finished run of a pipeline that has an `SLA_IN_HOURS`
records `SLA_STATUS` (`MET` or `BREACHED`) in `AUD_PIPELINES_RUN_LOG`, and `history` prints a breach
beside the run.

For an operational view across pipelines, query the Engine DB audit tables. For example, this lists
recent failed or still-running pipeline runs:

```sql
SELECT p.pipeline_code, r.pipeline_run_id, r.status, r.start_date, r.end_date
FROM aud_pipelines_run_log AS r
JOIN cfg_pipelines AS p ON p.pipeline_id = r.pipeline_id
WHERE r.status IN ('FAILED', 'IN-PROGRESS')
ORDER BY r.start_date DESC;
```

Inspect `AUD_TASK_RUN_LOG.ERROR_MESSAGE`, `TASK_LOG`, and `ATTEMPT_COUNT` for task-level failures.
The scheduler should alert on nonzero command exits and preserve stdout/stderr. A task left
`IN-PROGRESS` after loss of its entire process tree needs investigation against the scheduler and
workload runtime before any manual repair of audit rows.

The project does not ship a metrics exporter, central log sink, dashboard, or automated stale-run
reaper. Connect the scheduler's logging and alerting to the Engine DB audit data for a production
deployment.

## Security boundaries

- Use a separate service role for each person or automation that changes Engine DB metadata. The
  schema records the active PostgreSQL role in its configuration audit fields.
- Grant only the Engine DB and warehouse privileges required by the configured tasks. Review
  `CFG_` changes and project migrations like executable code.
- Keep secrets out of `craft-connector.yml` (the loader refuses a secret that isn't a variable);
  protect the environment, secret file, and any mounted private-key file. Restrict local secret
  files, for example with `chmod 600`.
- Rotate passwords, static tokens, client secrets and key passphrases in the secret source, then
  run `doctor` before resuming work. `oauth` and `sts` credentials are obtained per connection, so
  they need no rotation of their own; `doctor` warns for every authentication type not yet
  verified against a live service.
- `validate` performs useful SQL safety checks, but it is not a security boundary. Treat authors
  who can modify pipeline metadata as trusted code contributors.

See [../SECURITY.md](../SECURITY.md) for vulnerability reporting.

## Before each release or customer rollout

Use [release-checklist.md](release-checklist.md) to record the tested artifact, migration result,
configuration example checks, deployment verification, and the warehouse-specific acceptance
evidence.
