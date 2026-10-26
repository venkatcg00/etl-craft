# Operations

## Backups

The **Engine DB** holds your pipelines (the `CFG_` tables) and their history (the `AUD_` tables):
back it up like any database that matters.

- SQLite: copy the file while nothing runs, or use `sqlite3 engine.db ".backup engine-backup.db"`,
  which is safe while it is in use.
- PostgreSQL: use a custom-format schema dump with a client matching the server major version:

```sh
pg_dump --dbname="$ENGINE_DATABASE_URL" --format=custom --schema=craft --file=engine.dump
pg_restore --list engine.dump
```

Replace `craft` with `Engine.Schema`; include every schema the Engine DB uses. A schema-filtered
backup does not include roles or extensions outside that schema. Save required role definitions
separately (`pg_dumpall --globals-only`, protected as credentials), or recreate the destination
login and its grants before restoring. Do not expose passwords in shell arguments; use libpq's
protected password file or the platform credential mechanism. The dump can retain ownership and
ACLs when restoring into matching roles. For a different owner, use `--no-owner --no-acl` and
explicitly grant the destination engine login access afterwards. Do not restore over a running
deployment. PostgreSQL custom dumps contain SQL: restore only trusted backups.

Managed PostgreSQL platforms may provide snapshots and point-in-time restore instead. Record
the provider's recovery point, consistency guarantees, required roles, encryption keys and
restore-to-new-instance procedure; verify an actual restore, not just that a backup job ran.
Managed Engine DB credentials and TLS remain configured as in [Managed PostgreSQL](engine-db.md).

A complete deployment backup also includes the local `Log_dir` and `Logs.Store`, immutable
project bundles in `Artifacts.Store`, object-store profiles/credentials (through the team's
secret manager), and project config/migration/execution files in version control. Preserve the
original object locations or restore their prefixes/mounts at those locations: log and bundle
identities in the Engine DB refer to them. Quiesce/drain work for a coordinated recovery point;
a database dump alone cannot atomically capture external stores or warehouse side effects.
Keep retention/lifecycle policies from deleting objects needed by retained database backups.

Restore into a new database with the source deployment stopped and its old agents fenced:

```sh
createdb --owner=craft_engine craft_restored
pg_restore --dbname=craft_restored --no-owner --no-acl --exit-on-error engine.dump
etl-craft doctor
etl-craft reconcile
etl-craft controller
```

Set the restored deployment's `Engine.Jdbc_url`, schema and credential profile first and restore
its log/bundle stores before these checks. For the default `public` schema, an empty database
already contains that schema: remove that empty schema first, or use `pg_restore --clean
--if-exists` **only against the new disposable destination**. Required roles and schema grants
must be present. Run the same package version as the dump; apply the documented upgrade
procedure afterwards if needed. `doctor` checks the connections and configuration; `reconcile`
fences expired attempts as LOST, then the controller resumes interrupted runs and retains tasks
already committed SUCCESS/SKIPPED. Unexpired leases may need to expire before recovery proceeds.
Never manually adopt a prior owner's lease. Confirm warehouse side effects as in the recovery
contract before releasing the replacement deployment; outside/unmanaged writes need the team's
own reconciliation.

The PostgreSQL restore regression uses `pg_dump` and `pg_restore` inside the compose container
so client and server match. It kills real controller/child processes, restores the recorded
interruption into another database, runs `doctor` and `reconcile`, then verifies one logical
run and one successful row after LOST → SUCCESS recovery.

The `CFG_` rows are best also kept as the SQL that writes them, in version control, so a pipeline
change is reviewed like code. [Cloning](cloning.md) copies the Engine DB tables into the
warehouse on its own schedule, which gives analysts the history, but it is a copy, not a backup.

The **warehouse** holds your data; back it up as your platform does. etl-craft keeps no copy of
it.

## Upgrading etl-craft

Rolling upgrades are not supported for 1.0.0. Read the release notes, take a restorable backup,
and upgrade every role together:

1. `etl-craft agents --api https://craft.example.com`; drain each registered agent with
   `etl-craft agent drain --agent-id ID --api https://craft.example.com`.
2. Wait for running children to finish; stop the drained agents and the controller and UI
   copies. Preserve their configuration and credential identities.
3. Install the new package (with the extras used by each host) on every role, then run
   `etl-craft migrate` from the controller's configured project directory. Apply each project's
   own pending migrations too. Never start an old binary against the migrated schema.
4. Start the new controller and UI copies. Their package/schema check must pass.
5. Start the new agents. Registration, claim and renewal all check the package version on
   every request. A refusal names the agent/API versions and tells you to install the API's
   version and restart the agent; it stops new claims and uses normal child fencing/reconciliation.

```sh
pip install --upgrade 'etl-craft[ui]'  # use the extras deployed on this host
etl-craft migrate
etl-craft doctor
etl-craft controller
```

Use the same tested wheel/version on all roles. A schema mismatch refuses controller/UI startup;
there is no automatic startup migration or rolling compatibility fallback. If migration fails,
keep processes stopped, read its diagnostic and restore or repair as instructed before retrying.
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
