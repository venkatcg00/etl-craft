# Setting up and upgrading the Engine DB

`etl-craft setup` checks every connection and then does both steps below, as needed; see
[Checking a deployment](doctor-and-setup.md). This page describes each step on its own.

## A new Engine DB

Point the `Engine` section of `craft-connector.yml` at an empty database (see
[Engine DB](../connectors/engine-db.md)), then run:

```bash
etl-craft init-db
```

`init-db` creates every table and records the packaged migrations in one transaction under
the `MIGRATE` lock, so a failure leaves neither a partial schema nor a partial migration ledger. It refuses a
database that already has Engine DB tables and names them: use `migrate` for a database that
already holds an Engine DB. `--force` applies the schema anyway, for a database you know is empty
apart from a leftover table.

## Upgrading

After installing a new etl-craft version, and whenever your own migrations change, run:

```bash
etl-craft migrate
```

It applies two streams of `*.sql` files, each in filename order:

1. **ENGINE**: the migrations packaged with etl-craft, always first. A new Engine DB created by
   `init-db` already includes them.
2. **PROJECT**: your own migrations, from `--migrations-dir`, else `$ETL_CRAFT_MIGRATIONS_DIR`,
   else `migrations/` in the [project directory](project-layout.md) when that folder exists.

`SCHEMA_MIGRATIONS` records every applied file with the SHA-256 of its content. Each file runs in
its own transaction together with that record, so a failing file changes nothing and stops the
run before any later file. Running `migrate` again applies only what is new.

Before applying anything, `migrate` checks every file it has applied before: a file that was
removed or edited stops the run. Released migrations are never edited; add a new file instead.
Keep your project migrations directory for every later run, since `migrate` needs it for that check.

Name project files `NNNN_short_description.sql` so they sort in the order they must apply. Each
statement runs exactly as written: colons, percent signs and semicolons inside string literals
are safe.

Two `migrate` runs against the same Engine DB never overlap: the second waits for the first, then
finds nothing left to apply.

## Pipeline and task code checks

Pipeline and task codes start with an ASCII letter and contain only letters, digits and
underscores, at most 128 characters. Before the code-check migration applies, `migrate` lists
all codes that break this rule, including inactive rows. Rename them and run `migrate` again.

PostgreSQL adds and validates named check constraints. SQLite rebuilds the metadata tables in
one transaction, preserving ids, identity counters, dependencies, audit rows, indexes and
triggers, and checking foreign-key references before commit. It restores foreign-key enforcement
on both success and failure. A SQLite metadata table with extra project columns is refused
before rebuilding: move their values into a project table and remove the extra columns first.

## Run backfill constraint

Migration `0006_run_backfill_constraint.sql` names the run's backfill check
`ck_pipeline_run_backfill`, matching a fresh database. PostgreSQL renames the existing
constraint. SQLite rebuilds `AUD_PIPELINES_RUN_LOG` in one transaction, preserving history,
references, identity-counter high watermarks, custom indexes, triggers and views. It checks
foreign keys before committing and restores connection settings on success and failure.
Extra project columns on the SQLite run table are refused before rebuilding; move their
values into a project table and remove those columns before migrating.

PostgreSQL reports advisory-lock contention as a lock timeout. Other connection failures
report an Engine DB error naming the lock; check connectivity and authentication before retrying.

## Run identity and attempt history

Migration `0007_identity.sql` adds a unique `RUN_KEY` per pipeline, a checked `TRIGGER_KIND`,
nullable owner, lease and configuration fingerprint fields, and `OUTPUT_REVISION` starting at 1.
Existing runs receive `legacy:<pipeline_run_id>` keys and `BACKFILL` or `MANUAL` trigger kinds.
New command-created runs receive `manual:<uuid>`, `backfill:<date>:<uuid>` or `stand-in:<uuid>`
keys. Direct SQL inserts default to a generated manual key and `MANUAL`; supply the trigger
kind explicitly when inserting other kinds of runs.

`AUD_TASK_ATTEMPTS` stores attempt identities, lifecycle timestamps, ownership, process details,
counts and logs. The migration copies each non-skipped task summary into one attempt at its
current `ATTEMPT_COUNT`, mapping `IN-PROGRESS` to `RUNNING`. Earlier retry outcomes cannot be
reconstructed from a summary. Skipped tasks retain their summaries without an execution attempt.
There can be only one queued, claimed or running attempt per task run.

`AUD_GATE_DECISIONS` holds a downstream run or attempt's admission decision, exactly one dependency,
selected upstream identities and revision, result, reason and decision time. Consumption records
start at revision 1; dependency `CONSUME_REPAIRS` flags default to `Y`. The schema reference lists
all fields and constraints. Current execution still writes task summaries and evaluates gates
through the existing lifecycle; writing each new attempt and gate decision and enforcing leases
are subsequent roadmap items.

SQLite rebuilds the pipeline run table with the same reference checks, custom object preservation,
identity-counter protection and extra-column refusal as migration 0006. PostgreSQL alters it in
place. Both dialects roll back the schema, copied history and migration ledger together on failure.
