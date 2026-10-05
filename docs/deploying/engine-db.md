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
keys. Engine inserts default to a generated manual key and `MANUAL`; other trigger kinds
are supplied explicitly by the engine.

`AUD_TASK_ATTEMPTS` stores attempt identities, lifecycle timestamps, ownership, process details,
counts and logs. The migration copies each non-skipped task summary into one attempt at its
current `ATTEMPT_COUNT`, mapping `IN-PROGRESS` to `RUNNING`. Earlier retry outcomes cannot be
reconstructed from a summary. Skipped tasks retain their summaries without an execution attempt.
There can be only one queued, claimed or running attempt per task run.

`AUD_GATE_DECISIONS` holds a downstream run or attempt's admission decision, exactly one dependency,
selected upstream identities and revision, result, reason and decision time. Consumption records
start at revision 1; dependency `CONSUME_REPAIRS` flags default to `Y`. The schema reference lists
all fields and constraints. Execution writes immutable attempts and updates their task summaries atomically. Run and
  attempt supervisors renew ownership leases; reconciliation fences expired attempts as `LOST`.
Admission records the selected upstream identities and revisions in `AUD_GATE_DECISIONS`,
and successful downstream work consumes only its recorded satisfied decisions.

Migration `0010_gate_repairs.sql` adds `REPAIR_PENDING` to pipeline runs. Reopening sets the flag;
successful publication increments `OUTPUT_REVISION` once and clears it. Existing output revisions
are preserved, and active historical runs with a recorded REOPEN remain pending after migration.

SQLite rebuilds the pipeline run table with the same reference checks, custom object preservation,
identity-counter protection and extra-column refusal as migration 0006. PostgreSQL alters it in
place. Both dialects roll back the schema, copied history and migration ledger together on failure.

## Actors and write guards

Migration `0008_actors_and_audit_guards.sql` attributes new actions and metadata changes. Historical
rows keep unknown actor fields empty; the migration does not invent who performed them. SQLite
rebuilds the run, attempt, intervention and pause tables to add actor defaults, preserving rows,
references, custom objects and identity counters with the same rollback checks as migration 0007.

The CLI resolves one actor per command from `ETL_CRAFT_ACTOR`, or `user@hostname` when it is unset.
Names must be non-empty, contain no control characters and fit 128 characters. Set a stable name
in automation:

```bash
export ETL_CRAFT_ACTOR=github:alice
etl-craft migrate
```

CI uses `github:${{ github.actor }}`. `ETL_CRAFT_ACTOR_KIND` defaults to `HUMAN`; generated remote
DAGs set `ORCHESTRATOR` and include the DAG run id and Airflow's triggering user when available.
Other sources are `SCHEDULE`, `WORKER` and `SYSTEM`. These environment values identify an action;
they do not authenticate a user. Library callers can scope an `Actor` with
`etl_craft.core.actor.acting_as(actor)`; otherwise library work uses the engine's `SYSTEM` actor.
A run records its starter, and its ending actor: system finalization, or the person marking or
cancelling it. Pauses, resumes, interventions and attempts record their actor kind too.

`AUD_ACTIONS` contains one immutable request per state-changing command. `OUTCOME = REQUESTED`
means the action was to request work, whether the flow subsequently runs or is refused. Its
start and end timestamps describe recording that request, and `EXIT_CODE` remains empty. No
completion update is required: run and attempt tables hold execution outcomes. Sensitive argument
names are masked before recording. Read-only commands, including `audit`, make no audit writes.
For an empty or older database, initialization or migration records its request after the audit
table becomes available.

Every `CFG_` row change records its actor, before and after JSON, operation and row key in
`AUD_METADATA_CHANGES`. Project migrations include their filename and capture added columns;
project-created `CFG_` and `AUD_` tables receive write guards as their DDL runs. Make metadata
changes in a new project migration and run `etl-craft migrate`.

A plain connection cannot write `CFG_` or `AUD_` tables. PostgreSQL requires the engine's
transaction-local actor marker, including for `TRUNCATE`. SQLite requires functions registered
on engine connections; the SQLite shell or a BI tool instead reports `no such function:
etl_craft_actor` (or the corresponding actor-kind function). `CREATED_BY` and `UPDATED_BY` are
stamped with the actor. Interventions, actions, metadata changes, consumption and gate decisions
are append-only; terminal attempts cannot be changed. Retention is a separate future operation.

### PostgreSQL privileges

The marker guards prevent accidental edits. A database owner or a login that can set the marker
can bypass them; database privileges are the access boundary. Review the suggested grants:

```bash
etl-craft setup --print-grants
```

This prints SQL and executes nothing. Use a DDL owner for schema creation and migrations, a
separate engine login for DML, and a read-only role for people. Transfer existing table and
sequence ownership to the owner role, and remove other write grants. Default privileges must be
set for the role that creates tables. `doctor` reports explicit `INSERT`, `UPDATE`, `DELETE` and
`TRUNCATE` grants reaching other ordinary logins or `PUBLIC`, including inherited roles, with the exact
`REVOKE` against the role holding the grant.
Owners and administrators retain their administrative authority; keep those credentials separate.

### SQLite file access

Restrict the Engine DB file and its containing directory to the deployment account. `doctor`
warns when the file is writable by group or others and suggests `chmod go-w`. Filesystem access
is the boundary: anyone who can replace the file or its triggers can bypass the connection guard.

`AUD_TARGET_HASH_VERSION` records each qualified warehouse target's published change-hash version
and recomputation time. Migration `0011_target_hash_version.sql` creates this guarded tracker
without guessing the hash version of existing tables. Upgrade those merge targets with
[`etl-craft rehash`](../guides/sql-tasks.md#change-hash-version-2).
