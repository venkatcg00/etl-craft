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

Account authentication replaces the former `ETL_CRAFT_ACTOR` and `ETL_CRAFT_ACTOR_KIND`
environment overrides. After installing this version, run `etl-craft setup` locally with
writer credentials to migrate and provision your first admin, then `etl-craft login`.
Existing service tokens keep their values and inherited account roles. If all migrated
accounts are non-admin, local interactive setup provisions a recovery admin; no HTTP route
can do so. Replace actor overrides in CI with a registered service token in `ETL_CRAFT_TOKEN`.

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

Packaged ENGINE migrations declare special policies with SQL header comments:
`-- etl-craft: rebuild-metadata` enables SQLite's guarded metadata rebuild, and
`-- etl-craft: check-metadata-codes` validates existing codes before applying pending migrations.
`-- etl-craft: check-metadata-identities` checks stable config keys before applying any pending
migration and again inside the identity migration transaction before changing indexes. The packaged `migration-markers.toml` supplies these markers for existing files without changing
any SQL bytes or stored checksums. These internal markers are refused in PROJECT migrations.

Name project files `NNNN_short_description.sql` so they sort in the order they must apply. Each
statement runs exactly as written: colons, percent signs and semicolons inside string literals
are safe.

Two `migrate` runs against the same Engine DB never overlap: the second waits for the first, then
finds nothing left to apply.

## Query indexes

Migration `0046_query_indexes.sql` adds indexes for pending runs, gate decisions, rule runs,
upstream consumption, interventions and documentation requests on SQLite and PostgreSQL.
It also covers run and consumption retention. The existing attempt-number index already
starts with `TASK_RUN_ID`, so attempt writes gain no additional index to maintain.

Index creation is transactional and can block writes while it builds. Plan an upgrade
window appropriate to the size of the run and audit tables; apply the migration before
starting the controller and UI copies. No rows, results or configuration change.

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
counts and logs. Migration `0041` adds `FAILURE_CLASS`, `FAILURE_SOURCE`, `FAILURE_CODE` and
`RETRY_ON`: the attempt's original failure evidence and effective retry policy. These are written
with the outcome and remain immutable afterwards. Historical attempts retain `UNKNOWN` and no
policy; queued attempts from an earlier version capture the policy when first admitted after
upgrade. The initial attempt migration copies each non-skipped task summary into one attempt at its
current `ATTEMPT_COUNT`, mapping `IN-PROGRESS` to `RUNNING`. Earlier retry outcomes cannot be
reconstructed from a summary. Skipped tasks retain their summaries without an execution attempt.
There can be only one queued, claimed or running attempt per task run.

`AUD_GATE_DECISIONS` holds a downstream run or attempt's admission decision, exactly one dependency,
selected upstream identities and revision, result, reason and decision time. Consumption records
start at revision 1; dependency `CONSUME_REPAIRS` flags default to `Y`. The schema reference lists
all fields and constraints. Execution writes immutable attempts and updates their task summaries atomically. Run and
  attempt supervisors renew ownership leases; reconciliation fences expired attempts as `LOST`.
`AUD_GATE_WAITS` retains each run’s pipeline or task gate polling budget across restarts.
`TASK_ID` is NULL for pipeline admission; `FIRST_CHECK_AT`, `NEXT_CHECK_AT`, `LOOKS` and
`WAIT_UNTIL` track the original budget. Settled gates clear `NEXT_CHECK_AT`. Waiting gates
occupy no task agents. See [gate waits and dispatch](controller.md#gate-waits-and-dispatch).

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

The CLI resolves its actor once from a registered account's session or `ETL_CRAFT_TOKEN`.
Account names must be non-empty, contain no control characters and fit 128 characters.
Use `etl-craft setup` and `etl-craft login` for people; provision a SERVICE account and
store its token in your CI secret manager for automation. See [account security](security.md).
Library callers scope their actor explicitly with `etl_craft.core.actor.acting_as(actor)`.

A run records its starter, and its ending actor: system finalization, or the person marking or
cancelling it. Pauses, resumes, interventions and attempts record their actor kind too.

`AUD_ACTIONS` contains one immutable request per state-changing command. `OUTCOME = REQUESTED`
means the action was to request work, whether the flow subsequently runs or is refused. Its
start and end timestamps describe recording that request, and `EXIT_CODE` remains empty. No
completion update is required: run and attempt tables hold execution outcomes. Sensitive argument
names are masked before recording. Read-only commands, including `audit`, make no audit writes.
Catalog generation also appends an immutable completion row: `SUCCESS` or `FAILURE`,
with an exit code and the same start timestamp and arguments as its request. The request row
is never updated. This applies to manual and controller-scheduled catalog builds.

For an empty or older database, initialization or migration records its request after the audit
table becomes available.

### Configuration history

All `CFG_` tables require one primary-key column and non-null `CREATED_BY`, `CREATE_DATE`,
`UPDATED_BY`, `UPDATED_DATE` and `ACTIVE_FLAG`. Actor/date types and the `Y`/`N` active values
are checked for project tables too. Migration `0026_config_base_contract.sql` makes the core
fields mandatory. Existing creation/update values and IDs are preserved; missing historical
attribution is labelled `etl-craft-migration-0026`, with the migration time for missing dates.
Only changed rows get a new history version. Token update fields begin at the upgrade and
revoked tokens become inactive; earlier token history is not invented.

Stop running Engine DB writers during this schema upgrade and resume them after migration
succeeds. Before upgrading, bring custom `CFG_` tables into this contract using the previous etl-craft
version. The upgrade refuses incomplete custom tables rather than guessing their key or types.
On SQLite, extra columns on rebuilt core config tables are refused before rebuilding; move
their values into project tables first. Migrations are transactional per file: `0025` may
commit its safe token date rename before a refused `0026`, and retry resumes at `0026`.

Every change to a `CFG_` row is a version in that table's history table, `AUD_<table>_HISTORY`
(`AUD_CFG_PIPELINES_HISTORY`, `AUD_CFG_TASKS_HISTORY` and so on), in the slowly changing
dimension type 4 pattern: the `CFG_` table holds the current rows, the history table every
version. A version holds each non-secret column of the row, typed as in the `CFG_` table: its values
after an insert or update, or its values when deleted. With them are:

| Column | Holds |
|---|---|
| `CHANGE_ID` | the change's place in the order of every change, across the tables |
| `VALID_FROM`, `VALID_TO` | when the version held: from its change until the next one; `VALID_TO` is 9999-12-31 while it still holds, and a deletion's version holds for no time at all |
| `CURRENT_FLAG` | `Y` for the version that holds now; `N` for the rest and for a deletion |
| `OPERATION` | `INSERT`, `UPDATE` or `DELETE` |
| `ACTOR`, `ACTOR_KIND` | who made the change |
| `MIGRATION` | the project migration's filename, or `config@<revision>` for `etl-craft config apply` |

A version never changes, except that the next change closes it: its `VALID_TO` is set and its
`CURRENT_FLAG` becomes `N`. The Engine DB refuses every other update and every delete, except by
retention. Plain SQL answers what a row held and when:

```sql
-- The SALES_MART pipeline as it stood on the morning of 1 October
SELECT RUN_SCHEDULE, SLA_IN_HOURS, ACTIVE_FLAG, ACTOR, MIGRATION
FROM AUD_CFG_PIPELINES_HISTORY
WHERE PIPELINE_CODE = 'SALES_MART'
  AND VALID_FROM <= '2026-10-01 08:00:00+00' AND '2026-10-01 08:00:00+00' < VALID_TO;

-- Every version of it, oldest first
SELECT VALID_FROM, VALID_TO, OPERATION, ACTOR, RUN_SCHEDULE, SLA_IN_HOURS
FROM AUD_CFG_PIPELINES_HISTORY
WHERE PIPELINE_CODE = 'SALES_MART'
ORDER BY CHANGE_ID;
```

Changes to runs are kept apart from these. `AUD_RUN_INTERVENTIONS` records every change an
operator makes to a run (a task or run marked, cancelled, reopened, reset or rerun, a stand-in
run, a gate bypassed) with the status it held before, and `AUD_ACTIONS` records every command
that changes anything, from the command line or the API, with its actor and arguments.

`AUD_METADATA_CHANGES` is a view over the history tables: one row per change, with the row's
values before and after as JSON, which `etl-craft audit` reads. A column a project migration adds
to a `CFG_` table is added to its history table as the migration runs, and a project-created
`CFG_` table gets a history table of its own. Project-created `CFG_` and `AUD_` tables receive
write guards as their DDL runs. Make metadata changes in the
project's [configuration files](../guides/configuration-files.md) and run
`etl-craft config apply`, or in a new project migration and run `etl-craft migrate`.

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

`AUD_TARGET_HASH_VERSION` records each warehouse target's published change-hash version and
recomputation time. `AUD_TARGET_VIEW_STATEMENT` records the statement behind each view. Both are
keyed by the target's resolved catalog, schema and table (`TARGET_CATALOG_NAME`,
`TARGET_SCHEMA_NAME`, `TARGET_TABLE_NAME`), and target locks use the same resolved identity. A
target named with or without its catalog therefore shares one lock and one record. Migration
`0011_target_hash_version.sql` creates the guarded tracker without guessing the hash version of
existing tables.

Migration `0033_target_state_components.sql` splits each recorded name into those components.
It splits business-rule flags in `AUD_BUSINESS_RULES_RESULTS` the same way, keeping each one's
spelling as recorded, never today's configured catalog. Values and timestamps are unchanged. A
recorded name that cannot be split stops the upgrade before any change, naming the row; correct
or delete it with a project migration and migrate again. Upgrade those merge targets with
[`etl-craft rehash`](../guides/sql-tasks.md#change-hash-version-2).

Run admission stores `METADATA_VERSION` and `METADATA_SNAPSHOT`, plus `BUNDLE_ID` when
artifact storage is configured. `AUD_PROJECT_BUNDLES` holds immutable manifests and publication
actors. See [pinned project bundles](project-bundles.md) for storage and upgrade behaviour.

## Managed PostgreSQL on Snowflake and Databricks

Set `Engine.<profile>.Managed_provider` to `SNOWFLAKE` or `DATABRICKS`. Omission means
`NONE`, preserving ordinary PostgreSQL and SQLite profiles. Managed engines use the same
PostgreSQL schema, migrations, process reservations, leases and recovery paths. They require
`Auth_mode: OAUTH` and `sslmode=require`, `verify-ca` or `verify-full` on both `Jdbc_url` and
`Listen_jdbc_url` when supplied. Prefer `verify-full` with the provider's trusted certificate.
The connection addresses must name the same Engine DB.

Existing `Client_id`, `Secret` (a variable name), `Token_url` and optional `Scope` authenticate
the issuing platform. Each new PostgreSQL connection obtains a fresh database token; pooled
connections recycle after ten minutes. Long-lived authenticated sessions do not need a new
password until they reconnect. Provider errors identify the profile without repeating token
responses or secrets. Credential requests refuse redirects.

For **Databricks Lakebase**, set `Workspace_url` to the workspace HTTPS origin and `Endpoint`
to `projects/<project>/branches/<branch>/endpoints/<endpoint>`. Set `Token_url` to the workspace's
`/oidc/v1/token` endpoint and `Scope: all-apis`. The client-credentials exchange uses HTTP Basic
authentication; its workspace token is exchanged at `/api/2.0/postgres/credentials` for the
Postgres password. Provision a Lakebase OAuth database role for the service principal named by
`User`, with permission to use the endpoint and maintain the Engine DB schema. See the
[Databricks API connection guide](https://docs.databricks.com/aws/en/oltp/projects/external-apps-manual-api).
No Databricks SDK is required for this flow.

For **Snowflake Postgres**, install `etl-craft[snowflake]` and set `Account` to the Snowflake
account identifier and `Instance` to the Postgres instance name. `Token_url`, `Client_id`,
`Secret` and `Scope` configure an OAuth client-credentials issuer accepted by the Snowflake
account. The optional Snowflake driver presents that platform token, then executes
`GENERATE_POSTGRES_ACCESS_TOKEN_FOR_USER(instance, user)` with bound arguments. The returned
15-minute token becomes the PostgreSQL password. Enable Snowflake authorization on the
instance, map the Postgres `User` to the issuing Snowflake identity, and grant instance access.
The Snowflake platform OAuth token itself is **not** a Postgres password. See
[Snowflake Postgres token authentication](https://docs.snowflake.com/en/user-guide/snowflake-postgres/postgres-token-auth).

Keep `Cloning.Enabled: False`: managed-platform audit tables are directly accessible from the
platform. Enabled cloning fails configuration validation and doctor, with the setting to fix;
it is never silently disabled. The managed-provider choice and issuer credentials remain
host-local when projects use published bundles.

Complete examples are in [Lakebase](../examples/lakebase-engine.yml) and
[Snowflake Postgres](../examples/snowflake-postgres-engine.yml). Standard
[setup and migration commands](doctor-and-setup.md) apply; etl-craft does not provision cloud
instances, roles or network policies.

For live acceptance, set `ETL_CRAFT_TEST_DATABRICKS_ENGINE_CONFIG` or
`ETL_CRAFT_TEST_SNOWFLAKE_ENGINE_CONFIG` to an absolute configuration path for the managed
Engine DB. The tests create and drop a unique schema, so use a dedicated acceptance database
with schema-creation permission. They run `init-db`, `migrate`, a Python task, process loss
after reservation and a child exit after committing its outcome. Run the affected cloud
marker with `-k managed_engine_db`; missing configuration is reported as unavailable.
