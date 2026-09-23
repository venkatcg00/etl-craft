# Configuring etl-craft

`craft-connector.yml` describes how the engine runs and where it finds connection values. The
canonical manifest contains variable names, never passwords, tokens, private keys, or connection
URLs.

Commands below use an installed `etl-craft` executable. From this source checkout, prefix them
with `uv run`, for example `uv run etl-craft setup`.

Start from one of the canonical examples:

- [environment-backed config](craft-connector.env-secrets.example.yml) for CI and containers
- [file-backed config](craft-connector.file-secrets.example.yml) for a local development machine

## Canonical manifest

The canonical top-level sections are:

| Section | Purpose |
|---|---|
| `Orchestration` | execution mode, time limit, and local parallelism |
| `Secrets` | where named values are read from |
| `Engine` | the required PostgreSQL Engine DB connection |
| `Warehouse` | the optional data warehouse connection and table format |
| `Cloning` | optional Engine DB mirroring into the warehouse |
| `Dag_defaults` | defaults written into the Airflow-shaped descriptor |
| `Email` | optional SMTP profile for `EMAIL_ALERT` tasks |

`Engine`, `Warehouse`, and `Email` each have a `Profile` and `Variables` block. Every value in a
`Variables` block is the name of a variable in the configured secret source. For example:

```yaml
Secrets:
  Source_type: environment

Engine:
  Profile: prod
  Variables:
    jdbc_url: ENGINE_JDBC_URL
    user: ENGINE_USER
    auth_mode: ENGINE_AUTH_MODE
    secret: ENGINE_SECRET
```

With that configuration, `ENGINE_JDBC_URL` contains the actual URL and `ENGINE_SECRET` contains
the password or token. Neither value belongs in the manifest. `Profile` is the default tier;
The process-environment variables `ETL_CRAFT_ENGINE_PROFILE`,
`ETL_CRAFT_WAREHOUSE_PROFILE`, and `ETL_CRAFT_EMAIL_PROFILE` can select another tier at runtime.
They are selectors, so they are read from the process environment even when `Secrets.Source_type`
is `file`. When a tier-specific variable is present, it takes precedence over the plain name for
that tier.

`Secrets.Source_type` is either `environment` or `file`. For `file`,
`Secrets.Source_path` is required. The file reader accepts `KEY=VALUE` lines, ignores blank lines
and whole-line `#` comments, and removes one matching pair of surrounding single or double
quotes. Relative paths are resolved from the directory holding `craft-connector.yml`. It does not
support escape sequences or multi-line values.

For a local setup file, use `Source_type: file` and point `Source_path` at that file. A passed
`.env` is not automatically exported to later processes. For CI or a container, export every
referenced variable and use `Source_type: environment`.

## Bootstrap with `setup`

`etl-craft setup` takes bootstrap variables from `./.env`, `--env FILE`, or the process
environment with `--from-environment`. It writes the canonical manifest, then creates or upgrades
the Engine DB when it can connect.

The bootstrap input is a mix of a few `ETL_CRAFT_*` settings (`ETL_CRAFT_MODE`,
`ETL_CRAFT_SOURCE_TYPE`, `ETL_CRAFT_ENGINE_PROFILE`, `ETL_CRAFT_WAREHOUSE_PROFILE`,
`ETL_CRAFT_WAREHOUSE_TABLE_FORMAT`, ...) plus the *same* variable names the canonical manifest's
own `Variables` blocks use — `ENGINE_JDBC_URL`, `ENGINE_USER`, `ENGINE_AUTH_MODE`, `ENGINE_SECRET`,
`WAREHOUSE_JDBC_URL`, and so on. `setup` reads a connection value under the same name it then
writes into the manifest as a pointer, so the variable that bootstraps a deployment is the same one
`etl-craft run` resolves afterwards — nothing to keep in sync by hand. See
[craft-connector.variables.env](craft-connector.variables.env) for the full list. For a file-backed
local setup, set both:

```dotenv
ETL_CRAFT_SOURCE_TYPE=file
ETL_CRAFT_SOURCE_PATH=./.env
```

For environment-backed deployment, export the bootstrap variables and run:

```bash
etl-craft setup --from-environment
```

Run `etl-craft doctor` after setup. It resolves every active profile and tests each configured
connection. `setup` reports an unreachable Engine DB after writing the manifest; that report does
not prove the deployment is ready.

Older `Execution` / `Source` / `Postgres` / `Profiles` / `Orchestrator` manifests remain readable
for migration. Use the canonical shape for new files. Do not mix the two shapes in one manifest.

## Execution and orchestration

`Orchestration.Mode` is one of:

- `local`: `etl-craft run --pipeline_code X` schedules ready task waves itself.
- `remote`: a scheduler invokes individual tasks with `etl-craft run --pipeline_code X --task_code Y`.

`generate-yml` emits a YAML descriptor containing task commands and Airflow trigger rules. It does
not create, load, deploy, or operate an Airflow DAG. A deployment using Airflow needs its own
loader, DAG packaging, worker image, and scheduler policy. Other schedulers can call the same
single-task command, but this repository does not ship an adapter for them.

`--force` bypasses dependency and state checks and is available only in `local` mode.

| `Orchestration` setting | Default | Effect |
|---|---:|---|
| `Task_timeout_seconds` | 21,600 | task wall-clock limit; a task parameter can override it and `0` disables it |
| `Max_parallel_tasks` | 8 | maximum subprocesses in a local task wave |
| `Enforce_sla` | `false` | opt in to engine-side comparison with `CFG_PIPELINES.SLA_IN_HOURS` |

## Connections, warehouses, and formats

The Engine DB is always PostgreSQL. A warehouse is optional until a deployment runs `SQL` or
`BUSINESS_RULES` tasks.

| Target | Status | Supported authentication | Storage notes |
|---|---|---|---|
| PostgreSQL warehouse | reference launch path | `password` | native PostgreSQL tables |
| DuckDB | local development | `none` | file-backed and single-writer; tasks queue for warehouse access |
| Trino over Iceberg | integration-tested against the included local stack | `none` or `password` | use an Iceberg catalog when a task requests Iceberg |
| Databricks | **verified live 2026-09-23**: `native` (managed Delta) and `iceberg` (managed Delta with UniForm) both pass `CREATE_TABLE`/`OVERWRITE_TABLE`/`SCD1_MERGE` | static `token`, preferred connection (below) | `native` creates Delta; `iceberg` creates Delta with UniForm enabled — Databricks itself always reads/writes Delta, and UniForm additionally generates Iceberg metadata for external engines |
| Snowflake | **verified live 2026-09-23**: `native` and `iceberg` (Snowflake-managed) both pass `CREATE_TABLE`/`OVERWRITE_TABLE`/`SCD1_MERGE` via the preferred connection | static `token`, preferred connection (below), `key_file`, or `password` | `native` uses ordinary Snowflake tables; `iceberg` defaults to Snowflake's own internal storage — no cloud bucket to provision |

From this source checkout, install the dialect for the warehouse being used:

```bash
uv sync --extra databricks  # or: --extra snowflake, --extra trino
```

`Warehouse.Table_format` accepts `iceberg` (the default) or `native`; a task can override it with
`CFG_TASK_PARAMETERS.TABLE_FORMAT`. PostgreSQL and DuckDB use their native storage either way.
Trino's catalog determines the actual table format, so `validate` checks an Iceberg request against
the selected catalog. Do not describe an arbitrary SQLAlchemy dialect as supported: the SQL actions
are tested only against the targets in this table.

**Snowflake Iceberg tables are zero-config by default.** With no `EXTERNAL_VOLUME`/`BASE_LOCATION`
task parameters declared, the engine creates them with `EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'` —
Snowflake's own internal storage, not a customer-owned bucket, verified live to support the full
`CREATE_TABLE`/`OVERWRITE_TABLE`/`SCD1_MERGE`/`SCD2_MERGE` vocabulary. A task can still name its own
`EXTERNAL_VOLUME` (paired with `BASE_LOCATION`) to place a table's data in a specific customer-owned
volume instead — useful when another engine needs to read the same physical files.

Authentication is deliberately narrow:

- Engine DB: `password` or `key_file`.
- Warehouse: `none`, `password`, `key_file`, or a static `token`.
- Email: `none` or `password`.

`sso` is rejected. A warehouse token is a stored bearer token; the engine does not mint or refresh
OAuth or cloud-session credentials. For Snowflake, `key_file` refers to a mounted private-key path
and the secret variable supplies its passphrase. Keep the key outside the manifest.

### Preferred connection: Databricks and Snowflake (`auth_mode: token`)

For Databricks and Snowflake specifically, `Warehouse.Variables` can name separate connection
fields instead of one JDBC URL carrying everything — **the tested and recommended shape for both**
(see `warehouse.PREFERRED_CONNECTION_FIELDS`, and `docs/craft-connector.env-secrets.example.yml`/
`docs/craft-connector.file-secrets.example.yml` for full worked examples):

- **Databricks**: `jdbc_url` (host, port and `httpPath` only — no `ConnCatalog`/`ConnSchema`),
  `catalog`, `schema`, `token`. No `user`: the username is the literal `"token"`, supplied by the
  engine.
- **Snowflake**: `user`, `account` (the `<org>-<account>` identifier, not a hostname), `database`,
  `schema`, `warehouse`, `role`, `token` — a Programmatic Access Token (PAT), presented like a
  password. A PAT requires a network policy assigned to the account or the user
  (Snowsight → Admin → Security → Network Policies) before any connection using it will succeed;
  without one every attempt fails with `Network policy is required`, regardless of credentials.

Both resolve to `auth_mode: token` automatically — there is no separate `auth_mode` variable to set
for either shape, and specifying one alongside `token` is rejected as a conflicting configuration.
The engine assembles a full connection from the named fields at connect time
(`warehouse.preferred_connection_url`); nothing here is a JDBC URL a human has to hand-build with
an embedded query string.

## Schema lifecycle

```bash
etl-craft init-db   # an empty Engine DB only
etl-craft migrate   # an existing Engine DB
```

`init-db` refuses an Engine DB that already has engine tables. `migrate` always applies packaged
engine migrations first, then an optional project migration stream from `--migrations-dir`,
`ETL_CRAFT_MIGRATIONS_DIR`, or `./sql/migrations`. The two streams have separate migration
identities, so a project directory cannot hide packaged migrations.

Migration hashes are recorded. Changing or removing an applied migration stops the command before
new work is applied. Add a new migration instead of editing history. On a fresh Engine DB, the
packaged schema is already current; packaged migrations are recorded and project migrations still
run. An advisory lock prevents concurrent migration runs.

See [operations.md](operations.md) for a safe backup and upgrade sequence.
