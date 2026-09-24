# Configuring etl-craft

`craft-connector.yml` tells etl-craft how to run and where to find every connection. **You write
it; etl-craft only reads it.** No command creates or rewrites it: `setup` reads it and brings the
Engine DB up to date. It holds variable *names*, never passwords, tokens, keys or credentials, so
it is safe to commit.

Commands below use an installed `etl-craft` executable. From this source checkout, prefix them
with `uv run`, for example `uv run etl-craft setup`.

Start from an example:

- [craft-connector.example.yml](craft-connector.example.yml): the annotated reference, working
  as-is for local development.
- [examples/](examples/README.md): a complete file for each Engine DB, each warehouse dialect,
  each secrets source and each orchestration mode.

## Layout

Five sections. They must appear in this order, and the loader refuses any other:

| Section | Required | Holds |
|---|---|---|
| `Secrets` | yes | where variable names are looked up, and the default profile |
| `Orchestration` | yes | execution mode and limits, the `generate-yml` DAG defaults, and the Email relay |
| `Engine` | yes | the Engine DB connection, per profile |
| `Warehouse` | no (needed by `SQL`/`BUSINESS_RULES` tasks) | the one warehouse connection, per profile |
| `Cloning` | no | mirroring Engine DB tables into the warehouse |

Unknown sections and unknown keys are refused, so a typo such as `Retires:` fails loudly instead
of being ignored.

### Profiles

Any section can hold one block per environment (a *profile*): `dev`, `sit`, `uat`, `prod`, or
any names you like. A key beside the profile blocks applies to every profile; the same key inside
a block overrides it for that profile. `Engine` and `Warehouse` need at least one profile block.

```yaml
Orchestration:
  Mode: local                 # every profile...
  Max_parallel_tasks: 8
  prod:
    Mode: remote              # ...except prod
    Allow_schedule: true
```

Each section uses its own active profile, chosen most-specific first:

| Source | Example |
|---|---|
| `$ETL_CRAFT_<SECTION>_PROFILE` | `ETL_CRAFT_WAREHOUSE_PROFILE=prod` |
| `$ETL_CRAFT_PROFILE` | one switch for every section |
| `<Section>.Profile` | `Engine: {Profile: uat, ...}` |
| `Secrets.Profile` | the file-wide default |

A section with a single profile block needs no selection. The selectors are read from the
process environment even when `Secrets.Source_type` is `file`. A selected profile the section
does not declare is an error that names the profiles it does declare.

### Values are variable names

In `Engine`, `Warehouse` and `Orchestration`'s `Email` block, every value is the **name** of a
variable in the secrets source:

```yaml
Engine:
  prod:
    jdbc_url: ENGINE_JDBC_URL       # the environment holds jdbc:postgresql://...
    user: ENGINE_USER
    auth_mode: ENGINE_AUTH_MODE     # ...password
    secret: ENGINE_SECRET           # ...the password itself
```

Every profile can use the same names; each environment (a laptop, CI, the prod servers) sets them
to its own values. When one shell or `.env` file must hold several tiers at once, a
profile-specific name wins for that profile: `ENGINE_PROD_SECRET` is used over `ENGINE_SECRET`
for `prod`. The profile name goes before the field's own suffix (`ENGINE_SECRET` becomes
`ENGINE_PROD_SECRET`, `EMAIL_FROM` becomes `EMAIL_PROD_FROM`).

One exception keeps the local default free of variables: a `jdbc_url` written literally as
`jdbc:...` is used as-is. A URL carries no credentials; secrets always go through variables. A
literal URL that ends in a colon (`"jdbc:duckdb:"`) needs quotes in YAML.

## Secrets

```yaml
Secrets:
  Source_type: environment    # or: file
  Path: .env                  # Source_type: file only
  Profile: dev                # the default profile for every section
```

- `environment` reads names from the process environment. Use it for CI and containers.
- `file` reads a `.env`-style file at `Path`, resolved relative to `craft-connector.yml` (never
  the working directory) so every spawned task reads the same file. The file name can be
  anything. The reader accepts `KEY=VALUE` lines, ignores blank lines and whole-line `#`
  comments, and removes one matching pair of surrounding quotes. It supports no escape sequences
  and no multi-line values. Keep the file out of version control and `chmod 600` it.

## Orchestration

Execution settings, the DAG defaults `generate-yml` writes into its output, and the Email relay,
all in one section and all overridable per profile.

| Setting | Default | Effect |
|---|---:|---|
| `Mode` | (required) | `local`: `etl-craft run --pipeline_code X` runs the task waves itself. `remote`: an orchestrator runs each task with `etl-craft run --pipeline_code X --task_code Y`. |
| `Name` | none | informational only; see [Not yet implemented](#not-yet-implemented) |
| `Task_timeout_seconds` | 21,600 | a task's wall-clock limit; a task's `TASK_TIMEOUT_SECONDS` overrides it; `0` disables |
| `Max_parallel_tasks` | 8 | the most task subprocesses a local wave runs at once, and the cap on parallel business rules |
| `Enforce_sla` | `false` | parsed but not yet enforced; see [Not yet implemented](#not-yet-implemented) |
| `Global_dag` | `false` | allows `generate-yml --global` (the cross-pipeline trigger DAG) |
| `Catchup` | `false` | Airflow `catchup` |
| `Tags` | `[<refresh type>]` | Airflow `tags` |
| `Retries` | 1 | `default_args.retries` |
| `Retry_delay_minutes` | 5 | `default_args.retry_delay_minutes` |
| `Depends_on_past` | `false` | `default_args.depends_on_past` |
| `Email_on_failure` | `false` | `default_args.email_on_failure` |
| `Email_recipients` | none | `default_args.email`, emitted only when `Email_on_failure` is true |
| `Allow_schedule` | `true` | `false` emits `schedule: null` even when the pipeline has a `RUN_SCHEDULE` |
| `Email` | none | the SMTP relay for `EMAIL_ALERT` tasks (below) |

For the Airflow-facing settings, a pipeline's own `CFG_PIPELINES.PIPELINE_PARAMETERS` value
(`CATCHUP`, `TAGS`, `RETRIES`, ...) wins over the active profile's, which wins over the default.

`Allow_schedule` is how environments stay separate: every environment holds every pipeline, and
`generate-yml` gives the pipelines a timetable only where it is `true`, typically `prod`.
Elsewhere their DAGs run only when triggered.

`generate-yml` emits a YAML descriptor with task commands and Airflow trigger rules. It does not
create, load, deploy or operate an Airflow DAG: a deployment needs its own loader, packaging,
worker image and scheduler policy. `--force` bypasses dependency and state checks and is refused
under `remote`.

### Email

Needed only when a pipeline has an `EMAIL_ALERT` task. It lives in `Orchestration`, usually per
profile:

```yaml
Orchestration:
  Mode: remote
  prod:
    Email:
      host: EMAIL_HOST
      port: EMAIL_PORT
      from_address: EMAIL_FROM
      auth_mode: EMAIL_AUTH_MODE    # none | password
      user: EMAIL_USER              # password only
      use_tls: EMAIL_USE_TLS        # default true
      secret: EMAIL_SECRET          # password only
```

## Engine

The Engine DB holds every `CFG_`/`AUD_` table. `Name` is optional (`SQLite` or `Postgres`) and is
checked against `jdbc_url`.

### SQLite, the default

```yaml
Engine:
  dev:
    jdbc_url: jdbc:sqlite:etl-craft-engine.db
```

A relative path resolves against the directory holding `craft-connector.yml`, so every command and
every spawned task opens the same file. `jdbc:sqlite::memory:` is refused: each task runs in its own
process and would see an empty database. SQLite has nothing to authenticate, so `auth_mode` is
`none`, whether or not you write it.

SQLite is for local development and single-machine deployments. Use PostgreSQL in production:

- SQLite serializes every Engine DB write. Tasks queue behind each other's short audit writes
  (the busy timeout is 60 seconds).
- The file must be on the machine that runs every task. An orchestrator whose workers run on
  other hosts cannot use it, and `doctor` says so under `Mode: remote`.
- SQLite has no database users, so `CREATED_BY`/`UPDATED_BY` in the `CFG_` tables default to
  `etl-craft` unless your insert scripts supply a value.

The migration lock and the single-writer-warehouse queue are OS file locks beside the SQLite file
(`<file>.migrate.lock`, `<file>.warehouse.lock`) instead of Postgres advisory locks.

### PostgreSQL, for production

```yaml
Engine:
  Name: Postgres
  prod:
    jdbc_url: ENGINE_JDBC_URL       # jdbc:postgresql://host:5432/etl_craft[?sslmode=require]
    user: ENGINE_USER
    auth_mode: ENGINE_AUTH_MODE     # password | key_file
    secret: ENGINE_SECRET           # the password, or the key's passphrase
    key_file: ENGINE_KEY_FILE       # key_file only: the client key's path
```

Query parameters on the URL (`sslmode=require`, ...) are forwarded to the driver.

## Warehouse

Exactly one per deployment. `Name` (`Postgres`, `DuckDB`, `Trino`, `Databricks`, `Snowflake`) is
checked against the connection. `Table_format` is `native` (the default) or `iceberg`; a task's
`CFG_TASK_PARAMETERS.TABLE_FORMAT` overrides it for that task's table. Both settings may sit on the
section or inside a profile (for example a DuckDB `dev` beside a Postgres `prod`).

The connection and the table format together select one **warehouse dialect**:

| Dialect | Name + Table_format | Status | Authentication |
|---|---|---|---|
| `postgres` | Postgres, native | the reference launch path | `password` |
| `duckdb` | DuckDB, native | local development | `none` |
| `duckdb_iceberg` | DuckDB, iceberg | integration-tested against the included local Iceberg stack | `none` (object storage keys in the profile) |
| `trino_iceberg` | Trino, either (the catalog decides) | integration-tested against the included local stack | `none` or `password` |
| `databricks` | Databricks, native (Delta) | verified live 2026-09-23 | token fields |
| `databricks_iceberg` | Databricks, iceberg (Delta + UniForm) | verified live 2026-09-23 | token fields |
| `snowflake` | Snowflake, native | verified live 2026-09-23 | token fields, `key_file`, or `password` |
| `snowflake_iceberg` | Snowflake, iceberg | verified live 2026-09-23 (Snowflake-managed storage) | token fields, `key_file`, or `password` |

There is no `postgres_iceberg`. PostgreSQL has no Iceberg tables without a third-party extension
that this project neither ships nor tests, so `Table_format: iceberg` on a Postgres warehouse is
refused rather than silently ignored. On DuckDB the format is fixed by the connection (a file, or
an Iceberg catalog), so a task cannot override it there.

From this source checkout, install the dialect for a cloud warehouse:

```bash
uv sync --extra databricks  # or: --extra snowflake, --extra trino
```

### PostgreSQL

```yaml
Warehouse:
  Name: Postgres
  prod:
    jdbc_url: WAREHOUSE_JDBC_URL    # jdbc:postgresql://host:5432/analytics
    user: WAREHOUSE_USER
    auth_mode: WAREHOUSE_AUTH_MODE  # password
    secret: WAREHOUSE_SECRET
```

### DuckDB

A file (`jdbc:duckdb:warehouse.duckdb`) with `auth_mode: none`. DuckDB admits one writing process
at a time, so tasks queue for the warehouse behind an Engine DB lock. The file stem is the
catalog name in `catalog.schema.table`, so keep it a plain identifier. An in-memory file warehouse
is refused by `doctor`: every task is its own process.

**Over Iceberg.** DuckDB runs in memory and attaches an Iceberg REST catalog:

```yaml
Warehouse:
  Name: DuckDB
  Table_format: iceberg
  prod:
    jdbc_url: "jdbc:duckdb:"
    catalog: WAREHOUSE_CATALOG                       # the attach name, e.g. lake
    catalog_uri: WAREHOUSE_CATALOG_URI               # the REST endpoint
    iceberg_warehouse: WAREHOUSE_ICEBERG_WAREHOUSE   # e.g. s3://warehouse/
    s3_endpoint: WAREHOUSE_S3_ENDPOINT
    s3_region: WAREHOUSE_S3_REGION
    s3_url_style: WAREHOUSE_S3_URL_STYLE
    s3_use_ssl: WAREHOUSE_S3_USE_SSL
    s3_key_id: WAREHOUSE_S3_KEY_ID                   # omit both keys for the
    s3_secret: WAREHOUSE_S3_SECRET                   #   ambient credential chain
```

The `iceberg` and `httpfs` extensions install on first connection, which needs network access
once per machine. Each statement commits on its own: inside one transaction the catalog cannot
drop or rename a table the same transaction created. As on Trino, an action that fails partway
is made safe by idempotent retries, not by a rollback.

### Trino

```yaml
Warehouse:
  Name: Trino
  Table_format: iceberg
  prod:
    jdbc_url: WAREHOUSE_JDBC_URL    # jdbc:trino://host:8080/<catalog>/<schema>
    user: WAREHOUSE_USER
    auth_mode: WAREHOUSE_AUTH_MODE  # none | password
    secret: WAREHOUSE_SECRET
```

The catalog decides the table format; `validate` checks that it really is an Iceberg catalog.

### Databricks and Snowflake: token fields (recommended)

Separate connection fields and a stored token, the tested shape for both. The engine assembles
a credential-free URL at connect time, and `auth_mode` is `token` implicitly.

```yaml
Warehouse:
  Name: Databricks
  Table_format: native              # iceberg = managed Delta with UniForm enabled
  prod:
    jdbc_url: WAREHOUSE_JDBC_URL    # host, port and httpPath only
    catalog: WAREHOUSE_CATALOG
    schema: WAREHOUSE_SCHEMA
    token: WAREHOUSE_TOKEN          # a personal access token
```

Any `AuthMech`/`UID`/`PWD` pasted in from the workspace's JDBC tab is stripped before use. There is
no `user`: the username is the literal `token`.

```yaml
Warehouse:
  Name: Snowflake
  Table_format: native              # iceberg = CREATE ICEBERG TABLE
  prod:
    user: WAREHOUSE_USER
    account: WAREHOUSE_ACCOUNT      # <org>-<account>, not a hostname
    database: WAREHOUSE_DATABASE
    schema: WAREHOUSE_SCHEMA
    warehouse: WAREHOUSE_WAREHOUSE
    role: WAREHOUSE_ROLE
    token: WAREHOUSE_TOKEN          # a Programmatic Access Token
```

A Snowflake PAT needs a network policy on the account or user (Snowsight → Admin → Security →
Network Policies); without one, every connection fails with `Network policy is required`.
Snowflake Iceberg tables default to `EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'`, Snowflake's own
storage, so there is no bucket to provision. A task can name its own `EXTERNAL_VOLUME` (with
`BASE_LOCATION`) in `CFG_TASK_PARAMETERS`.

**Snowflake key pair.** The alternative to a token is a JDBC URL
(`jdbc:snowflake://<account>.snowflakecomputing.com/?db=<db>&schema=<schema>&...`) with
`auth_mode` `key_file`: `key_file` names the private key's path and `secret` its passphrase.
`password` also works on this shape. `key_file` is implemented only for Snowflake, and the loader
refuses it for any other warehouse.

A warehouse token is a stored bearer token: etl-craft does not mint or refresh OAuth or cloud
session credentials, and `sso` is not a supported mode.

## Cloning

```yaml
Cloning:
  prod:
    Enabled: true                   # default false
    Scope: all                      # cfg | aud | all | none
    # External_volume: MY_VOLUME    # Snowflake Iceberg warehouse only: literal names
    # Base_location: etl_craft      #   (the mirrors have no task to carry them)
```

After each pipeline run, the selected Engine DB tables are copied into the warehouse, so a team can
query its run history and config from inside the warehouse. `Scope: none` turns cloning off for a
profile without deleting the settings.

## How the dialect is chosen

Everything that differs between databases lives in `src/etl_craft/dialects/`, one file per
database:

```text
dialects/
  engine_dialects/
    postgres/   __init__.py  schema.sql  schema_test.sql  migrations/
    sqlite/     __init__.py  schema.sql  migrations/
  warehouse_dialects/
    base.py  postgres.py  duckdb.py  duckdb_iceberg.py  trino_iceberg.py
    databricks.py  databricks_iceberg.py  snowflake.py  snowflake_iceberg.py
```

- The **Engine DB dialect** comes from `Engine`'s `jdbc_url`. It owns the connection, the full
  schema `init-db` applies, the migration stream `migrate` applies, statement splitting, and
  locks.
- The **warehouse dialect** comes from `Warehouse`'s connection plus the task's resolved table
  format: its own `TABLE_FORMAT`, else `Warehouse.Table_format`. It supplies only what differs:
  the `CREATE TABLE` clause, audit column types, the hash expression, temporary-table support,
  `UPDATE` aliasing, `RENAME` syntax, and how `ROW_ID` is generated (identity, sequence, or
  computed per insert on Iceberg).

The seven SQL actions are written once in `sql_actions.py` and ask the dialect for those pieces, so
a fix to an action reaches every warehouse.

## `setup`, `doctor` and the schema lifecycle

```bash
etl-craft setup     # validate craft-connector.yml, then create or migrate the Engine DB
etl-craft doctor    # resolve every active profile and test every connection
etl-craft init-db   # an empty Engine DB only
etl-craft migrate   # an existing Engine DB
```

`setup` refuses to run without a `craft-connector.yml` and never writes one. It applies the
Engine DB dialect's packaged schema to an empty database, or its pending migrations to an existing
one, and is safe to repeat after any upgrade. An unreachable Engine DB is reported, not raised;
run `doctor` to see why.

`init-db` refuses an Engine DB that already has engine tables. `migrate` always applies the
dialect's packaged migrations first, then an optional project stream from `--migrations-dir`,
`ETL_CRAFT_MIGRATIONS_DIR`, or `./sql/migrations`. The two streams have separate identities, so a
project directory cannot hide a packaged migration. Migration hashes are recorded: changing or
removing an applied migration stops the command before any new work, so add a new migration
instead of editing history. A lock prevents concurrent runs.

See [operations.md](operations.md) for a safe backup and upgrade sequence.

## Not yet implemented

These are accepted in `craft-connector.yml` or in scope, but do nothing yet:

- **`Orchestration.Enforce_sla`** is parsed and validated, but nothing compares a run against
  `CFG_PIPELINES.SLA_IN_HOURS`. `SLA_IN_HOURS` is emitted into `generate-yml` output as
  `sla_hours` for the orchestrator to use.
- **`Orchestration.Name`** is informational. `generate-yml` emits the same Airflow-shaped
  descriptor whatever it says; there is no Databricks Workflows or other scheduler output.
- **Minted credentials.** `sso`, and tokens a provider mints per connection (OAuth
  client-credentials, cloud STS), are not supported anywhere; a warehouse `token` is a stored
  secret.
- **`key_file` outside Snowflake.** A warehouse `key_file` works only for Snowflake (refused
  elsewhere); the Engine DB's `key_file` is PostgreSQL client-certificate auth.
