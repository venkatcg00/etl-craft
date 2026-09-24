# Configuring etl-craft

`craft-connector.yml` tells etl-craft how to run and where to find every connection. **You write
it; etl-craft only reads it.** No command creates or rewrites it: `setup` reads it and brings the
Engine DB up to date. It holds variable *names* and plain values, never passwords, tokens, keys or
credentials, so it is safe to commit.

Commands below use an installed `etl-craft` executable. From this source checkout, prefix them
with `uv run`, for example `uv run etl-craft setup`.

Start from an example:

- [craft-connector.example.yml](craft-connector.example.yml): the annotated reference, working
  as-is for local development.
- [examples/](examples/README.md): a complete file for each Engine DB, each warehouse dialect,
  each secrets source, each orchestration mode and each authentication type.

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

Each section uses its own `Profile`, else `Secrets.Profile`. Like every setting, either one can
be a value (`Profile: prod`) or a variable (`Profile: ETL_CRAFT_PROFILE`, set to `prod` in the
production environment), which is how one file serves every environment. A section with a single
profile block needs no selection. A selected profile the section does not declare is an error that
names the profiles it does declare, and says when the profile came from a variable that isn't set.

### Variables and values

Every setting is either a **variable** or a **value**:

- If its text names a variable that the secrets source defines, the setting takes that variable's
  value. The source is the process environment, or the `.env`-style file `Secrets` points at.
- Anything else is used exactly as written.

```yaml
Engine:
  prod:
    jdbc_url: ENGINE_JDBC_URL       # variable: the environment holds jdbc:postgresql://...
    user: etl_service               # value: no variable named etl_service, so used as written
    auth_mode: password             # value
    secret: ENGINE_SECRET           # variable, and it must be set
```

So `Profile: dev` is the profile `dev`. `Profile: ETL_CRAFT_PROFILE` is whatever
`ETL_CRAFT_PROFILE` holds, or the text `ETL_CRAFT_PROFILE` if that variable isn't set. A number,
a flag or a list can come from a variable too: `Task_timeout_seconds: TASK_TIMEOUT` reads
`"3600"` and uses 3600, and a list setting reads a comma-separated value.

**Secrets are the one exception.** `secret`, `token` and `s3_secret` must name a variable that is
set; a secret is never taken as written. The file is meant to be committed, and falling back would
send a mistyped variable name to the server as a password.

The fallback can hide a missing variable: `user: ENGINE_USER` quietly becomes the user
`ENGINE_USER` when the variable isn't set. `etl-craft doctor` therefore lists every value used as
written that looks like a variable name (upper case with an underscore) as a `WARN`. When a value
used as written fails validation, the error says which variable was missing.

Every profile can use the same variable names; each environment (a laptop, CI, the prod servers)
sets them to its own values. When one shell or `.env` file must hold several tiers at once, a
profile-specific variable wins for that profile: `ENGINE_PROD_SECRET` is used over `ENGINE_SECRET`
for `prod`. The profile name goes before the setting's own suffix (`ENGINE_SECRET` becomes
`ENGINE_PROD_SECRET`, `EMAIL_FROM` becomes `EMAIL_PROD_FROM`).

## Secrets

```yaml
Secrets:
  Source_type: environment    # or: file
  Path: .env                  # Source_type: file only
  Profile: dev                # the default profile for every section (a value or a variable)
```

`Source_type` and `Path` can themselves be variables, looked up in the process environment, the
only source there is before the file is known. Everything else resolves against the source they
select.

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
| `Name` | none | informational: `generate-yml` emits one YAML DAG shape whatever it says, and teams convert it for their scheduler with their own scripts |
| `Task_timeout_seconds` | 21,600 | a task's wall-clock limit; a task's `TASK_TIMEOUT_SECONDS` overrides it; `0` disables |
| `Max_parallel_tasks` | 8 | the most task subprocesses a local wave runs at once, and the cap on parallel business rules |
| `Enforce_sla` | `false` | judge each finished run against its pipeline's `SLA_IN_HOURS` (below) |
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

### SLA enforcement

With `Enforce_sla: true`, every run of a pipeline that has a `CFG_PIPELINES.SLA_IN_HOURS` is
judged when it finishes, from its `START_DATE` to its `END_DATE`:

- `AUD_PIPELINES_RUN_LOG.SLA_STATUS` records `MET` or `BREACHED` (`NULL` when not judged).
- A breach is appended to the run's outcome message (`run`, and `run --finalize-only` under a
  remote orchestrator) and shown by `etl-craft history`.
- An `EMAIL_ALERT` sent after the SLA has passed is amber (`COMPLETED_WITH_ERRORS`), not green, and
  says so; `EMAIL_ON_STATUS: COMPLETED_WITH_ERRORS` can therefore alert on overruns alone.

A run's own `STATUS` is untouched. A late run still did its work, and marking it `FAILED` would make
every retry of it fail again. Off (the default), nothing is judged and `SLA_IN_HOURS` is only the
`sla_hours` metadata `generate-yml` writes for the orchestrator. The column arrives with migration
`0005`; run `etl-craft setup` or `migrate` after upgrading.

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
      auth_mode: EMAIL_AUTH_MODE    # none | password | oauth
      user: EMAIL_USER              # password and oauth
      use_tls: EMAIL_USE_TLS        # default true
      secret: EMAIL_SECRET          # the password, or the OAuth client secret
```

`oauth` is SMTP XOAUTH2 with a client-credentials access token (`client_id`, `token_url`,
optional `scope`), for relays that no longer accept passwords. See
[Authentication types](#authentication-types).

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
    auth_mode: ENGINE_AUTH_MODE     # password | key_file | token | oauth | sso | sts
    secret: ENGINE_SECRET           # the password, the key's passphrase, a token or client secret
    key_file: ENGINE_KEY_FILE       # key_file only: the client key's path
```

Query parameters on the URL (`sslmode=require`, ...) are forwarded to the driver. See
[Authentication types](#authentication-types) for what each mode needs.

## Warehouse

Exactly one per deployment. `Name` (`Postgres`, `DuckDB`, `Trino`, `Databricks`, `Snowflake`) is
checked against the connection. `Table_format` is `native` (the default) or `iceberg`; a task's
`CFG_TASK_PARAMETERS.TABLE_FORMAT` overrides it for that task's table. Both settings may sit on the
section or inside a profile (for example a DuckDB `dev` beside a Postgres `prod`).

The connection and the table format together select one **warehouse dialect**:

| Dialect | Name + Table_format | Status |
|---|---|---|
| `postgres` | Postgres, native | the reference launch path |
| `duckdb` | DuckDB, native | local development |
| `duckdb_iceberg` | DuckDB, iceberg | integration-tested against the included local Iceberg stack |
| `trino_iceberg` | Trino, either (the catalog decides) | integration-tested against the included local stack |
| `databricks` | Databricks, native (Delta) | verified live 2026-09-23 |
| `databricks_iceberg` | Databricks, iceberg (Delta + UniForm) | verified live 2026-09-23 |
| `snowflake` | Snowflake, native | verified live 2026-09-23 |
| `snowflake_iceberg` | Snowflake, iceberg | verified live 2026-09-23 (Snowflake-managed storage) |

Each warehouse's authentication types are listed under [Authentication types](#authentication-types).

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
    auth_mode: WAREHOUSE_AUTH_MODE  # password | key_file | token | oauth | sso | sts
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
    auth_mode: WAREHOUSE_AUTH_MODE  # none | password | token | oauth | sso | key_file
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

With the separate fields, a mode other than `token` is named in `auth_mode` (`oauth`, `sso`,
`key_file`, ... with that mode's fields); a `token` field means `auth_mode: token`.

**Snowflake key pair.** The alternative to the separate fields is a JDBC URL
(`jdbc:snowflake://<account>.snowflakecomputing.com/?db=<db>&schema=<schema>&...`) with
`auth_mode` `key_file`: `key_file` names the private key's path and `secret` its passphrase.
`password` also works on this shape.

## Authentication types

`auth_mode` selects how a connection authenticates. Each Engine DB and warehouse accepts its own
set, and each type needs its own profile fields. The loader refuses a type the target doesn't
offer, or a missing field, before anything connects.

| auth_mode | What it is | Fields |
|---|---|---|
| `none` | nothing to authenticate | |
| `password` | a stored password | `user`, `secret` |
| `token` | a stored bearer token | `secret` (or `token` with the separate-fields shape), `user` where the target needs one |
| `key_file` | a private key or client certificate on disk | `key_file`, `cert_file` (PostgreSQL, Trino), `secret` = the key's passphrase |
| `oauth` | an OAuth 2.0 client-credentials access token | `client_id`, `secret` = the client secret, `token_url`, optional `scope` |
| `sso` | the driver's own interactive login (browser or device) | per target, below |
| `sts` | the ambient AWS identity, optionally an assumed role | `region` and optional `role_arn` (PostgreSQL) |

**Verified** types have run against a live service in this project. **The others follow each
vendor's documentation and are untested here: they can be used, but success is not guaranteed.**
`etl-craft doctor` shows a `WARN` for every profile that uses one.

| Target | Verified | Implemented, untested |
|---|---|---|
| PostgreSQL (Engine DB and warehouse) | `password` | `key_file` (client certificate), `token` (as the password), `oauth` (an access token as the password: Azure Database for PostgreSQL with Entra ID), `sso` (libpq 18's OAuth device flow: `issuer`, `client_id`, optional `scope`; needs a server-side OAuth validator), `sts` (AWS RDS/Aurora IAM token; needs `pip install etl-craft[aws]`) |
| SQLite Engine DB | `none` | |
| DuckDB file | `none` | |
| DuckDB over Iceberg (the catalog's login) | `none`, `oauth` (against the repo's own Iceberg REST catalog) | `token` |
| Trino | `none` | `password`, `token` (a JWT), `oauth` (the access token sent as a JWT), `sso` (the cluster's OAuth 2.0 redirect), `key_file` (client certificate and key, no passphrase) |
| Databricks | `token` (personal access token) | `oauth` (service principal, M2M: `token_url` defaults to `https://<host>/oidc/v1/token`, `scope` to `all-apis`), `sso` (the connector's browser login) |
| Snowflake | `password`, `token` (PAT) | `key_file` (RSA key pair), `oauth` (the connector's `OAUTH_CLIENT_CREDENTIALS`), `sso` (external browser), `sts` (`WORKLOAD_IDENTITY` with the AWS provider) |
| Email relay | `none`, `password` | `oauth` (SMTP XOAUTH2) |

Notes that apply to every target:

- `oauth` and `sts` credentials are obtained per new connection, and pooled connections are
  recycled every 10 minutes so none outlives its credential. Where a driver runs the exchange
  itself (Snowflake's `oauth`, DuckDB's catalog `oauth`), etl-craft hands it the settings instead.
- `sso` needs a person to complete a browser or device login. It suits someone running
  etl-craft by hand, not an unattended scheduler: a headless worker fails before a browser opens.
- A private key, certificate or token never goes in the file: `key_file` and `cert_file` are
  paths, and every secret is a variable.

`docs/examples/auth-*.yml` has a complete file per target with one profile per type.

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

## Known limits

- **`Orchestration.Name` is informational.** `generate-yml` emits one YAML DAG shape (Airflow-style
  task commands and trigger rules) whatever the orchestrator is; teams convert it for their
  scheduler with their own scripts.
- **Untested authentication types** are listed above: they can be used, but success is not
  guaranteed until a team has run them against its own service.
- **`sso` is interactive** everywhere it exists.
