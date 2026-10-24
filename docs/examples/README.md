# craft-connector.yml examples

`craft-connector.yml` is written by you and only ever read by etl-craft. Each file here is a
complete, working example of one choice. Copy the closest one to `craft-connector.yml` beside
your project and change the values. `tests/unit/test_config_examples.py` loads every file, for every
profile it declares, so none of them can drift from the loader.

Every file follows the same layout, in this order (the loader refuses any other):

1. **Secrets**: where the variable names below are looked up, and the default profile.
2. **Orchestration**: limits, scheduling defaults and the Email
   relay, per profile.
3. **Engine**: the Engine DB, one block per profile.
4. **Warehouse**: the one warehouse, one block per profile.
5. **Cloning** (optional): mirror Engine DB tables into the warehouse.
6. **Docs_site** (optional): generate and publish the project catalog.

`docs/craft-connector.example.yml` is the annotated reference that explains every key.

## Variables and values

Every setting is either a **variable** or a **value**, and each line in these files is marked
with a `# variable` or `# value` comment:

- A setting whose text names a variable that the secrets source defines takes that variable's
  value. The source is the process environment, or the `.env`-style file `Secrets` points at.
- Anything else is used exactly as written. `Profile: dev` is the profile `dev`.
  `Profile: ETL_CRAFT_PROFILE` is whatever `ETL_CRAFT_PROFILE` holds, and if that variable is
  not set, the text itself.
- A secret (`Secret`, `Token`, `S3_secret`) must always name a variable that is set. A secret
  is never taken as written. Every command checks this when it loads the file, for the selected
  profile only, and stops with a "not set" or "empty" error naming the variable. Missing
  variables include suggestions for similarly named keys; values are never shown.

Audit arguments and logs redact secret-named fields even inside lists, and credentials in
connection URLs. This does not replace keeping credentials in the secrets source.

`etl-craft doctor` lists every value that was used as written but looks like a variable name,
which is how a missing variable shows up.

Secrets files use UTF-8, with or without a byte-order mark, and accept `export KEY=VALUE`.
All project paths start beside the config, including when it is a symbolic link. Duplicate
YAML keys are errors, with both source lines named. Numeric settings accept whole numbers or
ASCII decimal digits: `MAX_PARALLEL_TASKS` is at least 1, TCP ports are 1 to 65535, and timeouts,
retry counts and delays are at least 0.

## Start here

| File | Engine | Warehouse | Shows |
|---|---|---|---|
| `minimal-local.yml` | SQLite | DuckDB file | Nothing to install or set: one `dev` profile |

## Secrets

| File | Engine | Warehouse | Shows |
|---|---|---|---|
| `secrets-environment.yml` | PostgreSQL | PostgreSQL | Values from the process environment, and profile-specific names (`ENGINE_PROD_SECRET`) |
| `secrets-file.yml` | PostgreSQL | PostgreSQL | Values from a `.env`-style file (`secrets-file.env`), resolved relative to the config |

## Orchestration

| File | Engine | Warehouse | Shows |
|---|---|---|---|
| `orchestration-local.yml` | SQLite | DuckDB file | etl-craft dispatches ready tasks; per-profile limits and scheduling defaults |

## Engine DB

| File | Engine | Warehouse | Shows |
|---|---|---|---|
| `engine-sqlite.yml` | SQLite | PostgreSQL | One SQLite file per profile; nothing to authenticate |
| `engine-postgres.yml` | PostgreSQL | PostgreSQL | `PASSWORD` or `KEY_FILE` auth |

## Warehouse

One file per warehouse dialect, meaning each database paired with each table format it supports.

| File | Dialect | Table format | Connection |
|---|---|---|---|
| `warehouse-postgres.yml` | `postgres` | NATIVE (always) | JDBC URL + password |
| `warehouse-duckdb.yml` | `duckdb` | NATIVE | a local file, no auth |
| `warehouse-duckdb-iceberg.yml` | `duckdb_iceberg` | ICEBERG | in-memory DuckDB attached to an Iceberg REST catalog |
| `warehouse-trino-iceberg.yml` | `trino_iceberg` | ICEBERG (the catalog decides) | JDBC URL, `NONE` or `PASSWORD` |
| `warehouse-databricks.yml` | `databricks` | NATIVE (Delta) | separate fields + personal access token |
| `warehouse-databricks-iceberg.yml` | `databricks_iceberg` | ICEBERG (Delta + UniForm) | separate fields + personal access token |
| `warehouse-snowflake.yml` | `snowflake` | NATIVE | separate fields + Programmatic Access Token |
| `warehouse-snowflake-iceberg.yml` | `snowflake_iceberg` | ICEBERG (Snowflake-managed storage) | separate fields + PAT, with Cloning's volume |
| `warehouse-snowflake-key-pair.yml` | `snowflake` | NATIVE | JDBC URL + RSA key pair |

There is no `postgres_iceberg`. PostgreSQL has no Iceberg tables without a third-party
extension that this project neither ships nor tests, so `Table_format: ICEBERG` on a Postgres
warehouse is refused rather than silently ignored.

## Authentication

One file per target, with one profile per authentication type. Set `ETL_CRAFT_AUTH` to the
profile you want. Types marked *verified* have run against a live service in this project. The
others follow the vendor's documentation and are untested here: they can be used, but success
is not guaranteed, and `etl-craft doctor` warns about them.

| File | Types (verified in bold) |
|---|---|
| `auth-postgres.yml` | **PASSWORD**, KEY_FILE, TOKEN, OAUTH (e.g. Entra ID), SSO (libpq 18 OAuth), STS (AWS RDS IAM), for the Engine DB and a Postgres warehouse |
| `auth-snowflake.yml` | **PASSWORD**, **TOKEN** (PAT), KEY_FILE, OAUTH (client credentials), SSO (external browser), STS (AWS workload identity) |
| `auth-databricks.yml` | **TOKEN** (PAT), OAUTH (service principal, M2M), SSO (browser U2M) |
| `auth-trino.yml` | **NONE**, PASSWORD, TOKEN (JWT), OAUTH, SSO (OAuth 2.0 redirect), KEY_FILE (client certificate) |
| `auth-duckdb-iceberg.yml` | **NONE**, TOKEN, **OAUTH**: the Iceberg REST catalog's login |
| `auth-email.yml` | **NONE**, **PASSWORD**, OAUTH (SMTP XOAUTH2) |

`SSO` is interactive: someone completes a browser or device login. It suits a person running
etl-craft, not an unattended scheduler. `STS` needs `pip install etl-craft[aws]` on PostgreSQL.

## Cloning

| File | Shows |
|---|---|
| `cloning.yml` | `Enabled`/`Scope` per profile, including `Scope: NONE` |
| `docs-site.yml` | `Docs_site`: when the catalog site is written again, where, and how it is published |
| `docs-site-proxy.yml` | Organization-managed catalog URL, prefix and explicit proxy trust; no ngrok. |

## Profiles

The environment examples declare `dev`, `sit`, `uat` and `prod`, and select one with
`Secrets.Profile: ETL_CRAFT_PROFILE`, a variable each environment sets (`minimal-local.yml` has
only `dev`). A section's own `Profile` overrides `Secrets.Profile` for that section; either
can be a value or a variable.

Every profile names the same variables (`ENGINE_JDBC_URL`, `WAREHOUSE_SECRET`, ...). Each
environment sets those names to its own values. When one shell or `.env` file has to hold
several tiers at once, a profile-specific name (`WAREHOUSE_PROD_SECRET`) is used over the plain
one for that profile.

## Pipelines

`config/` holds [configuration files](../guides/configuration-files.md) for an example sales
project, one CSV file per supported package-owned config table: copy the folder into your project and change the rows.
`migrations/` holds the same configuration written as
[configuration migrations](../guides/configuration-migrations.md), one file per kind of change.
`config/` is exactly what `etl-craft config export` writes once the migrations are applied, and
the tests check both on SQLite and PostgreSQL.

[SQL source watermark](sql-watermark.md) shows an incremental external-source merge and an
audited reload.

After applying configuration, run `etl-craft lineage` to record table lineage versions.
`etl-craft lineage --table schema.table --version 1` reads the graph when that table's first
version began; `--as-of TIME` reads versions valid at a timestamp or pipeline run's start.
These commands use stored history and do not change configuration files.

For the generated catalog, run `etl-craft generate-docs` twice after configuration and execution.
Unchanged asset/run pages retain their timestamps using `.etl-craft-manifest.json`; search and
gaps refresh each build. No connector setting is needed. See [catalog regeneration](../guides/catalog.md#keeping-it-up-to-date)
for invalidation and recovery, and [run KPIs](../guides/catalog.md#runs-and-kpis) for the separate
scheduled, backfill, repair and other groups.

## API service credentials

With any example configured and its Engine DB initialized or migrated, create a service
credential locally (save the printed value securely):

```bash
etl-craft token create --name dashboard --role viewer --expires 30d
```

The name identifies one deployment account. Repeat with that name and role to issue another
credential for the same account; an inconsistent role is refused. Disabling the account
invalidates every credential belonging to it. See [tokens and roles](../deploying/http-api.md#tokens-and-roles)
for the upgrade procedure when old token names collide.

## Independent task execution

After applying your metadata, use a non-watermarked SQL task with no outside dependencies:

```bash
etl-craft run --pipeline-code SALES_DAILY --task-code load_orders --standalone
```

Add `--with-downstream` for its selected continuation, or explicitly justify current inputs
with `--ignore-dependencies --reason "upstream data verified"`. See
[manual selections](../guides/running-tasks.md#independent-manual-selections) for admission and
checkpoint limits and the equivalent HTTP request.

Scoped operator controls: see [holds, stop and abort](operator-controls.md).

The config-file pipelines carry `TAGS` such as `sales` and `daily`.
`etl-craft pipelines --tag sales --tag daily` requires both tags. Use the same filters with
`status`, `history`, `audit` and `validate`, or with `pause`/`resume` and explicit `--yes`
confirmation. Tags are current metadata labels, independent of schedules and permissions.

### Triggering a child

The [pipeline trigger configuration example](../guides/pipeline-triggers.md#configuration-file-example)
adds an ETL `RUN_PIPELINE` task and its three parameters through the existing config CSV files.
It covers controller execution, WAIT/DETACH modes and explicit rerun generations.

## Managed PostgreSQL Engine DBs

- `lakebase-engine.yml`: Lakebase OAuth database credentials, with a local DuckDB warehouse.
- `snowflake-postgres-engine.yml`: Snowflake-issued Postgres tokens, with a local DuckDB warehouse.

Both require TLS and keep cloning disabled. Replace the local Warehouse section with the team's
warehouse definition as needed. See [managed Engine DB setup](../deploying/engine-db.md#managed-postgresql-on-snowflake-and-databricks)
for provider permissions and live acceptance.
