# craft-connector.yml examples

`craft-connector.yml` is written by you and only ever read by etl-craft. Each file here is a
complete, working example of one choice. Copy the closest one to `craft-connector.yml` beside
your project and change the values. `tests/unit/test_config_examples.py` loads every file, for every
profile it declares, so none of them can drift from the loader.

Every file follows the same layout, in this order (the loader refuses any other):

1. **Secrets**: where the variable names below are looked up, and the default profile.
2. **Orchestration**: execution mode, limits, the `generate-yml` DAG defaults, and the Email
   relay, per profile.
3. **Engine**: the Engine DB, one block per profile.
4. **Warehouse**: the one warehouse, one block per profile.
5. **Cloning** (optional): mirror Engine DB tables into the warehouse.

`docs/craft-connector.example.yml` is the annotated reference that explains every key.

## Variables and values

Every setting is either a **variable** or a **value**, and each line in these files is marked
with a `# variable` or `# value` comment:

- A setting whose text names a variable that the secrets source defines takes that variable's
  value. The source is the process environment, or the `.env`-style file `Secrets` points at.
- Anything else is used exactly as written. `Profile: dev` is the profile `dev`.
  `Profile: ETL_CRAFT_PROFILE` is whatever `ETL_CRAFT_PROFILE` holds, and if that variable is
  not set, the text itself.
- A secret (`secret`, `token`, `s3_secret`) must always name a variable that is set. A secret
  is never taken as written. Every command checks this when it loads the file, for the selected
  profile only, and stops with a "not set" error naming the variable.

`etl-craft doctor` lists every value that was used as written but looks like a variable name,
which is how a missing variable shows up.

## Start here

| File | Engine | Warehouse | Mode | Shows |
|---|---|---|---|---|
| `minimal-local.yml` | SQLite | DuckDB file | local | Nothing to install or set: one `dev` profile |

## Secrets

| File | Engine | Warehouse | Mode | Shows |
|---|---|---|---|---|
| `secrets-environment.yml` | PostgreSQL | PostgreSQL | local | Values from the process environment, and profile-specific names (`ENGINE_PROD_SECRET`) |
| `secrets-file.yml` | PostgreSQL | PostgreSQL | local | Values from a `.env`-style file (`secrets-file.env`), resolved relative to the config |

## Orchestration

| File | Engine | Warehouse | Mode | Shows |
|---|---|---|---|---|
| `orchestration-local.yml` | SQLite | DuckDB file | local | etl-craft runs the waves; per-profile DAG defaults for previewing `generate-yml` |
| `orchestration-remote.yml` | PostgreSQL | PostgreSQL | remote | Airflow runs each task; `Allow_schedule: false` outside prod, Email per profile, `Global_dag` in prod |

## Engine DB

| File | Engine | Warehouse | Mode | Shows |
|---|---|---|---|---|
| `engine-sqlite.yml` | SQLite | PostgreSQL | local | One SQLite file per profile; nothing to authenticate |
| `engine-postgres.yml` | PostgreSQL | PostgreSQL | remote | `password` or `key_file` auth |

## Warehouse

One file per warehouse dialect, meaning each database paired with each table format it supports.

| File | Dialect | Table format | Connection |
|---|---|---|---|
| `warehouse-postgres.yml` | `postgres` | native (always) | JDBC URL + password |
| `warehouse-duckdb.yml` | `duckdb` | native | a local file, no auth |
| `warehouse-duckdb-iceberg.yml` | `duckdb_iceberg` | iceberg | in-memory DuckDB attached to an Iceberg REST catalog |
| `warehouse-trino-iceberg.yml` | `trino_iceberg` | iceberg (the catalog decides) | JDBC URL, `none` or `password` |
| `warehouse-databricks.yml` | `databricks` | native (Delta) | separate fields + personal access token |
| `warehouse-databricks-iceberg.yml` | `databricks_iceberg` | iceberg (Delta + UniForm) | separate fields + personal access token |
| `warehouse-snowflake.yml` | `snowflake` | native | separate fields + Programmatic Access Token |
| `warehouse-snowflake-iceberg.yml` | `snowflake_iceberg` | iceberg (Snowflake-managed storage) | separate fields + PAT, with Cloning's volume |
| `warehouse-snowflake-key-pair.yml` | `snowflake` | native | JDBC URL + RSA key pair |

There is no `postgres_iceberg`. PostgreSQL has no Iceberg tables without a third-party
extension that this project neither ships nor tests, so `Table_format: iceberg` on a Postgres
warehouse is refused rather than silently ignored.

## Authentication

One file per target, with one profile per authentication type. Set `ETL_CRAFT_AUTH` to the
profile you want. Types marked *verified* have run against a live service in this project. The
others follow the vendor's documentation and are untested here: they can be used, but success
is not guaranteed, and `etl-craft doctor` warns about them.

| File | Types (verified in bold) |
|---|---|
| `auth-postgres.yml` | **password**, key_file, token, oauth (e.g. Entra ID), sso (libpq 18 OAuth), sts (AWS RDS IAM), for the Engine DB and a Postgres warehouse |
| `auth-snowflake.yml` | **password**, **token** (PAT), key_file, oauth (client credentials), sso (external browser), sts (AWS workload identity) |
| `auth-databricks.yml` | **token** (PAT), oauth (service principal, M2M), sso (browser U2M) |
| `auth-trino.yml` | **none**, password, token (JWT), oauth, sso (OAuth 2.0 redirect), key_file (client certificate) |
| `auth-duckdb-iceberg.yml` | **none**, token, **oauth**: the Iceberg REST catalog's login |
| `auth-email.yml` | **none**, **password**, oauth (SMTP XOAUTH2) |

`sso` is interactive: someone completes a browser or device login. It suits a person running
etl-craft, not an unattended scheduler. `sts` needs `pip install etl-craft[aws]` on PostgreSQL.

## Cloning

| File | Shows |
|---|---|
| `cloning.yml` | `Enabled`/`Scope` per profile, including `Scope: none` |
| `docs-site.yml` | `Docs_site`: when the catalog site is written again, where, and how it is published |

## Profiles

The environment examples declare `dev`, `sit`, `uat` and `prod`, and select one with
`Secrets.Profile: ETL_CRAFT_PROFILE`, a variable each environment sets (`minimal-local.yml` has
only `dev`). A section's own `Profile` overrides `Secrets.Profile` for that section; either
can be a value or a variable.

Every profile names the same variables (`ENGINE_JDBC_URL`, `WAREHOUSE_SECRET`, ...). Each
environment sets those names to its own values. When one shell or `.env` file has to hold
several tiers at once, a profile-specific name (`WAREHOUSE_PROD_SECRET`) is used over the plain
one for that profile.
