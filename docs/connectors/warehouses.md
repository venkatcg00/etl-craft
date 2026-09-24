# Warehouses

A deployment writes to one warehouse, named in the `Warehouse` section of `craft-connector.yml`.
What the engine creates there depends on the warehouse and the table format:
`Warehouse.Table_format` (`native` by default), which a task may override with its own
`TABLE_FORMAT` parameter where the warehouse allows it.

| `Warehouse.Name` | `native` | `iceberg` | Extra to install |
|---|---|---|---|
| Postgres | ordinary tables | not available | none |
| DuckDB | tables in one local file | tables in an Iceberg REST catalog | none |
| Trino | Iceberg, whatever is asked: the catalog decides | Iceberg | `etl-craft[trino]` |
| Databricks | Delta tables | Delta tables with UniForm, readable as Iceberg | `etl-craft[databricks]` |
| Snowflake | ordinary tables | Iceberg tables | `etl-craft[snowflake]` |

A DuckDB file admits one writing process at a time, so tasks writing to it queue behind each
other; every other warehouse runs tasks in parallel. On DuckDB, the table format is fixed by the
connection and a task cannot override it.

## Connecting and authenticating

Each warehouse accepts the auth modes below. `secret` and `token` always name a variable, never a
value. Modes not verified here follow the vendor's documentation and can be used, but success is
not guaranteed.

| Warehouse | `auth_mode` (verified here in bold) |
|---|---|
| Postgres | **`password`**, `token`, `key_file`, `oauth`, `sso`, `sts` (as for a [PostgreSQL Engine DB](engine-db.md)) |
| DuckDB file | **`none`** |
| DuckDB over Iceberg | **`none`**, `token`, **`oauth`**: how DuckDB logs in to the REST catalog |
| Trino | **`none`**, `password`, `token`, `oauth`, `sso`, `key_file` |
| Databricks | **`token`**, `oauth` (a service principal), `sso` (browser login) |
| Snowflake | **`password`**, **`token`**, `key_file` (key pair), `oauth`, `sso` (browser), `sts` (workload identity) |

Databricks and Snowflake can also be given as separate fields instead of one JDBC URL; see the
[examples](../examples/README.md).

## Storage outside the warehouse's own

Where the data files live can be chosen per task, with task parameters:

- **Databricks**, native or Iceberg: `EXTERNAL_LOCATION` is the table's own path in cloud storage
  (`s3://…`, `abfss://…`, `gs://…`), which makes it an external table; Unity Catalog must cover the
  path with an external location. Without it, the table is managed and stored where its catalog or
  schema says, so a catalog backed by external storage needs nothing extra. Cloning mirrors go
  under the Cloning section's `Base_location` when it is set.
- **Snowflake, Iceberg:** `EXTERNAL_VOLUME` and `BASE_LOCATION` put the table on a customer volume
  (both are required together); without them it uses Snowflake-managed storage. `CATALOG` names a
  catalog integration for an externally managed Iceberg catalog, which needs a customer volume;
  whether Snowflake accepts writes there depends on the integration. Cloning mirrors need the
  Cloning section's `External_volume` and `Base_location`.
- **Snowflake, native:** ordinary tables always live in Snowflake's own storage.

These options have not been run against a live account in this project.
