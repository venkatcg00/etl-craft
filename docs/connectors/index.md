# Connectors

The databases etl-craft connects to, and how it authenticates to each.

Every connection is described in `craft-connector.yml`, which your team writes and etl-craft only
reads. Start from the [examples](../examples/README.md): one complete file per Engine DB,
warehouse, secrets source, orchestration mode and authentication type. The
[annotated reference](../craft-connector.example.yml) explains every key.

!!! note "Planned"
    - **Engine DB**: SQLite and PostgreSQL.
    - **Warehouses**: PostgreSQL, DuckDB, DuckDB over an Iceberg REST catalog, Trino over
      Iceberg, Databricks and Snowflake, each in their native or Iceberg table format.
    - **Authentication**: the modes each connection accepts (`none`, `password`, `token`,
      `key_file`, `oauth`, `sso`, `sts`), and which are verified against a live service.
