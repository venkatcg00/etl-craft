# Connectors

The databases etl-craft connects to, and how it authenticates to each.

!!! note "Planned"
    - **Engine DB**: SQLite and PostgreSQL.
    - **Warehouses**: PostgreSQL, DuckDB, DuckDB over an Iceberg REST catalog, Trino over
      Iceberg, Databricks and Snowflake, each in their native or Iceberg table format.
    - **Authentication**: the modes each connection accepts (`none`, `password`, `token`,
      `key_file`, `oauth`, `sso`, `sts`), and which are verified against a live service.
