# Security

## Secrets

`craft-connector.yml` never holds a secret. Every `secret` (and every other credential field)
names a variable, looked up in the process environment or in the `.env`-style file
`Secrets.Path` points at (see [Variables and values](../examples/README.md#variables-and-values)).
A secret whose variable is not set or is empty stops every command when the file is loaded; it is never
taken as written. Keep the secrets file out of version control and readable only by the account
that runs etl-craft.

Secrets never appear in logs, in error messages, or in the connection URLs etl-craft logs. An
ingestion script receives its own settings through `CFG_TASK_PARAMETERS`, which are not secret:
a script that needs a credential reads it from its environment. Values in
`CFG_TASK_PARAMETERS` and anything a script prints or logs are the team's responsibility.

Credential query keys (`password`, `pwd`, `passwd`, `token`, `access_token`, `secret` and
`private_key_file_pwd`, in any case) are refused in JDBC URLs; name the secret variable in
`secret` instead. DuckDB's `s3_secret` is also stored as a variable name and resolved for each
new connection, so its value is never retained in the configuration object.

Duplicate YAML keys are refused with their section and both line numbers. Relative key,
certificate, CA and sendmail paths, including `sslrootcert`, `sslcert` and `sslkey` URL settings,
start at the directory holding `craft-connector.yml`. A symlinked config uses the link's
folder. `doctor` reports missing or unreadable files at their resolved paths.

## Least privilege

etl-craft creates nothing outside a SQLite file: every schema must exist, and it creates tables
only where the metadata tells it to.

| Account | Needs |
|---|---|
| Engine DB (PostgreSQL) | `USAGE` on the Engine DB schema; `CREATE` on it for `setup` and `migrate`, which a deployment account can run instead; `SELECT`, `INSERT`, `UPDATE` and `DELETE` on its tables, and `USAGE` on their sequences |
| Warehouse | `SELECT` on the schemas tasks read; in the schemas they write, the rights to create tables (for `CREATE_TABLE` and `SETUP_TABLE`), insert, update, delete and drop them (for `DROP_TABLE`); the same in the cloning schema when [cloning](cloning.md) is on |
| Email relay | an account that may send as `from_address`, or a relay that needs none |

Your team writes the `CFG_` rows; the account etl-craft runs as only needs to read them, but
`setup` needs the same rights on them as on the other Engine DB tables.

## What a SQL task can do

A SQL task supplies one read-only `SELECT`. The engine checks it (one statement, a query, with
no write anywhere in its parse tree), and wraps it in one of nine actions that it owns, so a task's SQL cannot write, drop or grant
anything by itself. Ingestion scripts are Python run by your team, with the account's rights:
review them like any code that runs in production.

## Connections

Use TLS where the database offers it: for PostgreSQL, `sslmode=require` or `verify-full` in the
URL, or a client certificate with `auth_mode: key_file`. The auth modes each connection accepts,
and which are verified against a live service here, are in
[Authentication](../connectors/authentication.md).

## The catalog site

GET and HEAD requests share the same decoded-path check: hidden path segments, parent traversal
and links outside the site root are refused with 404, including directory-index symlinks.

The [catalog](../guides/catalog.md) shows every task's SQL, table columns, lineage and run
details. `publish-docs` serves it only to the addresses in `Docs_site.Allowed_ips`, with
headers that keep it out of search engines; leave it unpublished where that is too much to share.
