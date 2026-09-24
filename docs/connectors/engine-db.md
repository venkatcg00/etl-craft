# Engine DB

The Engine DB holds what to run (the `CFG_` tables) and what happened (the `AUD_` tables). It is
separate from the warehouse the tasks write to. Choose it in the `Engine` section of
`craft-connector.yml`, by its `jdbc_url`.

| | SQLite | PostgreSQL |
|---|---|---|
| `jdbc_url` | `jdbc:sqlite:<file>` | `jdbc:postgresql://host[:port]/database[?settings]` |
| Suits | local development, one machine | production, workers on several machines |
| Setup | nothing to install; the file is created on first use | an empty database and a role that can create tables |
| Cross-process locks | a lock file beside the database file | PostgreSQL advisory locks |

## SQLite

A relative path is resolved beside `craft-connector.yml`, so every command and task finds the same
file wherever it starts. An in-memory database (`jdbc:sqlite:` or `jdbc:sqlite::memory:`) is refused:
each task runs in its own process and would see an empty one. SQLite 3.35 or newer is required.

Every connection uses write-ahead logging, so readers continue while one process writes, and
enforces foreign keys. A SQLite file cannot be shared by workers on other machines, and every write
is serialized; use PostgreSQL once either matters.

## PostgreSQL

Settings in the URL's query string, such as `sslmode=require`, are passed to the driver. The
password is never part of the connection URL the engine logs.

| `auth_mode` | Profile fields | Verified here |
|---|---|---|
| `password` | `user`, `secret` | yes |
| `token` | `user`, `secret`: a stored bearer token, sent as the password | |
| `key_file` | `user`, `key_file`, optional `cert_file`, `secret`: the key's passphrase | |
| `oauth` | `user`, `client_id`, `secret`, `token_url`, optional `scope`: a client-credentials token, fetched for each new connection | |
| `sso` | `user`, `issuer`, `client_id`: libpq's own OAuth login, for interactive use | |
| `sts` | `user`, `region`, optional `role_arn`: an AWS RDS IAM token; needs `pip install etl-craft[aws]` | |

`secret` always names a variable; it is never written into the file. Modes not verified here
follow the vendor's documentation and can be used, but success is not guaranteed.

## The schema

Each Engine DB ships its own `schema.sql` with the same tables and columns, and the same rules:
one active row per code, one `IN-PROGRESS` run per pipeline, one task-run row per task per run,
and the value lists every `CHECK` constraint enforces. See
[Dependencies and run conditions](../guides/dependencies.md) for what the dependency tables mean.
