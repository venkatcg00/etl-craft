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

## Deployment accounts

Every local command signs in once using `ETL_CRAFT_TOKEN`, then the current person's stored
session. An invalid environment token fails even when a valid stored session exists. There is
no implicit OS-user identity. `login` prompts for a password, or offers OIDC device sign-in when
`Auth.Oidc_issuer` and `Auth.Oidc_client_id` are configured. Only registered active `USER`
accounts can sign in; SSO subjects must already match `CFG_ACCOUNTS.OIDC_SUBJECT`.

`setup` creates the first admin interactively, with a password or `--oidc-subject SUBJECT`.
When no active admin remains, the same local command can create a new recovery admin using
the Engine DB writer credentials. Recovery is never exposed through HTTP. `doctor` diagnoses
a missing admin without requiring sign-in. Empty/old databases can still be initialized or
migrated locally before accounts exist; other commands require a credential. Once an admin
exists, provisioning commands require an authenticated admin.

Sessions last `Auth.Session_hours` (default 12) and are stored per Engine DB in
`$XDG_CONFIG_HOME/etl-craft/credentials`, otherwise `~/.config/etl-craft/credentials`.
The file must be owned by the current user with mode **0600**. Insecure files are refused
with their path and the `chmod` remedy. `logout` revokes and removes that stored session;
it does not revoke the independent service token supplied by the environment. Each person
uses their own OS home/config directory. The Engine DB stores only SHA-256 token digests,
never raw credentials. Sessions and service token values never enter warehouse clones.

Admins manage identities through `account add`, `list`, `set-role`, `disable` and
`reset-password`; see the [account example](../examples/accounts/README.md).
Roles are inherited from the account on each command/API request. Disabling revokes all
credentials immediately; password reset revokes existing sessions. Accounts are never deleted.
Password hashes use salted `hashlib.scrypt` and never enter metadata histories or mirrors.
Service accounts use `token create`, then present the value in `ETL_CRAFT_TOKEN`.
The shared services API enforces one role policy for CLI, HTTP and Python callers. Roles
include every lower role; transports name operations, not their own permission rules.

| Minimum role | Operations |
| --- | --- |
| viewer | SCD provenance, reads, inspection, validation, configuration plan/export, YAML export, `docs-version --check`, `rehash --dry-run`, `setup --print-grants` |
| operator | Run, backfill, mark, cancel, pause/resume and reconcile |
| developer | `config apply`, project migrations, `generate-docs`, `lineage --refresh` |
| admin | SCD value edits and human unlocks, accounts/tokens, setup/init-db, packaged Engine DB migrations, upgrade-targets, rehash, clone, controller startup, publish-docs and docs-version writes |

`migrate` can apply project migrations as developer only when no packaged Engine DB migration
is pending. An admin must apply pending engine upgrades first. A refused operation names the
account, its current role, the required role and the administrator who can grant it. The CLI
returns exit **26**, HTTP returns **403**, and the immutable action record has outcome
`REFUSED` with the same exit code. No operation runs after refusal. The permission check adds no records for allowed reads. Unregistered operations fail closed until the shared policy names them.

Local first-admin/recovery, pre-account initialization/migration, diagnostic doctor, and
login/logout remain the explicit credential/bootstrap entry points described above.

OIDC uses RFC 8628 polling and the configured issuer's HTTPS discovery/token endpoints.
Register a public device client with RS256 ID tokens and nonce support. Tokens are accepted
only directly from the authenticated TLS token endpoint, as permitted by
[OIDC Core §3.1.3.7](https://openid.net/specs/openid-connect-core-1_0.html#IDTokenValidation),
with issuer, client audience, nonce and lifetime validation; arbitrary supplied ID tokens
are never accepted. Provider tokens are discarded after the local session is issued.


## Operating a server from your own installation

People develop on their own installation, against a local Engine DB and warehouse. To operate a
server installation they sign in to it instead, and never hold its Engine DB or warehouse
credentials:

```bash
etl-craft login --server https://etl.example.com      # password, or SSO when it is configured
etl-craft status --pipeline_code SALES --server https://etl.example.com
export ETL_CRAFT_SERVER=https://etl.example.com      # or name the server once per session
etl-craft history --pipeline_code SALES --all
etl-craft logs --attempt-id 4812
etl-craft logout
```

- **Sign-in.** `login --server` sends the password once to the server, or, for SSO, shows the
  issuer's code while the server runs the device flow and verifies the ID token. The server
  issues a one-hour access token and a refresh token that lasts `Auth.Refresh_days` (default
  30). The credential file keeps both, under the server's address, and the CLI renews the access
  token when it expires. `logout`, disabling the account or revoking the refresh token ends the
  session.
- **Only HTTPS.** `--server` refuses plain HTTP except to `localhost`, and refuses redirects.
- **What works.** `status`, `explain`, `history`, `logs`, `whoami`, `run` (a run, a backfill
  with `--backfill`, or a rerun with `--rerun --task_code`), `pause`, `resume`, `mark`,
  `stop`, `abort`, `hold` and `release`, within the account's role, with the same text and JSON output and exit codes as on
  the server's own project.
- **What does not.** `config`, `migrate`, `init-db`, `setup`, `account` and `token` change
  metadata or the Engine DB and are refused against a server, naming the server-side way:
  config files and migrations reach a server only through its own deployment, by CI or an
  admin. Other commands work on a local project only.
- **Automation.** CI and Airflow keep a service token in `ETL_CRAFT_TOKEN`, with no refresh.
- **Errors.** A server that cannot be reached, or that answers with something other than
  etl-craft JSON, exits **27**. Every other refusal keeps its own exit code.

Every command reaches the same operation whichever interface starts it: a local command calls
it directly, and a command against a server calls the server's API, which calls it.

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

## Explicit attribution and warehouse writers

New audit rows record the proven account name and kind: `USER` for people, `SERVICE` for
automation, and `SYSTEM` for the engine. Execution source remains in the run trigger; schedules
record the server service account. Finalization, reconciliation, gate skips, automatic retries
and local task children explicitly act as `etl-craft` (`SYSTEM`).

Each run retains its warehouse writer in `AUD_PIPELINES_RUN_LOG.WRITER`. ETL writes use
`etl-craft` in `CREATED_BY` and `UPDATED_BY`, even when a person starts the run. The warehouse
login remains a connection credential. Migration `0035` assigns existing runs the ETL writer
and preserves their historical actor names and kinds. Apply migrations before using the new
version; do not edit audit history.

Python callers must explicitly scope library writes with `core.actor.acting_as`;
`current_actor()` raises `ConfigurationError` outside a scope. Unauthenticated connection reads
remain available for sign-in, while Engine DB guards reject unscoped writes. Token metadata
returns the registered `account_kind`; a service token never claims to be a person.
