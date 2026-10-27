# Security

## Secrets

`craft-connector.yml` never holds a secret. Every `Secret` (and every other credential field)
names a variable, looked up in the process environment or in the `.env`-style file
`Secrets.Path` points at (see [Variables and values](../examples/README.md#variables-and-values)).
A secret whose variable is not set or is empty stops every command when the file is loaded; it is never
taken as written. Keep the secrets file out of version control and readable only by the account
that runs etl-craft.

Secrets never appear in logs, in error messages, or in the connection URLs etl-craft logs. An
Python script receives its own settings through `CFG_TASK_PARAMETERS`, which are not secret:
a script that needs a credential reads it from its environment. Values in
`CFG_TASK_PARAMETERS` and anything a script prints or logs are the team's responsibility.

Audit arguments and structured log fields mask secret-named keys recursively, including mappings
inside lists. Connection URL passwords and secret query parameters are redacted in audit
arguments and log messages, including exception text. Safe connection settings remain visible.
The audit API and `etl-craft audit --format json` return the same redacted arguments.

Credential query keys (`password`, `pwd`, `passwd`, `token`, `access_token`, `secret` and
`private_key_file_pwd`, in any case) are refused in JDBC URLs; name the secret variable in
`Secret` instead. DuckDB's `S3_secret` is also stored as a variable name and resolved for each
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
| admin | SCD value edits and human unlocks, accounts/tokens, setup/init-db, packaged Engine DB migrations, upgrade-targets, rehash, clone, controller/UI startup, publish-docs and docs-version writes |

`migrate` can apply project migrations as developer only when no packaged Engine DB migration
is pending. An admin must apply pending engine upgrades first. A refused operation names the
account, its current role, the required role and the administrator who can grant it. The CLI
returns exit **26**, HTTP returns **403**, and the immutable action record has outcome
`REFUSED` with the same exit code. No operation runs after refusal. The permission check adds no records for allowed reads. Unregistered operations fail closed until the shared policy names them.

### Pipeline permission groups

Admins manage groups with `etl-craft group add --name sales`, `group disable --name sales`,
`group add-member --name sales --account alice`, `group remove-member --name sales --account
alice`, and `group list`. Matching HTTP actions use `/api/v1/groups` and its `disable`,
`members`, and `members/remove` routes. Group identities and memberships are retired with
`ACTIVE_FLAG=N`, never deleted; all changes have typed, actor-attributed history.

Pipeline permissions use three levels: `CAN_VIEW` reads, `CAN_RUN` adds execution and operator
controls, and `CAN_MANAGE` adds permission delegation. Principals are an active group, an
account, or a deployment role (`viewer` includes everyone with at least viewer; `operator`
includes everyone with at least operator). Each grant names a project, optionally narrowed to one pipeline or tag.

```bash
etl-craft access grant --project sales --group analysts --permission CAN_VIEW --reason "Project access"
etl-craft access grant --project sales --group analysts --pipeline DAILY --permission CAN_VIEW --reason "Reporting"
etl-craft access grant --project sales --account alice --tag reporting --permission CAN_MANAGE --reason "Reporting owner"
etl-craft access revoke --project sales --group analysts --pipeline DAILY --reason "Team changed"
etl-craft access list --project sales
etl-craft access grant --project sales --file access.csv --dry-run --reason "Review permissions"
etl-craft access grant --project sales --file access.csv --reason "Apply permissions"
```

The exact HTTP actions are `POST /api/v1/access/grants`, `POST /api/v1/access/revocations`,
and `GET /api/v1/access/grants`. Lists include the caller's effective levels and their contributing sources.
An administrator or a holder of `CAN_MANAGE` on the pipeline can grant or revoke any level on
it. Granting on a tag requires `CAN_MANAGE` on that tag; managing one tagged pipeline is
insufficient. A project CAN_MANAGE holder can administer all project, pipeline and tag grants there,
including restricted pipelines, and can deploy that project's configuration. A pipeline or tag
manager cannot edit the project or reach another project. Only deployment admins manage
accounts, groups, pools and project creation/settings/disable.

Bulk input is admin-only, with columns `PROJECT,GROUP_NAME,ROLE,PIPELINE_CODE,TAG,PERMISSION,ACTION`.
Choose exactly one group or role; leave both pipeline and tag empty for a project grant,
or select one pipeline or tag; `ACTION` is `GRANT` or `REVOKE`.
Rows name their project; if --project is supplied, every row must match that scope. Account grants use the single-row `--account` form,
never the bulk file. The CLI sends content; the server validates every row and diffs the current
state inside the apply transaction. All bad lines are reported and nothing is applied.
Dry-run returns added, changed permission, revoked and unchanged rows without changing grants;
its request is still audited. Writes require a reason, retain attribution and history, and
revocation marks `ACTIVE_FLAG=N` without deleting rows.

Project access is default deny. Deployment admins have CAN_MANAGE everywhere. Other accounts
need a matching project grant even when they have a direct pipeline or tag grant. Missing and
inaccessible projects answer identically, as do missing and inaccessible pipelines (exit 24,
HTTP 404). Lists and searches omit hidden objects.

Project CAN_MANAGE covers every pipeline in that project. Otherwise an open pipeline (no active
pipeline/tag grants) uses the account's project level. A restricted pipeline uses only its
highest matching pipeline/tag grant, with no project-level fallback; no match means hidden.
Disabled groups' active grants keep pipelines restricted. Revocation takes effect on the next
request, without a cache. Revoking the last active pipeline/tag grant restores the project-level
fallback. An insufficient level on a visible object is a permission refusal (exit 26 / HTTP 403).

Tags remain the only pipeline grouping. Config plan/apply report grants starting or stopping
their match after tag edits, and open/restricted switches, in text and JSON. This is a report
only; it does not enumerate accounts or change grant rows.

Migration 0064 adds viewer/CAN_VIEW and operator/CAN_RUN project grants to existing projects,
retaining restricted-pipeline access exactly. It adds no project CAN_MANAGE. Every authenticated
account has a deployment role; service tokens and SSO sessions use that account role, so the
viewer role grant covers every existing grantee. Projects created with `project add` have no
grants: an admin must grant access explicitly.

`config plan`, `config apply`, project migrations, `bundle publish` and `generate-docs` require
project CAN_MANAGE. A full config export requires CAN_VIEW on every pipeline, so it cannot
disclose a restricted pipeline. Engine migrations and changed connection approvals still require
deployment admin. Declaring a cross-project dependency also requires CAN_VIEW on the upstream project.
Without upstream access, a recorded dependency reveals only its project/pipeline code and
decision, never upstream runs, logs or data. Incoming dependents are shown only when both sides
are visible.

With SSO, set optional `Auth.Oidc_groups_claim: groups` beside `Auth.Oidc_issuer` and
`Auth.Oidc_client_id`. The configured claim must be a list of group names. Only names matching
active groups at sign-in are retained; unmatched names appear in the session's login audit.
The session stores **group IDs**, so a rename keeps the identity and reusing a name cannot
transfer an old session's access to another group. Explicit Engine DB memberships are checked
on every request as well.

Refresh tokens are not rotated: the same token works until it expires or its session is
signed out. A stolen refresh token can be used for that entire time. Use a short
`Auth.Refresh_days` lifetime and run `etl-craft logout` for a lost device.

Refreshed access tokens retain the group set from the last SSO sign-in; they do not fetch new
provider claims. Access expiry is capped at the refresh token's expiry, so `Auth.Refresh_days`
bounds retained claims in a server session. A local session is bounded by `Auth.Session_hours`.
Removing someone from a group at the identity provider is seen at their next SSO sign-in,
not by an already admitted session or run. Disable the account or the local group, or retire
its grant, to withdraw its contribution immediately under the current policy.

Admitted runs retain their initiating group IDs after token expiry or revocation. A child
copies its parent's `SSO_GROUP_IDS`, and grandchildren carry the same set. Before each child
is admitted, and again immediately before a queued child or task starts, the engine checks that the
initiating account is active, every retained SSO group still has `ACTIVE_FLAG=Y`, and the
account still has CAN_RUN through current group, account or deployment-role grants (or the current
project level on an open pipeline). A refusal before admission
fails the trigger task; a queued child that loses permission is cancelled with the reason in
its interventions or attempt summary. Revoking project access also stops later task starts in
manual runs, including service-account runs. Already running children keep their admitted work. Current admins and the
controller's SERVICE account retain their existing access; scheduled execution is unaffected.

An organization-hosted static catalog is **one site per project**. Its host is responsible for
access control; IP restrictions alone are not project authorization. Projects on one instance
share a code trust boundary; use separate instances for separate security domains.

Signed-in, project-filtered serving by `publish-docs` needs an approved integration with API/UI
sessions. Its separate stdlib server currently has no sign-in context; that part of project
isolation is pending a design decision. `Docs_site.Allowed_ips` remains an extra network layer,
not a substitute for account/project checks.

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
etl-craft login --api https://etl.example.com      # password, or SSO when it is configured
etl-craft status --pipeline-code SALES --api https://etl.example.com
export ETL_CRAFT_API=https://etl.example.com      # or name the server once per session
etl-craft history --pipeline-code SALES --all
etl-craft logs --attempt-id 4812
etl-craft logout
```

- **Sign-in.** `login --api` sends the password once to the server, or, for SSO, shows the
  issuer's code while the server runs the device flow and verifies the ID token. The server
  issues a one-hour access token and a refresh token that lasts `Auth.Refresh_days` (default
  30). The credential file keeps both, under the server's address, and the CLI renews the access
  token when it expires. `logout`, disabling the account or revoking the refresh token ends the
  session.
- **Only HTTPS.** `--api` refuses plain HTTP except to `localhost`, and refuses redirects.
- **What works.** Every action the [actions reference](../reference/actions.md) marks as
  running anywhere: `run` (a run, a backfill with `--backfill`, or a rerun with `--rerun
  --task-code`), `mark`, `stop`, `abort`, `hold`, `release`, `pause`, `resume`, `status`,
  `explain`, `history`, `steps`, `graph`, `pipelines`, `audit`, `lineage`, `validate`,
  `logs`, `whoami`, `account`, `token` and the maintenance commands, within the account's role,
  with the same text and JSON output and exit codes as on the controller's own project.
- **What does not.** `config`, `migrate`, `init-db`, `setup`, `doctor`, `controller`, `ui` and
  `publish-docs` run on the controller's host only and are refused against a controller,
  naming where to run them: config files and migrations reach a controller only through its own
  deployment, by CI or an admin.
- **Automation.** CI and external schedulers keep a service token in `ETL_CRAFT_TOKEN`, with no refresh.
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
| Email relay | an account that may send as `From_address`, or a relay that needs none |

Your team writes the `CFG_` rows; the account etl-craft runs as only needs to read them, but
`setup` needs the same rights on them as on the other Engine DB tables.

## What a SQL task can do

A SQL task supplies one read-only `SELECT`. The engine checks it (one statement, a query, with
no write anywhere in its parse tree), and wraps it in one of nine actions that it owns, so a task's SQL cannot write, drop or grant
anything by itself. Python scripts are Python run by your team, with the account's rights:
review them like any code that runs in production.

## Connections

Use TLS where the database offers it: for PostgreSQL, `sslmode=require` or `verify-full` in the
URL, or a client certificate with `Auth_mode: KEY_FILE`. The auth modes each connection accepts,
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

## Shared instance trust boundary

Projects on one instance trust each other's executable code, as Airflow DAGs on shared workers
do. Use separate instances for separate security domains. In production, people use `--api`;
Engine DB writer credentials belong only on controller and API hosts because those credentials
bypass every role check.

Triggered child pipelines from a manual run are refused once the initiating account is disabled.
Use service accounts for long manual chains.
