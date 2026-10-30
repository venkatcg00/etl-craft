# HTTP API

Install `etl-craft[ui]`, initialize or migrate the Engine DB, and run `etl-craft controller`
for an all-in-one deployment. With PostgreSQL, split supervision from HTTP:

```bash
etl-craft controller --no-ui
etl-craft ui
```

Each process reads its project's configuration; set `Orchestration.Api_address` to a distinct
address for each process on the same host. Run two or more `ui` copies behind your TLS proxy.
Copies hold no session or run state: requests can alternate between them without sticky
sessions. Both controller and API copies hold Engine DB credentials and use the same
actor-scoped operations. The API copies admit whole-pipeline runs; the controller supervises those runs.
Task-scoped actions keep their existing synchronous operation behaviour.
SQLite supports all-in-one only; `ui` refuses it with the PostgreSQL remedy.

`controller --no-ui`, and a plain install without the `ui` extra, serve only the unsigned
`GET /health` and [Prometheus `/metrics`](metrics.md) using the standard library. They expose no API or documentation routes.
All-in-one and `ui` also expose `/health`; `/api/v1/health` still requires a bearer token.
On `ui`, `/health` is readiness: it returns 503 with the cause during database/schema failure
or shutdown, and recovers without restarting. On the controller it remains liveness.
See [Behind a load balancer](load-balancer.md) for trust, draining and deployment fragments.
Every role refuses to start unless the applied Engine DB migration names and checksums match
its installed package. Apply pending migrations with `etl-craft migrate`; an older package
against a newer schema must be upgraded. No automatic migration runs at process startup.

The `ui` extra includes FastAPI, uvicorn, Pydantic and Jinja2. `etl-craft[all]` adds `ui` and
every warehouse extra (including AWS support). The former `server` extra is removed without
an alias. UI pages are delivered separately; this role serves the shared HTTP actions.

`Orchestration.Api_address` defaults to `127.0.0.1:8730`. Set it to a `host:port`, or
`[IPv6-address]:port`. For a team URL, place a TLS reverse proxy in front of the listener.
Bearer credentials should travel over HTTPS when the client is not on localhost.

## Tokens and roles

Create the first admin credential locally:

```bash
etl-craft token create --name owner --role admin --expires 90d
etl-craft token create --name dashboard --role viewer --expires 30d
etl-craft token list
etl-craft token revoke --token-id 2
```

Creation prints the token once. Save it securely; the Engine DB stores only its SHA-256 hash.
Tokens may omit `--expires` for no expiry. Revocation and expiry take effect on the next request.
The account's name is the actor in action and lifecycle records. Each token is deployment-wide;
project-scoped tokens are not accepted until project authorization exists.

Each credential belongs to a deployment account in `CFG_ACCOUNTS`. `token create --name`
creates a service account when that name is new, or reuses the existing active account when
`--role` matches its current role. A conflicting role or inactive account is refused, with
nothing created. Multiple credentials can share one account; changing its role affects all
of them, and setting its `ACTIVE_FLAG` to `N` invalidates all of them on the next request.
Accounts retain their identity when disabled and cannot be deleted. Account changes currently
use explicit project migrations; interactive account administration and login are delivered
in the following authentication slice. Local commands retain their existing Engine DB trust
boundary until that rollout.

Before upgrading, legacy token names must be unique even among revoked tokens, non-empty,
at most 128 characters and free of control characters. Migration
`0034_accounts.sql` reports conflicting names, token IDs and roles before applying any pending
migration. Use the old installed version to apply an explicit project migration that gives
those tokens distinct `NAME` values, then upgrade and run `etl-craft migrate` again. The
migration preserves credential hashes, IDs, creation fields, expiry and revocation; tokens
continue working with their original roles. It never silently merges or renames accounts.
The token-local `ROLE` column is removed; join `CFG_API_TOKENS.ACCOUNT_ID` to `CFG_ACCOUNTS`
to read the current role.

Except for sign-in, `/health` and `/metrics`, every resource endpoint requires `Authorization: Bearer <token>`.
`viewer` can read, `operator` can also trigger, cancel, pause, resume, mark, rerun and backfill,
`developer` includes operator access, and `admin` can also create, list and revoke tokens
through `/api/v1/tokens`.
Token values and hashes are absent from token listings and audit arguments. The token table
is excluded from metadata snapshots and warehouse cloning. Safe lifecycle versions live in
`AUD_CFG_API_TOKENS_HISTORY` and appear in `AUD_METADATA_CHANGES`; these contain no token
value or hash. Account versions live in `AUD_CFG_ACCOUNTS_HISTORY`; password hashes never
enter these histories, metadata exports or warehouse mirrors. Revocation sets `ACTIVE_FLAG='N'`;
inactive credentials also fail authentication,
even without a revocation timestamp. Storage uses the common `CREATE_DATE` and update fields;
the public token document retains `created_at`. Run `etl-craft migrate` before using the new
version against an existing Engine DB.

## Calling actions

Every action has one route, generated from the action table; the
[actions reference](../reference/actions.md) lists each action's method, path, role and audit.
Reads are `GET` with their request in the query string; changes are `POST` with their request
as a JSON object. Run selection is always `run_id` or `run_key` in the query or body, or neither
for the pipeline's single non-terminal run. Only `GET /runs/{run_id}`, `GET /attempts/{attempt_id}`
and its `/log` carry an id in the path.

```bash
curl -H "Authorization: Bearer $ETL_CRAFT_API_TOKEN" \
  'http://127.0.0.1:8730/api/v1/projects/default/status?pipeline_code=SALES'
curl -X POST -H "Authorization: Bearer $ETL_CRAFT_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"pipeline_code":"SALES","run_date":"2026-10-01","reason":"manual refresh"}' \
  http://127.0.0.1:8730/api/v1/projects/default/runs
```

A whole-pipeline run over HTTP initializes or resumes the run and returns its exact identity;
the controller executes its tasks. Backfills and task runs execute in the request, as the CLI
does. `force`, `skip` and `ignore_dependencies` (except with `standalone`) work only on the
controller's host and are refused here, naming the field. A disconnected client does not cancel
committed work; inspect its exact run before retrying.

| Method | Path under `/api/v1` | Request |
|---|---|---|
| GET | `/pipelines`, `/pipelines/{pipeline_code}` | Active definitions and pause state |
| GET | `/graph`, `/steps`, `/status`, `/explanations` | `pipeline_code`; `run_id` or `run_key`; `task_code` for `/explanations` |
| GET | `/history` | `pipeline_code`; optional `task_code`, `all`, `limit` (1 to 100), `before` (an exclusive run-id cursor) |
| GET | `/capabilities` | `pipeline_code`, the selected run, optional `task_code`: each control's availability and reason |
| GET | `/control-requests/{control_id}` | One control request and its acknowledgements |
| GET | `/audit`, `/validation`, `/lineage`, `/catalog/gaps` | The `audit`, `validate`, `lineage` and catalog gap documents |
| GET | `/runs/{run_id}`, `/attempts/{attempt_id}`, `/attempts/{attempt_id}/log?offset=0` | Exact run, attempt and log bytes |
| POST | `/runs` | A `RunRequest`: `pipeline_code` and `task_code`, `standalone`, `rerun`, `with_downstream`, `run_date`, `backfill_from`/`backfill_to`, `reason` |
| POST | `/marks` | `pipeline_code`, `reason`, and `status` or `watermark`; optional `task_code`, `rows`, `stale`, `new_run` |
| POST | `/controls` | `pipeline_code`, `action` (`HOLD`, `RELEASE`, `STOP`, `ABORT`, `RECOVER`, `RESTART`, `RUN_PAUSED`, `PAUSE`, `RESUME`), `reason`; optional `task_code`, `request_key` |
| POST | `/controls/bulk` | `requests`: 1 to 100 controls, each with an explicit `run_id` or `run_key` and a unique `request_key` |
| POST | `/reconciliations` | Optional `pipeline_code`, `task_code` |
| GET, POST | `/tokens`; POST `/tokens/revoke` | Admin: list, create (`name`, `role`, optional `expires`), revoke (`token_id`) |
| GET, POST | `/accounts`; POST `/accounts/role`, `/accounts/disable`, `/accounts/password` | Admin account administration |
| GET | `/health`, `/whoami` | Health; the caller's account and role |

### Sign-in routes

These routes take no bearer token; each body is the proof. They call the same sign-in
operations as a local `etl-craft login`, and `etl-craft login --api` uses them.

| Method | Path under `/api/v1` | Input or result |
|---|---|---|
| GET | `/login/methods` | Whether SSO is configured |
| POST | `/login/password` | `name`, `password`; a one-hour access token and an `Auth.Refresh_days` refresh token |
| POST | `/login/sso` | Starts the issuer's device flow: the code to show, and the `device_code` and `nonce` to poll with |
| POST | `/login/sso/poll` | `device_code`, `nonce`; the session once the person approves, else the seconds to add to the polling interval |
| POST | `/login/refresh` | `token`: a refresh token; a new access token |
| POST | `/login/revoke` | `token`: revokes a session token, and a refresh token's access tokens; 204 |

Run ids select that stored execution; they never fall back to another active or recent run.
JSON uses the same schema-versioned documents as the [CLI and Python operations](../guides/service-operations.md).
A successful HTTP response means the operation returned; inspect its `status` and run/task state
for the execution outcome.

Missing, expired or revoked credentials return 401; insufficient roles return 403. An unknown
or malformed request field returns 400 naming the field, as do other usage errors; missing metadata or identities 404,
other domain refusals 409, and unavailable Engine DB requests 503. Domain errors include the
error class, message and CLI exit code.

The interactive contract is at `/api/v1/docs`; `/api/v1/openapi.json` supplies the machine-readable
contract. These two documentation endpoints are public and contain no deployment data or
credentials. The documentation build also publishes the [generated OpenAPI contract](../reference/openapi.json).

A task watermark reset posts `{"pipeline_code":"SALES","task_code":"load","run_id":42,
"watermark":"0","reason":"reload corrected source"}` to `/marks`. It has the same type
validation and live-attempt refusal as the CLI. Task and attempt documents include nullable
`watermark_from` and `watermark_to` fields.

## Catalog comparisons

`GET /api/v1/projects/default/catalog/gaps` returns `etl-craft/catalog-gaps/1`, with `generated_at`, nullable
`compared_at`, warehouse profile/catalog, the config/SQL `contract_version`, and `records`.
Each record names its table, optional column, attribute, state, declared/deployed values, reason,
affected tasks and whether the column is engine managed. `?compare=true` explicitly requests a
fresh warehouse metadata check. All roles can read; authentication is required.

The service reads no business rows and changes no config, cache or lineage history. Without a
comparison, records are **Unchecked**; an inaccessible warehouse produces **Unavailable** records
with reasons, rather than a healthy empty report. A 200 response delivers the report; inspect its
states for check failures. The generated catalog's `gaps.json` uses this same document.

```bash
curl -H "Authorization: Bearer $ETL_CRAFT_API_TOKEN" \
  'http://127.0.0.1:8730/api/v1/projects/default/catalog/gaps?compare=true'
```

## Shared role enforcement

A route authenticates the bearer token, then calls `invoke`, which applies the role the action
table gives the action; routes hold no role logic of their own. Insufficient roles return HTTP
403 with `PermissionDeniedError`, exit code 26 and the account and role remedy, and append one
`REFUSED` action record. Missing or invalid bearer credentials return HTTP 401. See the
[deployment role table](security.md#deployment-accounts).

## Protected SCD values

`POST /api/v1/projects/default/values` writes a reasoned value edit or insert (admin); its body names
`pipeline_code` and `task_code`. `/values/unlock` releases selected locks (human admin), and
`/values/provenance` reads known column writers and protection history (viewer). Each calls the shared Python operation.
See [Protecting human SCD edits](../guides/human-overrides.md) for request bodies, integration
identities, atomic warehouse support and recovery.

## Starting runs from another scheduler

Provision a SERVICE account with the operator role and keep its token in `ETL_CRAFT_TOKEN`.
Call `etl-craft run --pipeline-code SALES --api https://etl.example.com`, or
`POST /api/v1/projects/default/runs` with `{"pipeline_code":"SALES"}` and bearer authentication. The controller owns dispatch,
leases and finalization. Use the documented command exit codes and explicit run identities to
inspect progress; no scheduler-specific package or exported DAG is needed.

## Tag filters

The existing GET actions `/api/v1/projects/default/pipelines`, `/api/v1/projects/default/status`, `/api/v1/projects/default/history`,
`/api/v1/projects/default/audit`, `/api/v1/projects/default/validation` and `/api/v1/projects/default/catalog/gaps` accept repeated `tag` query
parameters, such as `?tag=team:sales&tag=tier:gold`. All tags must match exactly; an unknown tag
returns an empty selection. The tag values and request fields are validated at the shared
operation boundary. `history` provides the cross-pipeline run list with its existing `limit`
and `before` cursor. `status` without `pipeline_code` lists all non-terminal matching runs.
Direct run-id and task reads keep their existing selectors. See [tagged group controls](../guides/run-control.md#pause-or-resume-a-tagged-group)
for resolving a group before submitting explicit bulk controls.

Pipeline-trigger run details use the existing run and task read actions. Run documents add
`parent_run_id`, `triggered_by_task_run_id` and `child_run_ids`; task-run documents add
`child_run_ids`. A waiting trigger's explanation identifies its child and controller ownership.
The generated `POST /api/v1/projects/default/triggers` action is used by an owned trigger attempt. It requires
`task_run_id`, `attempt_id` and `owner`, checks the initiating account and ownership lease,
and returns `etl-craft/trigger/1`. Configure tasks and start their parent for ordinary usage;
there is no additional CLI execution command.

Project operations use `/api/v1/projects/{project}/...`. The examples above select
`default`; substitute your `PROJECT_CODE` for another project. Authentication, accounts,
groups, tokens, agent coordination and health remain deployment-wide.


## Attempt log bytes

The existing `GET /api/v1/projects/{project}/attempts/{attempt_id}/log?offset=0` operation requires
viewer access to the owning pipeline and the exact project/attempt identity. Offsets are whole numbers from 0 through 2^63−1 and count
UTF-8 bytes of acknowledged log content, not characters or lines. File and object downloads
use blocks of at most 65536 bytes; the live relay fetches at most sixteen ordered chunks per
batch, each at most 65536 bytes. A request at or past the available end returns no additional
bytes. Advance the offset by the number of bytes received before polling again.

While `ATTEMPT.log_path` is null, publication is pending and available chunks remain readable.
With no acknowledged bytes, the operation reports `log pending upload` rather than an empty
completed log. A non-null `log_path` acknowledges the complete stored log. A download already
reading chunks keeps that source until it ends; the next request uses the stored file at its
same byte offset. Chunks and stored files contain identical bytes, so this switch loses and
duplicates none. S3 reads use ranges, and downloads close their source when cancelled.

Retention returns `removed by retention on <timestamp>` even when physical cleanup is pending.
An acknowledged file that cannot be opened reports its location, expected access and remedy;
it never falls back to chunks or returns an empty log. Legacy terminal attempts without a
file or chunks report that absence and direct the operator to preserved `TASK_LOG` text.
The existing attempt document supplies task, task-run, run, attempt, owner and exit identity.
Run-log synthesis and whole-run zip manifests remain the S7.A.3 work described by the wireframes.

## Browser UI reads and account changes

The server-rendered `/ui` pages call the same action table. Viewer reads:
`GET /api/v1/project-health` (optional project, after and bounded limit),
`GET /api/v1/about`, and `GET /api/v1/projects/{project}/catalog/page?path=index.html`
(streamed, whole-project viewers only). Health returns rolling UTC window boundaries and
grant-filtered running, failed, SLA-miss and queued-task counts; failed/SLA are last 24 h.

`POST /api/v1/account/settings` changes only the caller's supplied preferences.
`POST /api/v1/account/password` takes current_password and password, then revokes sessions.
`POST /api/v1/accounts/unlock` takes name and requires admin. Cookie-authorized changes,
including sign-in and sign-out, require a CSRF challenge; bearer callers retain their JSON
protocol. Browser sign-in returns only secure cookies and a same-site redirect, never token JSON.
