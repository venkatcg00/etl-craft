# HTTP API

Install `etl-craft[server]`, initialize or migrate the Engine DB, and run `etl-craft controller`.
The API and local controller share the same configuration and shutdown lifecycle. Without the
server extra, `etl-craft controller` supervises runs without opening an HTTP listener.

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

Every resource endpoint, including health, requires `Authorization: Bearer <token>`.
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
  'http://127.0.0.1:8730/api/v1/status?pipeline_code=SALES'
curl -X POST -H "Authorization: Bearer $ETL_CRAFT_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"pipeline_code":"SALES","run_date":"2026-10-01","reason":"manual refresh"}' \
  http://127.0.0.1:8730/api/v1/runs
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
operations as a local `etl-craft login`, and `etl-craft login --controller` uses them.

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

`GET /api/v1/catalog/gaps` returns `etl-craft/catalog-gaps/1`, with `generated_at`, nullable
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
  'http://127.0.0.1:8730/api/v1/catalog/gaps?compare=true'
```

## Shared role enforcement

A route authenticates the bearer token, then calls `invoke`, which applies the role the action
table gives the action; routes hold no role logic of their own. Insufficient roles return HTTP
403 with `PermissionDeniedError`, exit code 26 and the account and role remedy, and append one
`REFUSED` action record. Missing or invalid bearer credentials return HTTP 401. See the
[deployment role table](security.md#deployment-accounts).

## Protected SCD values

`POST /api/v1/values` writes a reasoned value edit or insert (admin); its body names
`pipeline_code` and `task_code`. `/values/unlock` releases selected locks (human admin), and
`/values/provenance` reads known column writers and protection history (viewer). Each calls the shared Python operation.
See [Protecting human SCD edits](../guides/human-overrides.md) for request bodies, integration
identities, atomic warehouse support and recovery.

## Starting runs from another scheduler

Provision a SERVICE account with the operator role and keep its token in `ETL_CRAFT_TOKEN`.
Call `etl-craft run --pipeline-code SALES --controller https://etl.example.com`, or
`POST /api/v1/runs` with `{"pipeline_code":"SALES"}` and bearer authentication. The controller owns dispatch,
leases and finalization. Use the documented command exit codes and explicit run identities to
inspect progress; no scheduler-specific package or exported DAG is needed.
