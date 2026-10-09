# HTTP API

Install `etl-craft[server]`, initialize or migrate the Engine DB, and run `etl-craft server`.
The API and local overseer share the same configuration and shutdown lifecycle. Without the
server extra, `etl-craft server` supervises runs without opening an HTTP listener.

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
The token's name is the actor in action and lifecycle records. Each token is deployment-wide;
project-scoped tokens are not accepted until project authorization exists.

Every resource endpoint, including health, requires `Authorization: Bearer <token>`.
`viewer` can read, `operator` can also trigger, cancel, pause, resume, mark, rerun and backfill,
and `admin` can also create, list and revoke tokens through `/api/v1/tokens`.
Token values and hashes are absent from token listings and audit arguments. The token table
is excluded from metadata snapshots and warehouse cloning.

## Calling operations

```bash
curl -H "Authorization: Bearer $ETL_CRAFT_API_TOKEN" \
  http://127.0.0.1:8730/api/v1/pipelines
curl -X POST -H "Authorization: Bearer $ETL_CRAFT_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"run_date":"2026-10-01","reason":"manual refresh"}' \
  http://127.0.0.1:8730/api/v1/pipelines/SALES/runs
```

A trigger initializes or resumes the pipeline run and returns its exact identity; the overseer
executes its tasks. Backfills and task reruns call the same blocking service operations as the CLI.
A disconnected client does not cancel committed work; inspect its exact run before retrying.

| Method | Path under `/api/v1` | Input or result |
|---|---|---|
| GET | `/health` | Engine DB authentication and health |
| GET | `/pipelines`, `/pipelines/{code}` | Active definitions and pause state |
| GET | `/pipelines/{code}/runs?limit=20&before=123` | Up to 100 runs; exclusive run-id cursor |
| POST | `/pipelines/{code}/runs` | `run_date`, `reason`, both optional |
| POST | `/pipelines/{code}/backfills` | Inclusive `first`, `last` dates and `reason` |
| POST | `/pipelines/{code}/pause`, `/resume` | Nonblank `reason` |
| GET | `/runs/{id}`, `/runs/{id}/tasks` | Exact run and configured task states |
| GET | `/runs/{id}/tasks/{task}/explain` | Task code; shared explanation document |
| POST | `/runs/{id}/cancel` | Nonblank `reason` |
| POST | `/runs/{id}/tasks/{task}/mark` | `status`, `reason`; optional `rows`, `stale` |
| POST | `/runs/{id}/tasks/{task}/rerun` | `reason`; optional `with_downstream` |
| GET | `/attempts/{id}` | Stored attempt document |
| GET | `/attempts/{id}/log?offset=0` | Log bytes from a nonnegative byte offset |
| GET, POST | `/tokens` | Admin listing or creation: `name`, `role`, optional `expires` |
| POST | `/tokens/{id}/revoke` | Admin revocation |

Run ids select that stored execution; they never fall back to another active or recent run.
JSON uses the same schema-versioned documents as the [CLI and Python operations](../guides/service-operations.md).
A successful HTTP response means the operation returned; inspect its `status` and run/task state
for the execution outcome.

Missing, expired or revoked credentials return 401; insufficient roles return 403. Invalid
request bodies return 422, service usage errors 400, missing metadata or identities 404,
other domain refusals 409, and unavailable Engine DB requests 503. Domain errors include the
error class, message and CLI exit code.

The interactive contract is at `/api/v1/docs`; `/api/v1/openapi.json` supplies the machine-readable
contract. These two documentation endpoints are public and contain no deployment data or
credentials. The documentation build also publishes the [generated OpenAPI contract](../reference/openapi.json).
