---
name: test-security
description: Test etl-craft's security boundaries - the role and pipeline-grant matrix from the action table, tokens and refresh, OIDC sign-in and group claims, SQL generation and identifiers, file paths in logs, docs and uploads, secret masking, and the HTTP surface - with attacker-shaped inputs. Use with the built-in security-review when a change touches api/, services/operations, tokens, accounts, groups, grants, OIDC, SQL generation, file paths or secrets.
---

# Test security

The built-in `security-review` reads the diff. This skill writes the tests that keep each
boundary closed. Every check sends what an attacker would send and asserts nothing happened.

## How every check is written

- **Assert the refusal and the absence of effect.** The status code or `PermissionDeniedError`,
  the message (which names the role or object needed, never a secret), the refusal audit row,
  and that the operation never ran: no row, no file, no process.
- **Generate from the action table.** `ACTIONS` in `services/operations/actions.py` is the list
  of every action. A matrix over it covers new actions automatically; a hand-picked list does
  not. `tests/unit/test_action_routes.py` shows the pattern.
- **Real protocol where it matters.** OIDC uses the local TLS issuer (`tests/fixtures/oidc.py`,
  `tests/fixtures/tls.py`); HTTP goes through the generated app (`fastapi.testclient`) or the real
  server, never by calling the operation directly.
- **Prove the test bites:** remove the check (a role comparison, a path containment test) and see
  the test fail.

## Checklist

### Roles and grants

Code: `services/operations/actions.py` (`admitted`), `permissions.py`, `visibility.py`;
tests: `test_permissions.py`, `test_pipeline_permissions.py`, `test_action_routes.py`.

1. Every action at every role below its own: refused, never run, refusal recorded.
2. Every pipeline-scoped action on a restricted pipeline without a grant: refused; a list leaves
   the pipeline out; counts and pagination do not reveal it.
3. Indirect disclosure: run, attempt, log, lineage, dependency and catalog views of an allowed
   pipeline never show a restricted neighbour's runs, tasks or log text.
4. A request by exact id (run, attempt, control) for a restricted pipeline is refused, the same
   as by code.
5. A `RUN_PIPELINE` child is started only when the starter may operate it, with the retained SSO
   groups checked again at each child start.
6. A non-admin `config apply` that touches `grants.csv`, accounts or tokens is refused as a
   whole.

### Tokens and sessions

Code: `services/operations/tokens.py`, `sessions.py`, `api/login.py`; tests:
`test_account_login.py`, `test_login_boundary.py`.

1. Expired, revoked, disabled-account and unknown tokens are refused with the same response
   shape, so a caller cannot tell which.
2. Refresh tokens are not rotated: one issues access tokens while it is live. Signing it out
   revokes every access token it issued; an access token never outlives its refresh token.
3. An access token cannot be used as a refresh token, and the reverse.
4. Tokens are stored only as SHA-256 digests: no token value in any table, log or response
   after issue.

### OIDC

Code: `services/oidc.py`, `config/loader_auth.py`; fixtures: `tests/fixtures/oidc.py`.

1. Refused: `alg: none`, HS256, a wrong issuer, a wrong audience, an expired or not-yet-valid
   token, a missing `sub`, a token from a non-HTTPS endpoint.
2. A subject with no matching `OIDC_SUBJECT` account gets no access.
3. Group claims: names matching no active group are ignored and listed in the audit; a group
   disabled after sign-in no longer grants access; a renamed or reused group name never grants
   the old group's access (IDs, not names).

### SQL and identifiers

Code: `handlers/sql/spec.py`, `dialects/query_analysis.py`, `dialects/`.

1. A SQL file with a second statement, a write, a `;` inside a comment or string, or a CTE that
   writes: refused before anything runs (`SqlGuardError`).
2. Table, column and schema names from config files and task parameters that need quoting, or
   contain quotes, semicolons or comment markers: refused (`test_a_column_name_that_needs_quoting_is_refused`
   is the pattern), never interpolated.
3. `$$` parameter values are bound, never spliced into SQL text.

### File paths

Code: `services/operations/inspect.py` (log streaming), `services/docs_publish.py`,
`execution/file_events.py`, uploads (S7.M).

1. `..`, absolute paths, symlinks pointing outside, and encoded separators in any user-supplied
   path or recorded `LOG_PATH`: refused, naming the attempt or file, never read.
2. Downloads stream only files under the configured directory, after resolving symlinks.

### Secrets

Code: `engine/audit.py`, `core/log.py`, `core/text.py` (`mask_arguments`, `is_secret_name`).

1. Masking is by argument name: test secrets nested inside lists, inside free-text values
   (connection URLs with passwords, error messages from drivers) and in log lines; each must be
   masked or never recorded.
2. `craft-connector.yml` holds variable names only: a literal password value is refused.
3. No secret in `--json` output, API responses, the docs site or the audit tables.

### HTTP surface

1. Unknown fields, oversized bodies and malformed values: 400 naming the field, no operation run.
2. No response carries an `Access-Control-Allow-Origin` that admits a foreign origin (the API
   sets no CORS headers today; a change that adds them needs a test); no secret in a header;
   the UI's state-changing requests are protected against CSRF (S7.A).
3. Every route except sign-in and `/health` requires a bearer token.

## Running

```bash
make services-up
uv run pytest -q -p no:cacheprovider tests/unit/test_action_routes.py \
  tests/integration/engine/test_permissions.py tests/integration/engine/test_pipeline_permissions.py \
  tests/integration/engine/test_account_login.py tests/integration/api/
```

Then run the built-in `security-review` on the diff.

## Report

Per row: the test, the attacker input it sends, and the check it removed to prove it bites.
Rows with no test are gaps. Never paste a working secret or token into the report.
