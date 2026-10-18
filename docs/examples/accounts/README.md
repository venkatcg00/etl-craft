# Accounts on a server

With the project's Engine DB writer credentials available locally, run `etl-craft setup` in
an interactive terminal. Choose the first admin's name and password (12–1024 characters),
then run `etl-craft login`. For an SSO-only admin, use `setup --oidc-subject SUBJECT`.

```bash
etl-craft account add --name analyst --kind USER --role viewer
etl-craft account add --name airflow --kind SERVICE --role operator
etl-craft token create --name airflow --role operator --expires 90d
etl-craft account set-role --name analyst --role operator
etl-craft account reset-password --name analyst
etl-craft account disable --name analyst
etl-craft whoami
etl-craft logout
```

Password entry is interactive and never a command argument. Put the printed service token
in the Airflow connection password named by `Auth.Airflow_connection_id`, or provide it to
CI as `ETL_CRAFT_TOKEN`. Never commit its value. Token creation requires a SERVICE account;
users sign in for expiring sessions instead. Disabling retains the account and revokes all
credentials. Password reset revokes all existing sessions.

The same role policy holds for CLI, HTTP/UI and Python operations. The `airflow` operator can
run, pause and resume pipelines, but cannot apply configuration or administer tokens. Those
require developer and admin respectively. For example, with Airflow's operator token:

```bash
etl-craft list                                      # viewer read: allowed
etl-craft pause --pipeline_code SALES --reason hold  # operator: allowed
etl-craft account list                              # refused, exit 26; AUD_ACTIONS: REFUSED
```

Only an administrator grants roles. See the [full role table](../../deploying/security.md#deployment-accounts)
for maintenance and documentation permissions. Local commands and HTTP routes call the shared
services policy; no HTTP server is needed for local commands.

ETL rows use the run writer `etl-craft` in `CREATED_BY` and `UPDATED_BY`, including runs
started by a person. Action audit rows retain the signed-in account with kind `USER` or
`SERVICE`; automatic engine actions use `SYSTEM`. Starting `etl-craft server` requires an
admin service token in `ETL_CRAFT_TOKEN`.

## From your own installation

An analyst operates the server from their own installation, without its Engine DB or warehouse
credentials. `login --server` stores a one-hour access token and a refresh token for that
server; the CLI renews access until the refresh token expires (`Auth.Refresh_days`) or is
revoked:

```bash
etl-craft login --server https://etl.example.com
etl-craft whoami --server https://etl.example.com          # analyst operator
etl-craft run --pipeline_code SALES --reason "late file" --server https://etl.example.com
etl-craft --server https://etl.example.com config apply    # refused: apply config files on the server
etl-craft logout --server https://etl.example.com
```

See [operating a server](../../deploying/security.md#operating-a-server-from-your-own-installation)
for what works against a server and what stays on its host.

## Human corrections

An authenticated USER admin can use the [SCD value service/API](../../guides/human-overrides.md)
to correct business values, inspect their writers and explicitly unlock them. An admin SERVICE
integration may write through the same endpoint but cannot create or release human locks.
Operator/developer accounts cannot edit or unlock business values. Starting an ETL pipeline as
an admin still uses the engine writer and creates no human locks.
