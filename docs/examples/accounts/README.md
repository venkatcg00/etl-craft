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
