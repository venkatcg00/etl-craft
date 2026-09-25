# Checking a deployment: `doctor` and `setup`

## `etl-craft doctor`

`doctor` checks a configuration end to end and reports every problem it finds, not only the
first. Each line is one check:

```text
[OK  ] Configuration: /srv/etl-craft/craft-connector.yml (mode remote, project /srv/etl-craft)
[OK  ] Settings: 6 from variables, 3 as written
[WARN] Setting used as written: Engine.prod.user is 'ETL_USER': no variable of that name is set in the environment source, so the text itself is the value
[OK  ] Engine DB secret: resolved from ENGINE_PROD_SECRET
[OK  ] Engine DB connection: PostgreSQL, schema etl_craft
[FAIL] Engine DB migrations: 1 pending: project/0003_owner.sql; run `etl-craft migrate`
[OK  ] Warehouse connection: Trino, schema analytics exists
[OK  ] Warehouse catalog: an Iceberg catalog
[OK  ] Email relay: smtp.example.com:587 answers
[OK  ] Project folders: sql_files, migrations in /srv/etl-craft
8 ok, 1 warning(s), 1 failed
```

It exits 1 when any check fails, so it can gate a deployment. It checks:

| Check | Fails when | Warns when |
|---|---|---|
| Settings | | a value used as written reads like a variable name that is not set |
| Secrets | a profile's secret variable is not set | |
| Auth | | an `auth_mode` has not been verified against a live service |
| Engine DB connection | the database cannot be reached, or its `schema` does not exist | a SQLite Engine DB in remote mode |
| Engine DB tables and migrations | there are no Engine DB tables, a migration is pending, or an applied migration file has changed | |
| Warehouse | the database cannot be reached, its `schema` does not exist, it is an in-memory DuckDB, or a Trino catalog is not Iceberg | |
| Email | the relay does not answer, or the `sendmail` program cannot be run | |

The relay check connects and sends `NOOP` without logging in, so it never spends a login attempt
against a relay that locks accounts out.

## `etl-craft setup`

`setup` is the one command for a new deployment and after every upgrade. It runs the same checks,
apart from the Engine DB's tables and migrations, and then:

- on a database with no Engine DB tables, creates them (as `init-db` does);
- applies every pending migration, packaged and your own (as `migrate` does).

If any check fails, `setup` changes nothing and exits 1: every connection a run needs must work
before it builds anything. On an Engine DB that is up to date it changes nothing and says so, so
it is safe to run on every deployment.

`setup` creates only the Engine DB's own tables. The Engine DB's schema, the warehouse database and
schema, and the email relay must already exist; the failed check names what is missing. A SQLite
Engine DB is the exception: etl-craft creates its file, and the folder holding it.

`init-db` and `migrate` remain for doing either step alone; see
[Engine DB setup and upgrades](engine-db.md).
