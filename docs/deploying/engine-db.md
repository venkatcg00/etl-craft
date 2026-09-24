# Setting up and upgrading the Engine DB

## A new Engine DB

Point the `Engine` section of `craft-connector.yml` at an empty database (see
[Engine DB](../connectors/engine-db.md)), then run:

```bash
etl-craft init-db
```

`init-db` creates every table in one transaction, so a failure leaves nothing behind. It refuses a
database that already has Engine DB tables and names them: use `migrate` for a database that
already holds an Engine DB. `--force` applies the schema anyway, for a database you know is empty
apart from a leftover table.

## Upgrading

After installing a new etl-craft version, and whenever your own migrations change, run:

```bash
etl-craft migrate
```

It applies two streams of `*.sql` files, each in filename order:

1. **ENGINE**: the migrations packaged with etl-craft, always first. A new Engine DB created by
   `init-db` already includes them.
2. **PROJECT**: your own migrations, from `--migrations-dir`, else `$ETL_CRAFT_MIGRATIONS_DIR`,
   else `./sql/migrations` when that directory exists.

`SCHEMA_MIGRATIONS` records every applied file with the SHA-256 of its content. Each file runs in
its own transaction together with that record, so a failing file changes nothing and stops the
run before any later file. Running `migrate` again applies only what is new.

Before applying anything, `migrate` checks every file it has applied before: a file that was
removed or edited stops the run. Released migrations are never edited; add a new file instead.
Keep your project migrations directory for every later run, since `migrate` needs it for that check.

Name project files `NNNN_short_description.sql` so they sort in the order they must apply. Each
statement runs exactly as written: colons, percent signs and semicolons inside string literals
are safe.

Two `migrate` runs against the same Engine DB never overlap: the second waits for the first, then
finds nothing left to apply.
