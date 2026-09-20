# Configuring etl-craft

All connections resolve through `craft-connector.yml`. Nothing is read from an
orchestrator's own connection store — that is what keeps the engine orchestrator-agnostic.

See [craft-connector.example.yml](craft-connector.example.yml) for a fully commented file.

## Finding the file

In order:

1. `--config PATH`, accepted by every command.
2. `$ETL_CRAFT_CONFIG`.
3. The nearest `craft-connector.yml` searching upward from the current directory.

The upward search means running from a subdirectory of a configured project works the way
other developer tools behave. It matters in deployment too: an Airflow `BashOperator`'s
working directory is not something a DAG author controls reliably.

## Secrets are never in this file

Each profile names an `auth_mode` and the engine looks the secret up by name:

```
ETL_CRAFT_{SECTION}_{PROFILE}_SECRET
```

So the `dev` profile under `Postgres` reads `ETL_CRAFT_POSTGRES_DEV_SECRET`. Override it
per profile with `secret_var: MY_NAME`. Where those values are *read from* is `[Source]`:
the process environment, or a `.env`-style file.

`etl-craft setup` prints the exact names your configuration will expect.
`etl-craft doctor` then resolves each one and opens each connection, reporting every check
rather than stopping at the first failure:

```
$ etl-craft doctor
[OK  ] Execution mode: local
[OK  ] Secret source: environment
[FAIL] Engine DB secret: secret 'ETL_CRAFT_POSTGRES_DEV_SECRET' not found (...)
[OK  ] Data DB: no [Warehouse] section configured
...
```

## Execution mode

`Mode` is set once per environment with `etl-craft set-execution-mode`, and persists until
the environment is rebuilt. It is deliberately **not** a per-invocation flag — nothing in a
generated DAG passes `--mode`.

- `local` — `run --pipeline_code X` with no `--task_code` makes the engine its own wave
  scheduler, spawning one subprocess per ready task.
- `orchestrator` — that form is refused outright, because Airflow's own scheduling would
  race it. Airflow drives each task instead, via `run --task_code`, with synthetic
  `--init-only` and `--finalize-only` steps at either end.

`--force` bypasses dependency and state checks. It is only legal under `Mode: local`.

## Operational limits

`[Execution]` carries two limits, both with real defaults rather than being unset:

| Setting | Default | What it bounds |
|---|---|---|
| `Task_timeout_seconds` | `21600` (6 hours) | A single task. Overridden per task by the `TASK_TIMEOUT_SECONDS` parameter; `0` disables it. |
| `Max_parallel_tasks` | `8` | How many task subprocesses one wave spawns at once, and how many business rules in one `SEQUENCE_NUMBER` run concurrently. |

The timeout matters more than it looks. A task with no limit that hangs leaves its
`AUD_TASK_RUN_LOG` row stuck `IN-PROGRESS` — and an `IN-PROGRESS` task is never offered for
retry, so the pipeline can never recover without someone editing the table by hand.

## The two databases

**Engine DB** — always Postgres, no exceptions. Holds every `CFG_`/`AUD_` table. Postgres
specifically because a partial unique index is what makes concurrent run-id creation
race-safe; an application-level check cannot close that race.

**Data DB (`[Warehouse]`)** — exactly one per deployment, any SQLAlchemy-supported engine.
Optional: only `SQL` and `BUSINESS_RULES` tasks need it. Third-party dialects are optional
extras you install yourself (`uv add "etl-craft[clickhouse]"`); the engine never imports
one directly.

`TARGET_OBJECT` is stored as bare `schema.table`, deliberately environment-agnostic — the
database name always comes from the active `[Warehouse]` profile at runtime. The same
`CFG_` row therefore means a different real object in dev, uat and prod without any row
changing across a promotion.

## One command: `setup`

```bash
etl-craft setup                    # settings from ./.env
etl-craft setup --env prod.env     # ...or a named file
etl-craft setup --from-environment # ...or whatever is already exported
```

`setup` is the dbt shape: you keep your settings in a file (or the environment), and one
command reconciles reality with them. It writes or updates `craft-connector.yml`, then
brings the Engine DB to current — applying the packaged schema if the database is empty,
or pending migrations if it is not — and names the secrets the result expects.

It is idempotent by design. Run it on a fresh machine and it sets everything up; run it
again after any change and it updates. There is no separate first-run path to get wrong
and no prompts, so it behaves identically on a laptop and in CI.

If the Engine DB is not reachable yet, that is *reported* rather than raised: writing the
config is useful on its own, and is often exactly the step that fixes the connection.

## Creating and upgrading the schema

```bash
etl-craft init-db     # fresh, empty database — applies the full packaged schema
etl-craft migrate     # existing database — applies pending sql/migrations/*.sql
```

`init-db` refuses a database that already has engine tables: the schema is plain
`CREATE TABLE` and deliberately not idempotent, so re-running it would fail half-applied.
Use `migrate` to carry an existing database forward.

`migrate` finds its directory from `--migrations-dir`, then `$ETL_CRAFT_MIGRATIONS_DIR`,
then `./sql/migrations`, then the copy packaged with etl-craft. It takes an advisory lock,
so two concurrent runs cannot double-apply.
