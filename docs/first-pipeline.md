# Register and run your first pipeline

Creating a pipeline is deliberately **not** a CLI verb. Pipelines are git-managed SQL,
reviewed like any other change: a new external connection, a new action type, or a
destructive schema change should be harder to make than editing a row.

This walkthrough builds a two-task pipeline that refreshes a staging copy and then merges it
into a dimension table.
It assumes you have completed the [README](../README.md) quickstart: `uv run etl-craft setup` has
run and `uv run etl-craft doctor` passes.

## 1. A warehouse to write to

The warehouse is where your actual tables live. The README quickstart points the `Warehouse`
section at a DuckDB file, `./warehouse.duckdb`. Create a source table in it (the same SQL works
on a Postgres warehouse, through any client):

```sql
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;   -- where the pipeline's target will live
CREATE TABLE staging.customers_raw (id INT, name TEXT, updated_at TIMESTAMPTZ);
INSERT INTO staging.customers_raw VALUES
  (1, 'Ada',  now()),
  (2, 'Grace', now());
```

Without a DuckDB client installed, save that as `warehouse.sql` and run:

```bash
uv run python -c "import duckdb, sys; duckdb.connect('warehouse.duckdb').execute(sys.stdin.read())" < warehouse.sql
```

## 2. Register the pipeline and its tasks

Run this against your **Engine DB**. Every statement below works on both SQLite and
PostgreSQL. With the default SQLite Engine DB, save steps 2–4 as `pipeline.sql` and run it with
the `sqlite3` shell, or without one:

```bash
uv run python -c "import sqlite3, sys; sqlite3.connect('etl-craft-engine.db').executescript(sys.stdin.read())" < pipeline.sql
```

On PostgreSQL, use `psql -f pipeline.sql`.

```sql
INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE)
VALUES ('CUSTOMERS', 'Customer dimension', 'FULL');

-- Task 1: refresh a staging copy of the raw data.
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER)
SELECT 'build_customers', 'ETL', PIPELINE_ID, 'SQL'
FROM CFG_PIPELINES WHERE PIPELINE_CODE = 'CUSTOMERS';

-- Task 2: merge it into the dimension, keeping each row's identity.
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER)
SELECT 'merge_customers', 'ETL', PIPELINE_ID, 'SQL'
FROM CFG_PIPELINES WHERE PIPELINE_CODE = 'CUSTOMERS';
```

## 3. Give each task its parameters

```sql
-- build_customers: OVERWRITE_TABLE
WITH p(name, value) AS (VALUES
    ('SQL_ACTION',    'OVERWRITE_TABLE'),
    ('TARGET_OBJECT', 'staging.customers'),
    ('SOURCE_OBJECT', 'staging.customers_raw'),
    ('SOURCE_SQL',    'SELECT id, name, updated_at FROM staging.customers_raw')
)
INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE)
SELECT t.TASK_ID, p.name, p.value
FROM CFG_TASKS t CROSS JOIN p
WHERE t.TASK_CODE = 'build_customers';

-- merge_customers: SCD1_MERGE
WITH p(name, value) AS (VALUES
    ('SQL_ACTION',            'SCD1_MERGE'),
    ('TARGET_OBJECT',         'marts.customers'),
    ('SOURCE_OBJECT',         'staging.customers'),
    ('MERGE_KEY',             'id'),
    ('MERGE_COMPARE_COLUMNS', 'name'),
    ('MERGE_DEDUPE_ORDER',    'updated_at DESC'),
    ('SOURCE_SQL',            'SELECT id, name, updated_at FROM staging.customers')
)
INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE)
SELECT t.TASK_ID, p.name, p.value
FROM CFG_TASKS t CROSS JOIN p
WHERE t.TASK_CODE = 'merge_customers';
```

Neither target has to exist first: `OVERWRITE_TABLE` and `SCD1_MERGE` each create theirs on
the first run, with the audit columns that action needs. The staging copy is replaced every run;
the dimension is merged into, so a changed `name` updates its row in place and an unchanged one is
left alone.

Note what the `SELECT`s do **not** contain: no `pipeline_run_id`, no `CREATE_DATE`, no
`HASH_KEY`. The engine appends whichever audit columns the declared action needs. A step
cannot touch the warehouse outside its declared action.

## 4. Declare the order

```sql
INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE)
SELECT p.PIPELINE_ID, m.TASK_ID, b.TASK_ID, 'SUCCESS'
FROM CFG_PIPELINES p
JOIN CFG_TASKS b ON b.PIPELINE_ID = p.PIPELINE_ID AND b.TASK_CODE = 'build_customers'
JOIN CFG_TASKS m ON m.PIPELINE_ID = p.PIPELINE_ID AND m.TASK_CODE = 'merge_customers'
WHERE p.PIPELINE_CODE = 'CUSTOMERS';
```

That row *is* the DAG. Nothing enumerates order in code.

## 5. Check it before running it

```bash
uv run etl-craft validate
uv run etl-craft graph --name CUSTOMERS
uv run etl-craft steps --pipeline_code CUSTOMERS
```

`validate` catches dependency cycles, missing lineage declarations, invalid task parameters, and
warehouse constraints that cannot be represented in the Engine DB.

## 6. Run it

```bash
uv run etl-craft run --pipeline_code CUSTOMERS
uv run etl-craft history --pipeline_code CUSTOMERS
```

Run it a second time. `build_customers` and `merge_customers` both re-derive their effect
from current state, so the result is identical — that is the idempotence guarantee, not a
coincidence.

## 7. Generate an Airflow descriptor, if you use Airflow

```bash
uv run etl-craft generate-yml --pipeline_code CUSTOMERS
```

The emitted descriptor gives each task its `bash_command` and one Airflow `trigger_rule`, derived
from the dependency rows you wrote above. It does not install or load a DAG. Feed it to the loader
your Airflow deployment owns, or keep using `etl-craft run` locally.

## Documenting it

Add a `DOCUMENTATION` parameter to any task and it becomes prose on the generated
documentation site, searchable alongside everything else:

```sql
INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE)
SELECT TASK_ID, 'DOCUMENTATION',
       'Builds the customer dimension from the raw layer. Full refresh: the whole '
       || 'table is rebuilt each run.'
FROM CFG_TASKS WHERE TASK_CODE = 'build_customers';
```

```bash
uv run etl-craft docs-version     # records v1
uv run etl-craft generate-docs
```

The version comes from the text itself. Re-run `docs-version` after editing the wording
and it becomes v2; run it twice with no change and nothing moves. That is deliberate — a
version somebody has to remember to bump is a version that quietly lies. See the full
history for one task with
`uv run etl-craft docs-version --pipeline_code CUSTOMERS --task_code build_customers`.

## Tracing a column

```bash
uv run etl-craft lineage --column marts.customers.name
```

```
marts.customers.name is produced by:
  CUSTOMERS.merge_customers  <- staging.customers.name
```

This is parsed from the task's own `SOURCE_SQL`, so it follows aliases, joins and CTEs to
the real table rather than stopping at whatever the query happened to call it. Ask the same
question of a source column and you get the other direction — everything it feeds.

## Conditional dependencies

A task normally waits for **all** its dependencies. Set `CFG_TASKS.RUN_CONDITION` to change
that:

```sql
UPDATE CFG_TASKS SET RUN_CONDITION = 'ANY' WHERE TASK_CODE = 'merge_customers';
-- or: RUN_CONDITION = 'N', RUN_CONDITION_COUNT = 2
```

`ANY` maps to Airflow's `one_success`/`one_failed` in a generated DAG. `N` has no Airflow
equivalent, so the generated DAG lets the task start and the engine enforces the count.
