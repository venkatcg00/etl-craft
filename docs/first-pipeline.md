# Register and run your first pipeline

Creating a pipeline is deliberately **not** a CLI verb. Pipelines are git-managed SQL,
reviewed like any other change: a new external connection, a new action type, or a
destructive schema change should be harder to make than editing a row.

This walkthrough builds a two-task pipeline that creates a table and then merges into it.
It assumes you have completed the [README](../README.md) quickstart: `etl-craft setup` has
run and `etl-craft doctor` passes.

## 1. A warehouse to write to

The warehouse is where your actual tables live. For this walkthrough, point `[Warehouse]` at
a second database on the same Postgres and create a source table:

```sql
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE staging.customers_raw (id INT, name TEXT, updated_at TIMESTAMPTZ);
INSERT INTO staging.customers_raw VALUES
  (1, 'Ada',  now()),
  (2, 'Grace', now());
```

## 2. Register the pipeline and its tasks

Run this against your **Engine DB**:

```sql
INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE)
VALUES ('CUSTOMERS', 'Customer dimension', 'FULL');

-- Task 1: build the target.
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER)
SELECT 'build_customers', 'ETL', PIPELINE_ID, 'SQL'
FROM CFG_PIPELINES WHERE PIPELINE_CODE = 'CUSTOMERS';

-- Task 2: keep it current.
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER)
SELECT 'merge_customers', 'ETL', PIPELINE_ID, 'SQL'
FROM CFG_PIPELINES WHERE PIPELINE_CODE = 'CUSTOMERS';
```

## 3. Give each task its parameters

```sql
-- build_customers: CREATE_TABLE
INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE)
SELECT t.TASK_ID, p.name, p.value
FROM CFG_TASKS t, (VALUES
    ('SQL_ACTION',    'CREATE_TABLE'),
    ('TARGET_OBJECT', 'public.customers'),
    ('SOURCE_OBJECT', 'staging.customers_raw'),
    ('PRIMARY_KEY',   'id'),
    ('SOURCE_SQL',    'SELECT id, name FROM staging.customers_raw WHERE $$pipeline_id')
) AS p(name, value)
WHERE t.TASK_CODE = 'build_customers';

-- merge_customers: SCD1_MERGE
INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE)
SELECT t.TASK_ID, p.name, p.value
FROM CFG_TASKS t, (VALUES
    ('SQL_ACTION',            'SCD1_MERGE'),
    ('TARGET_OBJECT',         'public.customers'),
    ('SOURCE_OBJECT',         'staging.customers_raw'),
    ('MERGE_KEY',             'id'),
    ('MERGE_COMPARE_COLUMNS', 'name'),
    ('MERGE_DEDUPE_ORDER',    'updated_at DESC'),
    ('SOURCE_SQL',            'SELECT id, name FROM staging.customers_raw WHERE $$pipeline_id')
) AS p(name, value)
WHERE t.TASK_CODE = 'merge_customers';
```

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
etl-craft validate
etl-craft graph --name CUSTOMERS
etl-craft steps --pipeline_code CUSTOMERS
```

`validate` catches what no database constraint can: dependency cycles, missing lineage
declarations, a `TARGET_TABLE` without the single-column primary key business rules rely on.

## 6. Run it

```bash
etl-craft run --pipeline_code CUSTOMERS
etl-craft history --pipeline_code CUSTOMERS
```

Run it a second time. `build_customers` and `merge_customers` both re-derive their effect
from current state, so the result is identical — that is the idempotence guarantee, not a
coincidence.

## 7. Hand it to Airflow, if you want to

```bash
etl-craft generate-yml --pipeline_code CUSTOMERS
```

The emitted description gives each task its `bash_command` and one Airflow `trigger_rule`,
derived from the dependency rows you wrote above. Feed it to your own loader, or ignore it
entirely and keep using `etl-craft run`.

## Documenting it

Add a `DOCUMENTATION` parameter to any task and it becomes prose on the generated
documentation site, searchable alongside everything else:

```sql
INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE)
SELECT TASK_ID, 'DOCUMENTATION',
       'Builds the customer dimension from the raw layer. Full refresh: the whole '
       'table is rebuilt each run.'
FROM CFG_TASKS WHERE TASK_CODE = 'build_customers';
```

```bash
etl-craft docs-version     # records v1
etl-craft generate-docs
```

The version comes from the text itself. Re-run `docs-version` after editing the wording
and it becomes v2; run it twice with no change and nothing moves. That is deliberate — a
version somebody has to remember to bump is a version that quietly lies. See the full
history for one task with
`etl-craft docs-version --pipeline_code CUSTOMERS --task_code build_customers`.

## Tracing a column

```bash
etl-craft lineage --column public.customers.name
```

```
public.customers.name is produced by:
  CUSTOMERS.build_customers  <- staging.customers_raw.name
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
