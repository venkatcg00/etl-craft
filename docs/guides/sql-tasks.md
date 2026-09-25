# SQL tasks

A task with `HANDLER = 'SQL'` supplies a read-only SELECT and names what to do with its rows. The
engine wraps the SELECT in one of eight actions and performs every write itself. The task's
settings are rows in `CFG_TASK_PARAMETERS`.

## The SELECT

Write the SELECT inline, or keep it in a file under the project's `sql_files/` folder:

| Parameter | Value |
|---|---|
| `SOURCE_SQL` | the SELECT itself |
| `SOURCE_SQL_FILE` | a path inside `sql_files/`, such as `sales/orders.sql` |

Name every table in the SELECT at least as `schema.table`; a bare table name has no schema. The
connection is already in the database of the active `Warehouse` profile, which changes per
environment, so `schema.table` names run unchanged in development, test and production. A
`database.schema.table` name is used as written.

Set exactly one of them. The SELECT must be a single read-only statement (`SELECT`, `WITH`,
`TABLE` or `VALUES`); comments and a trailing `;` are fine. A task holds exactly one query: do
lookups and aggregations inside it, with CTEs or subqueries, not as statements of their own. It must not return the columns the
engine manages itself: `PIPELINE_RUN_ID`, `ROW_ID` and the audit columns below.

### The pipeline-id tokens

Every table the engine writes records the run that wrote each row in `PIPELINE_RUN_ID`. Two tokens
let a SELECT use the current run:

| Token | Enabled by | Becomes |
|---|---|---|
| `$$pipeline_id` | `PIPELINE_ID_SUBSTITUTION = true` | the run's `pipeline_run_id`, such as `97` |
| `$$pipeline_id_filter` | `PIPELINE_ID_FILTER = true` | `pipeline_run_id = 97`, or `1=1` when the pipeline's `REFRESH_TYPE` is `FULL` |

A typical incremental load reads only the rows an earlier task wrote in the same run:

```sql
SELECT order_id, amount, $$pipeline_id AS loaded_in_run
FROM staging.orders
WHERE $$pipeline_id_filter
```

A token is replaced only when its parameter is `true`. The task fails before anything runs, naming
the problem, when the SELECT uses a token whose parameter is not `true`, when a parameter is `true`
but its token is missing, or when the SELECT holds any other `$$` token.

## The actions

`SQL_ACTION` names the action and `TARGET_OBJECT` the table: `schema.table`, written in the
active `Warehouse` profile's database, so the same rows work in every environment, or
`database.schema.table`, written exactly there. A bare table name fails the task.

| `SQL_ACTION` | What happens | Also needs |
|---|---|---|
| `CREATE_TABLE` | drops the target and creates it again from the SELECT's rows | — |
| `SETUP_TABLE` | creates the target, empty, from the SELECT's shape plus the audit columns of the action that writes it, when it does not exist; an existing target is left as it is | `SETUP_FOR` when no task in the pipeline writes the target |
| `OVERWRITE_TABLE` | empties the target and inserts the SELECT's rows | an existing target |
| `APPEND_TABLE` | inserts the SELECT's rows, without comparing the shapes | an existing target |
| `SCD1_MERGE` | updates changed rows in place, by merge key, and inserts new keys | an existing target, `MERGE_KEY`, `MERGE_COMPARE_COLUMNS` |
| `SCD2_MERGE` | closes the active version of each changed key (`ACTIVE_FLAG = 'N'`) and inserts a new active one | an existing target, `MERGE_KEY`, `MERGE_COMPARE_COLUMNS` |
| `DROP_TABLE` | drops the target if it exists, once this pipeline's `CREATE_TABLE` task for it has succeeded in the run; a target already gone is not an error | no SELECT |
| `DELETE_ROWS` | flags the target rows whose `MERGE_KEY` the SELECT returns (`DELETE_FLAG = 'Y'`), or deletes them with `HARD_DELETE = true` | an existing target, `MERGE_KEY` |

### Creating tables

Only `CREATE_TABLE` and `SETUP_TABLE` create tables. Every other action needs its target to exist,
and fails with the remedy when it does not. Give each table a `SETUP_TABLE` task that runs before
its writers, or create the table yourself with the columns below.

`SETUP_TABLE` works out the audit columns from the other SQL tasks in the pipeline with the same
`TARGET_OBJECT` that write rows to it. It fails when they write with actions that need different
audit columns, such as an `SCD1_MERGE` and an `APPEND_TABLE` on one table, and when none writes
it, as when another pipeline or a script fills the table: then set `SETUP_FOR` to the writing
action. Its SELECT only gives the shape; `SELECT ... WHERE 1 = 0` is fine.

`MERGE_KEY` and `MERGE_COMPARE_COLUMNS` are column lists separated by `|`, such as
`customer_id|region`. A row counts as changed when the MD5 of its compare columns (`HASH_KEY`)
differs from the target's.

### Optional parameters

| Parameter | Applies to | Effect |
|---|---|---|
| `MERGE_DEDUPE_ORDER` | the merges | when the SELECT returns a merge key more than once, keep the first row in this order, such as `updated_at DESC`; without it, duplicate keys fail the task |
| `SETUP_FOR` | `SETUP_TABLE` | the action whose audit columns the table gets: `CREATE_TABLE`, `OVERWRITE_TABLE`, `APPEND_TABLE`, `SCD1_MERGE` or `SCD2_MERGE` |
| `SCHEMA_EVOLUTION` | `OVERWRITE_TABLE` and the merges | `true` adds a column the SELECT returns but the target lacks, in the SELECT's position |
| `PRESERVE_TARGET` | `SCD1_MERGE` | `true` keeps the target's value where the SELECT returns NULL |
| `HARD_DELETE` | `DELETE_ROWS` | `true` deletes rows instead of flagging them |
| `TABLE_FORMAT` | all | `native` or `iceberg`, for this task's target, where the warehouse lets a task choose |

Yes/no parameters take `true` or `false`; anything else fails the task.

### The columns the engine adds

After the SELECT's own columns, every table the engine creates has:

| Column | Added by |
|---|---|
| `PIPELINE_RUN_ID` | every action |
| `UPDATE_DATE` | `OVERWRITE_TABLE` |
| `CREATE_DATE` | `APPEND_TABLE` |
| `HASH_KEY`, `CREATE_DATE`, `CREATED_BY`, `UPDATE_DATE`, `UPDATED_BY`, `DELETE_FLAG` | `SCD1_MERGE` |
| the `SCD1_MERGE` columns and `ACTIVE_FLAG` | `SCD2_MERGE` |
| `ROW_ID` | every action; a generated key that business rules refer to |

`CREATED_BY` and `UPDATED_BY` hold the warehouse user the engine connects as.

## When the target and the SELECT disagree

Before `OVERWRITE_TABLE` or a merge writes a row, the target is compared with the SELECT, and the
task fails with the reason when:

- the target lacks a column the action maintains, such as `HASH_KEY` for a merge: run a
  `SETUP_TABLE` task for it, or add the column;
- the target has a column the SELECT no longer returns: columns are never dropped, so return it,
  NULL if need be;
- the SELECT returns a new column and `SCHEMA_EVOLUTION` is not `true`.

With `SCHEMA_EVOLUTION = true` the target is rebuilt with the new column, which is NULL in the
rows already there. Indexes and grants on the old table are not carried over.

## Logs and counts

Every statement is logged with what it does, the rows the database reports and how long it took;
`--log-level DEBUG` adds the SQL itself:

```
INFO etl_craft.handlers.sql [task_run_id=412 pipeline=SALES task=load_customers ...]: SCD1_MERGE analytics.sales.customers from SOURCE_SQL_FILE='customers.sql' on PostgreSQL
INFO etl_craft.handlers.sql.session [...]: stage the SELECT (0.84s)
INFO etl_craft.handlers.sql.session [...]: changed rows = 12
INFO etl_craft.handlers.sql.session [...]: update changed rows: 12 row(s) (0.21s)
```

When a statement fails, the task fails with the action, the target and the step, followed by the
database's own message, and the failing SQL is written to the attempt's log:

```
SCD1_MERGE analytics.sales.customers: stage the SELECT failed: UndefinedTable: relation "raw.custmers" does not exist
```

`AUD_TASK_RUN_LOG` records the source, target, insert, update and delete counts. Each count comes
from its own `COUNT(*)` query, not from the driver.

## Running a task again

On PostgreSQL and DuckDB an action's statements commit or roll back together. Trino, Databricks
and Snowflake commit each statement, so an action that fails part-way can leave its work half
done. Every action works out what to do from the target's current state, so running the task
again completes it. `APPEND_TABLE` is the exception: running it again after it succeeded appends
the rows again. Scratch tables the action creates (`etl_stage_<task_run_id>` and similar) are
dropped even when it fails.
