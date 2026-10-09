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
lookups and aggregations inside it, with CTEs or subqueries, not as statements of their own.

The columns the SELECT returns are checked before the target is touched, and the task fails,
naming them, when:

- it returns a column the engine writes itself: `PIPELINE_RUN_ID`, `ROW_ID` or one of the audit
  columns below. A `SELECT *` over a table the engine wrote returns them all; list the columns
  you need instead. `validate` reports this too, when the SELECT lists its columns;
- a column's name would need quoting: anything but letters, digits and `_`, or, on a warehouse
  that folds unquoted names (PostgreSQL and Trino to lower case, Snowflake to upper case), a name
  in another case, such as `"CustomerId"` on PostgreSQL. Alias it to a plain name.

### The pipeline-id tokens

Every table the engine writes records its execution identities. Named tokens let a SELECT use
the current pipeline definition, pipeline execution and task execution:

| Token | Enabled by | Becomes |
|---|---|---|
| `$$pipeline_id` | `PIPELINE_ID_SUBSTITUTION = true` | the pipeline definition's `pipeline_id` |
| `$$pipeline_run_id` | `PIPELINE_RUN_ID_SUBSTITUTION = true` | the run's `pipeline_run_id`, such as `97` |
| `$$task_run_id` | `TASK_RUN_ID_SUBSTITUTION = true` | this task execution's `task_run_id`, shared by its retries |
| `$$pipeline_run_id_filter` | `PIPELINE_RUN_ID_FILTER = true` | `pipeline_run_id = 97`, or `1=1` when the pipeline's `REFRESH_TYPE` is `FULL` |
| `$$run_date` | `RUN_DATE_SUBSTITUTION = true` | the date the run runs as of, as `DATE '2026-09-01'`: the day it started (UTC), the `--run-date` it was given, or the date of a [backfill](run-control.md#backfill-over-dates) run |

A task that reads by date follows the run's date, so a backfill of a past day reads that day:

```sql
SELECT order_id, amount FROM raw.orders WHERE order_date = $$run_date
```

A typical incremental load reads only the rows an earlier task wrote in the same run:

```sql
SELECT order_id, amount, $$pipeline_run_id AS loaded_in_run
FROM staging.orders
WHERE $$pipeline_run_id_filter
```

A token is replaced only when its parameter is `true`. The task fails before anything runs, naming
the problem, when the SELECT uses a token whose parameter is not `true`, when a parameter is `true`
but its token is missing, or when the SELECT holds any other `$$` token outside quotes and
comments. String literals, quoted identifiers and comments are preserved: `'$$pipeline_run_id'`
is literal text and does not require a substitution switch. Tokens inside dollar-quoted
strings are also preserved; tagged delimiters such as `$literal$...$literal$` distinguish
literal bodies from adjacent bare substitution tokens.

## The actions

`SQL_ACTION` names the action and `TARGET_OBJECT` the table: `schema.table`, written in the
active `Warehouse` profile's database, so the same rows work in every environment, or
`database.schema.table`, written exactly there. A bare table name fails the task.

| `SQL_ACTION` | What happens | Also needs |
|---|---|---|
| `CREATE_TABLE` | replaces the target with the SELECT's rows | — |
| `SETUP_TABLE` | creates the target, empty, from the SELECT's shape plus the audit columns of the action that writes it, when it does not exist; an existing target is left as it is | `SETUP_FOR` when no task in the pipeline writes the target |
| `OVERWRITE_TABLE` | replaces the target's rows from the SELECT | an existing target |
| `APPEND_TABLE` | replaces this task run's previous batch, then inserts the SELECT's rows, without comparing the shapes | an existing target |
| `SCD1_MERGE` | updates changed rows in place, by merge key, and inserts new keys | an existing target, `MERGE_KEY`, `MERGE_COMPARE_COLUMNS` |
| `SCD2_MERGE` | closes the active version of each changed key (`ACTIVE_FLAG = 'N'`) and inserts a new active one | an existing target, `MERGE_KEY`, `MERGE_COMPARE_COLUMNS` |
| `DROP_TABLE` | drops the target if it exists, once this pipeline's `CREATE_TABLE` task for it has succeeded in the run; a target already gone is not an error | no SELECT |
| `DELETE_ROWS` | flags the target rows whose `MERGE_KEY` the SELECT returns (`DELETE_FLAG = 'Y'`), or deletes them with `HARD_DELETE = true` | an existing target, `MERGE_KEY` |

### Replacement failures and table properties

`CREATE_TABLE` and `OVERWRITE_TABLE` stage the SELECT before changing the target. The warehouse
then protects publication using the strategy below. A rejected SELECT or a failed publication
retains the old rows; successful replacements report the staged row count.

| Warehouse | Publication strategy |
|---|---|
| PostgreSQL and native DuckDB | The replacement runs inside one transaction. An exception rolls back the old rows and definition. `CREATE_TABLE` carries the table comment; it refuses partitioned PostgreSQL parents. |
| Snowflake native tables | `CREATE OR REPLACE TABLE AS SELECT` carries the existing table properties and grants; `INSERT OVERWRITE INTO` retains the definition for overwrite. |
| Databricks Delta and Delta with Iceberg compatibility | `CREATE OR REPLACE TABLE AS SELECT` carries the existing table properties; `INSERT OVERWRITE TABLE` retains the definition for overwrite. |
| Trino Iceberg | `CREATE OR REPLACE TABLE AS SELECT` publishes one replacement, retaining the declared properties, partitioning and table comment. Overwrite casts each value to its target type. |
| DuckDB over Iceberg and Snowflake Iceberg | A prepared candidate replaces the target after its original object has been retained under a recovery name. Overwrite first copies the old rows and restores them into the existing definition if a later statement fails. |

Atomic CTAS refuses column constraints, defaults or comments that it cannot carry forward.
Use a write that retains the existing definition; on Snowflake and Databricks,
`OVERWRITE_TABLE` does this. On DuckDB over Iceberg, `CREATE_TABLE` carries table properties
but refuses partitioning, sort order and column metadata; use `OVERWRITE_TABLE` for those tables.
Snowflake Iceberg replacement refuses customer storage, clustering and column metadata it
cannot reproduce. Replacement at an explicitly supplied existing storage path is refused;
use overwrite to retain that table's location. PostgreSQL and native DuckDB still rebuild the
columns for `CREATE_TABLE`, so indexes, grants and column metadata are not copied.

The recovery strategy is compensation, with a brief window during rename or truncate when
readers can see a missing or empty target. It restores the original on a caught exception;
a hard process exit or outage can interrupt recovery. If restoration fails, the error names
`<target>__etl_keep_<token>`, which is retained for manual recovery. Stop writers and restore
that original object or its rows before retrying. A successful non-transactional publication
remains successful if scratch or backup cleanup fails; the warning names the table to remove.

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
differs from the target's. Every merge key column must be set: a row with a NULL in one would
match no target row, so the merges and `DELETE_ROWS` fail on it, with the count. A key that
`DELETE_ROWS` flagged deleted comes back when a merge's SELECT returns it again: `SCD1_MERGE`
updates it with `DELETE_FLAG = 'N'`, and `SCD2_MERGE` closes the flagged version and inserts a
live one. A soft `DELETE_ROWS` leaves rows already flagged as they were.

### Change hash version 2

`HASH_KEY` remains a 32-character MD5 digest. Its input encodes each compare column as `N`
for NULL, or `V<character-length>:<canonical-value>` for a non-NULL value, in the configured
column order. NULL and empty text are distinct; embedded separators and Unicode text cannot
shift column boundaries. Booleans use `true`/`false`, dates use `YYYY-MM-DD`, timestamps use UTC
ISO text with six fractional digits, and decimals keep their declared scale, including trailing
zeroes. Naive timestamps represent UTC. Every new warehouse connection starts in UTC, and the
hash expressions normalize timestamps even if the session time zone is changed afterwards.

Floating-point compare columns are refused: cast them to `DECIMAL(p,s)` in the SELECT and use
that declared decimal type in the target. Decimal columns without a declared scale and
unsupported types such as arrays are refused too; cast them to a supported scalar type.

A fresh `SETUP_TABLE` target for a merge records hash version 2 after its warehouse transaction
commits. Existing targets have an unknown or older version and refuse merges until upgraded:

```bash
etl-craft rehash --target sales.customers --dry-run
etl-craft rehash --target sales.customers
```

The command reads the active merge tasks for that qualified target; they must declare one
common ordered `MERGE_COMPARE_COLUMNS` list. Dry-run validates the types and prints the UPDATE
and row count without changing hashes or their version. Rehash updates every row in one warehouse
statement, including inactive SCD2 history, and changes only `HASH_KEY`. It then records version 2
in `AUD_TARGET_HASH_VERSION`. Target locks coordinate SQL writes and rehashing. The warehouse
and Engine DB commit separately: if publication fails after the warehouse update, rerun rehash;
merges stay refused until the version is recorded. Dropping or replacing a target clears its
recorded hash version.

### Joined merge updates

SCD1 updates read values directly from the deduplicated stage in one joined write. PostgreSQL,
DuckDB and Snowflake use `UPDATE ... FROM`; Databricks and Trino use a matched `MERGE` update.
SCD2 closes active versions by joining to the changed-key stage, then inserts their new versions.
Every merge-key column participates in the join. `PRESERVE_TARGET` still keeps target values
where SCD1 source values are NULL and hashes the values actually stored; unchanged rows keep
their audit fields. PostgreSQL indexes the stage's merge keys and runs `ANALYZE` before joined
updates so the planner has current stage statistics.

### Optional parameters

| Parameter | Applies to | Effect |
|---|---|---|
| `MERGE_DEDUPE_ORDER` | the merges | when the SELECT returns a merge key more than once, keep the first row in this order, such as `updated_at DESC`; without it, duplicate keys fail the task. An order that ties between rows that differ fails the task too: add a column that breaks the tie |
| `SETUP_FOR` | `SETUP_TABLE` | the action whose audit columns the table gets: `CREATE_TABLE`, `OVERWRITE_TABLE`, `APPEND_TABLE`, `SCD1_MERGE` or `SCD2_MERGE` |
| `SCHEMA_EVOLUTION` | `OVERWRITE_TABLE` and the merges | `true` appends nullable columns the SELECT returns but the target lacks, with their complete types |
| `PRESERVE_TARGET` | `SCD1_MERGE` | `true` keeps the target's value where the SELECT returns NULL |
| `HARD_DELETE` | `DELETE_ROWS` | `true` deletes rows instead of flagging them |
| `TABLE_FORMAT` | all | `native` or `iceberg`, for this task's target, where the warehouse lets a task choose |
| `EXTERNAL_LOCATION` | tables it creates, on Databricks and Trino (Iceberg) | where the table's files live, such as `s3://lake/sales/orders` |
| `EXTERNAL_VOLUME`, `BASE_LOCATION` | tables it creates, on Snowflake Iceberg | the customer volume and the path in it; set both, or neither for Snowflake-managed storage |
| `CATALOG` | tables it creates, on Snowflake Iceberg | a catalog integration for an externally managed Iceberg catalog |

A storage parameter a warehouse does not use fails the task rather than being ignored: the table
would otherwise land somewhere other than intended. Schema evolution adds columns in place, retaining the table's storage location. See
[Warehouses](../connectors/warehouses.md#storage-outside-the-warehouses-own) for each warehouse.

Yes/no parameters take `true` or `false`; anything else fails the task.

### One table format per target

A task resolves its format from `TABLE_FORMAT`, or from `Warehouse.Table_format` when the
parameter is absent. `etl-craft validate` compares active SQL writers across pipelines: every
writer and setup task on the same qualified target must resolve to the same format. Case
variants and targets qualified with the active database refer to the same table; an explicitly
different database refers to another target. Validation reads metadata without connecting to
the warehouse, so it cannot inspect tables created outside the engine.

Before `CREATE_TABLE`, `SETUP_TABLE`, `OVERWRITE_TABLE` or either merge acts on an existing
target, the engine checks its format under the target mutation lock. A mismatch fails before
the source is staged or the target is changed, including a setup that would otherwise do
nothing. Set `TABLE_FORMAT` to the existing format, or choose another target. Changing the
parameter is not a table migration.

Databricks distinguishes ordinary Delta (`native`) from Delta with UniForm Iceberg reads
(`iceberg`) using [table details](https://docs.databricks.com/aws/en/delta/iceberg-reads).
Snowflake reads [the table's `IS_ICEBERG` flag](https://docs.snowflake.com/en/sql-reference/info-schema/tables)
in its own database. Warehouses with only one supported table format use that fixed format.

### The columns the engine adds

After the SELECT's own columns, every table the engine creates has:

| Column | Added by |
|---|---|
| `PIPELINE_ID`, `PIPELINE_RUN_ID`, `TASK_RUN_ID` | every action |
| `UPDATE_DATE` | `OVERWRITE_TABLE` |
| `CREATE_DATE` | `APPEND_TABLE` |
| `HASH_KEY`, `CREATE_DATE`, `CREATED_BY`, `UPDATE_DATE`, `UPDATED_BY`, `DELETE_FLAG` | `SCD1_MERGE` |
| the `SCD1_MERGE` columns and `ACTIVE_FLAG` | `SCD2_MERGE` |
| `ROW_ID` | every action; a generated key that business rules refer to |

`PIPELINE_ID` identifies the pipeline definition, `PIPELINE_RUN_ID` its execution, and
`TASK_RUN_ID` the task execution that last inserted or changed the row. All three are BIGINT
columns. Merge updates, closed SCD2 versions and soft deletes stamp the executing task's
identities; unchanged rows retain their provenance. `CREATED_BY` and `UPDATED_BY` hold the
warehouse user the engine connects as.

`ROW_ID` is an internal key, not a business key or a gap-free row counter. PostgreSQL uses
identity columns and native DuckDB uses a sequence. New Databricks Delta tables, including
UniForm tables, declare `GENERATED ALWAYS AS IDENTITY`; native Snowflake tables use ordered
`AUTOINCREMENT`. Inserts omit generated keys, and overwrites and schema evolution retain their
generator. Native cloud `CREATE_TABLE` prepares an independent table with its identity and rows,
then atomically clones it into the target; a failed preparation or publication leaves the old
target intact. Table properties survive and Snowflake replacement copies existing grants.

Trino, DuckDB Iceberg and Snowflake Iceberg allocate `MAX(ROW_ID) + ROW_NUMBER()`. The Engine DB
lock for the qualified target is held from before opening the warehouse transaction until that
transaction commits, so competing task processes cannot read the same base. Older Databricks or
Snowflake targets without a generator keep this computed strategy; an ordinary write does not
rebuild them. A deliberate `CREATE_TABLE` replacement creates a new identity-backed target.

Generated keys need no `MAX(ROW_ID)` allocation. The same target mutation lock still coordinates
writes with table replacement and hash upgrades, and serializes Databricks identity writes, whose
concurrent transactions are unsupported. All competing etl-craft writers must use the same Engine
DB. Writes from other tools are outside this lock; concurrent external writes to computed-key
targets can produce duplicate keys.

## When the target and the SELECT disagree

Before `OVERWRITE_TABLE` or a merge writes a row, the target is compared with the SELECT, and the
task fails with the reason when:

- the target lacks a column the action maintains, such as `HASH_KEY` for a merge: run a
  `SETUP_TABLE` task for it, or add the column;
- the target has a column the SELECT no longer returns: columns are never dropped, so return it,
  NULL if need be;
- the SELECT returns a new column and `SCHEMA_EVOLUTION` is not `true`.

With `SCHEMA_EVOLUTION = true`, new columns are appended with `ALTER TABLE ... ADD COLUMN`.
They are nullable and NULL in existing rows. The engine reads complete types from the materialized
stage, preserving precision, scale, declared lengths and nested types where the warehouse supports
them. It keeps the existing table, rows, `ROW_ID` generator, storage location, comments, grants,
partitioning and snapshot history. An overwrite still replaces rows using its publication strategy.
Writes name their columns explicitly, so the SELECT's order need not match the table's order.

Evolution never changes an existing column's type. With `SCHEMA_EVOLUTION = true`, the engine
compares the complete types of existing business columns even if no columns need adding, and
refuses differences before any additions. The error names the target, column and both types:
cast the SELECT to the target's declared type or migrate that column explicitly. Without schema
evolution, ordinary writes retain the warehouse's existing conversion behavior.

The warehouse's stored types govern evolution: DuckDB normalizes `CHAR` and bounded `VARCHAR`
to `VARCHAR`, and Iceberg stores strings without declared lengths. DuckDB over Iceberg cannot
add nested columns with `ALTER TABLE`; the engine refuses a batch containing such an addition
before adding any columns. Add arrays, maps or structs through a capable catalog engine, such
as Trino, then return those columns from the SELECT.

All types are checked before additions begin. PostgreSQL and native DuckDB roll back additions
if the action fails. On warehouses whose DDL commits independently, a failure during additions
or a later write can leave some nullable new columns in place; existing rows survive the failed
addition. Retry the same SELECT to complete from the current column set. Evolution does not
rebuild the table as a fallback.

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

`AUD_TASK_RUN_LOG` records the source, target, insert, update and delete counts, and
`ROWS_WRITTEN`, their sum of inserts, updates and deletes, which a `HAS_DATA` dependency reads.
Each count comes from its own `COUNT(*)` query, not from the driver.

## Running a task again

On PostgreSQL and DuckDB an action's statements commit or roll back together. Trino, Databricks
and Snowflake commit each statement, so an action that fails part-way can leave its work half
done. Every action works out what to do from the target's current state, so running the task
again completes it. Scratch tables the action creates (`etl_stage_<task_run_id>_<token>` and similar,
with a token of its own per attempt) are dropped even when it fails. Where the warehouse has no
temporary tables (Trino, Databricks) they are created in the target's schema.


### Retrying an append

Every new SQL target includes `TASK_RUN_ID BIGINT`. An append stamps each inserted row with its
existing task-run identity, alongside `PIPELINE_ID` and `PIPELINE_RUN_ID`. Under the target
mutation lock, an append first stages its SELECT, deletes rows with that task run's `TASK_RUN_ID`,
then inserts the staged batch. Retrying the same task run replaces its earlier attempt's rows;
other task runs and historical rows with NULL task-run identities remain untouched. A new pipeline
run gets a new task-run identity and appends another batch. An empty retry removes its earlier
batch. `insert_count` and `ROWS_WRITTEN` describe the inserted batch, not the retry cleanup.

PostgreSQL and native DuckDB commit the delete and insert together. On warehouses that commit
each statement, a failure between them can leave the batch absent until its retry completes.
`ROW_ID` values may change when a batch is replaced; they are internal keys, not stable source
identities. Writers outside etl-craft do not participate in the target lock.

An append refuses a target without `TASK_RUN_ID`, because a retry could duplicate rows; the
error names the command that adds the column. Upgrade existing configured SQL and ingestion
targets:

```bash
etl-craft upgrade-targets --dry-run
etl-craft upgrade-targets
# Select append targets only, or one target:
etl-craft upgrade-targets --action APPEND_TABLE
etl-craft upgrade-targets --action APPEND_TABLE --target sales.events
```

The command adds missing `PIPELINE_ID`, `PIPELINE_RUN_ID` and `TASK_RUN_ID` columns as nullable
BIGINT under the same target lock. It validates the stored table format and refuses conflicting
writer formats or incompatible identity types. It scans active SQL tasks and Python ingestion
tasks with `TARGET_OBJECT` configured. For targets owned only by Python scripts without an explicit `TABLE_FORMAT`, it uses the stored format because the scripts own their table definitions. It preserves existing rows, leaving new identity fields NULL; it cannot identify
or remove duplicates from past retries. Already upgraded targets are left intact. Existing overwrite
and merge targets need these columns before their next write; `SCHEMA_EVOLUTION` does not add audit
columns. Each target is
upgraded separately, so rerun the command if an error interrupts a multi-target upgrade. A dry run
validates targets and prints additions without changing the warehouse; its request is still
recorded in `AUD_ACTIONS`.
