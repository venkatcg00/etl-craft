# Business rules

A task with `HANDLER = 'BUSINESS_RULES'` checks the rows of warehouse tables against rules and
flags the ones that break them. The flags are rows of `AUD_BUSINESS_RULES_RESULTS` in the Engine DB;
the warehouse tables are only read.

## Writing a rule

Each rule is a row of `CFG_BUSINESS_RULES` for the task:

| Column | Holds |
|---|---|
| `BUSINESS_RULE_NAME` | a name, used in logs and messages |
| `TARGET_TABLE` | the table checked: `schema.table` in the active warehouse database, or `database.schema.table` |
| `BUSINESS_RULE_KEY_COLUMN` | the column that identifies a row, usually `ROW_ID` |
| `BUSINESS_RULE_SQL` | a `SELECT` that returns a row when the table's row `t` breaks the rule |
| `BUSINESS_RULE_TYPE` | `INCOMPLETE`, `REJECT` or `REPORT`, copied onto each flag |
| `SEQUENCE_NUMBER` | the wave the rule runs in |

`BUSINESS_RULE_SQL` refers to the checked row as `t`. For example, to flag orders whose customer
is inactive:

```sql
SELECT 1 FROM sales.customers c WHERE c.id = t.customer_id AND c.active = 'N'
```

Correlate on equality (`c.id = t.customer_id`): some warehouses, Snowflake among them, cannot
evaluate a subquery correlated any other way. The rule must be one read-only statement.
A trailing semicolon or SQL comment is allowed, both when flagging rows and when checking
whether flagged rows now pass.

## Execution identities

Rule SQL receives `:pipeline_id`, `:pipeline_run_id` and `:task_run_id` as bound integer inputs.
They identify the pipeline definition, its execution and the business-rule task execution.
The table's `t.TASK_RUN_ID` records the SQL or ingestion task that wrote that row; the rule's
`:task_run_id` identifies the task evaluating the rule. Evaluating a rule preserves the row's
provenance. For example:

```sql
SELECT 1 FROM sales.customers c
WHERE c.id = t.customer_id
  AND t.PIPELINE_ID = :pipeline_id
  AND t.PIPELINE_RUN_ID = :pipeline_run_id
  AND c.active = 'N'
```

## What a run does

For each rule, the engine looks at the rows in scope: those written in the current run
(`t.PIPELINE_RUN_ID` is the run's id), or every row when the task is run with `--force`. On a
table with an `ACTIVE_FLAG` column, such as an `SCD2_MERGE` target, only the active versions
(`t.ACTIVE_FLAG = 'Y'`) are in scope: a closed version is history, not a row to judge. Then it:

1. flags every key whose row breaks the rule and is not flagged already (`ACTIVE_FLAG = 'Y'`);
2. clears the flag (`ACTIVE_FLAG = 'N'`, with `END_DATE`) of every flagged key whose row in scope
   no longer breaks it, and of every flagged key with no row (no active version) left in the
   table.

Any other flag outside the scope is left as it is, so a normal run changes only what the run
touched. A run with `--force` judges the whole table, so it clears every flag it does not find
again.
`AUD_TASK_RUN_LOG` records the keys flagged as `INSERT_COUNT` and those cleared as
`UPDATE_COUNT`.

## Waves, retries and failures

Rules with the same `SEQUENCE_NUMBER` form a wave and run in parallel, at most
`Orchestration.Max_parallel_tasks` at once; the next wave starts when every rule of the current one
has finished.

Each rule records its own row in `AUD_BUSINESS_RULES_RUN_LOG` and commits its flags separately.
Clearing uses batches of at most 1,000 keys in the Engine DB. Every batch, newly added flags
and the rule's success status share one transaction: a failed batch rolls all those changes back.
Existing cleared flags keep their original end date, and flags of other rules remain unchanged.
A rule that fails is recorded `FAILED`, the rest of its wave still runs, and then the task fails,
naming every rule that failed with the step and the database's message:

```
1 of 2 business rule(s) failed in wave SEQUENCE_NUMBER=1: broken on analytics.sales.orders: find the keys that break the rule failed: UndefinedTable: relation "sales.no_such_table" does not exist
```

When the task runs again under the same run, rules that already succeeded are not run again.
Before any rule runs, every rule of the task is checked: a key column that is not a plain name,
a table without a schema, or rule SQL that is not a single read-only `SELECT` stops the task.

Every step is logged with its counts and time, and `--log-level DEBUG` adds the SQL:

```
INFO etl_craft.handlers.business_rules [...]: rule inactive_customer on analytics.sales.orders: 2 key(s) break it, 2 newly flagged, 0 cleared (0.41s)
```
