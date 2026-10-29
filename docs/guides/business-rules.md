# Business rules

A task with `HANDLER = 'BUSINESS_RULES'` checks warehouse tables. A `ROW` rule flags the
individual rows that break it in `AUD_BUSINESS_RULES_RESULTS`. A `TABLE` rule records one
result on `AUD_BUSINESS_RULES_RUN_LOG`. The warehouse tables are only read.

## Writing a rule

Each rule is a row of `CFG_BUSINESS_RULES` for the task:

| Column | Holds |
|---|---|
| `BUSINESS_RULE_NAME` | a name, used in logs and messages |
| `TARGET_TABLE_ID` | the checked table’s `CFG_TABLES.TABLE_ID`; config files name its separate catalog, schema and table |
| `BUSINESS_RULE_LEVEL` | `ROW` (the insert default) or `TABLE` |
| `BUSINESS_RULE_KEY_COLUMN` | required for `ROW`; empty for `TABLE` |
| `BUSINESS_RULE_SQL` | one complete read-only `SELECT`; output follows the level below |
| `BUSINESS_RULE_TYPE` | `ROW`: `INCOMPLETE`, `REJECT`, `REPORT`; `TABLE`: `REJECT`, `REPORT` |
| `SEQUENCE_NUMBER` | the wave the rule runs in |

Declare only project objects that need a contract in `CFG_TABLES`; this does not copy the
warehouse information schema. A rule’s target contract must be active. An empty catalog in
config files resolves against the configured warehouse when applying current metadata.
Execution derives the qualified target from the referenced contract. Later target edits do
not change previously recorded rule-result targets or earlier configuration versions.

## Row rules

`BUSINESS_RULE_SQL` is a complete read-only SELECT. Its output must include the column named
by `BUSINESS_RULE_KEY_COLUMN`, containing the target row keys to flag; other output columns
are ignored. Keys are cast to text. Repeated keys produce one flag. A NULL returned key fails
the rule, naming the rule, the number of NULL keys and the remedy.

`$$rows_in_scope` expands to the target rows written by this pipeline run, restricted to active
versions when the target has `ACTIVE_FLAG`. Under `--force` it includes every active target row.
For example, with `BUSINESS_RULE_KEY_COLUMN=ROW_ID`:

```sql
SELECT t.ROW_ID
FROM $$rows_in_scope t
JOIN sales.customers c ON c.id = t.customer_id
WHERE c.active = 'N'
```

Queries run as written: ordinary WHERE predicates, joins, anti-joins, CTEs, window functions
and GROUP BY / HAVING are supported. A cross-row rule returns every target row key in the
breaking group, rather than a group identifier. For duplicate customer/order references:

```sql
WITH duplicates AS (
    SELECT customer_id, order_reference
    FROM $$rows_in_scope
    GROUP BY customer_id, order_reference
    HAVING COUNT(*) > 1
)
SELECT t.ROW_ID
FROM $$rows_in_scope t
JOIN duplicates d ON (d.customer_id = t.customer_id
                      OR (d.customer_id IS NULL AND t.customer_id IS NULL))
                 AND (d.order_reference = t.order_reference
                      OR (d.order_reference IS NULL AND t.order_reference IS NULL))
```

The engine intersects returned keys with the in-scope target keys. Keys outside that scope
are dropped deliberately. `validate` warns when the scope token is absent because reading the
whole target can cost more, even though the intersection still enforces scope. The key is one
column; composite keys are not supported. A trailing semicolon or SQL comment is allowed.

Upgrading converts stored legacy fragments to
`SELECT t.<quoted key> FROM $$rows_in_scope t WHERE EXISTS (<old fragment>)`, preserving the
old predicate and recorded results. The configured Warehouse dialect quotes the key, so run
`migrate` with the project's connector configuration. For a shared Engine DB, supply each
other project's connector config with repeatable `--project-config PATH`. Every project with
stored rules, including inactive rules, needs its actual Warehouse dialect. Missing configs
stop the upgrade before any pending file runs; no other project's dialect is substituted.
The files are read on the executing host and must name the same Engine DB address and schema.
The shared operation input is `MigrateInput.project_configs`; migration stays host-only.
For example:

```bash
etl-craft --config /srv/first/craft-connector.yml migrate \
  --project-config /srv/second/craft-connector.yml
```

Export configuration again after
upgrading to replace old CSV fragments with the converted queries. `validate` refuses an old
fragment and shows its converted SQL as the remedy. EXISTS is used for this conversion only;
new rules do not need it. Explicit projections are checked statically. Wildcard projections
and SQL whose output cannot be inferred are checked with a zero-row warehouse query; the
target and warehouse connection must be available for `validate` to verify those outputs.

The upgrade also requires one active flag per `(BUSINESS_RULE_ID, BUSINESS_RULE_KEY)`. It
checks before pending upgrades and again inside the migration transaction. Conflicting
legacy flags stop the upgrade with their rule, key and result IDs. Use the previous package
and an explicit project migration to clear the conflicts while retaining inactive evidence;
then retry the upgrade. The engine never deletes history or selects a winner automatically.

## Table rules

Set `BUSINESS_RULE_LEVEL=TABLE`, leave `BUSINESS_RULE_KEY_COLUMN` empty, and choose `REJECT`
or `REPORT`. The query must return **exactly one row** with:

| Output | Contract |
|---|---|
| `PASSED` | required text `Y` or `N`, exactly; booleans, numbers, lower case and NULL fail |
| `OBSERVED` | optional text or NULL, at most 4,096 characters |
| `DETAIL` | optional text or NULL, at most 4,096 characters |

No other output column is allowed. Cast measurements to text explicitly (`STRING` instead
of `VARCHAR` on Databricks). Both text fields
use a 4,096-character storage limit, consistent with bounded Engine DB error/reason evidence.
An overlong value fails with its field, length, limit and remedy; it is never truncated.
Malformed outputs are FAILED rule runs with the diagnostic in `DETAIL` and the attempt log.
Validation checks explicit output lists statically, as for ROW rules; wildcard or unresolved
outputs need a zero-row warehouse probe. This lets a pipeline refresh before its upstream
setup task creates the target. Execution always checks the actual columns and values.

A valid `PASSED=N` on `REJECT` fails the task and prevents its success-dependent tasks from
starting. `REPORT` stores `N` and continues. `INCOMPLETE` has no table meaning and is refused.
The result is stored once per rule/task run, including `PASSED`, `OBSERVED` and `DETAIL`;
a table check never appends or clears per-row flags, or rewrites earlier evidence.

At least one row this run (`REJECT`):

```sql
SELECT CASE WHEN COUNT(*) > 0 THEN 'Y' ELSE 'N' END AS PASSED,
       CAST(COUNT(*) AS VARCHAR) AS OBSERVED,
       'At least one row is required' AS DETAIL
FROM $$rows_in_scope
```

NULL rate under 5% (`REPORT`):

```sql
SELECT CASE WHEN COUNT(*) > 0
                 AND SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) * 100.0
                     < COUNT(*) * 5 THEN 'Y' ELSE 'N' END AS PASSED,
       CAST(SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS VARCHAR) AS OBSERVED,
       'NULL amounts; limit is under 5 percent' AS DETAIL
FROM $$rows_in_scope
```

Totals reconcile (whole-table form; `REJECT`):

```sql
WITH actual AS (SELECT SUM(amount) AS total FROM sales.orders),
     expected AS (SELECT SUM(amount) AS total FROM landing.order_totals)
SELECT CASE WHEN actual.total = expected.total THEN 'Y' ELSE 'N' END AS PASSED,
       CAST(actual.total - expected.total AS VARCHAR) AS OBSERVED,
       'Difference between order and landing totals' AS DETAIL
FROM actual CROSS JOIN expected
```

`$$rows_in_scope` uses the existing run/active-version scope. With `--force`, it includes all
active target rows. A whole-table query runs against the table it names; it does not receive
a row-key intersection. Sequence waves and retry/resume stay the same: successful rule
executions are skipped on a retry unless forced. A `REPORT` result of `N` is a successful
execution of a failing check and is retained; failed `REJECT` checks run again on retry.

`status`, `explain`, run detail and the catalog table page show the recorded fields. The
existing run-detail read exposes `rules`; pipeline/task history exposes recent observations
in `rules` over the latest fifty pipeline runs, with rule and run identities. These share
one repository read for the future table page and Pipeline Insights; no new route is needed.
UI pages remain separate work.

On upgrade, existing rules and their historical versions become `ROW`; earlier run-log
values are unchanged and the new result fields are NULL. Config files are partial helpers:
new rules default to `ROW`, while an omitted level keeps an existing rule's stored value.
Changing a level is an ordinary update that preserves the rule id. Export includes the level.

## Execution identities

Rule SQL receives `:pipeline_id`, `:pipeline_run_id` and `:task_run_id` as bound integer inputs.
They identify the pipeline definition, its execution and the business-rule task execution.
The table's `t.TASK_RUN_ID` records the SQL or Python script task that wrote that row; the rule's
`:task_run_id` identifies the task evaluating the rule. Evaluating a rule preserves the row's
provenance. For example:

```sql
SELECT t.ROW_ID
FROM $$rows_in_scope t
JOIN sales.customers c ON c.id = t.customer_id
WHERE t.PIPELINE_ID = :pipeline_id
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
   no longer breaks it.

Flags outside the run and flags for deleted rows remain active until an explicit forced
check. A normal run issues one check query per rule, with no standing-key batches or global
deletion scan. A run with `--force` judges the whole active table, clearing every flag it does
not find again, including deleted-row flags:

```bash
etl-craft run --pipeline-code SALES_DAILY --task-code check_orders --run-id 419 --force
```

Use the exact pipeline/task and run identity for your project. Teams own scheduling through
ordinary BR tasks and `CFG_PIPELINES.RUN_SCHEDULE`, time zone, catch-up, overlap, pause and
deactivation controls. A schedule or pipeline refresh does not imply a full check. No rules
are automatically rechecked periodically; request `--force` explicitly for global reconciliation.
`AUD_TASK_RUN_LOG` records the keys flagged as `INSERT_COUNT` and those cleared as
`UPDATE_COUNT`. Each rule also reports `returned`, `flagged` (new flags), `cleared` and
`dropped` counts in `BUSINESS_RULE_<id>` task values and the attempt log. Returned and dropped
count output rows before deduplication; flagged counts unique newly active keys.

## Waves, retries and failures

Rules with the same `SEQUENCE_NUMBER` form a wave and run in parallel, at most
the measured local task capacity at once; the next wave starts when every rule of the current one
has finished.

Each rule records its own row in `AUD_BUSINESS_RULES_RUN_LOG` and commits its flags separately.
A renewable Engine DB resource lock serializes checks of the same rule across attempts, from
binding through warehouse evaluation and flag publication. Different rules remain independent.
A bounded lock timeout identifies the rule and current holder and tells the operator to inspect
its attempt before retrying. The active-flag unique index protects publication as well.
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
INFO etl_craft.handlers.business_rules [...]: rule inactive_customer on analytics.sales.orders: 2 returned, 2 flagged, 0 cleared, 0 dropped (0.41s)
```
