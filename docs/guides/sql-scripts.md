# SQL scripts

`HANDLER=SQL_SCRIPT` runs arbitrary warehouse SQL from `sql_scripts/`, including procedures,
grants, maintenance and custom DML. It is an ordinary task: use `CFG_TASK_DEPENDENCY` to place
it before or after managed SQL or [Python tasks](python-scripts.md). There are no pre/post
parameters on other handlers. Run it with the existing `etl-craft run` command.

| Parameter | Meaning |
|---|---|
| `SCRIPT_FILE` | required `.sql` file's path from the project directory, inside `sql_scripts/`, such as `sql_scripts/maintenance.sql`; traversal and escaping symlinks are refused |
| `INPUT_PARAMS` | optional JSON object; each key is a bound `:name`, never SQL substitution |
| `RETRY_ON` | ordinary retry policy; **recommend `NONE`**, since repeating arbitrary SQL is rarely safe |

Every statement also receives `:pipeline_id`, `:pipeline_run_id`, `:task_run_id` and `:run_date`.
These engine-provided keys cannot be overridden. Bind values, including dates, go through the
native driver. Identifiers cannot be supplied as binds. Missing binds anywhere in the file fail
before its first statement runs. Comments, quoted semicolons and dollar-quoted procedure bodies
do not split a statement. Colons inside them are literal.

```sql
CALL load_orders(:run_date);
CALL refresh_region(:region);
```

Reusable SQL belongs in warehouse procedures, not templates or macros. The file is submitted to
the configured warehouse as written. Its syntax and capabilities come from that warehouse.
See the [task example](../examples/sql-script.md).

## Transactions and retries

Statements execute in file order on one native session, in autocommit by default. Earlier
commits **remain committed** when a later statement fails. `BEGIN`, `COMMIT` and `ROLLBACK` in
the file control explicit transactions where the warehouse supports them. An unfinished
transaction is rolled back when the session closes. Commands such as PostgreSQL `VACUUM`
therefore work outside explicit transactions. Warehouse DDL may commit independently of a
transaction; consult that warehouse's contract.

A failure stops the file immediately. Retries restart at statement 1, using the same ordinary
`RETRY_ON` policy and attempt history as other tasks. There is no statement checkpoint or
exactly-once promise. A process loss after a commit can leave work committed before its audit
completion; reconcile the target before explicitly retrying non-idempotent work.

## Audit and managed targets

Each statement records its 1-based index, short SQL text, rows affected and elapsed seconds
in the **full attempt log**, regardless of the selected log level,, available through the existing log command/API. Drivers that do
not report an affected-row count record `unknown`; SELECT results are consumed and discarded,
not treated as affected rows. The attempt summary records the completed statement count. A
failure names the file, statement index, short text and database message in its attempt outcome.
A starting record identifies a statement interrupted before its completion record.

Bound values are never logged; audit text omits literals, procedure bodies and comments. Keep
credentials in configuration, never inline SQL or `INPUT_PARAMS`; database error messages can
include data. The complete file is still available in your project and bundle.

These tasks do not publish hash versions, allocate ROW_ID or take managed target locks.
`validate` warns when parsed writes target a table also targeted by a managed SQL task, naming
both tasks. SQLGlot infers table and SELECT-column lineage where possible; the catalog labels
it **unmanaged**, and unknown procedure effects remain unknown. This warning does not make an
unmanaged write safe: avoid modifying managed targets outside their managed actions.
