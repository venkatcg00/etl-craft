# Unmanaged SQL task

Set a task's `HANDLER` to `SQL_SCRIPT`, with these `CFG_TASK_PARAMETERS`:

```csv
PARAMETER_NAME,PARAMETER_VALUE
SCRIPT_FILE,sql_scripts/maintenance.sql
RETRY_ON,NONE
INPUT_PARAMS,"{""region"": ""eu""}"
```

Save this as `sql_scripts/maintenance.sql`:

```sql
CALL refresh_region(:region, :run_date);
```

The procedure must already exist. Add ordinary task dependencies to run it before or after
another task. See [SQL scripts](../guides/sql-scripts.md) for the transaction and audit contract.
