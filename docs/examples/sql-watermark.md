# SQL source watermark

An external source exposes `source.events(event_id BIGINT, payload VARCHAR)`. Create a merge
target with a SETUP_TABLE task first, then configure the SQL task with these rows in
`CFG_TASK_PARAMETERS` (using the task's existing `TASK_ID`). Set its target in `tasks.csv`
with `TARGET_SCHEMA_NAME=mart` and `TARGET_TABLE_NAME=events`, referencing the matching
`tables.csv` row:

| PARAMETER_NAME | PARAMETER_VALUE |
| --- | --- |
| SQL_ACTION | SCD1_MERGE |
| SELECT_QUERY | SELECT event_id, payload FROM source.events WHERE event_id > $$watermark |
| MERGE_KEY | event_id |
| MERGE_COMPARE_COLUMNS | payload |
| WATERMARK_COLUMN | event_id |
| WATERMARK_TYPE | NUMBER |
| WATERMARK_START | 0 |

A first successful batch with ids 1 and 2 stores NUMBER 2. When id 3 arrives, the next run
reads it and stores 3. A failed run leaves 2 committed. To reload corrected earlier rows:

```bash
etl-craft mark --pipeline-code SALES --task-code events --run-id 42 \
  --watermark 0 --reason 'reload corrected source rows'
etl-craft run --pipeline-code SALES --task-code events --run-id 42 \
  --rerun --reason 'reload corrected source rows'
etl-craft explain --pipeline-code SALES --task-code events --run-id 42
```

The merge updates changed payloads and keeps one row per key. The reset and each execution's
range remain in the audit history. An id watermark sees inserted ids; use a changing timestamp
when existing source rows can be updated.

Keep each column once in the merge lists. For example, `event_id|EVENT_ID` is a repeated key
and fails validation. If correcting repeated compare columns on an existing target, follow
[the rehash procedure](../guides/sql-tasks.md#correcting-an-existing-repeated-compare-column)
before resuming the pipeline.

## Bounded ODS-to-mart example

For an engine-managed ODS source, configure `WATERMARK_TYPE=TIMESTAMP`,
`WATERMARK_COLUMN=ods_changed_at`, `WATERMARK_START=2000-01-01T00:00:00+00:00` and
`WATERMARK_SOURCE_PIPELINES=["CLIENT_ALPHA","CLIENT_BETA","STEWARD_BUSINESS"]`:

```sql
SELECT customer_id, status, UPDATE_DATE AS ods_changed_at
FROM ods.customers
WHERE UPDATE_DATE >= $$watermark AND UPDATE_DATE < $$watermark_max
```

The admitted high is stable across retries; even an empty successful window advances to it.
`UPDATE_DATE` means the Engine DB start of the attempt that wrote the row. Source and target
columns must retain microseconds; existing coarse columns require a project migration.
The alias avoids the target's reserved audit column. Compare only business columns in a
merge, such as `MERGE_COMPARE_COLUMNS=status`. Source run IDs remain provenance, not a cursor.
See [bounded timestamp windows](../guides/sql-tasks.md#bounded-timestamp-windows) for pending
ownership, source fences, repairs, resets and the upgrade rule.
