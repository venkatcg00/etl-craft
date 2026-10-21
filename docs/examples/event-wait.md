# A pipeline waiting for a delivery

This optional project migration runs on SQLite and PostgreSQL Engine DBs. It adds a local
pipeline with a one-minute schedule and a file wait before a Python script:

```sql
INSERT INTO CFG_PIPELINES
    (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE, RUN_SCHEDULE, OVERLAP_POLICY)
VALUES ('FILE_DELIVERY', 'Delivered CSV files', 'INCREMENTAL', '* * * * *', 'SKIP');

INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER)
SELECT 'wait', 'INGESTION', PIPELINE_ID, 'EVENT_WAIT'
FROM CFG_PIPELINES WHERE PIPELINE_CODE='FILE_DELIVERY' AND ACTIVE_FLAG='Y';

INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE)
SELECT TASK_ID, 'EVENT_FILE', 'inbox/*.csv' FROM CFG_TASKS
WHERE TASK_CODE='wait' AND PIPELINE_ID=(SELECT PIPELINE_ID FROM CFG_PIPELINES
    WHERE PIPELINE_CODE='FILE_DELIVERY' AND ACTIVE_FLAG='Y');
```

To keep this pipeline waiting indefinitely, add:

```sql
INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE)
SELECT TASK_ID, 'EVENT_TIMEOUT_MINUTES', 'NONE' FROM CFG_TASKS
WHERE TASK_CODE='wait' AND PIPELINE_ID=(SELECT PIPELINE_ID FROM CFG_PIPELINES
    WHERE PIPELINE_CODE='FILE_DELIVERY' AND ACTIVE_FLAG='Y');
```

The omitted parameters use a 30-second settling interval, a 60-second fallback scan,
a 60-minute timeout and a `FAILED`
outcome on timeout. Add a `PYTHON` task with `SCRIPT_FILE='python_scripts/delivery.py'` and a `SUCCESS`
dependency on `wait`. Its script reads `task.event_files`; alternatively,
`INPUT_PARAMS='{"files":"$$event_files"}'` supplies a list at `task.input_params['files']`.

The demo provides the full migration as `metadata/event_waits.sql` and its script as
`python_scripts/delivery.py`. After the normal demo setup, run from `examples/demo/`:

```bash
python prepare.py metadata metadata/event_waits.sql
etl-craft run --pipeline-code FILE_DELIVERY
# In another terminal, deliver a new filename:
mkdir -p inbox
printf 'id\n1\n' > inbox/batch-001.csv
```

The run waits, records the settled file and lets the script read it. A later run ignores
`batch-001.csv`; deliver `batch-002.csv` to trigger it. The scheduled version uses
`etl-craft controller start` with the project's connector configuration.

For a table instead of a file, replace `EVENT_FILE` with `EVENT_SQL`, such as
`SELECT delivery_id FROM source.deliveries WHERE ready=true`. Each run checks that query anew;
it must select rows for the delivery it intends to process. Query events do not consume rows.
See [Event waits](../guides/event-waits.md) for timeout and evidence behavior.
