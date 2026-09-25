# Ingestion scripts

A task with `HANDLER = 'PYTHON'` runs one of your team's Python scripts to bring data in: it reads
its source from where the last run left off and writes a warehouse table. The engine runs it,
captures everything it prints and logs, records its counts, and keeps its offset.

## The task

| Parameter | Value |
|---|---|
| `SCRIPT_NAME` | the script, a path inside the project's `ingestion_scripts/` folder, such as `crm/customers.py` |
| `INPUT_PARAMS` | optional: a JSON array the script receives as a list, such as `["eu", 30]` |

## The script

The script defines `run`, which takes a `ScriptTask` and returns a `ScriptResult`:

```python
from etl_craft.scripting import Offset, ScriptResult, ScriptTask


def run(task: ScriptTask) -> ScriptResult:
    region, days = task.input_params
    since = task.offset.value if task.offset else 0  # None on the first run
    rows = fetch_orders(region, after_id=since, days=days)  # your own code
    task.logger.info("fetched %d orders for %s", len(rows), region)
    with task.warehouse() as engine, engine.begin() as conn:
        written = insert_orders(conn, rows, pipeline_run_id=task.pipeline_run_id)
    return ScriptResult(
        row_count=written,
        offset=Offset.number(max(r.id for r in rows)) if rows else None,
    )
```

`ScriptTask` gives the script:

| Attribute | Holds |
|---|---|
| `offset` | where the last successful run left off, an `Offset`, or `None` on the first run |
| `input_params` | `INPUT_PARAMS` as a list, empty when unset |
| `pipeline_run_id` | the run's id; stamp it on every row you write |
| `refresh_type` | the pipeline's `FULL` or `INCREMENTAL` |
| `force` | true when the task was run with `--force` |
| `warehouse()` | a connection to the warehouse, queued behind other writers where the warehouse allows only one (a DuckDB file) |
| `logger` | a logger named after the task |
| `task_params`, `config` | the task's parameters and the loaded `craft-connector.yml` |

`ScriptResult` holds `row_count`, the rows the script wrote, a required whole number that the
engine records as the task's source, target and insert counts; `offset`, the new offset, or `None`
to keep the stored one; and `variables`, any values of your own, listed in the task log.

A script that needs neither the offset nor `INPUT_PARAMS` can define `run()` without a parameter;
it still returns a `ScriptResult`.

An offset is a number, a text or a timestamp: `Offset.number(1042)`, `Offset.text("cursor-9")`,
`Offset.timestamp(latest)`. It is stored in `AUD_TASK_OFFSET_TRACKER` only when the script
succeeds, so a failed run is retried from the same place. An offset keeps its type from one run to
the next.

Scripts may import helper modules kept beside them in `ingestion_scripts/`.

## Output and logs

The script runs inside the task's own process. Everything it prints, writes to stderr or logs
through `logging` goes to the attempt's log file, and log records carry the task's context:

```
2026-09-25 10:15:02,114 INFO etl_craft_script.load_customers [task_run_id=412 pipeline=CRM task=load_customers pipeline_run_id=97 attempt=1]: fetched 120 orders for eu
```

The engine adds its own lines around it: the offset and inputs the script started from, and what
it returned. `AUD_TASK_RUN_LOG` records the row count as the source, target and insert counts, and
`TASK_LOG` lists them with the new offset and the script's variables:

```
SOURCE_COUNT = 120
TARGET_COUNT = 120
INSERT_COUNT = 120
OFFSET = 1042 (NUMBER)
```

## When something is wrong

The task fails with a message that names the script and the problem when:

- `SCRIPT_NAME` is missing, points outside `ingestion_scripts/`, or names no `.py` file there;
- `INPUT_PARAMS` is not a JSON array;
- the script cannot be imported, or defines no `run`, or a `run` that takes more than the task;
- `run` raises: the message has the exception, and the traceback is in the attempt's log;
- `run` calls `sys.exit`: raise an exception to fail, return a `ScriptResult` to succeed;
- `run` returns something other than a `ScriptResult`, a `row_count` that is not a whole number of
  0 or more, or an offset of a different type from the stored one.

The task's time limit covers the script: a script still running when it passes is stopped, and the
task is recorded `FAILED`.
