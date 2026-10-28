# Python scripts

A task with `HANDLER = 'PYTHON'` runs your team's Python code. Ingestion is one use: it reads
its source from where the last run left off and writes a warehouse table. The engine runs it,
captures everything it prints and logs, records its counts, and keeps its offset.

Project upgrades move `ingestion_scripts/` to `python_scripts/` and change each Python task's
`SCRIPT_NAME` parameter to `SCRIPT_FILE` in its config CSV, written as a path from the project
directory (`python_scripts/load.py` instead of `load.py`). `etl-craft migrate` updates existing
Engine DB parameter rows the same way. Rename `Orchestration.Local_ingestion_slots` to `Local_python_slots`
in `craft-connector.yml`; the old setting and folder are not aliases. SQL files move from
`sql_files/` to `sql_scripts/`, shared by managed SELECTs and unmanaged SQL scripts, and
`SCRIPT_FILE` values gain the `sql_scripts/` prefix.

## The task

| Parameter | Value |
|---|---|
| `SCRIPT_FILE` | the script's path from the project directory, inside `python_scripts/`, such as `python_scripts/crm/customers.py` |
| `INPUT_PARAMS` | optional: a JSON object the script receives as a dictionary, such as `{"region": "eu", "days": 30}` |
| `SOURCE_OBJECT` | optional: where the script reads from, for the [catalog](catalog.md)'s lineage graphs |

## The script

The script defines `run`, which takes a `ScriptTask` and returns a `ScriptResult`:

```python
from etl_craft.scripting import Offset, ScriptResult, ScriptTask


def run(task: ScriptTask) -> ScriptResult:
    region, days = task.input_params["region"], task.input_params["days"]
    since = task.offset.value if task.offset else 0  # None on the first run
    rows = fetch_orders(region, after_id=since, days=days)  # your own code
    task.logger.info("fetched %d orders for %s", len(rows), region)
    with task.warehouse() as engine, engine.begin() as conn:
        written = insert_orders(
            conn,
            rows,
            pipeline_id=task.pipeline_id,
            pipeline_run_id=task.pipeline_run_id,
            task_run_id=task.task_run_id,
        )
    return ScriptResult(
        row_count=written,
        offset=Offset.number(max(r.id for r in rows)) if rows else None,
    )
```

`ScriptTask` gives the script:

| Attribute | Holds |
|---|---|
| `offset` | where the last successful run left off, an `Offset` with its `value` and `datatype`, or `None` on the first run |
| `input_params` | `INPUT_PARAMS` as a dictionary, empty when unset |
| `pipeline_id` | the pipeline definition's id; stamp it on every row you write |
| `pipeline_run_id` | the pipeline execution's id; stamp it on every row you write |
| `task_run_id` | this task execution's id, shared by its retries; stamp it on every row you write |
| `refresh_type` | the pipeline's `FULL` or `INCREMENTAL` |
| `force` | true when the task was run with `--force` |
| `run_date` | the date the run runs as of: the day it started (UTC), the `--run-date` it was given, or the date of a backfill run |
| `backfill` | true in a [backfill](run-control.md#backfill-over-dates) run: `offset` is then `None`, and an offset the script returns is not stored, so read the source for `run_date` |
| `warehouse()` | a connection to the warehouse, queued behind other writers where the warehouse allows only one (a DuckDB file) |
| `table(name)` | `schema.table` as its full name in the active warehouse database, such as `analytics.raw.orders`, so the same script writes to each environment's database |
| `logger` | a logger named after the task |
| `task_params`, `config` | the task's parameters and the loaded `craft-connector.yml` |

`ScriptTask.engine_db` has been removed: scripts never access the Engine DB directly.
`task.warehouse()` remains available locally and on agents, including its shared single-writer
queue. On agents, `task.config` contains no Engine DB connection or credentials.

Repeated `task.warehouse()` calls reuse the task's engine and pooled session, including its
session setup and attempt tag. The engine closes when the task ends; on a single-writer
warehouse the task retains its reservation until those connections close. A new task gets a
new engine. Pooled connections using minted credentials are recycled and authenticate afresh;
do not use temporary tables to carry durable state between tasks or across credential refreshes.

`ScriptResult` holds `row_count`, the rows the script wrote, a required whole number that the
engine records as the task's source, target and insert counts; `offset`, the new offset, or `None`
to keep the stored one; and `variables`, a mapping of names to any values of your own, listed
in the task log. The engine validates the whole result before storing its offset.

A script that needs neither the offset nor `INPUT_PARAMS` can define `run()` without a parameter;
it still returns a `ScriptResult`.

An offset is a value and its datatype, and the datatype is always declared: `Offset(1042,
"NUMBER")`, `Offset("cursor-9", "TEXT")` or `Offset(latest, "TIMESTAMP")`, or the shorthands
`Offset.number(1042)`, `Offset.text("cursor-9")` and `Offset.timestamp(latest)`. The value is cast
to its datatype, as `CAST(value AS type)` would in SQL: `Offset("1042", "NUMBER")` holds the number
1042, and `Offset("abc", "NUMBER")` fails. The type is never guessed from the value.

The offset is stored as text beside its datatype, so one column holds every kind. The next run
gets it back cast to that datatype: `task.offset.value` is an `int` or `Decimal` for `NUMBER`, a
`str` for `TEXT` and a `datetime` for `TIMESTAMP`, and `task.offset.datatype` names it.
Timestamp offsets keep microsecond precision. A datetime with nonzero nanoseconds, such as a
pandas `Timestamp`, is refused; round it to microseconds before building the offset.

A source with a position per partition, such as a Kafka topic, returns a `PARTITIONS` offset:
`Offset.partitions({"0": 1042, "1": 977})`, a map of partition key (text) to the next position
to read (a whole number, 0 or more). It is stored as JSON with sorted keys, and the next run
gets a read-only mapping back. The engine refuses the result, and stores nothing, when a
partition moves behind its stored position or a stored partition is missing; a new partition is
accepted and named in the task log. A partition that really went away is removed with
`etl-craft mark --offset` (see [Stepping in](run-control.md#reset-a-python-scripts-offset)),
never by leaving it out. [Reading from streams](streams.md) shows the whole pattern.

The offset is stored in `AUD_TASK_OFFSET_TRACKER` if and only if the attempt is recorded
`SUCCESS`. The attempt outcome, task summary, new offset and admitted task-dependency consumption
commit in one Engine DB transaction. A crash before that commit preserves the previous offset;
a crash after it preserves the complete success. A failed attempt is retried from the same place,
and the offset keeps its datatype from one run to the next. Warehouse writes made by the script
commit separately; make them safe to repeat when an attempt must be retried.

Scripts may import helper modules kept beside them in `python_scripts/`.
Each script is compiled from its current source and registered as a module named
`etl_craft_script_` followed by its path relative to `python_scripts/`, with `/` and `.`
replaced by `_`: `crm/customers.py` becomes `etl_craft_script_crm_customers_py`. The script's
own future imports apply; dataclasses, enums, pickling and `typing.get_type_hints` work as in
other modules. A script may use `ProcessPoolExecutor` with the `fork` start method on platforms
that support it. The `spawn` start method cannot re-import the script by its generated module
name; put functions needed by spawned agents in a normally importable helper module.

## Output and logs

The script runs inside the task's own process. Everything it prints, writes to stderr or logs
through `logging` goes to the attempt's log file, and log records carry the task's context.
HTTP and SDK libraries (`urllib3`, `httpx`, `httpcore`, `requests`, `botocore`, `boto3` and
`s3transfer`) are captured at WARNING or above unless etl-craft's log level is DEBUG, to keep
request URLs and credential query strings out of routine logs. DEBUG can include those details.
A script must never print or log secrets, including in its own request URLs or exception messages.

For example:

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

- `SCRIPT_FILE` is missing, does not start with `python_scripts/`, points outside that folder, or
  names no `.py` file there;
- `INPUT_PARAMS` is not a JSON object;
- the script cannot be imported, or defines no `run`, or a `run` that takes more than the task;
- `run` raises: the message has the exception, and the traceback is in the attempt's log;
- `run` calls `sys.exit`: raise an exception to fail, return a `ScriptResult` to succeed;
- `run` returns something other than a `ScriptResult`, a `row_count` that is not a whole number of
  0 or more, an offset of a different type from the stored one, or `variables` that is not a mapping;
- a timestamp offset has nanoseconds the offset store cannot keep.

The task's time limit covers the script: a script still running when it passes is stopped, and the
task is recorded `FAILED`.

## Files from event waits

A downstream script receives `task.event_files` from successful file waits under its exact
pipeline run. A top-level `INPUT_PARAMS` value `"$$event_files"` becomes a list of those paths.
Keyed SQL waits also supply `task.event_keys` (a tuple), `task.event_rows` (a tuple of mappings
of the other selected columns), and the `"$$event_keys"` input token. All come from the exact
run's saved successful evidence, including after recovery. See [external signals](external-events.md).
See [Event waits](event-waits.md) for settling, evidence and consumption.

Scripts receive optional `TARGET_OBJECT` from the task's active `TARGET_TABLE_ID` reference.
Set the separate target components in `tasks.csv`; do not write a target parameter row.


## Declaring a retryable failure

An ordinary script exception is `UNKNOWN`, including a driver exception raised by a script,
so the default retry policy stops after that attempt.
Raise `TransientError` for a failure another attempt can resolve, or `PermanentError` when the
input or configuration must change:

```python
from etl_craft.scripting import PermanentError, TransientError

# In run(task), after checking the source response:
# raise TransientError("orders source: HTTP 503; retry after the source recovers")
# raise PermanentError("orders.csv: missing order_id; supply the required column")
```

The [retry policy](running-tasks.md#retries-and-force) still controls whether that class is
eligible and how many additional attempts are allowed. The attempt retains the original
exception class even when the script handler adds its script-name diagnostic.

## Work items inside a task

Use `task.fan_out(keys, fn, *, max_items, max_parallel=None, retries=0,
processes=False)` when a script discovers independent files, pages or partitions. `keys` is
an iterable of unique, nonempty text identifiers (at most 512 characters each). The function
receives one key and returns a `ScriptResult` with its row count and no offset. The helper
returns a `ScriptResult` containing the total rows, including successful items retained from
previous attempts. Return the task's offset only after the helper succeeds.

```python
from etl_craft.scripting import ScriptResult


def run(task):
    def load(key):
        rows = read_page(key)  # Your source client, using the key as its stable identity.
        replace_landing_batch(task, key, rows)
        return ScriptResult(len(rows))

    return task.fan_out(discover_page_keys(), load, max_items=1000, max_parallel=16, retries=2)
```

`max_items` is required. Discovery reads at most one key beyond that bound, and refuses an
oversized list, duplicate key or NULL key before recording or executing any items. An empty
list returns zero rows immediately. The bound also covers keys retained from earlier attempts:
unfinished keys remain work even if a fresh listing omits them.

Threads are the default. The default width is the smaller of 16 and the memory bound. Each
worker requires 2 MB of the task's admitted `MEMORY_REQUEST_MB`, with a ceiling of 256 threads;
CPU count does not reduce I/O width. Requesting more fails before any work starts. This is a
scheduling allowance, not a limit on memory an arbitrary Python function can allocate. Size the
task's reservation for its actual workload and set `max_parallel` at or below the source's
rate limit. Items share the task's one slot and reservation.

`processes=True` uses spawned processes, requires an importable top-level function and allows
64 MB per worker, bounded also by admitted CPU. Put that function in an importable project
module; a nested function, lambda or captured `task` is refused. Use this mode for independent
processing with its own explicitly passed inputs. Threads can close over `task` and use
`task.warehouse()`. The helper serializes those warehouse contexts within the task, retains
the existing warehouse reservation, and preserves the attempt's session tag. Open warehouse
contexts inside the item function, never around `fan_out` itself.

Every item start and ending is append-only evidence in `AUD_TASK_ITEMS`, attributed to the
requesting actor. Agents read and write it through the API; they open no Engine DB connection.
Completed items are recorded as they finish, with at most `max_parallel` submitted futures.
Retries skip committed successes and retry only unfinished items. A timeout, lost attempt or
controller restart keeps finished evidence. Explicit `PermanentError` failures are not retried, and retain that class on the task.
Every item failure remaining after its retries
fails the task, naming its key and error. The attempt's offset is not committed on failure.

### Repeatable landing batches

The write and the item result are separate commits. A process can stop between them, so a
retried item must replace its own batch or merge by a stable key. Tag landing rows with
`task.task_run_id` and the item key; delete and replace that pair in one warehouse transaction.
Do not append the same batch on retry. Use bound SQL parameters for keys. The
[stream landing rules](streams.md) apply here too: one downstream
managed SQL task performs the final target write. See the
[executable example](../examples/python_scripts/fan_out.py).

### Inspecting and controlling items

```bash
etl-craft items list --pipeline-code P --task-code load --run-id 42
etl-craft run --pipeline-code P --task-code load --run-id 42 --rerun \
  --item-key page-17 --reason 'Source repaired'
etl-craft items mark --pipeline-code P --task-code load --run-id 42 \
  --all-failed --status SKIPPED --reason 'Pages deliberately excluded'
etl-craft items cancel --pipeline-code P --task-code load --run-id 42 \
  --item-key page-18 --reason 'Withdrawn by the source owner'
```

All controls require an operator grant and a reason. Each decision records its item key in
`AUD_RUN_INTERVENTIONS`. Select exactly one key or `--all-failed`. A rerun requires the task to
have ended, reuses the existing task-rerun path and executes only the selected items; other
failed items remain failed. New discovery keys are refused during an item-scoped rerun;
use a full-task retry or a new run to admit them. Marking changes item evidence; retry the task to settle its
summary after all required items are successful or skipped.

Cancelling a queued item prevents it from starting. Cancelling a running thread records
`CANCEL_REQUESTED`: it remains pending until the function returns, then becomes `CANCELLED`.
It does not pretend to interrupt a thread or roll back a write. Cancelling the whole task
uses the existing process and query cancellation; it stops all its workers. A pending
cancellation on a lost owner stays unknown until its ending can be established.

The attempt log reports progress and a projected finish time. The failed count includes
cancelled items; they need a rerun or an explicit skip before the task can succeed. `status`, `explain`, the run
API and catalog show counts from the same item rows. `items list` returns each key's current
status, item attempt number, row count, timestamps, error and task attempt id; that id links to
the existing attempt log API. The item API routes and their exact requests are in the
[operations reference](../reference/actions.md). Interactive graph strips and runs-grid
segments are delivered with the S7.A web UI.

## Published project files

A bundled run uses the admitted Python files and metadata for every attempt. Editing the
working tree does not change it. Install imported dependencies in the agent image and check
with `etl-craft doctor --agent --api URL`. See [project bundles](../deploying/project-bundles.md).
Every run uses the version that last passed `etl-craft pipelines refresh`. A script runs from
that version's verified copy, with the project directory as its working directory, so relative
paths it writes land in the project directory and the copy stays unchanged. Read the version's
own files through `task.config.project_dir`; bytecode is not written into the cache.
