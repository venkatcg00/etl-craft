# Waiting for files or warehouse rows

A local pipeline can start on its schedule and wait for a source to arrive. Add a task with
`HANDLER='EVENT_WAIT'` before the tasks that read the source. The controller checks the event
without submitting an agent; a whole-pipeline `run` does the same. A frequent schedule with
`OVERLAP_POLICY='SKIP'` leaves one run waiting instead of building a queue of empty runs.

| Parameter | Value |
|---|---|
| `EVENT_FILE` | a filesystem glob, absolute or relative to the directory holding `craft-connector.yml` |
| `EVENT_SQL` | one read-only SELECT on the configured warehouse; any returned row satisfies it |
| `EVENT_SETTLE_SECONDS` | seconds a file's size and modification time must stay unchanged; default `30` |
| `EVENT_TIMEOUT_MINUTES` | total wait budget in minutes; default `60`; `NONE` waits indefinitely; `0` checks files once and allows no SQL connection wait |
| `EVENT_POLL_SECONDS` | seconds between SQL probes or fallback file scans; default `60`, minimum `1` |
| `EVENT_ON_TIMEOUT` | `FAIL` (default) or `SKIP` |

Set exactly one of `EVENT_FILE` and `EVENT_SQL`. Numeric budgets must be finite and nonnegative. Set `EVENT_TIMEOUT_MINUTES=NONE` to wait until the event arrives or the run is cancelled.
The file matcher ignores directories and canonicalizes paths, including symbolic links. It
records every settled, unconsumed match. An already old file still gets a full observed settling
interval. A change to either size or modification time restarts that interval.

`AUD_GATE_WAITS` stores `WAIT_KIND='EVENT'`, the original deadline, the next check and file
observations. An indefinite wait has a NULL deadline; restarting an controller preserves it.
File waits use native filesystem notifications through watchdog (inotify on Linux and FSEvents
on macOS) to bring a check forward. The scheduler checks notification hints within one second,
without admitting a task agent. Notifications watch the fixed directory prefix of the glob,
including future subdirectories when needed; read access and unrelated file writes are ignored.
A timer checks a matching file when its settling interval ends. Periodic scans at
`EVENT_POLL_SECONDS` recover missed notifications, network filesystem changes and unavailable
watch handles. Globs without a fixed directory prefix use the fallback scan. Each resumed file
wait checks once immediately for deliveries received during downtime.

SQL waits probe at `EVENT_POLL_SECONDS`, close their connections between probes and use the
normal warehouse connection and single-writer lock. A busy single-writer
warehouse leaves the event waiting. A query or filesystem error fails the task with its cause.
SQL probes run in fresh interpreters outside the scheduler loop, without agent slots.
Each probe gets at most 30 seconds or the remaining event budget, whichever is shorter;
a slow probe is stopped and checked again while budget remains. Interruption stops the local
probe, which also exits if its supervisor dies. The original deadline is committed before querying. Keep queries short: stopping
the local client does not guarantee that a remote warehouse immediately cancels its query.

Idle waits retain only durable metadata, cached deadlines and file watch handles; they do not
hold a warehouse connection or agent slot. The existing supervisor still performs cancellation,
pause and lease checks, and the controller still manages schedules and deployment health. Resource
usage is low rather than zero. Choose a longer polling interval for expensive queries or remote
filesystems; this increases fallback detection latency. Use a dedicated inbox to keep recursive
watch counts small.

Upstream pipeline success already uses `CFG_PIPELINE_DEPENDENCY` with `DEPENDENCY_TYPE='SUCCESS'`;
that existing gate has its own finite `Orchestration.Gate_wait_minutes` budget. A webhook or
message broker can signal a file or insert a warehouse row for an event task to watch. There is
no generic notification receiver in this handler.

At timeout, the task ends `FAILED` or `SKIPPED`; ordinary `SUCCESS`, `FAILURE`, `ALWAYS` and
`HAS_DATA` dependency rules decide which tasks follow it. Use `SUCCESS` to follow a satisfied event;
an event task reports no target rows, so `validate` refuses `HAS_DATA` on it. Timeouts and probe errors do not
automatically retry or receive a fresh budget. To wait for a new delivery, start a new run.

## Evidence and downstream files

The task summary and `AUD_TASK_ATTEMPTS.TASK_LOG` contain `EVENT_EVIDENCE`: file paths, byte sizes and
nanosecond modification times, or the query's first row. `AUD_TASK_EVENTS` keeps that evidence
as immutable JSON. File consumption in `AUD_EVENT_FILES` commits with the successful attempt
and its dependency consumption. A failed completion consumes nothing. Once this event task
consumes a canonical path, later runs do not match it again, even if the file is rewritten;
use a new filename for each delivery. Consumption is scoped to the event task's `TASK_ID`.
An explicitly repeated successful event task keeps its evidence and does not claim new files.

A SQL file token without a successful file wait fails at runtime; metadata-only validation
and lineage use a placeholder instead.

Downstream SQL receives the paths from successful event tasks under its exact pipeline run:
`$$event_files` becomes comma-separated, escaped SQL string literals. For example, on DuckDB:

```sql
SELECT * FROM read_csv([$$event_files], header=true)
```

Python scripts receive `task.event_files`, a tuple of canonical paths. A top-level
`INPUT_PARAMS` value exactly equal to `"$$event_files"` becomes a JSON-style list of paths:

```json
{"files": "$$event_files"}
```

```python
from pathlib import Path
from etl_craft.scripting import ScriptResult


def run(task):
    rows = sum(len(Path(path).read_text().splitlines()) - 1 for path in task.event_files)
    return ScriptResult(row_count=max(0, rows))
```

A single-task `run --task_code` performs one check and exits `23` (`WAITING`) if the event is
not ready; it admits no agent or execution attempt. Resume with the same run identity or let
the local pipeline runner or controller continue. `explain` reports its durable event wait.


See the [complete file-delivery example](../examples/event-wait.md).
