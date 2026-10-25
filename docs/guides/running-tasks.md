# Running a task and reading its logs

## Running one task

```bash
etl-craft run --pipeline-code SALES_DAILY --task-code load_orders
```

The task receives its run from its caller. Pass `--run-id` or `--run-key` to select it;
without either, the command requires exactly one non-terminal run of the pipeline. It refuses
when none or several exist and lists candidates. A completed run is never selected by recency.
The whole-pipeline runner passes its selected run to every task. Use
`history --pipeline-code SALES --all` to find completed run ids.

The task does not run when:

- it already ended `SUCCESS` or `SKIPPED` under this run: a retry never repeats finished work (exit `0`);
- it is `IN-PROGRESS` under this run: it is never started twice (exit `22`, `INCOMPLETE`);
- its dependencies are not met yet: nothing is recorded, so run it again once they are (exit `23`, `WAITING`).

A task whose dependencies can never be met under this run, such as a `FAILURE` dependency on a task
that succeeded, is recorded `SKIPPED`, as is one whose
[dependencies on other pipelines](dependencies.md#dependencies-on-other-pipelines) are not
satisfied. See [Dependencies and run conditions](dependencies.md).

The task then runs in a process of its own. The command exits `0` when it ends `SUCCESS` and `1`
when it ends `FAILED`; every other status is listed under [Exit codes](../reference/exit-codes.md).

## Independent manual selections

```bash
etl-craft run --pipeline-code SALES_DAILY --task-code load_orders --standalone
etl-craft run --pipeline-code SALES_DAILY --task-code load_orders --standalone --with-downstream
```

The first command runs only the selected task. The second includes every active descendant
from `CFG_TASK_DEPENDENCY`, in dependency order. The selected ids are stored on the new run;
restarting a supervisor cannot expand it to unrelated tasks. No existing pipeline run is
adopted. Use `--run-key operator:orders` to name a new selection, or its exact `--run-id` or
`--run-key` with the original selection options to resume unfinished work. Successful tasks
are preserved. An ended manual run returns its existing result on a repeated named start; omit the selector
to request a new execution deliberately.

Dependencies outside the selection, including pipeline dependencies, are refused by default.
To use current warehouse inputs explicitly, add `--ignore-dependencies --reason "..."`.
Internal selected edges still determine order. The bypass and its actor/reason are audited.
Manual outcomes never become upstream publications or consume dependency revisions.

SQL selections hold the same exclusive pipeline admission slot as whole-pipeline work and
retain warehouse target locks and human-value protection. Finish or cancel the other exact
exclusive run before starting them. Email-only selections can overlap other pipeline work.
Python offsets, SQL watermarks, business-rule flags and event checkpoints currently have shared
state: standalone execution refuses those tasks, naming the available explicit pipeline-run
path. Pipeline pause still blocks
manual dispatch; resume the definition before starting a selection.

The API exposes the same request as `POST /api/v1/runs`, requiring the operator role. Its body
names `pipeline_code` and `task_code`, and accepts `standalone`, `run_id` or `run_key`,
`with_downstream`, `ignore_dependencies` (with `standalone`), `run_date` and `reason`. With `standalone: false`, it runs the task in the selected pipeline
run. The CLI uses this same operation against `--api`. Execution waits for its actual
outcome.

Run JSON includes `selected_task_ids` (null for a whole pipeline), `ignore_dependencies` and
`exclusive_run`. Selection survives disconnects in the Engine DB; foreground execution is
resumed explicitly after its ownership lease has been reconciled.

## Retries and `--force`

Local pipeline runs, the controller and ordinary `run --task-code` automatically retry failed,
timed-out or lost attempts when `RETRIES` permits another attempt. The task parameter overrides
`Orchestration.Retries`; without either, no automatic retry is enabled. `RETRY_DELAY_SECONDS`
defaults to 60 and `RETRY_BACKOFF` to 2.0. Delays grow after each failure and are capped at one
hour. Set the delay to `0` for immediate retries.

The next attempt is stored as `QUEUED` with `NOT_BEFORE`, so restarting the supervisor preserves
its due time and attempt budget. Waiting occupies no pipeline agent slot. Failure and always
dependencies wait for this retry to settle. Retry budgets count persisted attempt numbers under
one task-run identity; restarting a supervisor does not grant extra attempts.

Automatic retries also require an eligible failure class. `Orchestration.Retry_on` defaults to
`TRANSIENT`; the task parameter `RETRY_ON` overrides it:

| Policy | Eligible failures |
|---|---|
| `NONE` | none, including dropped connections |
| `TRANSIENT` | connection drops, serialization/deadlocks, resource shortages and timeouts |
| `TRANSIENT_AND_UNKNOWN` | transient failures and unclassified exceptions |
| `ANY` | every failure, including permanent errors; use deliberately |

Syntax, missing objects, privileges, invalid data and constraints are `PERMANENT`.
Configuration, metadata, usage and SQL input/target guards are also permanent. Unknown driver
errors and ordinary script exceptions are `UNKNOWN`. Trino internal errors remain unknown;
its external and resource errors are transient. Scripts can raise `TransientError` or
`PermanentError` from `etl_craft.scripting` (exit codes `28` and `29`) to declare their class.
A failure still needs budget under `RETRIES`; eligibility alone does not schedule another attempt.

Each attempt captures its effective `RETRY_ON` before execution and records `FAILURE_CLASS`,
`FAILURE_SOURCE`, `FAILURE_CODE` and derived `RETRYABLE` with its failure. `explain`,
`status --json`, the API attempt/run documents and catalog run details show the stored evidence,
such as `PERMANENT: UndefinedTable 42P01; RETRY_ON=TRANSIENT`. Later config edits do not rewrite
that decision. Upgraded attempts retain their old `RETRYABLE`, class `UNKNOWN` and an unrecorded
policy, because the old policy cannot be reconstructed.

Cancelled attempts never retry. Lost leases and supervisor timeouts remain transient process
failures. Lost attempts get a new owner only after reconciliation fences the old lease;
their side effects can be uncertain, so use retry-safe Python tasks. A process that exits
without reporting its outcome is unknown.


Running a `FAILED` task again is a new attempt on the same `AUD_TASK_RUN_LOG` row: `ATTEMPT_COUNT`
goes up, and the previous attempt's counts, message and log are cleared from the summary.
Each execution also has its own `AUD_TASK_ATTEMPTS` row. It moves through `QUEUED`, `CLAIMED`
and `RUNNING`; its terminal outcome is immutable. The attempt and summary receive the outcome,
counts and handler log in one transaction, so a retry preserves the earlier attempt's evidence.
A skipped task that never executed has a summary without an execution attempt.

Admission allows one active attempt per task run. A concurrent claim or a result from an old
attempt or wrong owner is refused with exit `20` (`STALE_TRANSITION`), naming the row, expected
status and owner, and state found. Check the run's history before retrying the command.

`--force` runs the task even if it already succeeded or its dependencies are not met.
When the explicitly selected run has already ended (`SKIPPED` included, except `CANCELLED`), the
run is reopened for the task, recorded as a `REOPEN` in its history, and ended again from its tasks'
statuses once the task ends: a forced task that fails leaves the run `FAILED`. If tasks of the run
have never run, the run stays `IN-PROGRESS` and `run --pipeline-code <code>` resumes it.

`--force` refuses a selected run that is `CANCELLED`. Start a new run with
`etl-craft run --pipeline-code <code> --init-only`, then run the task.

`--ignore-dependencies` and `--rerun` are the recorded, narrower overrides: see
[Stepping in](run-control.md#run-a-task-without-its-dependencies).

## Time limits

A task gets `Orchestration.Task_timeout_seconds` (six hours unless set; `0` for no limit), or its
own `TASK_TIMEOUT_SECONDS` parameter. When the limit passes, the task's process and everything it
started are stopped. Its attempt is recorded `TIMED_OUT`, and its task summary reads `FAILED`
with the reason.

## Logs

Each local attempt writes to its own log file:

```
<Log_dir>/<PIPELINE_CODE>/run-<pipeline_run_id>/<TASK_CODE>.attempt-<n>.log
```

`Orchestration.Log_dir` is `logs` beside `craft-connector.yml` unless set. The file holds everything
the task's process wrote: etl-craft's own log records, and anything a script prints or logs.
Every etl-craft record in it names the pipeline, task, run and attempt:

```
2026-09-25 10:15:02,114 INFO etl_craft.execution.child [task_run_id=412 pipeline=SALES_DAILY task=load_orders pipeline_run_id=97 attempt=2]: running the SQL handler
```

`--log-format json` writes the same records as one JSON object per line, with those names as
fields. `--log-level DEBUG` adds the detail: for example, the SQL each action runs.

Agents write to a temporary local file and send ordered UTF-8 chunks through the existing agent
API. After committing the task outcome, the API publishes the complete acknowledged chunks to
`Logs.Store` and records the location in `AUD_TASK_ATTEMPTS.LOG_PATH`. No agent opens a port or
needs log-store credentials. The default store is `file://<Log_dir>/agents`; clustered API and
controller copies must share that directory, or configure a shared S3 store with `Logs.Profile`.
Keys use immutable identities:
`engine-<Engine DB identity>/project-<id>/run-<id>/task-run-<id>/attempt-<id>.log`.
The existing Engine DB identity separates deployments sharing one store prefix.

`Logs.Tail_bytes` caps captured output in the task summary to 8192 UTF-8 bytes by default;
reported `NAME = value` lines are additional. Zero keeps reported values only. Incomplete UTF-8
characters at the boundary are omitted. Existing larger `TASK_LOG` values are preserved during
upgrade and stay in their existing audit rows until retention expires them.

`AUD_TASK_RUN_LOG` keeps the outcome of the latest attempt:

| Column | Holds |
|---|---|
| `STATUS` | `SUCCESS`, `FAILED`, `SKIPPED`, or `CANCELLED` when its run was [stopped or aborted](run-control.md#stop-or-abort-a-run) |
| `ERROR_MESSAGE` | the one-line cause of a failure or a skip |
| `SOURCE_COUNT`, `TARGET_COUNT`, `INSERT_COUNT`, `UPDATE_COUNT`, `DELETE_COUNT` | the counts the task reported |
| `ROWS_WRITTEN` | the rows the attempt inserted, updated or deleted; a `HAS_DATA` dependency is met when it is above 0 |
| `REJECT_COUNT` | the rows a SQL task with `REJECT_LIMIT` set aside in `<target>__rejects` |
| `TASK_LOG` | the values the task reported, one `NAME = value` per line, then the end of its log file |

A task whose process ends without reporting an outcome (it crashed, was killed, or ran out of
time) is recorded `FAILED` with what happened, for example
`the task process was killed by signal SIGKILL before recording an outcome`, and the end of its
log shows what it was doing. A task process writes its output unbuffered, so the log keeps
everything it printed up to the end. Once the outcome is recorded the process exits at once:
threads a script left running end with it, and are named in the log.

Local immutable attempts keep the handler's reported values; their supervisor appends captured
output to the task summary after the process ends. Agents commit reported values and the
bounded chunk tail in both attempt and task summary. Full output remains in the attempt file.
Supervisors renew attempt leases every 15 seconds.
Expired leases are reconciled before the next run or by `etl-craft reconcile`; see
[recovery and run controls](run-control.md#mark-a-task).
