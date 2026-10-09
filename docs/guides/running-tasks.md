# Running a task and reading its logs

## Running one task

```bash
etl-craft run --pipeline_code SALES_DAILY --task_code load_orders
```

The task receives its run from its caller. Pass `--run-id` or `--run-key` to select it;
without either, the command requires exactly one non-terminal run of the pipeline. It refuses
when none or several exist and lists candidates. A completed run is never selected by recency.
The whole-pipeline runner passes its selected run to every task. Use
`history --pipeline_code SALES --all` to find completed run ids.

In local mode, the task does not run when:

- it already ended `SUCCESS` or `SKIPPED` under this run: a retry never repeats finished work (exit `0`);
- it is `IN-PROGRESS` under this run: it is never started twice (exit `22`, `INCOMPLETE`);
- its dependencies are not met yet: nothing is recorded, so run it again once they are (exit `23`, `WAITING`).

A task whose dependencies can never be met under this run, such as a `FAILURE` dependency on a task
that succeeded, is recorded `SKIPPED`, as is one whose
[dependencies on other pipelines](dependencies.md#dependencies-on-other-pipelines) are not
satisfied. See [Dependencies and run conditions](dependencies.md).

The task then runs in a process of its own. The command exits `0` when it ends `SUCCESS` and `1`
when it ends `FAILED`; every other status is listed under [Exit codes](../reference/exit-codes.md).

## Retries and `--force`

Local pipeline runs, the overseer and ordinary `run --task_code` automatically retry failed,
timed-out or lost attempts when `RETRIES` permits another attempt. The task parameter overrides
`Orchestration.Retries`; without either, no automatic retry is enabled. `RETRY_DELAY_SECONDS`
defaults to 60 and `RETRY_BACKOFF` to 2.0. Delays grow after each failure and are capped at one
hour. Set the delay to `0` for immediate retries.

The next attempt is stored as `QUEUED` with `NOT_BEFORE`, so restarting the supervisor preserves
its due time and attempt budget. Waiting occupies no pipeline worker slot. Failure and always
dependencies wait for this retry to settle. Retry budgets count persisted attempt numbers under
one task-run identity; restarting a supervisor does not grant extra attempts.

Configuration, metadata, usage and SQL input/target guards are not retryable. The child stores
that decision in `AUD_TASK_ATTEMPTS.RETRYABLE`; a NULL merge key, for example, requires source
correction and ends after one attempt. Cancelled attempts never retry. Lost attempts get a new
owner only after reconciliation fences the old lease; their side effects can be uncertain, so
use retry-safe ingestion scripts. Remote mode continues to delegate retries to its orchestrator
and generated DAGs retain their Airflow retry settings.


Running a `FAILED` task again is a new attempt on the same `AUD_TASK_RUN_LOG` row: `ATTEMPT_COUNT`
goes up, and the previous attempt's counts, message and log are cleared from the summary.
Each execution also has its own `AUD_TASK_ATTEMPTS` row. It moves through `QUEUED`, `CLAIMED`
and `RUNNING`; its terminal outcome is immutable. The attempt and summary receive the outcome,
counts and handler log in one transaction, so a retry preserves the earlier attempt's evidence.
A skipped task that never executed has a summary without an execution attempt.

Admission allows one active attempt per task run. A concurrent claim or a result from an old
attempt or wrong owner is refused with exit `20` (`STALE_TRANSITION`), naming the row, expected
status and owner, and state found. Check the run's history before retrying the command.

`--force` runs the task even if it already succeeded or its dependencies are not met. It is only
available in local mode. When the explicitly selected run has already ended (`SKIPPED` included, except `CANCELLED`), the
run is reopened for the task, recorded as a `REOPEN` in its history, and ended again from its tasks'
statuses once the task ends: a forced task that fails leaves the run `FAILED`. If tasks of the run
have never run, the run stays `IN-PROGRESS` and `run --pipeline_code <code>` resumes it.

`--force` refuses a selected run that is `CANCELLED`. Start a new run with
`etl-craft run --pipeline_code <code> --init-only`, then run the task.

`--ignore-dependencies` and `--rerun` are the recorded, narrower overrides: see
[Stepping in](run-control.md#run-a-task-without-its-dependencies).

## In remote mode

The orchestrator decides when a task runs, so `run --task_code` runs it whenever it is told to.
Dependency and settled-task checks are delegated to it; the Engine DB still refuses a second
active attempt for the same task run. Run again after it succeeded, it is a new attempt that skips
nothing it did before; run after its run ended (a cleared task), it reopens that run. See
[Running under an orchestrator](../deploying/orchestrator.md).

## Time limits

A task gets `Orchestration.Task_timeout_seconds` (six hours unless set; `0` for no limit), or its
own `TASK_TIMEOUT_SECONDS` parameter. When the limit passes, the task's process and everything it
started are stopped. Its attempt is recorded `TIMED_OUT`, and its task summary reads `FAILED`
with the reason.

## Logs

Each attempt writes to its own log file:

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

`AUD_TASK_RUN_LOG` keeps the outcome of the latest attempt:

| Column | Holds |
|---|---|
| `STATUS` | `SUCCESS`, `FAILED`, `SKIPPED`, or `CANCELLED` when its run was [cancelled](run-control.md#cancel-a-run) |
| `ERROR_MESSAGE` | the one-line cause of a failure or a skip |
| `SOURCE_COUNT`, `TARGET_COUNT`, `INSERT_COUNT`, `UPDATE_COUNT`, `DELETE_COUNT` | the counts the task reported |
| `ROWS_WRITTEN` | the rows the attempt inserted, updated or deleted; a `HAS_DATA` dependency is met when it is above 0 |
| `TASK_LOG` | the values the task reported, one `NAME = value` per line, then the end of its log file |

A task whose process ends without reporting an outcome (it crashed, was killed, or ran out of
time) is recorded `FAILED` with what happened, for example
`the task process was killed by signal SIGKILL before recording an outcome`, and the end of its
log shows what it was doing. A task process writes its output unbuffered, so the log keeps
everything it printed up to the end. Once the outcome is recorded the process exits at once:
threads a script left running end with it, and are named in the log.

The immutable attempt keeps the handler's reported values. The supervisor appends the captured
output tail to the task summary once the process ends; the complete output remains in that
attempt's log file. Supervisors renew attempt leases every 15 seconds.
Expired leases are reconciled before the next run or by `etl-craft reconcile`; see
[recovery and run controls](run-control.md#mark-a-task).
