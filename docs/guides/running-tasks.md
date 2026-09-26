# Running a task and reading its logs

## Running one task

```bash
etl-craft run --pipeline_code SALES_DAILY --task_code load_orders
```

The task runs under its pipeline's active run: the one `IN-PROGRESS` run in `AUD_PIPELINES_RUN_LOG`.
Nothing passes it a run id. If the pipeline has no active run, the command stops with exit status
`9` (`RUN_STATE`); start one with `run --pipeline_code <code> --init-only`, or
[run the whole pipeline](running-pipelines.md).

In local mode, the task does not run, and the command exits `0`, when:

- it already ended `SUCCESS` or `SKIPPED` under this run: a retry never repeats finished work;
- it is `IN-PROGRESS` under this run: it is never started twice;
- its dependencies are not met yet: nothing is recorded, so run it again once they are.

A task whose dependencies can never be met under this run, such as a `FAILURE` dependency on a task
that succeeded, is recorded `SKIPPED`, as is one whose
[dependencies on other pipelines](dependencies.md#dependencies-on-other-pipelines) are not
satisfied. See [Dependencies and run conditions](dependencies.md).

The task then runs in a process of its own. The command exits `0` when it ends `SUCCESS` and `1`
when it ends `FAILED`; every other status is listed under [Exit codes](../reference/exit-codes.md).

## Retries and `--force`

Running a `FAILED` task again is a new attempt on the same `AUD_TASK_RUN_LOG` row: `ATTEMPT_COUNT`
goes up, and the previous attempt's counts, message and log are cleared from the row.

`--force` runs the task even if it already succeeded or its dependencies are not met, and may
bind a run that already finished. It is only available in local mode.

## In remote mode

The orchestrator decides when a task runs, so `run --task_code` runs it whenever it is told to:
none of the checks above apply. Run again after it succeeded, it is a new attempt that skips
nothing it did before; run after its run ended (a cleared task), it reopens that run. See
[Running under an orchestrator](../deploying/orchestrator.md).

## Time limits

A task gets `Orchestration.Task_timeout_seconds` (six hours unless set; `0` for no limit), or its
own `TASK_TIMEOUT_SECONDS` parameter. When the limit passes, the task's process and everything it
started are stopped, and the task is recorded `FAILED` with the reason.

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
| `TASK_LOG` | the values the task reported, one `NAME = value` per line, then the end of its log file |

A task whose process ends without reporting an outcome (it crashed, was killed, or ran out of
time) is recorded `FAILED` with what happened, for example
`the task process was killed by signal SIGKILL before recording an outcome`, and the end of its
log shows what it was doing.
