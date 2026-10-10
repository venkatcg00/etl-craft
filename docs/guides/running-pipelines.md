# Running a pipeline

## Running every task

```bash
etl-craft run --pipeline_code SALES_DAILY
```

In local mode this runs every active task under one pipeline run. Each completion releases any
newly ready tasks immediately; a slow independent task does not hold back another branch.
At most `Orchestration.Max_parallel_tasks` tasks run at once (8 unless set), each in its own
process with the same logs, time limits and outcomes as [`run --task_code`](running-tasks.md).
Cross-pipeline gate waits persist their deadline and next look without holding a worker slot.

```
INFO etl_craft.execution.scheduler [pipeline=SALES_DAILY pipeline_run_id=97]: SALES_DAILY: dispatch extract_orders
INFO etl_craft.execution.scheduler [pipeline=SALES_DAILY pipeline_run_id=97]: SALES_DAILY: dispatch extract_customers
INFO etl_craft.execution.scheduler [pipeline=SALES_DAILY pipeline_run_id=97]: SALES_DAILY: dispatch load_orders
```

The run ends `SUCCESS` when every data task is `SUCCESS` or `SKIPPED`, `SKIPPED` when every data task is
`SKIPPED`, and `FAILED` otherwise; the command exits `1` for a failed run and `0` otherwise. A failed run names each task that did not succeed:

```
SALES_DAILY: pipeline_run_id=97 FAILED — 2 task(s) did not succeed: load_orders (FAILED), publish (never started); 1 of them could not start because their dependencies were not met
```

A task whose dependencies can never be met under the run is recorded `SKIPPED`, and stays
`SKIPPED`: see [Dependencies and run conditions](dependencies.md).

## Before the run starts

The engine first tests the connections the run uses. A warehouse failure stops with exit status
`11` (`CONNECTION_TEST`), naming the failure; nothing is recorded and no task starts. An email
failure is logged and data tasks may still run; the alert task records any delivery failure.

| Tested | When |
|---|---|
| the warehouse | a task's `HANDLER` is `SQL` or `BUSINESS_RULES`, or cloning is on |
| the email relay | a task's `HANDLER` is `EMAIL_ALERT`, or `Enforce_sla` is on and the pipeline has an `SLA_IN_HOURS` |

A DuckDB file warehouse is not tested: there is no server to be down, and a running task may hold
its one writer's lock.

Email alert failures do not change the outcome of a pipeline containing data tasks; see
[Email alerts](email-alerts.md#when-sending-fails). A pipeline containing only alerts uses their
outcomes.

A new run is recorded `QUEUED` while its dependencies on other pipelines are checked.
`AUD_GATE_WAITS` retains its deadline and look count across restarts; admission changes it
to `IN-PROGRESS` only when there is no active sibling. When one is not
satisfied, the run is recorded `SKIPPED` with the reason, no task runs, and the command exits
`0`:

```
SALES_DAILY: pipeline_run_id=98 SKIPPED — upstream pipeline ORDERS_INGEST (SUCCESS) last finished run 41 ended FAILED, which does not satisfy a SUCCESS dependency
```

See [Dependencies on other pipelines](dependencies.md#dependencies-on-other-pipelines).

## Resuming a run

A run that is still `QUEUED` or `IN-PROGRESS`, for example because its process was stopped, is resumed by
running the pipeline again: its `SUCCESS` and `SKIPPED` tasks are not run again, and its failed
tasks get another attempt. An admitted run does not check the pipeline's dependencies again; a queued run resumes its
recorded gate budget.

Pressing Ctrl-C, or sending the process `SIGTERM` or `SIGHUP`, stops every running task's
process. The command stops at its next safe point, between Engine DB transactions, so an
interrupted transaction never leaves the Engine DB locked; a second signal stops it at once.
Those tasks are recorded `FAILED`, and the run stays `IN-PROGRESS` so the next run
resumes it. The same holds for `run --task_code`, `--rerun` and `--backfill`. `SIGKILL` cannot be
caught: the next run reconciles expired leases, records orphaned attempts `LOST`, stops
verified local children and retries failed tasks. `etl-craft reconcile` also requests recovery.
See [run controls](run-control.md#mark-a-task) for grace periods and uncertain side effects.

`--force` runs every task after its same-pipeline upstreams finish, regardless of their outcomes
or its previous status, and skips the
pipeline's dependency check. It is only available in local mode.

To step in on a run (mark a task or the run, record a stand-in upstream run, cancel it, run a
task again or without its dependencies, or relax dependency gates), see
[Stepping in](run-control.md).

## Under an orchestrator

In remote mode an orchestrator such as Airflow runs the tasks and is the only source of truth for
scheduling, so `run --pipeline_code` without `--task_code` is refused (exit status `10`). The
orchestrator runs three kinds of step:

```bash
etl-craft run --pipeline_code SALES_DAILY --init-only              # first
etl-craft run --pipeline_code SALES_DAILY --task_code load_orders  # one per task
etl-craft run --pipeline_code SALES_DAILY --finalize-only          # last
```

- `--init-only` refuses rules the orchestrator does not support (exit status `18`), tests the
  connections and starts the run (or resumes the one in progress). It prints
  `pipeline_run_id=<id> IN-PROGRESS`. It does not check the pipeline's dependencies: the DAG's
  sensors do.
- `--finalize-only` records a task the orchestrator never ran as `SKIPPED`, and ends the run
  `FAILED` when a task failed, `SKIPPED` when it ran none, and `SUCCESS` otherwise. It exits `1`
  for a `FAILED` run and `9` (`RUN_STATE`) when there is no run in progress.

In local mode, `--init-only` checks the pipeline's dependencies and prints `SKIPPED` with the
reason when they are not satisfied, and `--finalize-only` records `SKIPPED` for tasks that can
never run. See [Running under an orchestrator](../deploying/orchestrator.md).

## SLA

Every run of a pipeline with `SLA_IN_HOURS` is marked in `AUD_PIPELINES_RUN_LOG.SLA_STATUS`:
`MET`, or `BREACHED` when the run took longer. While a local run is going, it is marked
`BREACHED` as soon as the SLA passes, with a warning in the log; otherwise it is marked when the
run ends. A run's `STATUS` does not change either way: a late run still did its work.

With `Orchestration.Enforce_sla: true`, a breach also sends an SLA email through the `Email`
settings, once per run.

## Unfinished commands

A whole-run command that leaves its run unfinished exits `22` (`INCOMPLETE`), including a
pause during execution or work owned by another process. A stopped backfill also exits `22`
unless its stopping outcome failed or was cancelled (exit `1`). A paused pipeline that starts
nothing retains exit `0`, as does `--init-only`, which deliberately initializes a run for later
steps. Dependency waits that record nothing in `run --task_code` exit `23` (`WAITING`).
Use [`status` and `explain`](inspecting.md) to inspect the selected run before resuming it.
