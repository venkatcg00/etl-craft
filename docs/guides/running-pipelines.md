# Running a pipeline

## Running every task

```bash
etl-craft run --pipeline_code SALES_DAILY
```

In local mode this runs every active task of the pipeline under one run, in dependency waves.
Each wave holds the tasks that are ready, and runs at most `Orchestration.Max_parallel_tasks`
of them at once (8 unless set), each in a process of its own. When a wave ends, the engine looks
again at what is ready, until every task has settled or nothing more can start. Every task runs
as it would under [`run --task_code`](running-tasks.md): the same logs, time limits and
recorded outcome.

```
INFO etl_craft.execution.pipeline [pipeline=SALES_DAILY pipeline_run_id=97]: SALES_DAILY: wave 1: extract_orders, extract_customers
INFO etl_craft.execution.pipeline [pipeline=SALES_DAILY pipeline_run_id=97]: SALES_DAILY: wave 2: load_orders
```

The run ends `SUCCESS` when every task is `SUCCESS` or `SKIPPED`, and `FAILED` otherwise; the
command exits `0` or `1` to match. A failed run names each task that did not succeed:

```
SALES_DAILY: pipeline_run_id=97 FAILED — 2 task(s) did not succeed: load_orders (FAILED), publish (never started); 1 of them could not start because their dependencies were not met
```

A task whose dependencies can never be met under the run is recorded `SKIPPED`, and stays
`SKIPPED`: see [Dependencies and run conditions](dependencies.md).

## Before the run starts

The engine first tests the connections the run uses, and stops with exit status `11`
(`CONNECTION_TEST`) naming each one that failed. Nothing is recorded, and no task starts.

| Tested | When |
|---|---|
| the warehouse | a task's `HANDLER` is `SQL` or `BUSINESS_RULES`, or cloning is on |
| the email relay | a task's `HANDLER` is `EMAIL_ALERT`, or `Enforce_sla` is on and the pipeline has an `SLA_IN_HOURS` |

A DuckDB file warehouse is not tested: there is no server to be down, and a running task may hold
its one writer's lock.

A new run then checks the pipeline's dependencies on other pipelines. When one is not
satisfied, the run is recorded `SKIPPED` with the reason, no task runs, and the command exits
`0`:

```
SALES_DAILY: pipeline_run_id=98 SKIPPED — upstream pipeline ORDERS_INGEST (SUCCESS) last finished run 41 ended FAILED, which does not satisfy a SUCCESS dependency
```

See [Dependencies on other pipelines](dependencies.md#dependencies-on-other-pipelines).

## Resuming a run

A run that is still `IN-PROGRESS`, for example because its process was stopped, is resumed by
running the pipeline again: its `SUCCESS` and `SKIPPED` tasks are not run again, and its failed
tasks get another attempt. A resumed run does not check the pipeline's dependencies again.

Pressing Ctrl-C, or sending the process `SIGTERM`, stops every running task's process. Those
tasks are recorded `FAILED`, and the run stays `IN-PROGRESS` so the next run resumes it.

`--force` runs every task in its wave whatever its status or dependencies, and skips the
pipeline's dependency check. It is only available in local mode.

## Under an orchestrator

In remote mode an orchestrator such as Airflow runs the tasks, so `run --pipeline_code` without
`--task_code` is refused (exit status `10`). The orchestrator runs three kinds of step:

```bash
etl-craft run --pipeline_code SALES_DAILY --init-only              # first
etl-craft run --pipeline_code SALES_DAILY --task_code load_orders  # one per task
etl-craft run --pipeline_code SALES_DAILY --finalize-only          # last
```

- `--init-only` tests the connections, checks the pipeline's dependencies and starts the run
  (or resumes the one in progress). It prints `pipeline_run_id=<id> IN-PROGRESS`, or `SKIPPED`
  with the reason.
- `--finalize-only` records `SKIPPED` for tasks that can never run, and ends the run `SUCCESS` or
  `FAILED` from its tasks' statuses. It exits `1` for a `FAILED` run and `9` (`RUN_STATE`) when
  there is no run in progress.

## SLA

Every run of a pipeline with `SLA_IN_HOURS` is marked in `AUD_PIPELINES_RUN_LOG.SLA_STATUS`:
`MET`, or `BREACHED` when the run took longer. While a local run is going, it is marked
`BREACHED` as soon as the SLA passes, with a warning in the log; otherwise it is marked when the
run ends. A run's `STATUS` does not change either way: a late run still did its work.

With `Orchestration.Enforce_sla: true`, a breach also sends an SLA email through the `Email`
settings, once per run.
