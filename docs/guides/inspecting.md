# Inspecting pipelines

Read-only commands show what the Engine DB holds, without SQL. Tables print tab-separated columns, so their output also works with `cut`, `awk` or a spreadsheet.

## `etl-craft list`

Every active pipeline: its code, name, refresh type, schedule and SLA.

## `etl-craft graph --pipeline_code SALES`

The pipeline's order and what each task waits for:

```
Pipeline SALES
Waves, the order that is always safe:
  1: extract
  2: load
  3: alert
May start before their wave (an ANY or N run condition): alert
Task dependencies:
  alert <- load (FAILURE)
  load <- UP.publish (SUCCESS)
  load <- extract (SUCCESS)
Pipeline dependencies:
  UP (SUCCESS)
```

A dependency on a task in another pipeline is written `PIPELINE.TASK`. See
[Dependencies and run conditions](dependencies.md) for what waves and run conditions mean.

## `etl-craft steps --pipeline_code SALES`

The pipeline's active tasks: status under the selected run, handler, task type, run condition
and every active parameter. Pass `--run-id` or `--run-key`; without either, exactly one
non-terminal run must exist.

## `etl-craft status --pipeline_code SALES [--run-id 42 | --run-key KEY]`

The selected run's identity, key, trigger kind, run date, status, start, duration and SLA,
followed by every active task's status, attempt count, rows written, duration and first error
line. Tasks that have not run are included. The final line lists tasks blocked by failed
upstreams, including tasks behind another blocked task.

## `etl-craft explain --pipeline_code SALES --task_code load [--run-id 42 | --run-key KEY]`

Why the task is in its state and what would make it run. The explanation includes the run and
its pause, the task summary and ordered attempts, its run condition and required count,
each same-pipeline and cross-pipeline dependency, recorded admission reasons and selected
upstream identities, persisted gate waits and retry due time. It distinguishes a ready task
that has not run, a gate wait, failure blocking, an unsatisfiable condition, a scheduled retry,
a pause and settled success or skip.

Both commands inspect one database snapshot. They never reconcile leases, queue retries,
consume upstream output or write audit actions. Without a selector, exactly one non-terminal
run must exist; an ended run always needs its explicit id or key.

## `etl-craft history --pipeline_code SALES [--run-id 42 | --run-key KEY | --all]`

The selected run, or with `--all --limit 20`, recent runs newest first: for the pipeline, each run's status, start and end, SLA status, and starting and ending actors with their kinds;
for one task, each run's status, attempts, source and target counts, start and end, and error
message.

A code that does not exist stops the command with exit status `4` (`METADATA`) and suggests close
matches.

## `etl-craft audit [--pipeline_code SALES] [--since 2026-10-01]`

Command requests show their time, actor and kind, command, request outcome and masked arguments.
Metadata changes show their time, actor and kind, table, row key, operation, before and after JSON,
and project migration filename. `--since` accepts an ISO date or timestamp; a date or timestamp
without an offset uses UTC. Pipeline filtering includes its task metadata, even after a task is
deleted, and dependency edges involving that pipeline.

An action is complete when the request is recorded: `REQUESTED` does not promise that a flow
ran or succeeded. Use `history` for execution outcomes. See [Actors and write guards](../deploying/engine-db.md#actors-and-write-guards)
for identity configuration and why metadata changes go through migrations.

## JSON documents

These commands accept `--format json` and return the same schema-versioned documents as
their Python service operations. Task history includes the canonical `task_run_id` and its
ordered attempt records, including `not_before` and `retryable`. `status`, `explain`,
`validate`, `doctor` and `lineage` also accept `--format json`. See [Service operations and JSON](service-operations.md).
