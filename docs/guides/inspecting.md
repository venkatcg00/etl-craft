# Inspecting pipelines

Four read-only commands show what the Engine DB holds, without SQL. Each prints tab-separated
columns under a header line, so the output also works with `cut`, `awk` or a spreadsheet.

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

The pipeline's active tasks: handler, task type, run condition and every active parameter.

## `etl-craft history --pipeline_code SALES [--task_code load] [--limit 20]`

The latest runs, newest first: for the pipeline, each run's status, start and end and SLA status;
for one task, each run's status, attempts, source and target counts, start and end, and error
message.

A code that does not exist stops the command with exit status `4` (`METADATA`) and suggests close
matches.
