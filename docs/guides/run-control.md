# Stepping in: mark and cancel

In local mode etl-craft is the orchestrator, so it is where you step in when a run needs a hand:
a failure you have fixed by other means, an upstream that cannot run where you are, a run that
must stop now. Every change needs a reason, is recorded in `AUD_RUN_INTERVENTIONS` with who made
it and when, and erases nothing: each attempt keeps its row, its log file and, in the
intervention, the message it had before.

In remote mode the orchestrator is the only source of truth for runs, so `mark` and `cancel` are
refused (exit status `10`): mark, clear or stop the task in the orchestrator instead. See
[Running under an orchestrator](../deploying/orchestrator.md).

## Mark a task

```bash
etl-craft mark --pipeline_code SALES_DAILY --task_code load_orders --status SUCCESS \
    --reason "loaded by hand from the 06:00 file"
```

The task is set to `SUCCESS`, `FAILED` or `SKIPPED` under the pipeline's latest run, and its
`ERROR_MESSAGE` says who marked it and why. A task that is running is refused: cancel the run
first, or wait.

If that run has ended, it is reopened (`IN-PROGRESS` again), and the tasks the engine skipped
without running are reset, so running the pipeline again resumes it from there:

```bash
etl-craft run --pipeline_code SALES_DAILY
```

Mark a failed task `SUCCESS` and the tasks waiting on it run; the rest of the run is not repeated.
Mark a task `FAILED` and it gets another attempt.

A marked `SUCCESS` satisfies a `HAS_DATA` dependency only when you state the row count:

```bash
etl-craft mark --pipeline_code SALES_DAILY --task_code load_orders --status SUCCESS \
    --rows 1200 --reason "1200 rows loaded by hand"
```

## Mark a run

Without `--task_code`, `mark` sets the run itself, ending it now if it had not ended:

```bash
etl-craft mark --pipeline_code SALES_DAILY --status SUCCESS --reason "load_orders is not needed today"
```

The pipelines that depend on this one judge the marked status. Its tasks keep theirs, and no
upstream run is consumed. A run with a task still running is refused.

## Record a stand-in run

When a pipeline cannot run where you are (an upstream that only runs in production, say), its
dependents' gates would never pass. `mark --new-run` records a finished run of it that did not
really run, and the gates judge it like any other:

```bash
etl-craft mark --pipeline_code CRM_EXPORT --new-run --status SUCCESS \
    --reason "CRM_EXPORT only runs in production"
etl-craft mark --pipeline_code CRM_EXPORT --new-run --task_code publish --status SUCCESS \
    --rows 50 --reason "stand-in for a HAS_DATA dependency on CRM_EXPORT.publish"
```

With `--task_code`, the task gets a row in the stand-in run too, for dependencies on that task.
A pipeline with a run in progress is refused: mark that run, or cancel it.

## Cancel a run

```bash
etl-craft cancel --pipeline_code SALES_DAILY --reason "the source sent yesterday's file"
```

The pipeline's run in progress ends `CANCELLED`, and so does each of its running tasks. The
process running each task looks every two seconds, stops the task's process when it sees the
cancel, and the process running the pipeline starts nothing more; `run` then exits `1`. The next
`run --pipeline_code` starts a new run.

## What was changed

`etl-craft history --pipeline_code SALES_DAILY` lists the interventions on the runs it shows,
under the runs, and the [catalog](catalog.md) lists those on each pipeline's last run.
`AUD_RUN_INTERVENTIONS` holds every one:

| Column | Holds |
|---|---|
| `PIPELINE_RUN_ID`, `TASK_ID` | the run, and the task (`NULL` for a change to the run itself) |
| `ACTION` | `MARK`, `NEW_RUN`, `CANCEL`, `REOPEN` (a run a mark reopened), or `RESET` (a skipped task reset to run again) |
| `FROM_STATUS`, `TO_STATUS` | the status before and after; `TO_STATUS` is `NULL` for a reset |
| `TARGET_COUNT` | the row count stated with `--rows` |
| `PREVIOUS_MESSAGE` | the row's `ERROR_MESSAGE` before the change |
| `REASON`, `REQUESTED_BY`, `REQUESTED_AT` | why, who (`user@host`) and when |
