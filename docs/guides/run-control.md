# Stepping in: mark, cancel, pause, rerun and bypasses

In local mode etl-craft is the orchestrator, so it is where you step in when a run needs a hand:
a failure you have fixed by other means, an upstream that cannot run where you are, a task to run
again, a run that must stop now. Every change needs a reason, is recorded in `AUD_RUN_INTERVENTIONS` with who made
it and when, and erases nothing: each attempt keeps its row, its log file and, in the
intervention, the message it had before.

In remote mode the orchestrator is the only source of truth for runs, so all of these are refused
(exit status `10`): mark, clear or stop the task in the orchestrator instead. See
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

## Pause and resume a pipeline

```bash
etl-craft pause --pipeline_code SALES_DAILY --reason "the CRM is being migrated this week"
etl-craft resume --pipeline_code SALES_DAILY --reason "the migration is done"
```

While a pipeline is paused, `etl-craft run` starts nothing of it (whole runs, single tasks and
`--init-only` alike): it says the pipeline is paused, by whom and why, and exits `0`, so a
scheduler that keeps calling it raises no alarm. A run in progress when the pipeline is paused
lets its running tasks finish, starts no more, and stays `IN-PROGRESS`; after `resume`, the next
`run --pipeline_code` goes on with it. `etl-craft list` and the catalog show every paused
pipeline, and `AUD_PIPELINE_PAUSES` keeps every pause with who paused and resumed it, when and
why.

## Skip a run on purpose

```bash
etl-craft run --pipeline_code SALES_DAILY --skip --reason "public holiday: no sales file"
```

Records a run of the pipeline `SKIPPED`, running nothing, as a stand-in run does (see
[Record a stand-in run](#record-a-stand-in-run)). The pipelines that depend on it see a run that
did nothing: a `SUCCESS` dependency on it is not satisfied, an `ALWAYS` one is.

## Run a task without its dependencies

```bash
etl-craft run --pipeline_code SALES_DAILY --task_code load_orders --ignore-dependencies \
    --reason "extract_orders is late; the file is already in place"
```

The task runs under the pipeline's run in progress without its dependencies being checked, in
its pipeline or elsewhere, and consumes no upstream run. A task that already ended `SUCCESS` or
`SKIPPED` is not run again: that is `--rerun`.

## Run a task again

```bash
etl-craft run --pipeline_code SALES_DAILY --task_code load_orders --rerun \
    --reason "the source resent the file"
etl-craft run --pipeline_code SALES_DAILY --task_code load_orders --rerun --with-downstream \
    --reason "the source resent the file"
```

The task runs again under the pipeline's latest run, although it already ended, as a new attempt
on its row that skips nothing (a business-rules task checks every rule again), without its
dependencies being checked. With `--with-downstream`, every task after it runs again too, in
dependency order, each once its own dependencies are satisfied; those whose dependencies are not
(an alert that waits for a failure, say) keep their rows. A run that had ended is reopened for
this and ended again from its tasks' statuses; a run still in progress is left for
`run --pipeline_code` to finish.

## Relax dependency gates

A local run checks its dependencies on other pipelines before it starts, and a task its
dependencies on other pipelines' tasks (see
[Dependencies on other pipelines](dependencies.md#dependencies-on-other-pipelines)). Where an
upstream cannot run at all, in a development environment, say, `Orchestration.Dependency_gates`
relaxes that per profile:

```yaml
Orchestration:
  Mode: local
  dev:
    Dependency_gates: warn     # enforce (the default) | warn | off
```

- `enforce` checks every dependency and skips the run or the task when one is not satisfied.
- `warn` checks them all and goes ahead anyway, with a warning.
- `off` checks none.

Each dependency let through is recorded against the run, or the task, as a `GATE_BYPASS`, and the
run's summary says so. Only upstream runs that satisfied their dependency are consumed. `doctor`
warns whenever gates are not enforced, and remote mode refuses anything but `enforce`: there the
orchestrator's sensors are the gates.

## What was changed

`etl-craft history --pipeline_code SALES_DAILY` lists the interventions on the runs it shows,
under the runs, and the [catalog](catalog.md) lists those on each pipeline's last run.
`AUD_RUN_INTERVENTIONS` holds every one:

| Column | Holds |
|---|---|
| `PIPELINE_RUN_ID`, `TASK_ID` | the run, and the task (`NULL` for a change to the run itself) |
| `ACTION` | `MARK`, `NEW_RUN`, `CANCEL`, `REOPEN` (a run a mark or a rerun reopened), `RESET` (a skipped task reset to run again), `RERUN`, `IGNORE_DEPENDENCIES`, or `GATE_BYPASS` |
| `FROM_STATUS`, `TO_STATUS` | the status before and after; `TO_STATUS` is `NULL` for a reset |
| `TARGET_COUNT` | the row count stated with `--rows` |
| `PREVIOUS_MESSAGE` | the row's `ERROR_MESSAGE` before the change |
| `REASON`, `REQUESTED_BY`, `REQUESTED_AT` | why (for a bypass, the dependencies let through), who (`user@host`) and when |
