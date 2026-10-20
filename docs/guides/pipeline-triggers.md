# Pipeline trigger tasks

A `RUN_PIPELINE` task starts a child pipeline through the controller. Give it `TASK_TYPE=ETL`
and ordinary task dependencies to sequence children or fan them out. Run the controller while
using trigger tasks, including when starting the parent with `etl-craft run` locally.

| Task parameter | Value |
| --- | --- |
| `TRIGGER_PIPELINE_CODE` | Required active pipeline code, different from the parent |
| `TRIGGER_MODE` | `WAIT` (default) or `DETACH` |
| `TRIGGER_TIMEOUT_MINUTES` | `WAIT` only: `NONE` (default), or a finite positive duration; zero is refused |

In `WAIT` mode the attempt creates the child and hands its wait to the controller atomically.
The attempt ends `HANDED_OFF`, its agent slot becomes free, and the task remains `IN-PROGRESS`.
The controller restores the wait after a restart. Child `SUCCESS` and `SKIPPED` become the same
task outcome; child `FAILED` and `CANCELLED` also transfer unchanged, with child failure
details. Child row counts are not aggregated into the trigger task. A timeout fails the task
and leaves the child running. The failure message names the child and the next action. Child outcome
failures are permanent for `RETRY_ON`; only `ANY` can retry them within the configured budget,
and those attempts find the same child instead of launching another.

In `DETACH` mode the task succeeds once the child is durably admitted as `QUEUED`, freeing
its worker slot. It does not wait for the child to start. The child's eventual outcome does
not change the trigger task or parent. The child runs independently under its own dependencies, gates and
SLA. It queues behind an existing non-terminal run of its pipeline. A paused child definition
refuses admission with its pause reason; resume it or mark the trigger task.

The child inherits the parent run's logical date and initiating actor. Its audit row records
`TRIGGER_KIND=PIPELINE_TASK` and `TRIGGERED_BY_TASK_RUN_ID`. The child's key is
`trigger:<task_run_id>:<generation>`: an automatic retry retains that generation, and an
explicit `run --task-code ... --rerun --reason ...` increments it while retaining the task
summary's identity. A crash before the atomic admission commits leaves no partial child;
a committed handoff is resumed instead of launching a second child.

Stopping or aborting the parent also stops or aborts its waiting `WAIT` children, with a
separate intervention naming the parent run. Detached children continue. Marking a waiting
trigger closes its wait and leaves its child running. Holds retain the ordinary scoped control
behavior. `status` and `explain` name the child and the controller-held wait; run-detail JSON
exposes `parent_run_id`, `triggered_by_task_run_id` and `child_run_ids`. The catalog shows
labelled trigger edges and links between parent and child run pages.

Run `etl-craft validate` before deployment. Missing, inactive and self targets fail, as do
cycles involving trigger tasks and pipeline dependencies. Children retain their own schedules;
usually leave their `RUN_SCHEDULE` empty so the parent controls when they start.

## Configuration file example

Create the parent and child in `CFG_PIPELINES.csv`, then add a parent task in `CFG_TASKS.csv`:

```csv
TASK_CODE,TASK_TYPE,PIPELINE_ID,HANDLER,ACTIVE_FLAG
launch_child,ETL,1,RUN_PIPELINE,Y
```

For its assigned `TASK_ID` (here `10`), add `CFG_TASK_PARAMETERS.csv` rows:

```csv
TASK_ID,PARAMETER_NAME,PARAMETER_VALUE,ACTIVE_FLAG
10,TRIGGER_PIPELINE_CODE,CHILD,Y
10,TRIGGER_MODE,WAIT,Y
10,TRIGGER_TIMEOUT_MINUTES,30,Y
```

Use the IDs assigned by your project's exported configuration. Apply through the ordinary
[configuration-file workflow](configuration-files.md), validate, then initialize the parent:

```bash
etl-craft config plan
etl-craft config apply
etl-craft validate
etl-craft run --pipeline-code PARENT --init-only
etl-craft status --pipeline-code PARENT
```
