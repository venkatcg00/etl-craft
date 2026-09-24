# Dependencies and run conditions

A pipeline's order comes from its `CFG_TASK_DEPENDENCY` rows, never from code. Each row says
that a task waits on an upstream task, and which outcome of that upstream it waits for.

## Dependency types

| `DEPENDENCY_TYPE` | Satisfied when the upstream task is |
|---|---|
| `SUCCESS` | `SUCCESS` |
| `FAILURE` | `FAILED`, for example an email alert that reports a failure |
| `ALWAYS` | finished in any way: `SUCCESS`, `FAILED` or `SKIPPED` |
| `HAS_DATA` | `SUCCESS` and wrote at least one row |

An upstream that has not run yet, or is still `IN-PROGRESS`, satisfies no dependency.

## Run conditions

`CFG_TASKS.RUN_CONDITION` sets how many of a task's dependencies must be satisfied before it
starts. It counts every dependency of the task, including those on tasks in other pipelines.

| `RUN_CONDITION` | The task starts when |
|---|---|
| empty or `ALL` | every dependency is satisfied |
| `ANY` | at least one dependency is satisfied |
| `N` | at least `RUN_CONDITION_COUNT` dependencies are satisfied |

`RUN_CONDITION_COUNT` is set only with `N`, must be at least 1, and cannot exceed the task's
number of dependencies, since the task could then never start.

## Waves

The engine checks the graph before a run: a task that depends on itself, a dependency on a task
that does not exist, and a cycle are all errors (exit status `1`). `etl-craft graph` shows the
pipeline as waves, where each wave depends only on earlier ones. Waves are the guaranteed-safe
order: an `ANY` or `N` task appears after all its upstreams, even though it may start sooner
during a run.

## During a run

The engine starts every task that is ready, runs it, and looks again, until nothing is left
to start.

- **Ready:** a task is ready when it has not run under the active run, or it `FAILED`, and
  its run condition is met.
- **Retries resume:** a task that is `SUCCESS` or `SKIPPED` never runs again under the same run,
  and one that is `IN-PROGRESS` is not started twice.
- **Skipped:** a task that can never become ready is recorded `SKIPPED`. This happens when
  too many of its upstreams are `SUCCESS` or `SKIPPED` without satisfying their dependency: a
  `FAILURE` dependency on a task that succeeded, for example. Skipping cascades: a task that
  waits for the skipped task's `SUCCESS` is skipped too, while an `ALWAYS` dependency on it is
  satisfied.
- **A failed upstream is not final:** a retry of the run may still turn it into `SUCCESS`, so
  its dependents wait rather than being skipped.

Dependencies on tasks in other pipelines are checked when the task itself starts, so they never
cause a task to be skipped before it runs.
