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
that does not exist, and a cycle are all errors, each with its own
[exit status](../reference/exit-codes.md). The dependencies of an inactive task are ignored; an
active task that depends on an inactive one in the same pipeline is an error, since it could
never run. `etl-craft graph` shows the
pipeline as waves, where each wave depends only on earlier ones. Waves are the guaranteed-safe
order: an `ANY` or `N` task appears after all its upstreams, even though it may start sooner
during a run.

## During a run

This section is local mode, where etl-craft is the orchestrator. In remote mode the orchestrator
applies these rules instead, from the DAGs `generate-yml` writes; see
[Running under an orchestrator](../deploying/orchestrator.md).

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
- **A failed upstream is not final while the run goes on:** a retry may still turn it into
  `SUCCESS`, so its dependents wait rather than being skipped. Once nothing else can start, no
  retry will come in this run, so the tasks waiting on the failure are recorded `SKIPPED`, and those that wait on them
  with `ALWAYS` or `FAILURE`, such as an alert, then run. The run ends `FAILED`, and its summary
  names the tasks skipped because of the failure.

Dependencies on tasks in other pipelines are checked when the task itself starts, so they never
cause a task to be skipped before it runs.

## Dependencies on other pipelines

A pipeline can depend on another pipeline (`CFG_PIPELINE_DEPENDENCY`), and a task on a task in
another pipeline (a `CFG_TASK_DEPENDENCY` row whose `DEPENDS_ON_PIPELINE_ID` is another
pipeline). A pipeline's dependencies are checked before a new run of it starts; a task's, before
the task runs.

1. **Wait for a running upstream.** While the upstream's latest run is `IN-PROGRESS`, the check
   waits for it. It looks again at 70% of the upstream's average run length, then 80%, 90% and so
   on; one check waits at most an hour and looks at most 30 times, across all its dependencies.
2. **The last run decides.** The upstream's latest finished run must satisfy the dependency
   type, just as within a pipeline. An older run that would have satisfied it does not count: if
   the upstream succeeded yesterday and failed today, a `SUCCESS` dependency is not satisfied.
3. **Each upstream run is consumed once.** The run must also be newer than the one the dependency
   last consumed, recorded in `AUD_PIPELINE_DEPENDENCY_TRACKER` or
   `AUD_TASK_DEPENDENCY_TRACKER`. The tracker moves forward only when the downstream run or task
   succeeds, so a downstream that failed is retried against the same upstream run.

A pipeline whose dependencies are not satisfied has its run recorded `SKIPPED`. A task whose
dependencies on other pipelines are not satisfied is recorded `SKIPPED` too, unless an upstream in
its own pipeline has not finished yet. `SKIPPED` is final for that run: the task does not run
until a new run finds its dependencies satisfied.

In remote mode none of this is checked by etl-craft and no tracker moves: the orchestrator's
sensors wait for the upstream instead (see
[Running under an orchestrator](../deploying/orchestrator.md)).
