# Running under an orchestrator

In remote mode (`Orchestration.Mode: remote`) an orchestrator such as Airflow schedules and runs
the tasks, and it is the only source of truth for scheduling. etl-craft describes each pipeline as
a DAG holding every rule, and then runs each task whenever the DAG says, checking none of those
rules itself. Nothing competes with the orchestrator: what its DAG run shows is what happened.

```bash
etl-craft generate-yml --pipeline_code SALES --output dags/sales.yml
etl-craft generate-yml --global --output dags/global.yml     # with Global_dag: true
```

Without `--output` the YAML is written to standard output. Convert it into your orchestrator's own
DAG format; the YAML is the same whatever you run it on.

## What the engine does, and what the orchestrator does

| The orchestrator decides | etl-craft keeps |
|---|---|
| when a pipeline runs, and which of its tasks run and when | the run each task binds to, and one `IN-PROGRESS` run per pipeline |
| run conditions and dependency types, as trigger rules | every write, through the eight SQL actions |
| dependencies on other pipelines and their tasks, as sensors | time limits, connections and secrets |
| retries, and running a task again after it is cleared | the audit of every attempt, its log and its counts |

So in remote mode:

- `run --task_code` runs the task when told to, with no dependency, run-condition or
  cross-pipeline check. A task run again after it succeeded is a new attempt on the same
  `AUD_TASK_RUN_LOG` row, and skips nothing it did before (a business-rules task checks every
  rule again). A task run after `__finalize__` ended the run, because it was cleared, reopens
  that run; the cleared `__finalize__` ends it again.
- `--init-only` starts the run (or resumes the one in progress) without checking the pipeline's
  dependencies: the DAG's sensors wait for them.
- `--finalize-only` records the orchestrator's decisions: a task it never ran is `SKIPPED`
  (`not run by the orchestrator`), and a task still `IN-PROGRESS`, whose process was lost, is
  `FAILED`. The run ends `FAILED` when a task failed, `SKIPPED` when the orchestrator ran none of
  its tasks, and `SUCCESS` otherwise. No dependency tracker moves.
- `--force` and running a whole pipeline are refused (exit status `10`): they are local mode's.

## Rules an orchestrator does not support

Some rules have no equivalent in an orchestrator's DAG. Rather than drop them, remote mode refuses
them wherever they would be dropped, in `validate`, `generate-yml` and `run --init-only` (exit
status `18`, `REMOTE_UNSUPPORTED`), naming the pipeline, the task and the rule, with the remedy:

| Rule | Why | Remedy |
|---|---|---|
| `RUN_CONDITION = 'N'` | a trigger rule waits for all or one of the upstream steps, never N of them | `ALL` or `ANY` |
| a `HAS_DATA` dependency, on a task or a pipeline | an orchestrator sees whether a step succeeded, not whether it wrote rows | `SUCCESS`, with the task handling an empty input |
| a task whose dependencies have different types | a trigger rule applies to every upstream step alike | one type, or split the task |
| with `Global_dag` on, a pipeline whose dependencies on other pipelines have different types | the global DAG gives each pipeline one trigger rule | one type, or `Global_dag` off |

Or run the pipeline in local mode, where etl-craft applies every one of them. For example:

```text
$ etl-craft run --pipeline_code SUPPORT_DM --init-only
error: SUPPORT_DM has 2 rule(s) the remote orchestrator does not support, so they cannot be applied
in remote mode: SUPPORT_DM.setup_fact: depends on interactions with DEPENDENCY_TYPE = 'HAS_DATA';
the remote orchestrator does not support this. ...
```

## A pipeline's DAG

| Key | Holds |
|---|---|
| `dag_id`, `description`, `schedule`, `sla_hours`, `refresh_type` | from the pipeline's row; `schedule` is `null` when `Allow_schedule: false` |
| `catchup`, `tags`, `default_args` | the pipeline's `PIPELINE_PARAMETERS`, else the Orchestration DAG defaults, else a built-in default |
| `max_active_runs` | always `1`: a pipeline has one run in progress at a time |
| `tasks` | the steps, in order: `__init__`, the sensors, one per active task, `__finalize__` |

Each step has what it runs, the steps it waits for (`depends_on`), and one `trigger_rule`:

- `__init__` runs `etl-craft run --pipeline_code SALES --init-only --run-date {{
  data_interval_end | ds }}`: it tests connections and starts the run as of the orchestrator's
  date, so `$$run_date` means the day the run fires, as in local mode, and a backfill in the
  orchestrator runs each day as of its own date. The date is an Airflow template; with another
  orchestrator, pass its own date for the run.
- `__wait_for_<PIPELINE>__` waits for another pipeline this one depends on: a `sensor` on that
  pipeline's DAG run (`external_task_id: null`). It comes after `__init__`, so every task the
  orchestrator runs, even an alert that runs because a sensor failed, has a run to bind to.
  With `Global_dag: true` there is none: the global DAG triggers the pipelines in order instead.
- `__wait_for_<PIPELINE>.<task>__` waits for a task of another pipeline this task depends on: a
  `sensor` on that task, after the pipeline sensors and before the task.
- each task runs `etl-craft run --pipeline_code SALES --task_code <task>`. A task with no other
  upstream step waits for the pipeline sensors, or for `__init__`.
- `__finalize__` runs `--finalize-only` after the last tasks, whatever happened to them.

A sensor succeeds once its upstream is in one of its `allowed_states`, and fails once it is in one
of its `failed_states`:

| Dependency type | On a pipeline (DAG run states) | On a task (task states) |
|---|---|---|
| `SUCCESS` | allowed `success`; failed `failed` | allowed `success`; failed `failed`, `upstream_failed`, `skipped` |
| `FAILURE` | allowed `failed`; failed `success` | allowed `failed`, `upstream_failed`; failed `success`, `skipped` |
| `ALWAYS` | allowed `success`, `failed` | allowed every finished state |

Point each sensor at the upstream run it should judge: for Airflow's `ExternalTaskSensor`, the run
with the same logical date by default, or `execution_delta` or `execution_date_fn` when the two
DAGs' schedules differ.

A task's trigger rule comes from its run condition and its dependencies' types, where a sensor
counts as a `SUCCESS` dependency (it succeeds when its dependency is satisfied):

| Run condition | `SUCCESS` | `FAILURE` | `ALWAYS` |
|---|---|---|---|
| `ALL` | `all_success` | `all_failed` | `all_done` |
| `ANY` | `one_success` | `one_failed` | `one_done` |

## In local mode

`generate-yml` also works in local mode, where etl-craft checks every rule itself when a run or a
task starts. There, a task with dependencies of different types, run condition `N` or a
`HAS_DATA` dependency gets `all_done`, and etl-craft records it `SKIPPED` when its condition is not
met; dependencies on other pipelines are listed under `pipeline_dependencies` and
`cross_pipeline_task_dependencies` for reading only.

## The global DAG

With `Orchestration.Global_dag: true`, `--global` writes one DAG with a node for every pipeline that
depends on another or is depended on, each triggering that pipeline's own DAG in dependency order.

## The docs DAG

`etl-craft generate-yml --docs` writes the `etl_craft_docs` DAG, which runs
`etl-craft generate-docs` on the `Docs_site` section's `Schedule`, so the
[catalog site](../guides/catalog.md) keeps its run details fresh.
