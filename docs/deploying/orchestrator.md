# Running under an orchestrator

In remote mode (`Orchestration.Mode: remote`) an orchestrator such as Airflow schedules and runs
the tasks, and etl-craft describes each pipeline as a DAG for it:

```bash
etl-craft generate-yml --pipeline_code SALES --output dags/sales.yml
etl-craft generate-yml --global --output dags/global.yml     # with Global_dag: true
```

Without `--output` the YAML is written to standard output. Convert it into your orchestrator's own
DAG format; the YAML is the same whatever you run it on.

## A pipeline's DAG

| Key | Holds |
|---|---|
| `dag_id`, `description`, `schedule`, `sla_hours`, `refresh_type` | from the pipeline's row; `schedule` is `null` when `Allow_schedule: false` |
| `catchup`, `tags`, `default_args` | the pipeline's `PIPELINE_PARAMETERS`, else the Orchestration DAG defaults, else a built-in default |
| `tasks` | one per active task, between `__init__` and `__finalize__` |
| `pipeline_dependencies`, `cross_pipeline_task_dependencies` | for reading only; etl-craft checks them itself when a run or task starts |

Each task has the command it runs, the tasks it waits for, and one `trigger_rule`:

- `__init__` runs `etl-craft run --pipeline_code SALES --init-only`: it tests connections, checks
  the pipeline's dependencies and starts the run;
- each task runs `etl-craft run --pipeline_code SALES --task_code <task>`;
- `__finalize__` runs `--finalize-only` after the last tasks, whatever happened to them.

The trigger rule comes from the task's run condition and its dependencies' types:

| Run condition | `SUCCESS` | `FAILURE` | `ALWAYS` | `HAS_DATA` |
|---|---|---|---|---|
| `ALL` | `all_success` | `all_failed` | `all_done` | `all_success` |
| `ANY` | `one_success` | `one_failed` | `one_done` | `one_success` |

A task with dependencies of different types, or run condition `N`, gets `all_done`: the
orchestrator starts it once its upstreams have finished, and etl-craft checks the real condition,
recording the task `SKIPPED` when it is not met. `HAS_DATA` is checked the same way.

## The global DAG

With `Orchestration.Global_dag: true`, `--global` writes one DAG with a node for every pipeline that
depends on another or is depended on, each triggering that pipeline's own DAG in dependency order.

## The docs DAG

`etl-craft generate-yml --docs` writes the `etl_craft_docs` DAG, which runs
`etl-craft generate-docs` on the `Docs_site` section's `Schedule`, so the
[catalog site](../guides/catalog.md) keeps its run details fresh.
