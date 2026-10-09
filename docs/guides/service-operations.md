# Service operations

The CLI and Python callers use the same functions in `etl_craft.services.operations`.
Operations take a frozen `OperationContext` and a pipeline code, return frozen result
dataclasses, and never print or dispose the caller's engine.

## Calling from Python

Use an initialized, upgraded [Engine DB](../deploying/engine-db.md). The caller owns its engine
and supplies the acting identity:

```python
import json
from datetime import date
from pathlib import Path

from etl_craft.config import load_config
from etl_craft.core.actor import resolve_actor
from etl_craft.engine.connection import check_reachable, engine_db
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations import OperationContext, to_json
from etl_craft.services.operations import runs

config = load_config(Path("craft-connector.yml"))
engine = engine_db(config)
try:
    check_reachable(engine, config.engine.active.schema)
    ctx = OperationContext(engine, config, resolve_actor())
    done = runs.initialize_run(
        ctx,
        "SALES",
        run_date=date(2026, 10, 1),
        selector=RunSelector(run_key="python:2026-10-01"),
    )
    print(json.dumps(to_json(done), indent=2))
finally:
    engine.dispose()
```

`initialize_run` performs the CLI's initialization step. `trigger_run` runs the pipeline
in the foreground; task execution, gates, leases, reconciliation and finalization use the same
execution functions as the CLI. `ctx.child` controls the child process's logging and execution
options. Project files remain relative to `config.project_dir`.

| Area | Operations |
| --- | --- |
| `runs` | `trigger_run`, `initialize_run`, `finalize_run`, `skip_run`, `mark`, `mark_run`, `stand_in_run`, `cancel_run`, `reconcile_runs` |
| `tasks` | `run_task`, `force_task`, `rerun_task`, `mark_task` |
| `pipelines` | `set_pause` |
| `backfills` | `run_backfill` |
| `inspect` | `list_pipelines`, `pipeline_graph`, `pipeline_steps`, `run_history`, `audit` |

Pass `verb="pause"` or `verb="resume"` to `pipelines.set_pause(ctx, pipeline_code, reason, ...)`.

`execute_run(ctx, RunRequest(...))` dispatches the same request used by `etl-craft run`.
`reason` on `tasks.run_task` requests the dependency override and must be non-empty; without
one the task follows its normal dependency checks. Force, rerun, backfill, pause and intervention
restrictions remain the same as their CLI equivalents.

Domain refusals raise the existing `core.errors` classes. Read operations use one consistent
database snapshot for related views and do not create action records.

## Execution identities and outcomes

Every stored run view names its `pipeline_id` and `pipeline_run_id`. Every task and attempt
view also names its `task_run_id`, with `task_id` for the task definition and `attempt_id`
for an individual attempt. The service reads the identities returned by execution or selected
explicitly; it never attaches a result to a different run by recency.

`OperationResult.status` and `message` describe the call's reported outcome. Its `run`,
`task` and `pipeline` fields hold the relevant stored views. Marking a run `FAILED` can be a
successful operation: the result reports `SUCCESS` while `run.status` reports `FAILED`.

A paused pipeline that starts nothing has no invented run or task view. A configured task that
has never run has a null `task_run_id` in its step document. A skipped or stand-in task can have
an empty `attempts` list. Backfill documents retain each date's outcome and the outcome that
stopped the range.

The actor scopes request attribution and dispatched work, then is restored when the operation
returns or raises. Automatic lifecycle transitions retain their system actor. Each mutating
request inserts one complete `REQUESTED` action, including domain refusals after the Engine DB
is available. Delegation from the CLI does not insert another copy. Flow outcomes remain in run
and attempt history; action records are never updated.

## JSON on the command line

These commands accept `--format json`: `run`, `mark`, `cancel`, `pause`, `resume`,
`reconcile`, `list`, `graph`, `steps`, `history` and `audit`. Text remains the default.

```bash
etl-craft list --format json
etl-craft history --pipeline_code SALES --run-id 42 --format json
etl-craft history --pipeline_code SALES --task_code load --run-id 42 --format json
etl-craft run --pipeline_code SALES --init-only --run-key daily:2026-10-01 --format json
```

Standard output contains one JSON document produced by `to_json`, including empty lists.
Errors and logs remain on standard error, and command exit behavior is unchanged. Audit text
renders captured JSON consistently on SQLite and PostgreSQL.

Dates and timestamps use ISO 8601; typed Engine DB timestamps use UTC with an explicit offset.
Enums use their values and tuples become arrays. Captured metadata JSON keeps its original values.
The serializer returns a fresh dictionary.

| Schema | Document |
| --- | --- |
| `etl-craft/operation/1` | Call outcome with stored run, task or pipeline views |
| `etl-craft/run/1` | Stored pipeline run |
| `etl-craft/task-run/1` | Stored task summary and ordered attempts |
| `etl-craft/attempt/1` | Stored attempt outcome, ownership and execution counts |
| `etl-craft/backfill/1` | Date range, outcomes and stopping result |
| `etl-craft/pipeline/1`, `etl-craft/pipeline-list/1` | Pipeline definitions and pauses |
| `etl-craft/graph/1`, `etl-craft/step/1`, `etl-craft/steps/1` | Configuration and selected-run steps |
| `etl-craft/history/1` | Bounded run or task history and interventions |
| `etl-craft/reconciliation/1` | Lost attempt IDs and released pipeline-run IDs |
| `etl-craft/audit/1`, `etl-craft/action/1`, `etl-craft/metadata-change/1` | Immutable audit records |

The [Python API reference](../api/etl_craft/services/operations/index.md) lists the functions,
context, requests and result fields.

## Execution inspection and diagnostics

`operations.status.pipeline_status(ctx, code, selector=...)` returns `StatusView`;
`explain_task(ctx, code, task_code, selector=...)` returns `Explanation`. The pure
`explain(snapshot)` function computes the same explanation from a captured `TaskSnapshot`.
These reads never modify run or admission state.

`operations.diagnostics.validate_metadata(ctx, code)` returns the validation report;
`check_configuration(config)` returns `DoctorView`, including unreachable-service findings;
`trace_lineage(ctx, table=..., column=..., upstream=True, downstream=True, depth=..., refresh=False)`
returns `LineageView`. Lineage retains its existing metadata cache behavior. All these documents
use `to_json` and a versioned `schema` identifier, matching CLI `--format json` output.

Run results include `waiting=True` when a single-task command recorded nothing because
its dependencies were not met. That call exits 23; unfinished executions exit 22. Initializing
a run with `--init-only` retains exit 0 for orchestrator first steps.
