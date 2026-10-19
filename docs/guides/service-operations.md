# Service operations

Every action (a run, a mark, a control, a sign-in, a token, a docs build) is one row of the
action table in `etl_craft.services.operations.actions`. The CLI, the HTTP API and Python
callers reach it the same way: build the action's request and call
`invoke(ctx, ActionName.X, request)`. `invoke` checks the caller's role, records the request
once when the action is audited, and calls the operation. Operations never print or dispose the
caller's engine. The [actions reference](../reference/actions.md) lists every action with its
command, route, role, audit and placement, generated from the table.

## Calling from Python

Use an initialized, upgraded [Engine DB](../deploying/engine-db.md). The caller owns its engine
and authenticates its service token before supplying the acting identity:

```python
import json
import os
from dataclasses import replace
from datetime import date
from pathlib import Path

from etl_craft.config import load_config
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ConfigurationError
from etl_craft.engine.connection import check_reachable, engine_db
from etl_craft.services.operations import OperationContext, to_json, tokens
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.requests import RunRequest

config = load_config(Path("craft-connector.yml"))
engine = engine_db(config)
try:
    check_reachable(engine, config.engine.schema)
    ctx = OperationContext(engine, config, SYSTEM_ACTOR)
    with acting_as(SYSTEM_ACTOR):
        identity = tokens.authenticate(ctx, os.environ["ETL_CRAFT_TOKEN"])
    if identity is None:
        raise ConfigurationError("ETL_CRAFT_TOKEN is invalid; obtain a service token from an admin")
    ctx = replace(ctx, actor=identity.actor, role=identity.role)
    request = RunRequest(
        pipeline_code="SALES",
        init_only=True,
        run_date=date(2026, 10, 1),
        run_key="python:2026-10-01",
    )
    done = invoke(ctx, ActionName.RUN, request)
    print(json.dumps(to_json(done), indent=2))
finally:
    engine.dispose()
```

Requests are frozen, keyword-only dataclasses that validate themselves when built; a refusal
names the request field in backticks (`` `task_code` ``). The CLI shows that field as its flag,
and the HTTP API returns it as it is. Run selection is always `run_id` or `run_key`, or neither
for the pipeline's single non-terminal run.

| Request | Actions |
| --- | --- |
| `RunRequest` | `run`: a whole run, one task, a standalone selection, a rerun, a skip or a backfill (`backfill_from`, `backfill_to`) |
| `MarkRequest` | `mark`: a run, a task, a stand-in run (`new_run`) or a watermark reset |
| `ControlRequest` | `control`: `STOP`, `ABORT`, `HOLD`, `RELEASE`, `RECOVER`, `RESTART`, `RUN_PAUSED`, and `PAUSE`/`RESUME` of a definition |
| `BulkControlRequest` | `control bulk`: up to 100 keyed controls, each invoked and recorded on its own |
| `RunSelection`, `TaskSelection`, `CapabilitiesInput`, `HistoryInput` | `status`, `steps`, `explain`, `capabilities`, `history` |
| `PipelineInput`, `NoInput`, `AuditInput`, `ValidateInput`, `LineageInput` | `graph`, `pipeline`, `pipelines`, `audit`, `validate`, `lineage` |

Over HTTP (`ctx.transport` is `HTTP`) a whole-pipeline run is admitted for the controller,
which runs it, and `force`, `skip` and `ignore_dependencies` (except with `standalone`) are
refused with where they work. `ctx.child` controls the child process's logging and execution
options. Project files remain relative to `config.project_dir`.

Long-running processes (`controller`, `publish-docs`) are started by their interface inside
`actions.admitted(ctx, name, request)`, which applies the same role check and audit.

Domain refusals raise the existing `core.errors` classes. Read actions use one consistent
database snapshot for related views and record nothing.

## Shared permissions

`OperationContext.role` carries the role proven by the caller's token or session. Leaving it
unset refuses every action that needs a role; a claimed actor name supplies no permission. The
role each action needs is a column of the action table, so changing it changes every interface.
Do not compare roles in a command or a route.

See the [deployment role table](../deploying/security.md#deployment-accounts). A role refusal
raises `PermissionDeniedError` with exit code 26, records one `AUD_ACTIONS.OUTCOME=REFUSED` row,
and names the account, its role, the action, the role it needs and the remedy. Local
provisioning (`setup`, `init-db`, `migrate`, `doctor` before any admin exists) skips only the
role check of the actions the table marks as provisioning.

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

Catalog generation also appends an immutable completion row: `SUCCESS` or `FAILURE`,
with an exit code and the same start timestamp and arguments as its request. The request row
is never updated. This applies to manual and controller-scheduled catalog builds.

## JSON on the command line

These commands accept `--format json`: `run`, `mark`, `stop`, `abort`, `hold`, `release`, `pause`, `resume`,
`reconcile`, `pipelines`, `graph`, `steps`, `history` and `audit`. Text remains the default.

```bash
etl-craft pipelines --format json
etl-craft history --pipeline-code SALES --run-id 42 --format json
etl-craft history --pipeline-code SALES --task-code load --run-id 42 --format json
etl-craft run --pipeline-code SALES --init-only --run-key daily:2026-10-01 --format json
```

Standard output contains one JSON document produced by `to_json`, including empty lists.
Errors and logs remain on standard error, and command exit behavior is unchanged. Audit text
renders captured JSON consistently on SQLite and PostgreSQL.

Dates and timestamps use ISO 8601; typed Engine DB timestamps use UTC with an explicit offset.
Enums use their values and tuples become arrays. Captured metadata JSON keeps its original values.
The serializer returns a fresh dictionary.

| Schema | Document |
| --- | --- |
| `etl-craft/catalog-gaps/1` | Dated schema comparison coverage and lineage gaps, shared with generated documentation |
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

`status` returns `StatusView` and `explain` returns `Explanation`; the pure
`operations.status.explain(snapshot)` computes the same explanation from a captured
`TaskSnapshot`. `validate` returns the validation report, `doctor` returns `DoctorView`, and
`lineage` (`LineageInput`, with `direction` `BOTH`, `UPSTREAM` or `DOWNSTREAM`) returns
`LineageView`; `lineage refresh` works every task's lineage out again first. Every document uses
`to_json` and a versioned `SCHEMA`, matching CLI `--format json` output.

Run results include `waiting=True` when a single-task command recorded nothing because
its dependencies were not met. That call exits 23; unfinished executions exit 22. Initializing
a run with `--init-only` exits 0 while the controller runs it.

## Actor scopes and run writers

`ActorKind` lives in `core.enums` and contains `USER`, `SERVICE` and `SYSTEM`; its existing
import through `core.actor` also works. Replace the old `HUMAN` kind with `USER`, and
`SCHEDULE`, `ORCHESTRATOR` and `WORKER` with `SERVICE`. Historical audit values remain
unchanged. Authenticate once and use the returned identity and current role in
`OperationContext`; library calls that write directly must use `acting_as` explicitly.
An unset `current_actor()` raises `ConfigurationError`. Database connection reads used for
authentication do not invent an actor.

A handler receives the exact run's durable `TaskContext.writer`. SQL merge and rejection audit
columns use this value; ordinary ETL runs retain `etl-craft`, including runs requested by users.
The initiator remains in the action/run audit separately from the warehouse writer.

The `capabilities` action exposes exact-run and task control availability and data effects.
`control bulk` applies only explicit keyed selections, retaining individual refusals beside
accepted controls. `RECOVER`, `RESTART` and `RUN_PAUSED` go through `control`; preparation
returns promptly with `execution_run_id`, separately from the actual execution outcome. Recovery preserves successful work; restart keeps current checkpoints
in a new context. See the [control guide](run-control.md#recover-or-restart-an-exact-execution).
