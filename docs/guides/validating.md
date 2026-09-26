# Validating pipelines

`etl-craft validate` checks every active pipeline's metadata and reports every problem it finds,
without running anything. It reads the Engine DB, the SQL files under `sql_files/` and the
scripts under `ingestion_scripts/`, and checks each task with the same code a run uses, so a
definition that passes `validate` passes the same checks when the task runs. Scripts are parsed,
not imported: none of their code runs.

```text
$ etl-craft validate
[FAIL] SALES.load: SQL_ACTION='SCD1_MERG' is not one of CREATE_TABLE, ... — did you mean: SCD1_MERGE
[WARN] SALES.load: SQL task parameter MERGE_KEYS is not read by etl-craft, so it has no effect; did you mean MERGE_KEY
[FAIL] SALES.alert: reports on the whole run but does not depend on publish, so it can run before they finish and report on a run still in progress; add an ALWAYS dependency on each
checked 4 pipeline(s) and 23 task(s): 2 failed, 1 warning(s)
```

`--pipeline_code SALES` reports on one pipeline. The command exits 1 when anything fails, so it
can gate a deployment or a pull request that changes the metadata. Warnings do not change the
exit status.

## What it checks

A `FAIL` is something that would fail a run, hold it up for ever, or make it quietly do
something other than what the metadata says.

| Area | Fails when |
|---|---|
| Codes | a `PIPELINE_CODE` or `TASK_CODE` holds anything but letters, digits, `_` and `-`; codes appear in commands, DAG ids and file names |
| `PIPELINE_PARAMETERS` | a value has the wrong type, such as `"RETRIES": "3"` |
| A pipeline's graph | a cycle, a self-dependency, a run condition its dependencies cannot meet, or a dependency on an inactive task |
| Pipeline dependencies | a cycle between pipelines, or a dependency on an inactive pipeline |
| Task dependencies | a dependency on an inactive task or pipeline elsewhere; `DEPENDS_ON_PIPELINE_ID` naming a pipeline the upstream task is not in; a `HAS_DATA` dependency on a task that never reports target rows (`BUSINESS_RULES`, `EMAIL_ALERT`, and the SQL actions `SETUP_TABLE`, `DROP_TABLE` and `DELETE_ROWS`) |
| `SQL` tasks | everything the task checks before it starts: the action, the target, exactly one read-only SELECT (inline, or a file that exists), the pipeline-id tokens, the merge parameters, `TABLE_FORMAT`, storage parameters the warehouse would ignore, and which audit columns a `SETUP_TABLE` target gets |
| `BUSINESS_RULES` tasks | no active rule; a rule's key column, target table or SQL; a rule keyed on `ROW_ID` of a table a `CREATE_TABLE` or `OVERWRITE_TABLE` task rebuilds, since its flags could never be cleared |
| `PYTHON` tasks | `SCRIPT_NAME` missing or outside `ingestion_scripts/`, `INPUT_PARAMS` not a JSON object, or a script with a syntax error, no top-level `run`, an `async` `run`, or a `run` that takes more than the task |
| `EMAIL_ALERT` tasks | recipients, `EMAIL_ON_STATUS`, or tokens in a subject or body; a subject or body missing for any outcome the task can send on; or an alert that does not depend on every task that nothing else depends on, so it could report on a run still in progress (an alert whose every dependency is `FAILURE` watches those tasks instead, and is not checked for this) |
| Every task | a `TASK_TIMEOUT_SECONDS` that is not a whole number; a SQL, business-rules or alert task in a project without the `Warehouse` or `Email` section it needs |
| Remote mode | a rule the orchestrator does not support: run condition `N`, a `HAS_DATA` dependency, a task whose dependencies have different types, or with `Global_dag` on a pipeline whose dependencies on other pipelines do; see [Running under an orchestrator](../deploying/orchestrator.md#rules-an-orchestrator-does-not-support) |

A `WARN` works, but deserves a look:

- a task parameter, or a `PIPELINE_PARAMETERS` key, that etl-craft does not read, with the
  nearest names. `PYTHON` tasks are not checked, since a script may read any parameter of its
  own;
- an alert that depends on a task through `SUCCESS` or `HAS_DATA` while it sends on `FAILED` or
  `COMPLETED_WITH_ERRORS`: when that task fails, the alert is skipped and sends nothing.

`validate` does not connect to the warehouse or the email relay; [`doctor`](../deploying/doctor-and-setup.md)
does. Column lineage is checked by [`lineage --strict`](lineage.md).
