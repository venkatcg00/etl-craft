# Validating pipelines

`etl-craft validate` checks every active pipeline's metadata and reports every problem it finds,
without running anything. It reads the Engine DB, the SQL files under `sql_scripts/` and the
scripts under `python_scripts/`, and checks each task with the same code a run uses, so a
definition that passes `validate` passes the same checks when the task runs. Scripts are parsed,
not imported: none of their code runs.

```text
$ etl-craft validate
[FAIL] SALES.load: SQL_ACTION='SCD1_MERG' is not one of CREATE_TABLE, ... — did you mean: SCD1_MERGE
[WARN] SALES.load: SQL task parameter MERGE_KEYS is not read by etl-craft, so it has no effect; did you mean MERGE_KEY
[FAIL] SALES.alert: reports on the whole run but does not depend on publish, so it can run before they finish and report on a run still in progress; add an ALWAYS dependency on each
checked 4 pipeline(s) and 23 task(s): 2 failed, 1 warning(s)
```

`--pipeline-code SALES` reports on one pipeline. The command exits 1 when anything fails, so it
can gate a deployment or a pull request that changes the metadata. Warnings do not change the
exit status.

Deployment-wide validation also reports `metadata normalization` warnings before the relational
contract upgrade. It checks active and inactive config keys, source-column identities and
positions, and task/rule target references. A source written as `raw.orders` and another as
`analytics.raw.orders` resolve to the same object when `analytics` is the configured catalog.
Warehouse case rules distinguish or fold quoted names as the dialect requires; a quoted dot
belongs to one identifier component. This readiness parser does not enable quoted targets in
the current execution handlers.

Each conflict names the row IDs and active flags. Assign distinct identities and repair their
dependent references before upgrading. Setting a duplicate inactive is insufficient: inactive
rows must reserve their identities in the normalized contract. These warnings do not edit rows
or history, choose a surviving duplicate, or prevent currently supported legacy runs. SQL
expressions, file paths and JSON parameters are not interpreted as object names.
`--pipeline-code` omits these deployment-wide checks. The same warnings appear in `config plan`
and `config apply` because those commands validate the resulting deployment configuration.
The archived legacy `0006_retire_and_reuse.sql` example deliberately reuses the retired `publish` task
code and therefore produces a readiness warning. It runs only before Engine migration 0027; give
the retired task a distinct code and repair references before the identity upgrade. The current example reactivates the existing task instead.

## What it checks

A `FAIL` is something that would fail a run, hold it up for ever, or make it quietly do
something other than what the metadata says.

| Area | Fails when |
|---|---|
| Codes | a `PIPELINE_CODE` or `TASK_CODE` holds anything but one ASCII letter followed by letters, digits or `_`, at most 128 characters; codes appear in commands and file names |
| `PIPELINE_PARAMETERS` | a value has the wrong type, such as `"RETRIES": "3"` |
| A pipeline's graph | a cycle, a self-dependency, a run condition its dependencies cannot meet, or a dependency on an inactive task |
| Pipeline dependencies | a cycle between pipelines, or a dependency on an inactive pipeline |
| Task dependencies | a dependency on an inactive task or pipeline elsewhere; `DEPENDS_ON_PIPELINE_ID` naming a pipeline the upstream task is not in; a `HAS_DATA` dependency on a task that never reports target rows (`BUSINESS_RULES`, `EMAIL_ALERT`, and the SQL actions `SETUP_TABLE`, `DROP_TABLE`, `DELETE_ROWS` and `CREATE_VIEW`) |
| `SQL` tasks | everything the task checks before it starts: the action, the target, exactly one read-only SELECT (inline, or a file that exists), the pipeline-id tokens, the merge parameters, `TABLE_FORMAT`, storage parameters the warehouse would ignore, and which audit columns a `SETUP_TABLE` target gets |
| `BUSINESS_RULES` tasks | no active rule; a rule's key column, target table or SQL; a rule keyed on `ROW_ID` of a table a `CREATE_TABLE` or `OVERWRITE_TABLE` task rebuilds, since its flags could never be cleared |
| `SQL_SCRIPT` tasks | `SCRIPT_FILE` missing, outside `sql_scripts/`, unreadable or empty; invalid `INPUT_PARAMS`, engine-key overrides or missing binds; warns when a parsed write overlaps a managed target, naming both tasks |
| `PYTHON` tasks | `SCRIPT_FILE` missing or outside `python_scripts/`, `INPUT_PARAMS` not a JSON object, or a script with a syntax error, no top-level `run`, an `async` `run`, or a `run` that takes more than the task |
| `EMAIL_ALERT` tasks | recipients, `EMAIL_ON_STATUS`, or tokens in a subject or body; a subject or body missing for any outcome the task can send on; or an alert that does not depend on every task that nothing else depends on, so it could report on a run still in progress (an alert whose every dependency is `FAILURE` watches those tasks instead, and is not checked for this) |
| Every task | a `TASK_TIMEOUT_SECONDS` that is not a whole number; invalid retry budgets, delays or backoff; a SQL, business-rules or alert task in a project without the `Warehouse` or `Email` section it needs |

A `WARN` works, but deserves a look:

- a task parameter, or a `PIPELINE_PARAMETERS` key, that etl-craft does not read, with the
  nearest names. `PYTHON` tasks are not checked, since a script may read any parameter of its
  own;
- an alert that depends on a task through `SUCCESS` or `HAS_DATA` while it sends on `FAILED` or
  `COMPLETED_WITH_ERRORS`: when that task fails, the alert is skipped and sends nothing;
- a business rule's SQL that sqlglot cannot read
  in the warehouse's dialect, with the line and column where it stopped. The warehouse decides
  whether it is valid when the task runs, and column lineage cannot trace it.

`validate` compares active [source definitions](lineage.md#source-definitions-and-stars) with
warehouse tables that exist, reporting added, missing and differently typed columns. Tables
that do not yet exist are skipped. With no active definitions it stays metadata-only; it never
connects to the email relay. [`doctor`](../deploying/doctor-and-setup.md) checks connections.
Column lineage is checked by [`lineage --strict`](lineage.md).

## Refresh and the version that runs

`etl-craft pipelines refresh` checks the selected project's **Engine DB state**, SQL, Python
scripts, lineage and immutable bundle. Use `--pipeline SALES_DAILY` to check one pipeline.
Project `CAN_MANAGE` is required. `config apply` performs the same refresh after committing
its metadata changes. A failed refresh does not roll back those changes: correct its findings
and refresh again.

Each result records its actor, time, metadata hash, bundle and findings. Findings identify the
file and line where available, or the exact Engine DB row when there is no source file, plus
the value and remedy. The API exposes the refresh action and the pipeline-refresh history read.
A broken pipeline remains listed. Admission uses the most recent **passing** metadata snapshot
and bundle, so later edits cannot change its tasks or executable files. Existing runs keep
their own pinned version.

A pipeline with no passing version cannot start. Its refusal lists the findings and the
`etl-craft pipelines refresh` remedy. `BLOCK_ON_REFRESH_ERROR=Y` in `pipelines.csv` also blocks
new runs when the latest refresh failed; its default `N` allows the last passing version.
PostgreSQL direct config edits are detected through their `sql:` history on the controller's
existing maintenance cycle. SQLite direct SQL edits remain unsupported.

An unchanged refresh still records a result and shows the findings. Text says
`SALES_DAILY: no changes since the last refresh at <time> (<result>)`; JSON includes
`"changed": false`. Changed results name the affected tables, tasks and files.

### Stores, upgrades and retention

With a SQLite Engine DB and no `Artifacts.Store`, bundles use
`file://<project_dir>/.etl-craft/artifacts`. Doctor names this path. Ignore `.etl-craft/` in git
and back it up together with the SQLite file. PostgreSQL deployments must configure a durable
shared `Artifacts.Store`; setup, doctor and refresh refuse without it.

The upgrade seeds passing versions only from successful runs that retained both their metadata
snapshot and bundle. Setup or the first controller start automatically refreshes pipelines
without such a seed. Passing pipelines get a baseline; failing pipelines remain listed in
setup and doctor and cannot start until corrected. Other pipelines are unaffected.

Retention keeps the latest result, last passing snapshot and bundle while the pipeline exists.
Older results follow `Retention.Config_history_days`. Bundles referenced by any retained run
or refresh are protected, and shared file objects are removed only after their last bundle
reference disappears.

Managed SQL task SELECTs must parse with sqlglot in the project's warehouse dialect. Both
`validate` and refresh report an error with the file, line, column, parser message and dialect.
Fix the SQL, or move vendor syntax sqlglot cannot parse into a `SQL_SCRIPT` task. Unmanaged
SQL_SCRIPT statements are not required to parse. Existing admitted runs keep their pinned
last-good version. Upgrade refresh failures remain listed and do not abort the upgrade.
