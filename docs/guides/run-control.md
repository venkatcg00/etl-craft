# Stepping in: mark, cancel, pause, backfill, rerun and bypasses

etl-craft is the orchestrator, so it is where you step in when a run needs a hand:
a failure you have fixed by other means, an upstream that cannot run where you are, a task to run
again, a run that must stop now. Every change needs a reason, is recorded in `AUD_RUN_INTERVENTIONS` with who made
it and when, and erases nothing: each attempt keeps its row, its log file and, in the
intervention, the message it had before.


## Choose the run

`run`, `mark`, `stop`, `abort`, `hold`, `release`, `history` and `steps` accept `--run-id 42` or
`--run-key 'manual:...'`. The identity must belong to `--pipeline_code`. Without a selector,
operations use the pipeline's single non-terminal whole-pipeline run. Independent manual
selections always require an explicit selector to resume. When none or several exist, the command
refuses and lists candidates with their id, key, trigger kind, run date and status. It never
selects a completed run by recency.

```bash
etl-craft history --pipeline_code SALES_DAILY --all
etl-craft history --pipeline_code SALES_DAILY --run-id 42
etl-craft steps --pipeline_code SALES_DAILY --run-id 42
```

`history --all --limit 20` lists runs for finding an identity. `steps` shows each task's status
under the selected run alongside its current metadata parameters. Creating a run with a whole
`run` or `--init-only` needs no existing identity; a missing `--run-key` creates that key.
`--backfill`, `--skip` and `mark --new-run` create their own new identities and refuse selectors.

## Mark a task

```bash
etl-craft mark --pipeline_code SALES_DAILY --run-id 42 --task_code load_orders --status SUCCESS \
    --reason "loaded by hand from the 06:00 file"
```

The task is set to `SUCCESS`, `FAILED` or `SKIPPED` under the selected run, and its
`ERROR_MESSAGE` says who marked it and why. A task that is running is refused: cancel the run
first, or wait.

A task left `IN-PROGRESS` after its supervisor dies is recovered when its lease expires.
Whole-pipeline supervisors and task supervisors hold 60-second leases, renewed every 15 seconds.
A second whole-pipeline command refuses while the run remains owned. Every `run` reconciles
expired leases before admission, or you can request reconciliation directly:

```bash
etl-craft reconcile --pipeline_code SALES_DAILY
etl-craft reconcile --pipeline_code SALES_DAILY --task_code load_orders
```

Without filters, `reconcile` checks every pipeline. An expired local attempt becomes `LOST` and
its task summary becomes `FAILED`. A verified local child and its observed descendants are
stopped; host, PID and process birth identity protect against signalling a reused PID. An
attempt on another host receives another 120 seconds of grace after expiry. An expired run
with no active attempts becomes unowned and can resume under the same run identity.

`LOST` means the outcome is uncertain: a warehouse commit or external script side effect may
already have happened. Check the target before retrying operations that cannot safely repeat.
Old attempt owners cannot write outcomes after expiry or after a newer attempt starts.

`mark --stale` remains a compatibility option: it reconciles this task's expired attempts before
marking. It refuses while the task remains live; it does not override a valid lease.

```bash
etl-craft mark --pipeline_code SALES_DAILY --task_code load_orders --status FAILED --stale \
    --reason "the host restarted at 03:10"
```

If an explicitly selected run has ended, it is reopened (`IN-PROGRESS` again), and the tasks the engine skipped
without running are reset, so running the pipeline again resumes it from there:

```bash
etl-craft run --pipeline_code SALES_DAILY
```

Mark a failed task `SUCCESS` and the tasks waiting on it run; the rest of the run is not repeated.
Mark a task `FAILED` and it gets another attempt.

A marked `SUCCESS` satisfies a `HAS_DATA` dependency only when you state the row count:

```bash
etl-craft mark --pipeline_code SALES_DAILY --run-id 42 --task_code load_orders --status SUCCESS \
    --rows 1200 --reason "1200 rows loaded by hand"
```

## Reset a SQL watermark

```bash
etl-craft mark --pipeline_code SALES --task_code events --run-id 42 \
  --watermark '2026-01-01T00:00:00' --reason 'reload corrected source history'
```

The task must be a SQL task with `WATERMARK_COLUMN`, `WATERMARK_TYPE` and `WATERMARK_START`.
The value is checked against that type. Select an exact existing run with `--run-id` or
`--run-key`, or the single active run, for the intervention record. This resets the task's
position for its next execution; it changes no status or past range. To rerun an already
successful task, use `run --rerun --task_code ... --run-id ... --reason ...` afterwards.

The request needs a reason and cannot combine `--watermark` with `--status`, `--rows`,
`--stale` or `--new-run`. Queued, claimed and running attempts refuse the reset until they end
or are cancelled and reconciled. The target lock serializes resets with SQL reads and writes.
`AUD_RUN_INTERVENTIONS` records the actor, reason and old/new typed positions as a `MARK`;
`AUD_ACTIONS` records the full request. Backfills do not change the stored position.

Merges can reload without duplicate keys. Resetting an append can append a row already written
by a different task run: use a merge when reloading earlier successful batches.

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
A pipeline with a run in progress is refused: mark that run, or stop or abort it.

## Stop or abort a run

| Command | Running attempts | New attempts and retries | Stored outcome |
|---|---|---|---|
| `stop` | finish and keep their own outcome | blocked | the run ends `CANCELLED` once they have ended |
| `abort` | their task processes are stopped now, ending `CANCELLED` | blocked | the run ends `CANCELLED` |
| `hold` | finish and keep their own outcome | blocked until `release` | the run stays `IN-PROGRESS` |

```bash
etl-craft abort --pipeline_code SALES_DAILY --reason "the source sent yesterday's file"
etl-craft stop --pipeline_code SALES_DAILY --run-id 42 --reason "drain before maintenance"
etl-craft abort --pipeline_code SALES_DAILY --run-id 42 --task_code extract --reason "wrong input"
etl-craft status --request-id 17 --format json
```

Each acts on the selected run: `--run-id`, `--run-key`, or the pipeline's single active run.
Add `--task_code TASK` to affect only that task under the selected run. Other tasks, pipelines and
independent manual runs keep running. Abort does not undo warehouse commits. Successful results,
counts, logs, checkpoints and recorded consumption remain intact.

The response is `etl-craft/control/1`, with a durable `control_id`, exact scope, pinned attempt
ids, actor and reason. `REQUESTED` means the fence is committed; `ACKNOWLEDGED` means the agent
has observed it but process/query endings are still pending; `COMPLETED` requires actual exit
and query-ending observations. The separate `run_status` always shows the stored run outcome.
A run-scope stop ends the run `CANCELLED` only after those observations; a task-scope stop leaves
the run active and retains that task's fence. `--request-key KEY` makes retries refer to the
original request. Repeating the same latest scoped action also returns its original identity.
Inspect a completed request by id even after its run ends.

Abort stops only the affected owned child processes, checking their PID birth identities before
signalling. Embedded DuckDB queries end with those processes. Server-side warehouse queries
currently have no durable cancellation adapter: abort refuses before signalling or changing the
scope; use `stop` for supported handlers to let them finish. Python scripts on
server-side warehouses cannot certify every script-owned query ending;
scoped stop refuses while such a script is active, with definition pause as the dispatch-only
alternative. Built-in synchronous handlers commit their query acknowledgement with its outcome; an abnormal server-side
query failure without such evidence stays pending. A crashed owner, expired lease, terminal task
summary or disconnected browser is never query-ending evidence. Unknown acknowledgements remain
pending, with their attempt ids in the response. Reconcile records expired active owners as `LOST` before observing verified local exits;
existing terminal outcomes are preserved. Server-side query status remains unknown without
its own acknowledgement. Native controls
in legacy external-orchestrator mode report the missing adapter and direct the operator to that
orchestrator. Server-side `EVENT_SQL` stop/abort is unavailable without a query adapter; use a
definition pause or wait for a successful check. An active event probe can refuse a scoped
control until its current check finishes, rather than claiming its query has stopped.

## Hold or release dispatch in a selected run

```bash
etl-craft hold --pipeline_code SALES_DAILY --run-id 42 --reason "hold dispatch"
etl-craft hold --pipeline_code SALES_DAILY --run-id 42 --task_code transform --reason "inspect input"
etl-craft release --pipeline_code SALES_DAILY --run-id 42 --task_code transform --reason "ready"
etl-craft release --pipeline_code SALES_DAILY --run-id 42 --reason "resume dispatch"
```

Holds block new admission, including force and retry claims, while existing attempts finish. A task hold
leaves its dependents pending and lets unrelated ready tasks run. The run stays `IN-PROGRESS`;
release the exact held scope, then resume with `etl-craft run` if its supervisor has exited.
Releasing a task does not release a run hold, and releasing a run does not release task holds.
Release refuses until an earlier stop/abort in that scope has completed.

`pause` and `resume` act on the pipeline definition instead (below). Definition pauses and
holds are independent: `release` does not clear a definition pause, and `resume` does not release
a hold.

HTTP clients use `POST /api/v1/pipelines/{code}/controls` with `action` (`HOLD`, `RELEASE`,
`STOP`, `ABORT`), `reason`, optional `run_id`/`run_key`, `task_code` and `request_key`.
`GET /api/v1/control-requests/{control_id}` returns the same document as `status --request-id`.
Controls require operator; inspection requires viewer. Local and `--server` commands call the
same services and preserve the authenticated actor.

## Pause and resume a pipeline

```bash
etl-craft pause --pipeline_code SALES_DAILY --reason "the CRM is being migrated this week"
etl-craft resume --pipeline_code SALES_DAILY --reason "the migration is done"
```

While a pipeline is paused, `etl-craft run` starts nothing of it (whole runs, single tasks and
`--init-only` alike): it says the pipeline is paused, by whom and why, and exits `0`, so a
scheduler that keeps calling it raises no alarm. A run in progress when the pipeline is paused
lets its running tasks finish, starts no more, and stays `IN-PROGRESS`; the interrupted run
command exits `22` (`INCOMPLETE`). After `resume`, the next
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

## Backfill over dates

```bash
etl-craft run --pipeline_code SALES_DAILY --backfill 2026-09-01:2026-09-07 \
    --reason "the source resent the first week of September"
```

Runs the pipeline once for each date, oldest first, each a run of its own as of its date: SQL
tasks read it as [`$$run_date`](sql-tasks.md#the-pipeline-id-tokens), scripts as
`task.run_date`. Backfill runs leave the scheduled runs as they were: they check no dependency on
other pipelines and consume no upstream run (each run records that as a `GATE_BYPASS` with the
reason), and scripts get no offset and store none. The backfill stops at the first run that does
not end `SUCCESS` or `SKIPPED`, and says how to take up from that date once it is fixed. It runs
at most 366 dates, and not while the pipeline has a run in progress or is paused.

A single run can be given its date too: `etl-craft run --pipeline_code SALES_DAILY --run-date
2026-09-01`. `etl-craft history --all` shows each run's date, and which were backfill runs.

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
etl-craft run --pipeline_code SALES_DAILY --run-id 42 --task_code load_orders --rerun \
    --reason "the source resent the file"
etl-craft run --pipeline_code SALES_DAILY --run-id 42 --task_code load_orders --rerun --with-downstream \
    --reason "the source resent the file"
```

The task runs again under the selected run, although it already ended, as a new attempt
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
  dev:
    Dependency_gates: warn     # enforce (the default) | warn | off
```

- `enforce` checks every dependency and skips the run or the task when one is not satisfied.
- `warn` checks them all and goes ahead anyway, with a warning.
- `off` checks none.

Each dependency let through is recorded against the run, or the task, as a `GATE_BYPASS`, and the
run's summary says so. Only upstream runs that satisfied their dependency are consumed. `doctor`
warns whenever gates are not enforced.

## What was changed

`etl-craft history --pipeline_code SALES_DAILY --all` lists the interventions on the runs it shows,
under the runs, and the [catalog](catalog.md) lists those on each pipeline's last run.
`AUD_RUN_INTERVENTIONS` holds marks, reruns and dependency overrides below. Historical `CANCEL`
rows remain readable. New scoped controls, including cancellation, live in `AUD_CONTROL_REQUESTS`;
append-only `AUD_ATTEMPT_ACKNOWLEDGEMENTS` records each physical process/query ending independently
of the immutable attempt outcome:

| Column | Holds |
|---|---|
| `PIPELINE_RUN_ID`, `TASK_ID` | the run, and the task (`NULL` for a change to the run itself) |
| `ACTION` | `MARK`, `NEW_RUN`, `CANCEL`, `REOPEN` (a run a mark or a rerun reopened), `RESET` (a skipped task reset to run again), `RERUN`, `IGNORE_DEPENDENCIES`, or `GATE_BYPASS` |
| `FROM_STATUS`, `TO_STATUS` | the status before and after; `TO_STATUS` is `NULL` for a reset |
| `TARGET_COUNT` | the row count stated with `--rows` |
| `PREVIOUS_MESSAGE` | the row's `ERROR_MESSAGE` before the change |
| `REASON`, `REQUESTED_BY`, `REQUESTED_AT` | why (for a bypass, the dependencies let through), who (`user@host`) and when |

## Recover or restart an exact execution

Recovery prepares the selected run to resume unfinished work. It retains `SUCCESS` and
`SKIPPED` task summaries, immutable attempts, committed offsets/watermarks, admission decisions
and human data locks. A failed or cancelled run reopens as a repair; a subsequent successful
ending publishes its repaired output revision through the existing consumption policy.
Recovery does not roll back partially committed external work. Check the failed handler's
message and data effects before resuming it.

Restart prepares a **new run** with the original logical date. It keeps the source run's history
and the **current** task offsets/watermarks. It does not reset to the source run's historical
checkpoint and does not truncate, restore or unlock data. The new context runs tasks again;
for example, an append action can append again. Use the explicit supported watermark-reset
operation separately when replay is intended. A whole-pipeline restart goes through fresh
pipeline admission and records fresh dependency decisions. A manual restart retains its stored
selection and dependency-override intent; it never publishes or consumes pipeline outputs.

```bash
etl-craft run --pipeline_code SALES_DAILY --run-id 42 --recover \
    --request-key recover-42 --reason "repaired input" --format json
etl-craft run --pipeline_code SALES_DAILY --run-id 42 --restart \
    --request-key restart-42 --reason "new execution with current checkpoints" --format json
```

These commands **prepare** work and return promptly with a durable `control_id` and
`execution_run_id`. `COMPLETED` means preparation committed, not that any task succeeded.
`pipeline_run_id` remains the source identity; inspect the execution run separately. On a server,
the controller picks up eligible whole-pipeline contexts. Locally, execute the returned identity
with `etl-craft run --pipeline_code SALES_DAILY --run-id EXECUTION_ID`; add `--task_code TASK`
when recovering only a task in an existing run. An independent restart selection can be driven
with `run --run-id EXECUTION_ID` without recreating its selection. Reusing a request key returns
the original preparation even after the execution finishes. A new restart key deliberately
creates a new context; keys cannot change scope or action.

Neither operation adopts an expired owner or assumes a query ended. Stop/drain the exact scope,
wait for actual process/query acknowledgement and reconcile expired ownership first. A live
supervisor, a pending stop, an unobserved process exit or an unknown server query prevents
preparation. A task restart needs the same target/checkpoint/dependency isolation as a standalone
selection. Task-only recovery of a terminal whole-pipeline run refuses to reopen its siblings;
recover the whole run, use a supported isolated restart, or explicitly rerun the task instead.
Recovery clears its own dispatch fence, retaining other task holds. Restart does not clear any
source fence. Successful and skipped evidence is never reset by recovery.

## Run once while automatic execution remains paused

For an existing exact execution, authorize a one-run exception with a reason:

```bash
etl-craft run --pipeline_code SALES_DAILY --run-id 43 --allow-paused \
    --request-key manual-43 --reason "approved maintenance run"
etl-craft run --pipeline_code SALES_DAILY --run-id 43
```

This permission lasts for that run. The definition pause stays open and blocks automatic starts
and other executions. It grants no release of a scoped hold, stop fence, dependency requirement
or human data lock. To create a new context directly, omit the run selector and provide a request key:
`run --pipeline_code SALES_DAILY --allow-paused --request-key maintenance-1 --reason ...`.
Add `--task_code TASK` for an isolated one-task context; the existing standalone isolation and
outside-dependency checks still apply. Creation and permission commit together, and repeated
keys return the same context. A new whole-pipeline context is queued for ordinary dependency
admission; isolated task contexts require explicit execution. Automatic schedules remain paused.

## Inspect capabilities and apply an explicit selection

`status --capabilities` exposes the same scoped action/state/handler and role decisions as the
HTTP API. A refusal includes its remedy; inspection does not request or release a control.
An action always revalidates under the execution's locks, so an available snapshot is not a
reservation. Existing task `explain` output describes run readiness and dependency blockers.

```bash
etl-craft status --pipeline_code SALES_DAILY --run-id 43 --task_code transform \
    --capabilities --format json
etl-craft run --controls-file selected-controls.json --format json
```

The selection file is a UTF-8 JSON list of 1–100 control objects:

```json
[
  {"pipeline_code":"SALES_DAILY","run_id":43,"task_code":"transform",
   "action":"HOLD","reason":"inspect input","request_key":"hold-transform-43"},
  {"pipeline_code":"OTHER","run_id":12,"action":"STOP",
   "reason":"maintenance","request_key":"stop-other-12"}
]
```

Each object requires an explicit `run_id` or `run_key`, nonblank reason and unique request key.
Only selected objects are affected. Results include each accepted control or its independent
refusal; this is not an all-or-nothing transaction. A nonzero CLI exit means at least one object
refused, while earlier accepted actions remain committed. Retry the same selection with the
same keys to inspect accepted requests safely. Bulk preparation does not wait for task execution
or agent/query endings. The same flags support `--server`.

The shared HTTP routes are `GET /api/v1/pipelines/{code}/capabilities`,
`POST /api/v1/pipelines/{code}/controls` (including `RECOVER`, `RESTART`, `RUN_PAUSED`) and
`POST /api/v1/controls/bulk` with `{"requests": [...]}`. Viewers can inspect capabilities and
requests; operators can submit actions. `execution_run_id` is additive in `etl-craft/control/1`.
Legacy external orchestrators still require their own agent/query adapter; capabilities name
that limitation rather than claiming a native action completed.
