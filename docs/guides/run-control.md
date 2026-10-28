# Stepping in: mark, cancel, pause, backfill, rerun and bypasses

etl-craft is the orchestrator, so it is where you step in when a run needs a hand:
a failure you have fixed by other means, an upstream that cannot run where you are, a task to run
again, a run that must stop now. Every change needs a reason, is recorded in `AUD_RUN_INTERVENTIONS` with who made
it and when, and erases nothing: each attempt keeps its row, its log file and, in the
intervention, the message it had before.


## Choose the run

`run`, `mark`, `stop`, `abort`, `hold`, `release`, `history` and `steps` accept `--run-id 42` or
`--run-key 'manual:...'`. The identity must belong to `--pipeline-code`. Without a selector,
operations use the pipeline's single non-terminal whole-pipeline run. Independent manual
selections always require an explicit selector to resume. When none or several exist, the command
refuses and lists candidates with their id, key, trigger kind, run date and status. It never
selects a completed run by recency.

```bash
etl-craft history --pipeline-code SALES_DAILY --all
etl-craft history --pipeline-code SALES_DAILY --run-id 42
etl-craft steps --pipeline-code SALES_DAILY --run-id 42
```

`history --all --limit 20` lists runs for finding an identity. `steps` shows each task's status
under the selected run alongside its current metadata parameters. Creating a run with a whole
`run` or `--init-only` needs no existing identity; a missing `--run-key` creates that key.
`--backfill`, `--skip` and `mark --new-run` create their own new identities and refuse selectors.

## Mark a task

```bash
etl-craft mark --pipeline-code SALES_DAILY --run-id 42 --task-code load_orders --status SUCCESS \
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
etl-craft reconcile --pipeline-code SALES_DAILY
etl-craft reconcile --pipeline-code SALES_DAILY --task-code load_orders
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
etl-craft mark --pipeline-code SALES_DAILY --task-code load_orders --status FAILED --stale \
    --reason "the host restarted at 03:10"
```

If an explicitly selected run has ended, it is reopened (`IN-PROGRESS` again), and the tasks the engine skipped
without running are reset, so running the pipeline again resumes it from there:

```bash
etl-craft run --pipeline-code SALES_DAILY
```

Mark a failed task `SUCCESS` and the tasks waiting on it run; the rest of the run is not repeated.
Mark a task `FAILED` and it gets another attempt.

A marked `SUCCESS` satisfies a `HAS_DATA` dependency only when you state the row count:

```bash
etl-craft mark --pipeline-code SALES_DAILY --run-id 42 --task-code load_orders --status SUCCESS \
    --rows 1200 --reason "1200 rows loaded by hand"
```

## Reset a SQL watermark

```bash
etl-craft mark --pipeline-code SALES --task-code events --run-id 42 \
  --watermark '2026-01-01T00:00:00' --reason 'reload corrected source history'
```

The task must be a SQL task with `WATERMARK_COLUMN`, `WATERMARK_TYPE` and `WATERMARK_START`.
The value is checked against that type. Select an exact existing run with `--run-id` or
`--run-key`, or the single active run, for the intervention record. This resets the task's
position for its next execution; it changes no status or past range. To rerun an already
successful task, use `run --rerun --task-code ... --run-id ... --reason ...` afterwards.

The request needs a reason and cannot combine `--watermark` with `--status`, `--rows`,
`--stale` or `--new-run`. Queued, claimed and running attempts refuse the reset until they end
or are cancelled and reconciled. The target lock serializes resets with SQL reads and writes.
`AUD_RUN_INTERVENTIONS` records the actor, reason and old/new typed positions as a `MARK`;
`AUD_ACTIONS` records the full request. Backfills do not change the stored position.

## Reset a Python script's offset

```bash
etl-craft mark --pipeline-code CLICKS --task-code read_topic \
  --offset '{"3": 120, "7": null}' --reason 'partition 7 removed; replay partition 3'
```

The task must be a Python task. For a `PARTITIONS` offset the value is a JSON object: each named
partition is set or added, `null` removes it, and partitions not named keep their positions.
Removing a partition the offset does not hold fails. For a `NUMBER`, `TEXT` or `TIMESTAMP`
offset the value is the new offset, cast to the stored datatype. The reset needs a reason, takes
the same run selection as `--watermark`, cannot be combined with `--status`, `--rows`, `--stale`
or `--new-run`, and is refused while the task has a queued, claimed or running attempt.
`AUD_RUN_INTERVENTIONS` records the old and new offsets as a `MARK`, with the actor and reason.

Merges can reload without duplicate keys. Resetting an append can append a row already written
by a different task run: use a merge when reloading earlier successful batches.

## Mark a run

Without `--task-code`, `mark` sets the run itself, ending it now if it had not ended:

```bash
etl-craft mark --pipeline-code SALES_DAILY --status SUCCESS --reason "load_orders is not needed today"
```

The pipelines that depend on this one judge the marked status. Its tasks keep theirs, and no
upstream run is consumed. A run with a task still running is refused.

## Record a stand-in run

When a pipeline cannot run where you are (an upstream that only runs in production, say), its
dependents' gates would never pass. `mark --new-run` records a finished run of it that did not
really run, and the gates judge it like any other:

```bash
etl-craft mark --pipeline-code CRM_EXPORT --new-run --status SUCCESS \
    --reason "CRM_EXPORT only runs in production"
etl-craft mark --pipeline-code CRM_EXPORT --new-run --task-code publish --status SUCCESS \
    --rows 50 --reason "stand-in for a HAS_DATA dependency on CRM_EXPORT.publish"
```

With `--task-code`, the task gets a row in the stand-in run too, for dependencies on that task.
A pipeline with a run in progress is refused: mark that run, or stop or abort it.

## Stop or abort a run

| Command | Running attempts | New attempts and retries | Stored outcome |
|---|---|---|---|
| `stop` | stop their processes and tagged queries | blocked | the run ends `CANCELLED` after confirmation |
| `abort` | their task processes are stopped now, ending `CANCELLED` | blocked | the run ends `CANCELLED` |
| `hold` | finish and keep their own outcome | blocked until `release` | the run stays `IN-PROGRESS` |

```bash
etl-craft abort --pipeline-code SALES_DAILY --reason "the source sent yesterday's file"
etl-craft stop --pipeline-code SALES_DAILY --run-id 42 --reason "drain before maintenance"
etl-craft abort --pipeline-code SALES_DAILY --run-id 42 --task-code extract --reason "wrong input"
etl-craft control-request --control-id 17 --format json
```

Each acts on the selected run: `--run-id`, `--run-key`, or the pipeline's single active run.
Add `--task-code TASK` to affect only that task under the selected run. Other tasks, pipelines and
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

Stop and abort signal only the affected owned child processes, checking their PID birth
identities first. Local supervisors signal their children immediately. Agents wake through the
API's pool and agent notifications; lease renewal is the fallback if a notification is lost.
Queued attempts become `CANCELLED` immediately. Running attempts acknowledge cancellation only
after their processes and warehouse queries have ended.

PostgreSQL, Snowflake, Trino and Databricks adapters cancel by the existing holder/session tag,
then confirm that no tagged statement remains. Python scripts must use `task.warehouse()` and
must not replace the session tag. SQL event probes carry their run-bound holder tag too.
Embedded DuckDB queries end with the process. No per-statement Engine DB record is needed.
A failed or unconfirmed cancellation stays pending with its reason; a cancel receipt, expired
lease, terminal task summary or disconnected browser is never query-ending evidence.
Reconciliation records expired active owners as `LOST` before observing verified local exits.

The capability document reads the warehouse adapter. A dialect without cancellation support
keeps the scoped server-query refusal; use definition pause to stop new dispatch while its
queries finish. Legacy external-orchestrator deployments continue to direct controls to that
orchestrator. An event probe with an unknown run identity must finish or be reconciled before
a scoped control can be accepted.

## Hold or release dispatch in a selected run

```bash
etl-craft hold --pipeline-code SALES_DAILY --run-id 42 --reason "hold dispatch"
etl-craft hold --pipeline-code SALES_DAILY --run-id 42 --task-code transform --reason "inspect input"
etl-craft release --pipeline-code SALES_DAILY --run-id 42 --task-code transform --reason "ready"
etl-craft release --pipeline-code SALES_DAILY --run-id 42 --reason "resume dispatch"
```

Holds block new admission, including force and retry claims, while existing attempts finish. A task hold
leaves its dependents pending and lets unrelated ready tasks run. The run stays `IN-PROGRESS`;
release the exact held scope, then resume with `etl-craft run` if its supervisor has exited.
Releasing a task does not release a run hold, and releasing a run does not release task holds.
Release refuses until an earlier stop/abort in that scope has completed.

`pause` and `resume` act on the pipeline definition instead (below). Definition pauses and
holds are independent: `release` does not clear a definition pause, and `resume` does not release
a hold.

HTTP clients use `POST /api/v1/controls` with `pipeline_code`, `action` (`HOLD`, `RELEASE`,
`STOP`, `ABORT`), `reason`, optional `run_id`/`run_key`, `task_code` and `request_key`.
`GET /api/v1/control-requests/{control_id}` returns the same document as `control-request`.
Controls require operator; inspection requires viewer. Local and `--api` commands call the
same services and preserve the authenticated actor.

## Pause and resume a pipeline

```bash
etl-craft pause --pipeline-code SALES_DAILY --reason "the CRM is being migrated this week"
etl-craft resume --pipeline-code SALES_DAILY --then wait --reason "the migration is done"
```

While a pipeline is paused, `etl-craft run` starts nothing of it (whole runs, single tasks and
`--init-only` alike): it says the pipeline is paused, by whom and why, and exits `0`, so a
scheduler that keeps calling it raises no alarm. A run in progress when the pipeline is paused
lets its running tasks finish, starts no more, and stays `IN-PROGRESS`; the interrupted run
command exits `22` (`INCOMPLETE`). After `resume`, the next
`run --pipeline-code` goes on with it. `etl-craft pipelines` and the catalog show every paused
pipeline, and `AUD_PIPELINE_PAUSES` keeps every pause with who paused and resumed it, when and
why.

## Skip a run on purpose

```bash
etl-craft run --pipeline-code SALES_DAILY --skip --reason "public holiday: no sales file"
```

Records a run of the pipeline `SKIPPED`, running nothing, as a stand-in run does (see
[Record a stand-in run](#record-a-stand-in-run)). The pipelines that depend on it see a run that
did nothing: a `SUCCESS` dependency on it is not satisfied, an `ALWAYS` one is.

## Backfill over dates

```bash
etl-craft run --pipeline-code SALES_DAILY --backfill 2026-09-01:2026-09-07 \
    --reason "the source resent the first week of September"
```

Runs the pipeline once for each date, oldest first, each a run of its own as of its date: SQL
tasks read it as [`$$run_date`](sql-tasks.md#the-pipeline-id-tokens), scripts as
`task.run_date`. Backfill runs leave the scheduled runs as they were: they check no dependency on
other pipelines and consume no upstream run (each run records that as a `GATE_BYPASS` with the
reason), and scripts get no offset and store none. The backfill stops at the first run that does
not end `SUCCESS` or `SKIPPED`, and says how to take up from that date once it is fixed. It runs
at most 366 dates, and not while the pipeline has a run in progress or is paused.

A single run can be given its date too: `etl-craft run --pipeline-code SALES_DAILY --run-date
2026-09-01`. `etl-craft history --all` shows each run's date, and which were backfill runs.

## Run a task without its dependencies

```bash
etl-craft run --pipeline-code SALES_DAILY --task-code load_orders --ignore-dependencies \
    --reason "extract_orders is late; the file is already in place"
```

The task runs under the pipeline's run in progress without its dependencies being checked, in
its pipeline or elsewhere, and consumes no upstream run. A task that already ended `SUCCESS` or
`SKIPPED` is not run again: that is `--rerun`.

## Run a task again

```bash
etl-craft run --pipeline-code SALES_DAILY --run-id 42 --task-code load_orders --rerun \
    --reason "the source resent the file"
etl-craft run --pipeline-code SALES_DAILY --run-id 42 --task-code load_orders --rerun --with-downstream \
    --reason "the source resent the file"
```

The task runs again under the selected run, although it already ended, as a new attempt
on its row that skips nothing (a business-rules task checks every rule again), without its
dependencies being checked. With `--with-downstream`, every task after it runs again too, in
dependency order, each once its own dependencies are satisfied; those whose dependencies are not
(an alert that waits for a failure, say) keep their rows. A run that had ended is reopened for
this and ended again from its tasks' statuses; a run still in progress is left for
`run --pipeline-code` to finish.

## Relax dependency gates

A local run checks its dependencies on other pipelines before it starts, and a task its
dependencies on other pipelines' tasks (see
[Dependencies on other pipelines](dependencies.md#dependencies-on-other-pipelines)). Where an
upstream cannot run at all, in a development environment, say, `Orchestration.Dependency_gates`
relaxes that per profile:

```yaml
Orchestration:
  dev:
    Dependency_gates: WARN     # enforce (the default) | warn | off
```

- `enforce` checks every dependency and skips the run or the task when one is not satisfied.
- `warn` checks them all and goes ahead anyway, with a warning.
- `off` checks none.

Each dependency let through is recorded against the run, or the task, as a `GATE_BYPASS`, and the
run's summary says so. Only upstream runs that satisfied their dependency are consumed. `doctor`
warns whenever gates are not enforced.

## What was changed

`etl-craft history --pipeline-code SALES_DAILY --all` lists the interventions on the runs it shows,
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
etl-craft run --pipeline-code SALES_DAILY --run-id 42 --recover \
    --request-key recover-42 --reason "repaired input" --format json
etl-craft run --pipeline-code SALES_DAILY --run-id 42 --restart \
    --request-key restart-42 --reason "new execution with current checkpoints" --format json
```

These commands **prepare** work and return promptly with a durable `control_id` and
`execution_run_id`. `COMPLETED` means preparation committed, not that any task succeeded.
`pipeline_run_id` remains the source identity; inspect the execution run separately. On a server,
the controller picks up eligible whole-pipeline contexts. Locally, execute the returned identity
with `etl-craft run --pipeline-code SALES_DAILY --run-id EXECUTION_ID`; add `--task-code TASK`
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
etl-craft run --pipeline-code SALES_DAILY --run-id 43 --allow-paused \
    --request-key manual-43 --reason "approved maintenance run"
etl-craft run --pipeline-code SALES_DAILY --run-id 43
```

This permission lasts for that run. The definition pause stays open and blocks automatic starts
and other executions. It grants no release of a scoped hold, stop fence, dependency requirement
or human data lock. To create a new context directly, omit the run selector and provide a request key:
`run --pipeline-code SALES_DAILY --allow-paused --request-key maintenance-1 --reason ...`.
Add `--task-code TASK` for an isolated one-task context; the existing standalone isolation and
outside-dependency checks still apply. Creation and permission commit together, and repeated
keys return the same context. A new whole-pipeline context is queued for ordinary dependency
admission; isolated task contexts require explicit execution. Automatic schedules remain paused.

## Inspect capabilities and apply an explicit selection

`etl-craft capabilities` exposes the same scoped action/state/handler and role decisions as the
HTTP API. A refusal includes its remedy; inspection does not request or release a control.
An action always revalidates under the execution's locks, so an available snapshot is not a
reservation. Existing task `explain` output describes run readiness and dependency blockers.

```bash
etl-craft capabilities --pipeline-code SALES_DAILY --run-id 43 --task-code transform \
    --format json
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
or agent/query endings. The same flags support `--api`.

The shared HTTP routes are `GET /api/v1/capabilities`,
`POST /api/v1/controls` (including `RECOVER`, `RESTART`, `RUN_PAUSED`) and
`POST /api/v1/controls/bulk` with `{"requests": [...]}`. Viewers can inspect capabilities and
requests; operators can submit actions. `execution_run_id` is additive in `etl-craft/control/1`.
Server-side queries without a cancellation adapter are named as a limitation in the
capabilities rather than claimed as stopped.

## Pause or resume a tagged group

A tag selection uses the same exact AND filter as `pipelines`:

```bash
etl-craft pause --tag team:sales --tag tier:gold --reason "source outage"
etl-craft pause --tag team:sales --tag tier:gold --reason "source outage" --yes
etl-craft resume --tag team:sales --tag tier:gold --reason "source returned" --yes
```

The first command prints the resolved codes and exits with usage status 2 without changing
anything. Review that list before repeating with `--yes`. Use either `--pipeline-code` or
`--tag`. Each pipeline succeeds or fails independently; a partial refusal exits 1. Unknown
tags select nothing. With JSON output, the preview goes to stderr and stdout remains one JSON
document containing every selected code and outcome. Large CLI selections use the existing
bulk action in batches of at most 100, after resolving the whole selection.

The API first reads `/api/v1/pipelines?tag=team:sales&tag=tier:gold`, then posts explicit
`pipeline_code`, `action` (`PAUSE` or `RESUME`) and `reason` objects in `requests` to
`/api/v1/controls/bulk`. Definition controls need no run id or request key. Run and task controls
still need their explicit run identity and unique request key. The audit records a `control`
request for each code; editing tags later cannot change that selection.

## Standing task pauses

A task pause applies across runs without changing config or requiring a refresh. Other branches
continue. With no outcome the task waits; an optional `SUCCESS`, `FAILED` or `SKIPPED` replaces
execution. Reasons are required in STRICT projects and optional in RELAXED projects.

```bash
etl-craft pause --pipeline-code SALES_DAILY --task-code load --reason "source maintenance"
etl-craft pause --pipeline-code SALES_DAILY --task-code load --replace --outcome SKIPPED \
  --expires-at 2026-12-01T12:00:00+00:00 --reason "skip during maintenance"
etl-craft resume --pipeline-code SALES_DAILY --task-code load --reason "source restored"
```

Expiry must be a future ISO-8601 timestamp with an offset; durations and timestamps without
an offset are refused. Expiry acts as unpause when a task is reached, without modifying history.
A repeated pause is refused unless `--replace` is given. Replacement closes the old version
and opens the new one in one transaction, with no unpaused window. Resume removes both the
pause and its outcome and creates no runs. Tasks have no schedule ticks, so task resume refuses
`--then`.

A run admitted before a pause or before a replacement never inherits the later outcome: it
waits at that task until unpaused, then executes normally. Runs admitted while a pause is in
force record its version and use that version's outcome. An outcome of FAILED creates no
attempt, retry or failure alert. A supplied SUCCESS writes no rows, satisfies no HAS_DATA gate
and publishes no fresh output revision to consumers. History records the applied pause id.
Status, explain, the metadata and graph reads, and config plan/apply show the control's actor,
reason, outcome and expiry. For example:

```text
load: standing pause 12: actor=operator, outcome=SKIPPED, expiry=until resume, reason='maintenance'
```

## Choosing work after pipeline resume

Scheduled pipeline resume requires `--then run-now|catch-up|wait`:

- `run-now` queues one immediate run.
- `catch-up` queues missed ticks retained by CATCHUP and MAX_CATCHUP_RUNS.
- `wait` creates nothing until the next scheduled tick.

An interactive CLI shows the retained ticks and asks for a choice. The API and non-interactive
CLI refuse an omitted choice. Unscheduled pipeline resume needs no choice and refuses `--then`.
Already-running work and independent run/task holds are preserved. The resume history records
the actor, reason, choice and created run ids; a controller restart preserves the next-tick
boundary. Bulk resume uses the same operation for each selected pipeline.
