---
name: test-run-state
description: Test etl-craft's run and attempt state - every allowed and refused transition, run identity and selection, leases and fencing, retries that resume, recorded gate and consumption decisions, trigger tasks, holds and operator controls - including races between two callers. Use when a change touches engine/transitions, execution/, controller/, run creation, leases, locks, gates, retries or operator controls, or when auditing orchestration coverage.
---

# Test run state

The Engine DB must always tell the truth about every run and attempt. This skill lists how that
breaks and how to prove it cannot.

## How every check is written

- **The oracle is the Engine DB itself, read by named column.** Assert the `STATUS`, owner,
  lease, counts and audit rows of the exact run and attempt ids the test created. Never assert
  "the latest run".
- **Explicit times, never `sleep`.** Pass `now` values; poll with a bounded wait. Races use
  `tests/fixtures/races.py`: `two_at_once` pauses both callers at a named function,
  `paused_after` holds one change open while another runs.
- **Both Engine DBs.** Use `engine_db` (`tests/fixtures/engine_db.py`); it is parametrized over
  SQLite and PostgreSQL and each case carries its marker.
- **Repeat races 20 times** locally (`AGENTS.md`, "Concurrency"). CI runs each test once.
- **Prove the test bites.** Stash `src`, see it fail; then remove one guard on purpose (a
  `STATUS = :expected` condition, a row-count check) and see it fail.

## Checklist

### Transitions

Code: `engine/transitions.py`; tests: `test_transitions.py` (the attempt and run state
matrices).

1. Every pair (from, to) in the state machine: allowed ones succeed once, all others raise
   `StaleTransitionError` and change nothing. Generate the pairs from the enums, so a new
   status is covered automatically.
2. Every status change is compare-and-set: two callers making the same transition, one wins,
   the other sees a stale transition (`two_at_once`).
3. Outcome, task summary, script offset and consumption commit together: inject
   `attempt.before_summary`, `attempt.after_status`, `script.after_offset` and
   `attempt.after_consumption` (`ETL_CRAFT_FAULT`) and check none is half-written.
4. Pipeline endings commit status, SLA and consumption together (`pipeline.before_consumption`,
   `pipeline.after_consumption`); finalization hooks run only after the commit.

### Identity and selection

Code: `execution/run_start.py`, `services/operations/`; tests: `test_explicit_runs.py`,
`test_manual_runs.py`, `test_admission.py`.

1. Nothing resolves a run by recency: with two runs, a command without `--run-id` or `--run-key`
   refuses unless exactly one non-terminal whole-pipeline run exists.
2. The same run key requested twice (including at once): one run.
3. One `IN-PROGRESS` exclusive run per pipeline, enforced by the partial unique index even when
   the service check is bypassed.
4. Manual task selections never bind to a run implicitly; isolated selections overlap only
   where target, checkpoint and consumption isolation holds.

### Leases and ownership

Code: `execution/leases.py`, `execution/reconcile.py`; tests: `test_leases.py`.

1. An expired owner cannot renew, finish, append a log or advance an offset; only reconciliation
   moves it to `LOST`, and only then is a retry created.
2. A reused PID, or a process with a different birth identity, is never signalled.
3. Lease times come from the Engine DB clock: skew the process clock and the outcome is the
   same.
4. A supervisor killed after `supervisor.after_popen` or `runner.after_bind`: the next one
   reconciles before retrying, and the task runs once.

### Retries, gates and consumption

Code: `execution/`, `engine/`; tests: `test_retries.py`, `test_failure_classes.py`,
`test_gates.py`, `test_gate_decisions.py`.

1. A retry skips tasks already `SUCCESS` or `SKIPPED`; it never re-runs them.
2. Only retryable failure classes retry; a permanent one fails at once with its class recorded.
3. Cross-pipeline admission uses the recorded decision; changing the upstream after admission
   does not change the decision. A repaired run publishes one new revision, only on `SUCCESS`.
4. Gate waits use no agent slot and keep their budget across a restart (`AUD_GATE_WAITS`).

### Controls and triggers

Code: `services/operations/` controls, `handlers/run_pipeline.py`; tests:
`test_scoped_controls.py`, `test_recovery_controls.py`, `test_pipeline_triggers.py`,
`test_interventions.py`.

1. Hold, stop, abort, recover, restart and mark each refuse a run that changed after the
   operator read it, naming the run and its current status.
2. A `RUN_PIPELINE` task's child is admitted once, even across a controller restart; `WAIT`
   frees the slot; `DETACH` ends at the child's `QUEUED` admission.
3. A control and a run ending at the same moment (`two_at_once`): exactly one outcome, and the
   audit says which.

## Running

```bash
make services-up
uv run pytest -q -p no:cacheprovider -m "engine_sqlite or engine_postgres" <tests>
for i in $(seq 20); do
  ETL_CRAFT_REQUIRE_SERVICES=1 uv run pytest -q -p no:cacheprovider <race tests> || break
done
```

## Report

Per checklist row: the covering test, both dialects run, the 20-round count for races, and the
deliberate break it caught. Rows with no test are gaps.
