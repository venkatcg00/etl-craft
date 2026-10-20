---
name: test-chaos-recovery
description: Test that etl-craft rides out real failures - kill -9 of a task, supervisor or controller at each commit point, Engine DB restart and failover, warehouse and network drops, full disks, unwritable folders, clock skew and duplicate delivery - and that a restart recovers with no lost or doubled work and no operator. Use when a change touches process supervision, recovery, leases, outage handling or any write that spans two commits, or when closing S7.P catalog rows.
---

# Test chaos and recovery

1.0.0's promise is that outages are ridden out and a restart reconciles without an operator.
This skill proves it with real processes and real services, not mocks.

## How every check is written

- **Real failures.** Kill real processes (`SIGKILL`), stop and start the real PostgreSQL
  container, fill a real small filesystem. A mocked exception is not a crash: it runs `finally`
  blocks a real crash skips.
- **Named fault points.** `core/faults.py`: `ETL_CRAFT_FAULT=<point>` raises there and
  `<point>:kill` exits with 137 at once. List the points with `git grep -n 'fault_point(' src`.
  A new write that spans two commits gets a fault point between them, in the same PR.
- **The oracle after recovery.** Run the recovery (controller restart, reconcile, retry), then
  check: every attempt is terminal or legitimately live, no task ran twice where it must not,
  target rows equal the expected rows (`test-etl-correctness`), and the audit tables name what
  happened.
- **Both Engine DBs**, and the `chaos` marker (`release/required-suites.toml`). Linux-only cases
  are listed under `[suites.chaos.platform_only]`.
- **Prove the test bites:** remove the recovery step it guards and see it fail.

## Checklist

Each row: the failure, where it can happen, and what must be true afterwards.

### Process loss at every commit point

Existing tests: `test_chaos.py`, `test_leases.py`, `test_cli_lifecycle.py`, `test_server.py`.

1. For every `fault_point` name, run the `:kill` variant: after recovery the attempt is `LOST`
   and retried once, or already committed and not re-run. Generate the cases from the list of
   points so a new point is covered.
2. Writes with no fault point yet are untested crash windows: check SQL merges and every
   two-commit path in `handlers/` and `execution/` for one before claiming coverage.
3. A task's child and grandchild processes are stopped when their supervisor is killed, and an
   orphan never writes after its attempt is `LOST`.
4. The controller killed mid-tick, mid-schedule creation and mid-finalization: the next leader
   finishes the work once.

### Engine DB outages

Existing: `test_postgres_outage_fences_the_supervisor_and_recovers`.

1. A restart during each phase (admission, attempt start, attempt end, finalization) is ridden
   out within `Engine_db_outage_seconds`; past it, the run fails naming the outage.
2. A failover that drops the leadership session: the controller retakes leadership or stops; two
   leaders never act at once.
3. A lost acknowledgement (the commit succeeded, the connection dropped before the reply): the
   retry recognises the committed state instead of doing the work again.

### Warehouse and network

Existing: `test_retry_classification.py`, `tests/fixtures/retry_warehouse.py`.

1. A connection dropped mid-statement classifies as retryable; the retry leaves the target as
   if the statement ran once (appends and replacements are retry-safe).
2. A query timeout, a DNS failure and a refused connection each name the profile and the host.

### Local resources

1. A full disk or unwritable `logs/` or `uploads/` directory is refused before the attempt
   starts, naming the path; never after a partial write (S7.P).
2. A port in use or a second server on the host: refused at start, naming it.

### Time and duplication

1. Clock skew between processes does not change lease outcomes (Engine DB clock).
2. Duplicate task delivery runs one attempt (`test_duplicate_task_delivery_executes_one_attempt`).
3. Downtime across schedules and a DST change: bounded catch-up on the right logical dates.

## Running

```bash
make services-up
uv run pytest -q -p no:cacheprovider -m chaos
```

Outage tests stop and start the shared `etl-craft-postgres` container: run them alone, never in
parallel with another suite.

## Report

Per row: the test, the fault point or signal used, both dialects, and the recovery it proved.
Each S7.P catalog row the work closes names its test in the roadmap. Crash windows without a
fault point are gaps.
