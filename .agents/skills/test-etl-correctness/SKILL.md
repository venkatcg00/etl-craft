---
name: test-etl-correctness
description: Test that etl-craft writes the right rows - the nine SQL actions, SCD1/SCD2 merges and their hashes, appends, replacements, rejects, watermarks, business rules and schema evolution - on every warehouse, against an independently computed expected result. Use when a change touches handlers/sql, dialects/warehouse, the hash contract, rejects, watermarks or business rules, or when auditing data-correctness coverage before a release.
---

# Test ETL correctness

A wrong row is the worst defect etl-craft can ship: nothing fails, and the team finds out from a
report. This skill lists the ways a write goes wrong here and how to prove each one cannot.

## How every check is written

- **An oracle that is not the code under test.** Compute the expected target from the source
  rows in the test itself, in plain Python, or as a hand-written literal table. Never derive
  the expectation by running the same action again or by reading what etl-craft wrote.
- **Compare whole rows by named column**, sorted by key: business columns, the audit columns
  (`PIPELINE_RUN_ID`, `TASK_RUN_ID`, the SCD flags and dates, `ROW_ID`) and the hash. A count
  alone proves nothing.
- **Every warehouse the action supports.** Use `sql_world` from `tests/fixtures/sql_warehouse.py`
  (DuckDB, PostgreSQL, Trino over Iceberg, DuckDB over Iceberg); narrow with
  `@warehouses(...)` only where the dialect cannot do it, never to skip a failure. Shared
  assertions live in `tests/fixtures/sql_*.py` so the cloud suites (`tests/acceptance/cloud/`)
  run the same checks.
- **Prove the test bites.** `git stash push src`, run it, see it fail, `git stash pop`. Then break
  the code once on purpose (drop a column from the hash, flip a merge condition) and see it fail.

## Checklist

Work through the rows that the change touches; for an audit, all of them. Each row names the
code and the existing tests to extend before writing new ones.

### Values that look the same but are not

Code: the hash contract (`tests/fixtures/hash_contract.py`, `test_hash_contract.py`), the
dialects' hash expressions.

1. NULL vs empty string vs the literal text `'NULL'`: three distinct hashes, three distinct
   rows after a merge.
2. Trailing and leading spaces, and a value containing the hash's separator: distinct.
3. Unicode: composed vs decomposed forms (`é` as one or two code points) are different bytes;
   the merge treats them as different, the same way on every warehouse.
4. Numbers: `1`, `1.0` and `1.00` in a `DECIMAL` column; the largest and smallest value of the
   declared precision; negative zero. A cast is to the declared type, never inferred.
5. Timestamps: the same instant in two session time zones hashes the same; a naive timestamp
   is UTC; daylight-saving transitions (the repeated hour) keep both rows.
6. Booleans and dates across dialects: a value read back from Trino or Iceberg compares equal to
   the one DuckDB wrote.

### Merges (SCD1, SCD2)

Code: `handlers/sql/actions.py`, `handlers/sql/tables.py`; tests: `test_sql_actions.py`,
`test_scd_lifecycle.py`, `test_scd_protection.py`, `tests/fixtures/sql_merges.py`.

1. Insert, update and unchanged rows in one batch: only the changed rows get a new
   `TASK_RUN_ID` and, for SCD2, a new version; `rows_written` counts what changed.
2. Duplicate keys in the source fail unless a dedupe order chooses; ties in that order fail
   unless the tied rows are identical.
3. A NULL key is refused before the target changes; a composite key with one NULL part too.
4. SCD2: exactly one current row per key after every run; validity ranges do not overlap or
   leave gaps; a soft-deleted key that returns gets a new live version.
5. A key deleted and re-inserted in the same batch; a key that changes and changes back across
   two runs.
6. Protected (human-override) columns are never overwritten, on every dialect that supports
   protection, and refused clearly where it does not.
7. Rehash: after `etl-craft rehash`, an unchanged merge writes nothing.

### Appends, replacements and ROW_ID

Code: `handlers/sql/actions.py`, `handlers/sql/replacement.py`; tests: `test_sql_appends.py`,
`test_sql_replacement.py`, `test_sql_row_ids.py`.

1. A retried append replaces only its own batch (`TASK_RUN_ID`), keeps every other run's rows.
2. A replacement interrupted at each `sql.replace.*` fault point leaves either the old table or
   the new one, never a mix or an empty table.
3. Concurrent writers to one target get unique, gap-tolerant ROW_IDs; no ROW_ID is reused after
   a failed attempt.
4. `OVERWRITE_TABLE`, `APPEND_TABLE`, the merges, `DELETE_ROWS` and `DROP_TABLE` fail on a missing
   target; only `CREATE_TABLE` and `SETUP_TABLE` create one; `CREATE_VIEW` never creates a table.

### Rejects, watermarks and rules

Code: `handlers/sql/rejects.py`, `handlers/sql/watermarks.py`, `handlers/business_rules.py`.

1. **Rows are conserved:** source rows = rows written + rows in `<target>__rejects`, for every
   batch, with each reject naming its column and reason.
2. `REJECT_LIMIT` exceeded: no good rows load, every bad row is still set aside.
3. A watermark offset is committed with the attempt's success and never on failure; a retry
   reads the same range; rows landing at the boundary value are read exactly once.
4. A rule flags exactly the rows that break it, recomputed by the test from the source.

### Schema evolution

Code: `handlers/sql/tables.py`; tests: `test_sql_evolution.py`.

1. An added source column is added to the target with its declared type; existing rows get
   NULL; hashes of untouched rows do not change.
2. A dropped or retyped column fails, naming the column and both types, before any write.

## Running

```bash
make services-up
uv run pytest -q -p no:cacheprovider -m "warehouse_duckdb or warehouse_postgres or \
  warehouse_trino_iceberg or warehouse_duckdb_iceberg" tests/integration/warehouse/<file>.py
```

A change to SQL every warehouse runs also needs the cloud suites once
(`.agents/skills/pre-pr-checks/SKILL.md`, step 4).

## Report

For each checklist row touched: the test that covers it, the warehouses it ran on, and the
deliberate break it caught. List rows with no test as gaps, not as passes.
