---
name: test-upgrades
description: Test that a released etl-craft installation upgrades without losing data - Engine DB migrations from every released schema on SQLite and PostgreSQL with real rows in them, matching indexes and constraints, immutable migration files, config-file and craft-connector.yml compatibility, and refusals that name the remedy. Use when a change adds a migration, edits schema.sql, changes a config file's columns or a craft-connector.yml setting, or before a release.
---

# Test upgrades

People run released versions. An upgrade that drops a row, loses an index or leaves a column
out is found in production. This skill proves the path from every release.

## How every check is written

- **Every released schema.** `tests/fixtures/schemas/<version>/` holds each tag's packaged
  schema; `tests/fixtures/released_schema.py` (`install`) applies it with its migration ledger. Loop
  over the folder, never a hand-written list.
- **Real rows, not an empty schema.** Seed each released schema with rows a team of that
  release would have: pipelines, tasks, dependencies, runs, attempts, audit, history, tokens.
  After `apply_pending_migrations`, every seeded row is present with its values, moved or
  converted exactly as the migration intends.
- **Upgraded equals fresh.** The upgraded database matches `init-db` on today's `schema.sql`:
  tables and columns in order, and also indexes (including partial ones), unique and check
  constraints, foreign keys, triggers and actor guards.
- **Both dialects** via `empty_engine_db` / `postgres_database`.
- **Prove the test bites:** delete one statement from the new migration and see it fail.

## Checklist

### Engine DB

Existing: `test_upgrade.py`, `test_schema_and_migrations.py`, the `test_*_upgrade.py` files.

1. Columns: covered by `test_an_earlier_engine_db_upgrades_to_the_current_schema`.
2. Indexes, constraints, triggers and guards: compare them too (`inspector.get_indexes`,
   `get_unique_constraints`, `get_check_constraints`, `get_foreign_keys`, and the trigger
   catalog on PostgreSQL). A migration that forgets an index passes a column-only check.
3. Data: seed every released schema, not only `0.1.0`, and assert each seeded row after the
   upgrade by named column. Include rows the migration converts (renamed values, moved
   columns, backfilled defaults).
4. Guards still hold after the upgrade: a direct write without an actor is refused, and audit
   tables refuse updates.
5. A released migration file is never edited: its checksum in the ledger matches
   (`test_an_edited_applied_file_is_refused_before_anything_runs`). A change to an old file
   fails CI.
6. Each migration runs in one transaction: inject a failure in its last statement and the
   database is unchanged, with no ledger row.
7. Large tables: a migration that rewrites a growing table states its cost; time it on the
   S5.AF seeded volume when it touches `AUD_` tables.

### Config files and craft-connector.yml

Code: `services/config_files.py`, `config/loader*.py`; tests: `test_config_files.py`,
`test_config_upload_examples.py`.

1. A project written for the last release loads, or fails naming the setting or column and the
   new form. Never silently ignored.
2. Every CSV config file of the last release applies with `config apply` after the upgrade, or
   fails naming the file, column and remedy.
3. `docs/examples/` and `examples/demo/` load with today's code (they are what users copy).

### Version mismatches

1. A process whose package expects other migrations than the Engine DB has applied refuses to
   start, naming both versions and the remedy (`etl-craft migrate`, or upgrade the process).
2. After S5.AE: an agent or `ui` copy of another version is refused, naming both versions.

## Running

```bash
make services-up
uv run pytest -q -p no:cacheprovider -m "engine_sqlite or engine_postgres" \
  tests/integration/engine/test_upgrade.py tests/integration/engine/test_schema_and_migrations.py \
  tests/integration/engine/test_*_upgrade.py
```

## Report

Per release schema and dialect: tables, indexes, constraints and seeded rows compared, and the
migration statement removed to prove it bites. Anything compared by columns only is a gap.
