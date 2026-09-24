# dialects/engine_dialects/postgres/migrations/

Incremental packaged **ENGINE** schema changes for an already-deployed
PostgreSQL Engine DB, applied via `etl-craft migrate` (see `src/etl_craft/migrate.py`).
`../schema.sql` stays the single authoritative full definition for a brand-new
install. This directory carries an existing database forward from one version
of that schema to the next, one file at a time.

The runner always reads this packaged ENGINE stream first. A deployment may
also supply a separate PROJECT stream through `--migrations-dir`,
`ETL_CRAFT_MIGRATIONS_DIR`, or `./sql/migrations`. The streams have separate
identities, so a project filename cannot hide a packaged migration. Keep a
project migration directory available for every later migration run: the
runner verifies the checksum of each previously applied file before applying
new work.

## Convention

- One file per change: `NNNN_short_description.sql`, `NNNN` a zero-padded,
  strictly increasing integer (`0001`, `0002`, ...). Files are applied in
  filename order within their own stream.
- Each file must also be reflected directly in `../schema.sql` itself
  (with its own `[ADDITION]`/`[DEVIATION]`/`[CHOICE]` flag and a note in
  schema.sql's own "POST-SIGNOFF CHANGES" block) — the two are kept in sync
  by hand, not generated from each other. `schema.sql` is what a fresh
  install runs once; a migration file is what an existing database runs to
  catch up to that same state.
- Write every migration to be safe to run against a database that's
  otherwise already up to date with everything *before* it — `IF NOT
  EXISTS`/`IF EXISTS` where the statement supports it — since `etl-craft
  migrate` applies whatever `SCHEMA_MIGRATIONS` doesn't yet list, in order,
  and stops at the first failure rather than guessing.
- Each file runs inside its own transaction (`migrate.py`'s own job, not
  something a migration file needs to open/close itself) and is recorded with
  its source and SHA-256 checksum only once it succeeds. Never edit or delete
  an applied file; add a new migration instead.
- No down-migrations / rollback files. Given `schema.sql` predates this
  mechanism (`CLAUDE.md` open question #7 — "no migration tooling... has
  been discussed" — resolved 2026-09-20), this is deliberately a small,
  Alembic-*lite* runner, not a full migration framework — reversing a
  change is a new forward migration, same as everywhere else in this
  engine's "no destructive shortcuts" philosophy.

## Why history starts at 0001

Every schema change made before this mechanism existed already landed as a
direct edit to `schema.sql` (each one flagged in its own "POST-SIGNOFF
CHANGES" block there) — none of those got a retroactive migration file,
since that would misstate when and how they actually happened. This
directory started genuinely empty, and the first real file here is for the
first schema change made *after* the mechanism existed:

- `0001_add_run_condition.sql` — CFG_TASKS.RUN_CONDITION /
  RUN_CONDITION_COUNT (iteration 2, E2-41). Also the first file this runner
  has ever actually applied, so it is what proves the plumbing works rather
  than only the tests that pass it a `tmp_path`.

## Other Engine DB dialects

Each Engine DB dialect owns its own stream beside its own `schema.sql`
(`../../sqlite/migrations/` for SQLite). A schema change ships as one file
per dialect with the same `NNNN_short_description.sql` name.
