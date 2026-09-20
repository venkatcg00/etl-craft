# sql/migrations/

Incremental schema changes for an **already-deployed** Engine DB, applied via
`etl-craft migrate` (see `src/etl_craft/migrate.py`). `sql/schema.sql` stays
the single authoritative *full* definition for a brand-new install — that
role doesn't change. This directory is for carrying an existing database
forward from one version of `schema.sql` to the next, one file at a time.

## Convention

- One file per change: `NNNN_short_description.sql`, `NNNN` a zero-padded,
  strictly increasing integer (`0001`, `0002`, ...). Applied in filename
  order.
- Each file must also be reflected directly in `sql/schema.sql` itself
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
  something a migration file needs to open/close itself) and is recorded
  into `SCHEMA_MIGRATIONS` only once it succeeds.
- No down-migrations / rollback files. Given `schema.sql` predates this
  mechanism (`CLAUDE.md` open question #7 — "no migration tooling... has
  been discussed" — resolved 2026-09-20), this is deliberately a small,
  Alembic-*lite* runner, not a full migration framework — reversing a
  change is a new forward migration, same as everywhere else in this
  engine's "no destructive shortcuts" philosophy.

## Why no files exist here yet

Every schema change made before this mechanism existed already landed as a
direct edit to `sql/schema.sql` (each one flagged in its own "POST-SIGNOFF
CHANGES" block there) — none of those get a retroactive migration file,
since that would misstate when and how they actually happened. This
directory starts genuinely empty; the first real file here should be for
the *next* schema change from this point forward, not a reconstruction of
history.
