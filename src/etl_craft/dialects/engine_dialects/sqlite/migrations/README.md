# dialects/engine_dialects/sqlite/migrations/

The packaged **ENGINE** migration stream for a SQLite Engine DB (added
2026-09-24, when SQLite became the default Engine DB).

It started empty on purpose. `../schema.sql` was written against the PostgreSQL
schema *as of* `0004_migration_streams_and_checksums.sql`, so PostgreSQL's
0001-0004 are already part of it and have no SQLite twin. The first file here is
`0005_pipeline_run_sla_status.sql` (2026-09-24), paired with PostgreSQL's own
0005.

From here on, a schema change ships as a pair: the PostgreSQL file in
`../../postgres/migrations/` and a file here with the same
`NNNN_short_description.sql` name, each also reflected in its own dialect's
full `schema.sql`. The conventions in `../../postgres/migrations/README.md`
apply unchanged, with one addition: statements are split with
`sqlite3.complete_statement`, so `CREATE TRIGGER ... BEGIN ...; END;` bodies
are safe here.
