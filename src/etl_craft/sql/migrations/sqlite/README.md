# sql/migrations/sqlite/

The packaged **ENGINE** migration stream for a SQLite Engine DB (added
2026-09-24, when SQLite became the default Engine DB).

It starts empty on purpose. `schema_sqlite.sql` was written against the
schema *as of* `../0004_migration_streams_and_checksums.sql`, so every
Postgres migration in the parent directory is already part of it. There is no
SQLite database old enough to need any of them.

From here on, a schema change ships as a pair: the Postgres file in the parent
directory and a file here with the same `NNNN_short_description.sql` name, each
also reflected in its own full schema file (`schema.sql` /
`schema_sqlite.sql`). The conventions in `../README.md` apply unchanged, with
one addition: statements are split with `sqlite3.complete_statement`, so
`CREATE TRIGGER ... BEGIN ...; END;` bodies are safe here.
