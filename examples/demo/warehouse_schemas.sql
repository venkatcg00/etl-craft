-- The warehouse schemas the demo writes; etl-craft never creates schemas, so make them first.
-- On DuckDB over Iceberg or Trino, create them in the catalog: CREATE SCHEMA lake.lnd, ...
CREATE SCHEMA IF NOT EXISTS lnd;     -- landed, as each client sends it
CREATE SCHEMA IF NOT EXISTS prs;     -- parsed and typed
CREATE SCHEMA IF NOT EXISTS ds;      -- reference data: support areas, agents
CREATE SCHEMA IF NOT EXISTS cdc;     -- history of changes: agents' teams over time
CREATE SCHEMA IF NOT EXISTS pre_dm;  -- every client in one shape
CREATE SCHEMA IF NOT EXISTS dm;      -- the data mart: the support fact and summaries
CREATE SCHEMA IF NOT EXISTS aud;     -- the Engine DB's tables, cloned after every run
