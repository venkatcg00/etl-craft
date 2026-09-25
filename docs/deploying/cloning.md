# Cloning the Engine DB into the warehouse

Cloning copies the Engine DB tables into the warehouse after every pipeline run, so a team can
query its pipelines' configuration and run history with the rest of its data, without a
connection to the Engine DB.

```yaml
Cloning:
  prod:
    Enabled: true
    Scope: all        # cfg | aud | all | none
```

| `Scope` | Copies |
|---|---|
| `cfg` | the `CFG_` tables: pipelines, tasks, dependencies, parameters, business rules |
| `aud` | the `AUD_` tables: run logs, business-rule results, offsets, trackers, lineage, documentation versions |
| `all` | both |
| `none` | nothing; the same as `Enabled: false`, so a profile can turn cloning off without deleting it |

## Where the copies go

Each table is copied to the Warehouse profile's `schema`, under its own name, such as
`analytics.etl_craft.CFG_PIPELINES`. The schema must already exist: etl-craft never creates
warehouse schemas, and `doctor` and every run's connection test check it. On Snowflake with
Iceberg tables, set the Cloning section's `External_volume` and `Base_location` too; on Databricks,
`Base_location` makes the copies external tables under that path.

Cloning refuses a warehouse schema that is the Engine DB's own, in the same PostgreSQL
database: it would empty the tables it copies. `doctor` reports that as a failed `Cloning` check.

## How a table is copied

- A copy that does not exist is created, with the Engine DB table's columns in portable types:
  whole numbers `BIGINT`, other numbers `DECIMAL(38, 10)`, timestamps the warehouse's timestamp
  with time zone, and everything else text. JSON, such as `PIPELINE_PARAMETERS`, is copied as its
  text.
- Every clone replaces every row, one transaction per table where the warehouse has them.
- A copy whose columns no longer match, after an upgrade adds a column, is dropped and created
  again. Cloning writes nothing else: only these copies, only in that schema.
- Two runs that end together never clone at once: the second waits, then copies the newer state.
  On a DuckDB warehouse, cloning waits for any task writing to it.

## When cloning fails

Cloning runs after a run has ended, so a failure does not change the run's status. It is logged
at `ERROR` with the table and the database's message. To run it by hand and see the error:

```text
$ etl-craft clone
CFG_PIPELINES	analytics.etl_craft.CFG_PIPELINES	12	created
CFG_TASKS	analytics.etl_craft.CFG_TASKS	87
...
```

Each line is the Engine DB table, its copy, the rows copied, and `created` when the copy was
created. A failure exits with status `17` (`CloningError`), naming the table.
