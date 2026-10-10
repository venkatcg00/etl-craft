# Configuration migrations

[Configuration files](configuration-files.md) are the usual way to keep pipelines in the Engine
DB. A project migration writes the same `CFG_` rows with SQL instead: a file in the project's
`migrations/` folder, which `etl-craft migrate` applies once. Use migrations when a change is
easier to say in SQL than in rows, such as copying a pipeline for a list of regions, and for
the project's own tables. Keep a `CFG_` table in one place: `etl-craft config apply` retires
active rows its files do not hold, so a project that keeps configuration files adds rows there,
not in migrations.

Each example below is a file in
[`docs/examples/migrations/`](https://github.com/venkatcg00/etl-craft/tree/main/docs/examples/migrations),
and the test suite applies them in order on SQLite and PostgreSQL, running `validate` after each.

## Why a migration

The Engine DB accepts writes only from etl-craft. A plain connection, such as the `sqlite3`
shell, `psql`, a BI tool or a CSV import, is refused: SQLite reports `no such function:
etl_craft_actor`, and PostgreSQL reports `CFG_TASKS is written only by etl-craft; ... metadata
with a project migration (etl-craft migrate)`.

`etl-craft migrate` applies each new file under your actor name, and records:

- every row it inserts, updates or retires, with the values before and after and the file's
  name, in `AUD_METADATA_CHANGES`, which `etl-craft audit` shows;
- the file and its SHA-256 in `SCHEMA_MIGRATIONS`, so an applied file can be neither edited nor
  removed.

## The workflow

1. **Write the file.** Copy the example closest to your change into `migrations/` with the
   next number, such as `0012_sales_mart.sql`. Files apply in filename order, so pad the numbers
   to one width.
2. **Try it on a development Engine DB.** `etl-craft migrate` applies it; `etl-craft validate
   --pipeline_code SALES_MART` reports what is still wrong, and `etl-craft graph --pipeline_code
   SALES_MART` shows the order the tasks run in.
3. **Review it like code**, in a pull request, with the `validate` output.
4. **Deploy it.** The deployment job runs `migrate` and then `validate` against each Engine DB,
   with a stable actor name:

    ```bash
    export ETL_CRAFT_ACTOR=github:alice      # in CI: github:${{ github.actor }}
    etl-craft migrate
    etl-craft validate
    ```

    `etl-craft setup` applies the project's migrations too, when it creates a new Engine DB.

5. **Check what changed.** `etl-craft audit --pipeline_code SALES_MART --since 2026-11-01` lists
   each row the upload changed, with its values before and after, and the file that changed it.

## Rules for every upload

- **One upload, one new file.** Never edit, rename or delete an applied file: `migrate` stops
  before applying anything when one has changed or is missing. Correct a mistake in a new file,
  and keep every applied file in `migrations/`.
- **One file, one transaction.** A statement that fails rolls back the whole file and stops
  `migrate` before any later file, with the file's name and the database's message.
- **Rows refer to each other by code, never by id.** Ids differ between Engine DBs
  (development, test, production); codes do not. Look ids up by code among active rows
  (`ACTIVE_FLAG = 'Y'`), as every example does.
- **A misspelt code fails the upload.** Look codes up with `LEFT JOIN`: a code with no active
  row leaves an id `NULL`, and the column's `NOT NULL` constraint stops the file. A plain `JOIN`
  drops that row without an error, and a dependency row dropped that way silently changes the
  order a pipeline runs in. An `UPDATE` whose codes match nothing changes nothing, so a file that
  updates rows first checks every code it uses (see [Changes in place](#changes-in-place)).
- **Retire rows; never delete them.** Set `ACTIVE_FLAG = 'N'`. Run history refers to rows by
  id, and etl-craft reads only active rows. A retired code can be used again.
- **The same rows in every environment.** What differs between development, test and production
  (connections, schemas, credentials) belongs in `craft-connector.yml` profiles, not in `CFG_`
  rows.
- **Write values as SQL literals.** Text goes in single quotes, with each quote inside it doubled
  (`'the day''s orders'`); `NULL`, unquoted, means no value; numbers stay as they are. JSON values
  are text too. SQL in a value (`SOURCE_SQL`, `BUSINESS_RULE_SQL`) is text like any other, and
  bound names such as `:pipeline_run_id`, `%` and `$$` tokens inside it are stored as written.
- **Keep the SQL portable** when one file goes to SQLite and PostgreSQL. Everything in the
  examples runs on both: `VALUES` lists (whose columns are `column1`, `column2` and so on),
  `LEFT JOIN`, scalar subqueries, `||`, `REPLACE`, `UPPER`, `CAST`, and temporary tables
  dropped at the end of the file.

## The examples

Each example builds on the ones before it.

| File | Strategy | What it shows |
|---|---|---|
| `0001_new_pipeline.sql` | [A new pipeline](#a-new-pipeline) | every `CFG_` table a pipeline uses: a schedule with catch-up and overlap settings, a task of each handler, shared and handler parameters, the four dependency types, an alert on failure, business rules in two waves |
| `0002_dependent_pipeline.sql` | [A pipeline that waits on another](#a-pipeline-that-waits-on-another) | a pipeline dependency, a task waiting on a task in another pipeline, a run condition of `N`, an inline SELECT |
| `0003_copy_a_pipeline.sql` | [Copies of a pipeline](#copies-of-a-pipeline) | new pipelines copied from one, for a list of regions, with the values that differ rewritten; the pipelines downstream of the original wait on the copies too |
| `0004_change_values.sql` | [Changes in place](#changes-in-place) | a schedule, an SLA, `PIPELINE_PARAMETERS`, one parameter on several pipelines, a dependency type, a run condition, a business rule, a task's code |
| `0005_add_a_step.sql` | [A step between two others](#a-step-between-two-others) | a task inserted into an existing order, a parameter added and one retired |
| `0006_retire_and_reuse.sql` | [Retiring rows](#retiring-rows) | tasks and a whole pipeline retired with everything that refers to them, and a retired code used for a new task |

### A new pipeline

A pipeline's rows go in this order, because each statement looks up the codes the earlier ones
wrote: pipelines, tasks, task parameters, task dependencies, pipeline dependencies, business
rules. The columns are described in [Pipelines and tasks](pipelines-and-tasks.md); each handler's
guide lists its parameters.

```sql
--8<-- "docs/examples/migrations/0001_new_pipeline.sql"
```

### A pipeline that waits on another

A `CFG_PIPELINE_DEPENDENCY` row makes a whole pipeline wait on another; a `CFG_TASK_DEPENDENCY`
row whose upstream is in another pipeline makes one task wait. See
[Dependencies and run conditions](dependencies.md).

```sql
--8<-- "docs/examples/migrations/0002_dependent_pipeline.sql"
```

### Copies of a pipeline

For pipelines that differ only in a few values, such as one per region or client, copy an
existing pipeline's rows rather than writing each out. A temporary table lists the copies and
what differs, and `REPLACE` rewrites the values that name the original. Rewrite exact spellings
only (`'_uk'`, `'"uk"'`), so text that merely contains the letters stays as it is. Check the
copies with `validate`, and their values with `etl-craft audit --pipeline_code SALES_DAILY_EU`,
which lists every row the upload inserted.

```sql
--8<-- "docs/examples/migrations/0003_copy_a_pipeline.sql"
```

### Changes in place

`UPDATE` the active rows, found by code. A file of updates starts by listing every code it changes
in a temporary table with a `NOT NULL` id column, so a code with no active row stops the file;
on PostgreSQL the error names the code (`Failing row contains (SALES_DAILY.load_order, null)`).
The updates then read their ids from that table.

```sql
--8<-- "docs/examples/migrations/0004_change_values.sql"
```

### A step between two others

A new task needs its parameters and its dependencies both ways: what it waits on, and what now
waits on it. Retire the dependency it replaces, and check that alerts still wait on, or hear the
failures of, every task they report on; `validate` reports an alert that does not.

```sql
--8<-- "docs/examples/migrations/0005_add_a_step.sql"
```

### Retiring rows

Retiring a task also retires its parameters, its business rules, its dependencies and the
dependencies on it; retiring a pipeline retires its tasks that way, and the pipeline
dependencies both ways. A task that waited on a retired one starts without it from then on, so
review what waited on it. A retired code is free: the example replaces `publish` with a new
task under the same code, whose history starts afresh.

```sql
--8<-- "docs/examples/migrations/0006_retire_and_reuse.sql"
```

## When a migration fails

The file changed nothing; correct it and run `migrate` again. A file that failed was never
applied, so it can still be edited.

| Message | Cause |
|---|---|
| `NOT NULL constraint failed: CFG_TASK_PARAMETERS.TASK_ID` (SQLite), `null value in column "task_id" of relation "cfg_task_parameters" violates not-null constraint` (PostgreSQL) | a code in that statement has no active row: misspelt, retired, or written by a later statement or file |
| `NOT NULL constraint failed: codes_in_use.ID`, or `Failing row contains (SALES_DAILY.load_order, null)` on PostgreSQL | a code the file updates has no active row |
| `UNIQUE constraint failed: CFG_PIPELINES.PIPELINE_CODE`, or `duplicate key value violates unique constraint "ux_pipelines_code_active"` | the code is already active: choose another, or retire the active row first |
| `CHECK constraint failed: ck_tasks_run_condition_count`, or `violates check constraint "ck_tasks_run_condition_count"` | `RUN_CONDITION = 'N'` without a `RUN_CONDITION_COUNT`, or a count without `N` |
| `applied project migration '0001_new_pipeline.sql' has changed since it was applied` | an applied file was edited: restore it from version control, and put the change in a new file |
| `applied project migration '0002_dependent_pipeline.sql' is missing from '.../migrations'` | an applied file was removed or renamed: restore it |

A file can apply cleanly and still describe a pipeline that cannot run, such as a script that
does not exist or a dependency cycle. `etl-craft validate` finds those; see
[Validating pipelines](validating.md).
