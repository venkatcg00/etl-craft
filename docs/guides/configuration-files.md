# Configuration files

Pipelines, tasks, parameters, dependencies and business rules are rows of the Engine DB's
`CFG_` tables. Keep them as CSV files in the project's `config/` folder, one file per table, and
version them with the rest of the project. `etl-craft config apply` merges the files into the
tables by key: change a file, review the change, and the deployment applies it.

```text
etl-craft/
├── craft-connector.yml
├── config/
│   ├── pipelines.csv              CFG_PIPELINES
│   ├── tasks.csv                  CFG_TASKS
│   ├── task_parameters.csv        CFG_TASK_PARAMETERS
│   ├── task_dependencies.csv      CFG_TASK_DEPENDENCY
│   ├── pipeline_dependencies.csv  CFG_PIPELINE_DEPENDENCY
│   ├── business_rules.csv         CFG_BUSINESS_RULES
│   ├── tables.csv                 CFG_TABLES
│   └── columns.csv                CFG_COLUMNS
├── sql_scripts/
└── python_scripts/
```

The files are the whole configuration: every row of every table, active or not, each with an
`ACTIVE_FLAG`. [`docs/examples/config/`](https://github.com/venkatcg00/etl-craft/tree/main/docs/examples/config)
is a complete example to copy: three sales pipelines, with a fourth switched off.

The Engine DB accepts writes only from etl-craft. A plain connection, such as the `sqlite3`
shell, `psql`, a BI tool or a database's CSV import, is refused, so the files reach the tables
through `etl-craft config apply`, which records who changed what.

## The commands

| Command | Does |
|---|---|
| `etl-craft config export` | writes the Engine DB's rows as config files: the starting point for a project whose pipelines are in the Engine DB already. It refuses to overwrite files there unless given `--force`. It also seeds [source definitions](lineage.md#source-definitions-and-stars) from existing warehouse tables. With `--as-of`, it writes the rows as they stood at a moment instead (see [Configuration as of a moment](#configuration-as-of-a-moment)) |
| `etl-craft config plan` | shows what `apply` would change and runs [`validate`](validating.md) on the result, then rolls it all back. Exits 1 when `validate` fails |
| `etl-craft config apply` | makes the changes, in one transaction, unless `validate` fails the result: then it changes nothing and exits 1 |

Each takes `--config-dir PATH` for a folder other than `config/`, and `--format json` for a
document (`etl-craft/config-sync/1`, or `etl-craft/config-export/1` from `export`).

```text
$ etl-craft config plan
config 6da809c4700d from /srv/sales/etl-craft/config: 6 change(s)
  retire     pipelines.csv              SALES_DAILY_EU  (ACTIVE_FLAG is N)
  insert     tasks.csv                  SALES_MART.archive
  retire     task_parameters.csv        SALES_DAILY.load_orders DOCUMENTATION  (not in the file)
  update     task_parameters.csv        SALES_DAILY.fetch_orders RETRIES  (PARAMETER_VALUE: '2' -> '3')
  insert     task_parameters.csv        SALES_MART.archive SCRIPT_FILE
  insert     task_dependencies.csv      SALES_MART.archive on SALES_MART.publish SUCCESS
[FAIL] SALES_MART: depends on pipeline SALES_DAILY_EU, which is inactive and no longer runs, so once its last run is consumed the dependency is never satisfied again; deactivate the dependency too, or reactivate the pipeline
[FAIL] SALES_MART.alert: reports on the whole run but does not depend on archive, so it can run before they finish and report on a run still in progress; add an ALWAYS dependency on each
validate failed the result, so nothing was changed
```

Setting the `SALES_MART on SALES_DAILY_EU` row of `pipeline_dependencies.csv` to `N`, and adding
an `ALWAYS` dependency of `alert` on `archive`, makes the plan pass; `apply` then makes the eight
changes.

## How the merge works

Each file names its rows by key, never by id, so the same files load every Engine DB:
development, test and production.

| File | Key |
|---|---|
| `grants.csv` | `GROUP_NAME`, `PIPELINE_CODE`, `TAG` (exactly one pipeline or tag) |
| `pipelines.csv` | `PIPELINE_CODE` |
| `tasks.csv` | `PIPELINE_CODE`, `TASK_CODE` |
| `task_parameters.csv` | `PIPELINE_CODE`, `TASK_CODE`, `PARAMETER_NAME` |
| `task_dependencies.csv` | `PIPELINE_CODE`, `TASK_CODE`, `DEPENDS_ON_PIPELINE_CODE`, `DEPENDS_ON_TASK_CODE`, `DEPENDENCY_TYPE` |
| `pipeline_dependencies.csv` | `PIPELINE_CODE`, `DEPENDS_ON_PIPELINE_CODE`, `DEPENDENCY_TYPE` |
| `business_rules.csv` | `PIPELINE_CODE`, `TASK_CODE`, `BUSINESS_RULE_NAME` |
| `tables.csv` | `CATALOG_NAME`, `SCHEMA_NAME`, `TABLE_NAME` |
| `columns.csv` | `CATALOG_NAME`, `SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAME` |

For each row of a file, `apply`:

- **inserts** it when the Engine DB has no row with its key and its `ACTIVE_FLAG` is `Y`;
- **updates** the row in place when its values changed. The row keeps its id, so the runs,
  consumption and rule results that refer to it stay with it;
- **reactivates** the row, with the same id, when the file sets `ACTIVE_FLAG` back to `Y`;
- **retires** the row (`ACTIVE_FLAG = 'N'`) when the file sets `ACTIVE_FLAG` to `N`.

An active row that no file holds any more is retired too. Keep a row in its file with `N`
rather than deleting it: the change is clearer in review, and setting it back to `Y` restores it.

All configuration keys retain one row across retirement and reactivation. Column ordinals
are unique among active columns of a table; retiring a column releases its ordinal, while
its name and ID remain reserved.
Legacy deployments could create a replacement row under a retired code; migration 0027
requires those ambiguous identities to be repaired before upgrading. Existing audit versions
remain immutable.

Flags apply row by row. Setting a pipeline's `ACTIVE_FLAG` to `N` switches the whole pipeline
off: its tasks keep their own flags, and etl-craft ignores them while the pipeline is inactive.
A dependency of another pipeline on it fails `validate`, as above, until that dependency is
switched off too.

Everything happens in one transaction, which also holds the lock `migrate` takes. `validate`
checks the result inside that transaction, so a configuration it fails never reaches the
tables. Every change is recorded as a version in the table's history (see
[Configuration history](../deploying/engine-db.md#configuration-history)), with its actor and
`config@<revision>`, the first twelve characters of the files' SHA-256; `etl-craft audit`
shows them.

## The files

UTF-8 CSV, with a header row of column names in any order. A cell holding a comma, a quote or a
line break is quoted, with each quote inside it doubled, as spreadsheets write CSV; SQL can span
lines that way. An empty cell is NULL, or the column's default. Columns not marked required can
be left out of the header, which gives every row their default.

| File | Columns (required in bold) | Defaults |
|---|---|---|
| `pipelines.csv` | **`PIPELINE_CODE`**, **`PIPELINE_NAME`**, `DESCRIPTION`, **`REFRESH_TYPE`**, `RUN_SCHEDULE`, `SCHEDULE_TIMEZONE`, `SCHEDULE_START_DATE`, `CATCHUP`, `MAX_CATCHUP_RUNS`, `OVERLAP_POLICY`, `SLA_IN_HOURS`, `PIPELINE_PARAMETERS`, `ACTIVE_FLAG` | `CATCHUP` `N`, `MAX_CATCHUP_RUNS` `1`, `OVERLAP_POLICY` `SKIP` |
| `tasks.csv` | **`PIPELINE_CODE`**, **`TASK_CODE`**, **`TASK_TYPE`**, **`HANDLER`**, `RUN_CONDITION`, `RUN_CONDITION_COUNT`, `TARGET_CATALOG_NAME`, `TARGET_SCHEMA_NAME`, `TARGET_TABLE_NAME`, `TARGET_ACTIVE_FLAG`, `ACTIVE_FLAG` | `TARGET_ACTIVE_FLAG` `Y` |
| `task_parameters.csv` | **`PIPELINE_CODE`**, **`TASK_CODE`**, **`PARAMETER_NAME`**, `PARAMETER_VALUE`, `ACTIVE_FLAG` | |
| `task_dependencies.csv` | **`PIPELINE_CODE`**, **`TASK_CODE`**, **`DEPENDS_ON_PIPELINE_CODE`**, **`DEPENDS_ON_TASK_CODE`**, **`DEPENDENCY_TYPE`**, `CONSUME_REPAIRS`, `ACTIVE_FLAG` | `CONSUME_REPAIRS` `Y` |
| `pipeline_dependencies.csv` | **`PIPELINE_CODE`**, **`DEPENDS_ON_PIPELINE_CODE`**, **`DEPENDENCY_TYPE`**, `CONSUME_REPAIRS`, `ACTIVE_FLAG` | `CONSUME_REPAIRS` `Y` |
| `business_rules.csv` | **`PIPELINE_CODE`**, **`TASK_CODE`**, **`BUSINESS_RULE_NAME`**, **`SEQUENCE_NUMBER`**, **`BUSINESS_RULE_TYPE`**, **`BUSINESS_RULE_KEY_COLUMN`**, `CATALOG_NAME`, **`SCHEMA_NAME`**, **`TABLE_NAME`**, **`BUSINESS_RULE_SQL`**, `ACTIVE_FLAG` | |

| `tables.csv` | `CATALOG_NAME`, **`SCHEMA_NAME`**, **`TABLE_NAME`**, `OBJECT_KIND`, `ACTIVE_FLAG` | Empty catalog means absent; empty kind means undeclared |
| `columns.csv` | `CATALOG_NAME`, **`SCHEMA_NAME`**, **`TABLE_NAME`**, **`COLUMN_NAME`**, **`DATA_TYPE`**, **`COLUMN_POSITION`**, `IS_NULLABLE`, `ACTIVE_FLAG` | Empty nullability means undeclared |

Every file's `ACTIVE_FLAG` defaults to `Y`. The columns mean what they mean in the tables:
see [Pipelines and tasks](pipelines-and-tasks.md),
[Dependencies and run conditions](dependencies.md) and [Business rules](business-rules.md).
Each handler's guide lists its task parameters. Values are written as follows:

- codes start with an ASCII letter, then letters, digits or `_`, at most 128 characters;
- `Y` and `N`, and every other fixed choice, are upper case;
- `PIPELINE_PARAMETERS` is a JSON object, such as `{"TAGS": ["sales"]}`;
- `SCHEDULE_START_DATE` is a date written `YYYY-MM-DD`;
- `SLA_IN_HOURS` is a number; `MAX_CATCHUP_RUNS`, `RUN_CONDITION_COUNT` and `SEQUENCE_NUMBER`
  are whole numbers;
- a task parameter's value is text, stored as written, JSON included.

A row may name only pipelines and tasks its project's files hold: a task's pipeline must be in
`pipelines.csv`, and both tasks of a dependency in `tasks.csv`, whatever their flags.
A column, task target or business-rule target names its parent in `tables.csv`; active columns require an active parent.
Table and column files may be omitted when no file references them, preserving existing contracts. A header-only
column file retires its active columns. Retire those columns before retiring their parent.
`OBJECT_KIND` accepts `TABLE`/`VIEW`; `IS_NULLABLE` accepts `Y`/`N`. Components are stored
separately, and column rows hold a `TABLE_ID` foreign key rather than a repeated object name. Rules use
`TARGET_TABLE_ID` on the existing rule row; changing the target keeps the rule ID.

Task targets live on the task row as an optional `TARGET_TABLE_ID` foreign key. In `tasks.csv`,
set `TARGET_SCHEMA_NAME` and `TARGET_TABLE_NAME` together, with optional `TARGET_CATALOG_NAME`;
leave all three empty for a task without a target. `TARGET_ACTIVE_FLAG=N` preserves a disabled
reference without making it available to handlers. `TARGET_OBJECT` is derived for handlers and
scripts from the active reference; it cannot be written to `task_parameters.csv`.

Migration `0030_task_target_contracts.sql` moves active and inactive `TARGET_OBJECT` parameters
onto their task rows, reuses `CFG_TABLES`, and preserves task IDs, actors, timestamps and parameter
history. Current relative names resolve against the configured catalog once; historical exports
keep the earlier components. Invalid, duplicate or lookalike parameter names and custom parameter
columns or task/parameter triggers require repair with the previous package before upgrading.
Export after migrating, then move edits from old target parameter rows to `tasks.csv`. Keep
already-applied project migrations unchanged; update pending uploads to use the scoped reference.

### Upgrading source contracts

Migration `0028_source_contracts.sql` converts existing source rows to the two contract tables.
It retains column IDs and audit stamps, leaves typed legacy history unchanged and keeps the
source rows at conversion time in immutable `AUD_SOURCE_COLUMNS_BASELINE` evidence. These
audit tables reconstruct historical exports; they are not another writable source contract.
The configured warehouse catalog resolves relative current names once. Historical exports
retain catalog absence in earlier evidence and never infer today's catalog. Unknown object
kind and nullability remain undeclared.

Duplicates, malformed legacy names and custom source columns/indexes/triggers/dependents stop
the upgrade before any pending migration changes the schema or ledger. Repair them using the
previous installed package. Already-applied project SQL must remain byte-for-byte unchanged;
move legacy source writes from pending project SQL to `CFG_TABLES`/`CFG_COLUMNS`.

Move an old `source_columns.csv` out of the config folder and export after upgrading, then
reapply your edits to `tables.csv` and `columns.csv`. Config apply explicitly refuses the old
file so it cannot silently ignore those edits.

## Configuration as of a moment

`etl-craft config export --as-of WHEN` writes the config files as the rows stood at `WHEN`, rebuilt
from the configuration history. `WHEN` is a `pipeline_run_id`, meaning the moment that run
started, or an ISO 8601 time such as `2026-10-11T08:00:00Z`; a time without a zone is UTC. Export
it beside the project's files and compare, to see what a run ran with or what changed since:

```bash
etl-craft config export --as-of 412 --config-dir /tmp/config-412
diff -r /tmp/config-412 config/
```

A moment before the history begins, a run id that does not exist, and a time that is not ISO 8601
are refused. So is a row created before the Engine DB recorded changes, before 0.2.0, and changed
since: its values before that change were never recorded.

## Deploying

Run `plan` on every change before it merges, and `apply` when it deploys, after the Engine DB's
migrations:

```bash
# Supply ETL_CRAFT_TOKEN from your CI secret manager
etl-craft migrate
etl-craft config apply
```

The plan's text is a review of the change in Engine DB terms; with `--format json`, a pull
request bot can post it.

## When it fails

| Exit | What happened | What to do |
|---|---|---|
| `25` | a file cannot be read or holds a bad row. Every problem is listed at once, with its file, line and column, the value found and what was expected, such as `tasks.csv line 7, column RUN_CONDITION_COUNT: 'two' is not a whole number` or `task_dependencies.csv line 4: task SALES.lod is not in tasks.csv` | correct the files; nothing was changed |
| `1` | `validate` failed the result, as in the plan above | correct the files; nothing was changed |
| `12` | the Engine DB has no `CFG_` tables yet | run `etl-craft setup`, then the command again |
| `25` | a row is active, but its pipeline or task has no row in the Engine DB and is inactive in its file | set the pipeline's or task's `ACTIVE_FLAG` to `Y`, or the row's to `N` |

## Configuration files and migrations

[Configuration migrations](configuration-migrations.md) write the same tables with SQL. Keep a
table in one place: `apply` retires active rows its files do not hold, so in a project with
configuration files, a row a migration adds is retired by the next `apply` unless the files hold
it too. Migrations stay the way to create and change the project's own tables, and the Engine
DB's schema is upgraded by `etl-craft migrate`.

### Upgrading rule targets

Migration `0029_rule_target_contracts.sql` converts rule `TARGET_TABLE` strings to
`TARGET_TABLE_ID`, reusing existing scoped contracts and resolving current catalog aliases
once. It preserves rule IDs, audit stamps, counters and earlier typed versions. The conversion
version retains the original target string as historical evidence; future config writes store
only the FK. Historical exports rebuild the old components without today’s catalog inference.
Rule-result records keep their recorded target until the later audit endpoint conversion.

Invalid legacy identifiers and custom columns or target-dependent schema objects stop the
upgrade with a repair instruction. Export after upgrading and reapply edits to the separate
catalog/schema/table fields in `business_rules.csv` and `tables.csv`. Old CSV headers are
rejected. Pending project SQL must write `TARGET_TABLE_ID`; already-applied migration files
must remain unchanged. Original tutorial files are retained under `legacy-migrations/`, and
new projects use the current `*_contracts.sql` examples.

## Pipeline grants

`grants.csv` holds `GROUP_NAME,PIPELINE_CODE,TAG,ROLE,ACTIVE_FLAG`. Create groups with the
admin-only `group` commands first. Choose one pipeline code or tag per row and `viewer` or
`operator` as the role. Changing grants through `config apply` requires admin; a developer's
entire apply is refused, including changes in other files. The file must exist, like the pipeline/task files; an empty file with its header retires
the existing grants. Export the configuration to create it when upgrading.
See [Pipeline permission groups](../deploying/security.md#pipeline-permission-groups).
