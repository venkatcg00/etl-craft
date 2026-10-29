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

The files hold the selected project's pipeline metadata. An administrator creates the project
first with `etl-craft project add --project sales --reason "New team"`; config apply requires
that project to exist. Dependency rows may set `DEPENDS_ON_PROJECT_CODE` for another project;
an empty value selects this project. Applying them never retires another project's rows.
All eight files are optional. Submit any subset; a header-only file changes nothing. A folder
with no known file fails with nothing to apply. Unknown CSV filenames are refused with a
suggestion, so a misspelling cannot silently drop a change.

The Engine DB is the source of truth. Files are helpers carrying changes, not a complete
snapshot. After deploying ten pipelines, a later deployment can contain only one new pipeline:

```csv
PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE
NEW_PIPELINE,New pipeline,FULL
```

Copy the [one-pipeline change](../examples/config-upserts/pipelines.csv) into
`config/pipelines.csv`, then run `etl-craft config plan` and `etl-craft config apply`.
Existing pipelines and their children stay unchanged. Export writes the full current state,
including inactive rows, for inspection or bootstrapping an existing deployment's files.

The Engine DB accepts writes only from etl-craft. A plain connection, such as the `sqlite3`
shell, `psql`, a BI tool or a database's CSV import, is refused, so the files reach the tables
through `etl-craft config apply`, which records who changed what.

## Project selection and deployment rights

Use `--project P` or set `Orchestration.Project` in that folder's `craft-connector.yml`.
A conflict is refused; the folder name never selects a project. `config plan` and `config apply`
require project CAN_MANAGE. Use `config apply --project-dir sales/` to load `sales/`'s own file;
`--config-dir` may select a subset of change CSVs without changing the project's identity.
Missing files and rows remain untouched. See the [multi-project layout and CI example](../deploying/project-layout.md#several-projects-in-one-repository).

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
config 6da809c4700d from /srv/sales/etl-craft/config: 5 change(s)
  retire     pipelines.csv              SALES_DAILY_EU  (ACTIVE_FLAG is N)
  insert     tasks.csv                  SALES_MART.archive
  update     task_parameters.csv        SALES_DAILY.fetch_orders RETRIES  (PARAMETER_VALUE: '2' -> '3')
  insert     task_parameters.csv        SALES_MART.archive SCRIPT_FILE
  insert     task_dependencies.csv      SALES_MART.archive on SALES_MART.publish SUCCESS
[FAIL] SALES_MART: depends on pipeline SALES_DAILY_EU, which is inactive and no longer runs, so once its last run is consumed the dependency is never satisfied again; deactivate the dependency too, or reactivate the pipeline
[FAIL] SALES_MART.alert: reports on the whole run but does not depend on archive, so it can run before they finish and report on a run still in progress; add an ALWAYS dependency on each
validate failed the result, so nothing was changed
```

Setting the `SALES_MART on SALES_DAILY_EU` row of `pipeline_dependencies.csv` to `N`, and adding
an `ALWAYS` dependency of `alert` on `archive`, makes the plan pass; `apply` then makes the seven
changes.

## How the merge works

Each file names its rows by key, never by id, so the same files load every Engine DB:
development, test and production.

| File | Key |
|---|---|
| `pipelines.csv` | `PIPELINE_CODE` |
| `tasks.csv` | `PIPELINE_CODE`, `TASK_CODE` |
| `task_parameters.csv` | `PIPELINE_CODE`, `TASK_CODE`, `PARAMETER_NAME` |
| `task_dependencies.csv` | `PIPELINE_CODE`, `TASK_CODE`, `DEPENDS_ON_PROJECT_CODE`, `DEPENDS_ON_PIPELINE_CODE`, `DEPENDS_ON_TASK_CODE`, `DEPENDENCY_TYPE` |
| `pipeline_dependencies.csv` | `PIPELINE_CODE`, `DEPENDS_ON_PROJECT_CODE`, `DEPENDS_ON_PIPELINE_CODE`, `DEPENDENCY_TYPE` |
| `business_rules.csv` | `PIPELINE_CODE`, `TASK_CODE`, `BUSINESS_RULE_NAME` |
| `tables.csv` | `CATALOG_NAME`, `SCHEMA_NAME`, `TABLE_NAME` |
| `columns.csv` | `CATALOG_NAME`, `SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAME` |

Administrators manage pools with `etl-craft pool add|set|list|disable`, each with `--reason`.
Tasks without `POOL` use their project's `DEFAULT_POOL`. Pipelines also have
`MAX_PARALLEL_TASKS` (default 8), `PRIORITY` (default 0), and
`BLOCK_ON_REFRESH_ERROR` (default `N`). Apply ends with a
[pipeline refresh](validating.md#refresh-and-the-version-that-runs); changes remain committed
if refresh fails, while admission keeps the last passing version.
[Placement](../deploying/agents.md#pools-limits-and-resource-placement) explains the limits.

For each row of a file, `apply`:

- **inserts** it when the Engine DB has no row with its key and its `ACTIVE_FLAG` is `Y`;
- **updates** the row in place when its values changed. The row keeps its id, so the runs,
  consumption and rule results that refer to it stay with it;
- **reactivates** the row, with the same id, when the file sets `ACTIVE_FLAG` back to `Y`;
- **retires** the row (`ACTIVE_FLAG = 'N'`) when the file sets `ACTIVE_FLAG` to `N`.

Rows absent from a file, and absent files, are untouched. Only an explicit `ACTIVE_FLAG=N`
retires a stored key. A new key with `N` is refused because no row exists to retire.

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
lines that way. An empty cell is NULL, or the column's default. Key columns must be present.
Other columns may be omitted: updates preserve their stored values, while inserts use defaults
and require any remaining mandatory values.

| File | Columns (required in bold) | Defaults |
|---|---|---|
| `pipelines.csv` | **`PIPELINE_CODE`**, **`PIPELINE_NAME`**, `DESCRIPTION`, **`REFRESH_TYPE`**, `RUN_SCHEDULE`, `SCHEDULE_TIMEZONE`, `SCHEDULE_START_DATE`, `CATCHUP`, `MAX_CATCHUP_RUNS`, `OVERLAP_POLICY`, `SLA_IN_HOURS`, `PIPELINE_PARAMETERS`, `ACTIVE_FLAG` | `CATCHUP` `N`, `MAX_CATCHUP_RUNS` `1`, `OVERLAP_POLICY` `SKIP` |
| `tasks.csv` | **`PIPELINE_CODE`**, **`TASK_CODE`**, **`TASK_TYPE`**, **`HANDLER`**, `RUN_CONDITION`, `RUN_CONDITION_COUNT`, `TARGET_CATALOG_NAME`, `TARGET_SCHEMA_NAME`, `TARGET_TABLE_NAME`, `TARGET_ACTIVE_FLAG`, `ACTIVE_FLAG` | `TARGET_ACTIVE_FLAG` `Y` |
| `task_parameters.csv` | **`PIPELINE_CODE`**, **`TASK_CODE`**, **`PARAMETER_NAME`**, `PARAMETER_VALUE`, `ACTIVE_FLAG` | |
| `task_dependencies.csv` | **`PIPELINE_CODE`**, **`TASK_CODE`**, **`DEPENDS_ON_PIPELINE_CODE`**, **`DEPENDS_ON_TASK_CODE`**, **`DEPENDENCY_TYPE`**, `CONSUME_REPAIRS`, `ACTIVE_FLAG` | `CONSUME_REPAIRS` `Y` |
| `pipeline_dependencies.csv` | **`PIPELINE_CODE`**, **`DEPENDS_ON_PIPELINE_CODE`**, **`DEPENDENCY_TYPE`**, `CONSUME_REPAIRS`, `ACTIVE_FLAG` | `CONSUME_REPAIRS` `Y` |
| `business_rules.csv` | **`PIPELINE_CODE`**, **`TASK_CODE`**, **`BUSINESS_RULE_NAME`**, **`SEQUENCE_NUMBER`**, **`BUSINESS_RULE_TYPE`**, `BUSINESS_RULE_LEVEL`, `BUSINESS_RULE_KEY_COLUMN`, `CATALOG_NAME`, **`SCHEMA_NAME`**, **`TABLE_NAME`**, **`BUSINESS_RULE_SQL`**, `ACTIVE_FLAG` | |

| `tables.csv` | `CATALOG_NAME`, **`SCHEMA_NAME`**, **`TABLE_NAME`**, `OBJECT_KIND`, `REJECT_RETENTION_DAYS`, `SNAPSHOT_RETENTION_DAYS`, `ORPHAN_RETENTION_DAYS`, `ACTIVE_FLAG` | Empty catalog means absent; empty kind means undeclared; empty retention cells keep their category forever |
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

References resolve against the Engine DB plus the submitted changes. New or changed active
rows require active parents; unchanged children remain untouched when a pipeline is disabled.
Stored-only pipelines, tasks and tables need not be repeated in files. Column positions are
checked against all stored active columns too. Moving a column into an occupied position
requires explicitly moving or retiring the conflicting column. To retire a table, submit its
active columns with `ACTIVE_FLAG=N` as well; columns retire first. Missing and header-only
column files retire nothing.
`OBJECT_KIND` accepts `TABLE`/`VIEW`; `IS_NULLABLE` accepts `Y`/`N`. Components are stored
separately, and column rows hold a `TABLE_ID` foreign key rather than a repeated object name. Rules use
`TARGET_TABLE_ID` on the existing rule row; changing the target keeps the rule ID.

The optional retention fields apply to operational auxiliary data, never target business rows.
`REJECT_RETENTION_DAYS` accepts 1–36500 whole days and expires only this project's old reject
rows by their ingestion `CREATE_DATE`. Snapshot and orphan periods accept 7–36500 days and
require a supported Iceberg maintenance dialect (currently Trino). Unsupported policies fail;
empty or omitted columns preserve existing behavior. Policies remain on retired table rows
until explicitly cleared. See [warehouse retention](../deploying/engine-db.md#warehouse-auxiliary-retention).

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

[Configuration migrations](configuration-migrations.md) write the same tables with SQL.
Rows they create remain untouched by later file deployments unless those keys are submitted.
Migrations also create and change the project's own tables; `etl-craft migrate` upgrades the
Engine DB schema.

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

## Pipeline permissions

Permission changes use `etl-craft access grant|revoke|list` and their API actions, independently
of configuration deployment. See [Pipeline permission groups](../deploying/security.md#pipeline-permission-groups)
and the [bulk command input](../examples/access.csv). Configuration export writes only the eight
metadata files; it does not export administrator identities or permissions.

## Tag edits and access

Tags are the only pipeline grouping; there are no pipeline folders. Editing `TAGS` in a
pipeline's `PIPELINE_PARAMETERS` can change which access grants match it. Both `config plan`
and `config apply` report each grant that starts or stops matching, naming its principal,
tag, permission and pipeline. JSON returns these under `tag_access_changes`.

The report also names open/restricted switches. When a pipeline becomes restricted,
project-level access no longer applies: only matching pipeline/tag grants and project
CAN_MANAGE reach it. When its last matching grant disappears, the project level applies again.
An ungranted tag edit reports no access change. This report neither changes grants nor refuses
a deploy. It reports grants rather than accounts, because SSO group membership cannot be
completely enumerated at plan time. Per-project repository folders are deployment layout,
not pipeline grouping.

### Business-rule levels

`business_rules.csv` accepts optional `BUSINESS_RULE_LEVEL`: new keys default to `ROW`;
an omitted header leaves a stored level unchanged. `ROW` requires `BUSINESS_RULE_KEY_COLUMN`.
`TABLE` requires it empty, refuses `INCOMPLETE`, and returns one `PASSED` result with optional
`OBSERVED` and `DETAIL`. See [Business rules](business-rules.md#table-rules) for output types,
limits and examples. Updating the level preserves the id; absence never retires a rule.
