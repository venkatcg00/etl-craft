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
│   └── business_rules.csv         CFG_BUSINESS_RULES
├── sql_files/
└── ingestion_scripts/
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
| `etl-craft config export` | writes the Engine DB's rows as the six files: the starting point for a project whose pipelines are in the Engine DB already. It refuses to overwrite files there unless given `--force` |
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
  insert     task_parameters.csv        SALES_MART.archive SCRIPT_NAME
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
| `pipelines.csv` | `PIPELINE_CODE` |
| `tasks.csv` | `PIPELINE_CODE`, `TASK_CODE` |
| `task_parameters.csv` | `PIPELINE_CODE`, `TASK_CODE`, `PARAMETER_NAME` |
| `task_dependencies.csv` | `PIPELINE_CODE`, `TASK_CODE`, `DEPENDS_ON_PIPELINE_CODE`, `DEPENDS_ON_TASK_CODE`, `DEPENDENCY_TYPE` |
| `pipeline_dependencies.csv` | `PIPELINE_CODE`, `DEPENDS_ON_PIPELINE_CODE`, `DEPENDENCY_TYPE` |
| `business_rules.csv` | `PIPELINE_CODE`, `TASK_CODE`, `BUSINESS_RULE_NAME` |

For each row of a file, `apply`:

- **inserts** it when the Engine DB has no row with its key and its `ACTIVE_FLAG` is `Y`;
- **updates** the row in place when its values changed. The row keeps its id, so the runs,
  consumption and rule results that refer to it stay with it;
- **reactivates** the row, with the same id, when the file sets `ACTIVE_FLAG` back to `Y`;
- **retires** the row (`ACTIVE_FLAG = 'N'`) when the file sets `ACTIVE_FLAG` to `N`.

An active row that no file holds any more is retired too. Keep a row in its file with `N`
rather than deleting it: the change is clearer in review, and setting it back to `Y` restores it.

A key's row is its active row, or else the last one retired. When a pipeline or task was retired
and created again under the same code, the rows of the earlier one are history, and `apply`
leaves them as they are.

Flags apply row by row. Setting a pipeline's `ACTIVE_FLAG` to `N` switches the whole pipeline
off: its tasks keep their own flags, and etl-craft ignores them while the pipeline is inactive.
A dependency of another pipeline on it fails `validate`, as above, until that dependency is
switched off too.

Everything happens in one transaction, which also holds the lock `migrate` takes. `validate`
checks the result inside that transaction, so a configuration it fails never reaches the
tables. Every change is recorded in `AUD_METADATA_CHANGES` with its actor, its values before and
after, and `config@<revision>`, the first twelve characters of the files' SHA-256; `etl-craft
audit` shows them.

## The files

UTF-8 CSV, with a header row of column names in any order. A cell holding a comma, a quote or a
line break is quoted, with each quote inside it doubled, as spreadsheets write CSV; SQL can span
lines that way. An empty cell is NULL, or the column's default. Columns not marked required can
be left out of the header, which gives every row their default.

| File | Columns (required in bold) | Defaults |
|---|---|---|
| `pipelines.csv` | **`PIPELINE_CODE`**, **`PIPELINE_NAME`**, `DESCRIPTION`, **`REFRESH_TYPE`**, `RUN_SCHEDULE`, `SCHEDULE_TIMEZONE`, `SCHEDULE_START_DATE`, `CATCHUP`, `MAX_CATCHUP_RUNS`, `OVERLAP_POLICY`, `SLA_IN_HOURS`, `PIPELINE_PARAMETERS`, `ACTIVE_FLAG` | `CATCHUP` `N`, `MAX_CATCHUP_RUNS` `1`, `OVERLAP_POLICY` `SKIP` |
| `tasks.csv` | **`PIPELINE_CODE`**, **`TASK_CODE`**, **`TASK_TYPE`**, **`HANDLER`**, `RUN_CONDITION`, `RUN_CONDITION_COUNT`, `ACTIVE_FLAG` | |
| `task_parameters.csv` | **`PIPELINE_CODE`**, **`TASK_CODE`**, **`PARAMETER_NAME`**, `PARAMETER_VALUE`, `ACTIVE_FLAG` | |
| `task_dependencies.csv` | **`PIPELINE_CODE`**, **`TASK_CODE`**, **`DEPENDS_ON_PIPELINE_CODE`**, **`DEPENDS_ON_TASK_CODE`**, **`DEPENDENCY_TYPE`**, `CONSUME_REPAIRS`, `ACTIVE_FLAG` | `CONSUME_REPAIRS` `Y` |
| `pipeline_dependencies.csv` | **`PIPELINE_CODE`**, **`DEPENDS_ON_PIPELINE_CODE`**, **`DEPENDENCY_TYPE`**, `CONSUME_REPAIRS`, `ACTIVE_FLAG` | `CONSUME_REPAIRS` `Y` |
| `business_rules.csv` | **`PIPELINE_CODE`**, **`TASK_CODE`**, **`BUSINESS_RULE_NAME`**, **`SEQUENCE_NUMBER`**, **`BUSINESS_RULE_TYPE`**, **`BUSINESS_RULE_KEY_COLUMN`**, **`TARGET_TABLE`**, **`BUSINESS_RULE_SQL`**, `ACTIVE_FLAG` | |

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

## Deploying

Run `plan` on every change before it merges, and `apply` when it deploys, after the Engine DB's
migrations:

```bash
export ETL_CRAFT_ACTOR=github:alice      # in CI: github:${{ github.actor }}
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
