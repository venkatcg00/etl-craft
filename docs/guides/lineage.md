# Lineage and documentation versions

## Column lineage

`etl-craft lineage` shows which columns every table the engine writes is made from, and where
each column goes next. It reads every active SQL task's SELECT, inline or from its file, with the
pipeline-id tokens replaced, and traces each column through CTEs, subqueries and joins to the
source columns it is made from:

| A column that is | Is recorded as |
|---|---|
| copied as it is: `o.id` | `copy`, from `orders.id` |
| computed: `o.amount * r.rate` | the expression, from each column in it |
| made from no column: `COUNT(*)`, `'eu'` | the expression, with no source |

A task's target is the next task's source, so lineage joins up across tasks and pipelines. Tables
are named as the SQL names them, in lower case, with the active warehouse database left off, so
`analytics.sales.orders` and `sales.orders` are the same table.

```bash
etl-craft lineage                                   # every task: target, columns, source tables
etl-craft lineage --table sales.orders              # tables upstream and downstream, all the way
etl-craft lineage --table sales.orders --column amount_usd [--upstream | --downstream] [--depth 2]
```

```
sales.orders.amount_usd
  <- ref.rates.rate  [o.amount * r.rate]  (SALES.convert)
  <- staging.orders.amount  [o.amount * r.rate]  (SALES.convert)
    <- raw.orders.amt  [copy]  (INGEST.stage)
  -> mart.daily.total  [SUM(amount_usd)]  (MART.daily)
```

Each line names the task that makes the link. The summary lists every task whose SQL cannot be
traced, with the reason, such as `SELECT *` over a table whose columns are not known; `--strict`
makes the command exit `1` when there is one.

Lineage is stored in `AUD_COLUMN_LINEAGE` and worked out again only for a task whose SELECT,
target, source column names/types/order, warehouse dialect, active catalog or installed sqlglot
version changed; `--refresh` works every task out again. Python scripts are
not traced: their SQL, if any, is inside the script.

## Table lineage versions

Every recording combines the edges of active SQL writers per target table. Changed edges open
one new version and close the previous one in the same transaction. Recomputing identical edges
or upgrading sqlglot adds no version. Retiring the last writer closes the current version.
Publication uses only edges computed or confirmed against their cache key in that collection.
If any writer of a table cannot be traced, its current version closes and no replacement opens.
CLI output and the table's catalog page say "history not updated", naming the failed task and
reason. Earlier edges remain readable for their closed interval but cannot be presented as
current. Fix the task and compute lineage again to open a new version.
Validity begins when lineage is computed, independently of when pipelines run.

```bash
etl-craft lineage --as-of 2026-10-13T08:00:00Z
etl-craft lineage --as-of 123                         # that pipeline run's start time
etl-craft lineage --table sales.orders --version 5
etl-craft lineage --table sales.orders --column amount_usd --version 5 --upstream
```

`--as-of` accepts the same run ID or ISO 8601 time as `config export --as-of`; a time without
a timezone means UTC. `--version` requires `--table` because each table has its own numbering.
It selects that table's version and reads the rest of the graph at the time that version began.
The selectors are mutually exclusive and cannot be combined with `--refresh`. Historical reads
use retained version edges, never today's SQL, source contracts or task cache. A time before
history starts fails with the earliest recorded time; an unknown table version fails with its
number and the remedy. Version 5 remains readable after version 1000 becomes current.

Table pages show the current version and its opening time, followed by earlier versions with
validity intervals and added/removed column sources. Tables never traced have an explicit
no-versions message. Writing tasks retain their stable IDs; displayed task labels use their
current metadata names.

Upgrade the Engine DB with `etl-craft migrate` before using these commands. Plain-SQL migration
`0031_lineage_versions.sql` replaces the derived cache with separate catalog/schema/table
components and creates `AUD_LINEAGE_VERSIONS` and `AUD_LINEAGE_VERSION_EDGES`, guarded against
outside writes and rewriting history. It discards old cached rows, which were never history.
History begins with version 1 at the first computation after the upgrade; earlier lineage
cannot be reconstructed from this cache.

## Source definitions and stars

Lineage uses metadata only, including in CI and before warehouse tables exist. In dependency
order, it learns an active SQL task's target columns from that task's traced SELECT. External
sources and tables landed by Python scripts take their columns from `CFG_COLUMNS`, joined to their `CFG_TABLES` parent.
The catalog uses the same definitions and shows their declared types without `--with-warehouse`.

Keep object contracts in `config/tables.csv`:

```csv
CATALOG_NAME,SCHEMA_NAME,TABLE_NAME,OBJECT_KIND,ACTIVE_FLAG
,raw,orders,TABLE,Y
,ref,rates,TABLE,Y
```

Keep their declared columns in `config/columns.csv`:

```csv
CATALOG_NAME,SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,DATA_TYPE,COLUMN_POSITION,IS_NULLABLE,ACTIVE_FLAG
,raw,orders,id,BIGINT,1,,Y
,raw,orders,amount,"DECIMAL(12,2)",2,,Y
,ref,rates,id,BIGINT,1,,Y
,ref,rates,rate,"DECIMAL(12,2)",2,,Y
```

Catalog absence is an empty CSV cell/SQL NULL, never a made-up catalog name. Components are
separate values. Current source tracing retains its unquoted identifier requirement; the
remaining object-reference conversion in S5.O.2b widens quoting support across consumers.
`DATA_TYPE` is a type in the warehouse's SQL dialect. Equivalent SQL type names are normalized.
String lengths and timestamp precisions are checked when explicitly declared; omitted ones
are not asserted. Snowflake's generic `TIMESTAMP` alias does not assert an account-dependent
timezone mapping; use `TIMESTAMP_NTZ`, `TIMESTAMP_LTZ` or `TIMESTAMP_TZ` to check it.
`OBJECT_KIND` and `IS_NULLABLE` are optional declared expectations; warehouse comparisons
report mismatches or unavailable observations when these are declared. Export seeds known
kinds and nullability and keeps existing expectations when an observation is unavailable.
Positions are positive and distinct per
active table, and determine star expansion order. Definitions participate in config apply,
retirement, actor auditing and `config export --as-of`, like the other config rows. Older projects
can omit both files: applying their other files leaves existing source definitions unchanged.
An empty file with its header explicitly retires the active definitions.

After applying these rows, `SELECT * FROM raw.orders` traces both columns, and
`SELECT amount * rate AS converted FROM raw.orders o JOIN ref.rates r ON o.id = r.id`
resolves both bare columns. `SELECT *` over an upstream SQL target uses its inferred output,
so it does not need a duplicate source definition. Ambiguous joins and undefined columns stay
reported with their tables and reason; `--strict` fails on them.

`etl-craft config export` seeds source columns from existing warehouse tables referenced by
active SQL tasks or landed by scripts, and refreshes existing external definitions. Missing
tables retain their definitions. Export never changes the Engine DB; review the files, then
apply them. `--as-of` exports historical metadata without consulting the current warehouse.
The lower-level Python export without a connector config also remains metadata-only.

`etl-craft validate` compares active definitions with tables that exist and reports every added,
missing or differently typed column as schema drift. It skips tables that do not yet exist;
it reports an unavailable warehouse instead of claiming the definitions passed. Config plan
and apply validate their transaction's metadata and files without probing the warehouse.

`generate-docs --with-warehouse` also compares documented and deployed columns and shows
declared versus deployed source types in the [catalog](catalog.md#comparing-documentation-with-the-deployment).

See [the example definitions](../examples/source-columns.csv) and
[configuration files](configuration-files.md).

## Documentation versions

A task's `DOCUMENTATION` parameter holds its description. `etl-craft docs-version` records a new
version in `AUD_TASK_DOCUMENTATION` for each task whose text changed since its last version, and
lists every documented task with its version; `--check` shows what would change and records
nothing. The version follows the text itself, so it cannot fall out of step with it.
