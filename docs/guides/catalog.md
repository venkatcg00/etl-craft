# The catalog site

`etl-craft generate-docs` writes a searchable catalog of everything etl-craft runs: every
pipeline, task, table, business rule and ingestion script, with lineage graphs that follow each
table and column from its first sources to its last consumers, across tasks and pipelines.

```bash
etl-craft generate-docs                      # into catalog/ in the project directory
etl-craft generate-docs --output /srv/docs   # anywhere else
etl-craft generate-docs --with-warehouse     # add column types and comments from the warehouse
etl-craft generate-docs --strict             # write nothing if any SQL task cannot be traced
```

The site is plain files, with nothing loaded from elsewhere: open `catalog/index.html` from disk,
serve the folder from any web server, or publish it with `publish-docs`. Pages ask search engines
not to index them.

## What is in it

| Page | Shows |
|---|---|
| Home | counts, search, every pipeline with its last run, every table, and the SQL tasks whose columns could not be traced |
| Pipeline | name, description, schedule, SLA, refresh type, last run, the pipelines it depends on and that depend on it, its tasks with their last run and row counts |
| Task | handler, run condition, the tables it reads and writes, its documentation and documentation version, its column mapping, its parameters with the SELECT (a `SOURCE_SQL_FILE` is shown too), and its last run with its counts and error |
| Table | the tasks that write and read it, its business rules, its last write and row counts, its columns with where each is made from, and its lineage graph |
| Business rule | type, table, key column, wave, task, and its condition |
| Ingestion script | whether the file is there, the tasks that run it, and what they read and write |

A table is every SQL task's `TARGET_OBJECT`, every table a SELECT reads (joins and filters
included), every business rule's `TARGET_TABLE`, and every ingestion script's `TARGET_OBJECT`.
Tables are named as [lineage](lineage.md) names them: lower case, without the active warehouse
database.

## Lineage graphs

Each table page draws its lineage: the tables it is made from to the left, as far as they go, and
the tables made from it to the right. Each box lists the columns lineage connects, every column
for the page's own table.

| Line | Means |
|---|---|
| solid | the column is copied as it is |
| dashed | the column is derived: an expression, `CASE` or aggregate; the tooltip shows it, and a column may have several sources |
| dotted, box to box | the tables are linked, but not column by column: an ingestion script, or a task whose columns cannot be traced |

A column marked `ƒ` is made from no source column, such as `COUNT(*)` or a constant.

- **Click a column** to trace it: every path into it and out of it lights up, across every
  table shown. Click it again, or *Clear trace*, to stop. A column's name in the table above the
  graph traces it too, and the link can be shared: `…/sales.orders.html#col=amount_usd`.
- **Direction** shows only what is upstream or downstream.
- **Depth** shows fewer levels. A deep graph opens three levels each way.

A graph draws at most 200 tables. Past that it stops at the last level that fits and says so;
`etl-craft lineage --table` follows every level.

## Search

The box at the top of every page searches as you type: pipelines, tasks, tables, columns, rules,
scripts, and descriptions and documentation. Names count most, then the start of a word, then
descriptions; letters in order also match, so `slsord` finds `sales.orders`. Enter opens every
result on the home page, where they can be filtered by kind.

## SQL that cannot be traced

A SQL task whose columns cannot be traced, such as `SELECT *` over a table whose columns are not
known, keeps its table on the graph, linked box to box from the tables its SELECT reads. The task
page shows *column lineage unavailable* with the reason, the home page lists every such task, and
`generate-docs` prints them. With `--strict` it writes nothing and exits 1 while any remain.

## Ingestion scripts

A script's code is not read, so a `PYTHON` task says what it moves in two optional parameters:

| Parameter | Value |
|---|---|
| `TARGET_OBJECT` | the table the script writes, `schema.table` or `database.schema.table` |
| `SOURCE_OBJECT` | where it reads from, in words: `CRM API`, `s3://landing/orders/` |

The source appears on the graph as an external box, linked to the table.

## Column types and comments

With `--with-warehouse`, each table that exists in the warehouse gets its columns in the
warehouse's order, with their types, and their comments where the warehouse's
`information_schema` has them (DuckDB, Snowflake, Databricks). A table the warehouse cannot
describe keeps the columns lineage found, and the command logs why.

## The output folder

`generate-docs` writes only into a folder that is new, empty, or one it wrote before, which it
marks with a `.etl-craft-catalog` file. It empties that folder and writes it again, so pages for
removed tasks and tables go too. A folder holding anything else is refused.
