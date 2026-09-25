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

Three tabs at the top of every page, and a trail of links back up from wherever you are:

| Tab | Leads to |
|---|---|
| **Search** | counts, and search across everything |
| **DAGs** | every pipeline with its name, description, schedule, the pipelines it depends on and its last run; the ingestion scripts; and the SQL tasks whose columns cannot be traced |
| **Warehouse** | every database, then schema, then table, with the tasks that write and read each table, its pipelines and business rules; and the external sources ingestion scripts read |

Every task, table, pipeline, rule and script a page mentions is a link to its own page, so any
chain can be followed: DAGs › pipeline › task › the table it writes › the task that reads that
table › its pipeline, and so on.

| Page | Shows |
|---|---|
| Pipeline | its description (`CFG_PIPELINES.DESCRIPTION`), schedule, SLA, refresh type, last run, the pipelines it depends on and that depend on it, its **DAG**, and its tasks with what each waits for, writes and reads |
| Task | its pipeline, the tasks it waits for and that wait for it (with the dependency type), handler, the tables it reads, writes and checks, its business rules, documentation and documentation version, column mapping, parameters with the SELECT (a `SOURCE_SQL_FILE` is shown too), and its last run with its counts and error |
| Table | the tasks that write and read it, their pipelines, its business rules, its last write and row counts, its columns with where each is made from, and its lineage graph |
| Business rule | type, table, key column, wave, task, and its condition |
| Ingestion script | whether the file is there, the tasks that run it, and what they read and write |

## A pipeline's DAG

Each pipeline page draws its tasks left to right, each after the tasks it waits for. Tasks of
other pipelines it waits for sit on the left. Lines show the dependency type: `SUCCESS` plain,
`FAILURE` red and dashed, `ALWAYS` dotted, `HAS_DATA` green. Each task box lists the table it
writes (→), the tables it reads (←) and the tables its business rules check (✓); click a task or
a table to open it. The DAG pans and zooms like the lineage graphs.

A table is every SQL task's `TARGET_OBJECT`, every table a SELECT reads (joins and filters
included), every business rule's `TARGET_TABLE`, and every ingestion script's `TARGET_OBJECT`.
Tables are named as [lineage](lineage.md) names them: lower case, without the active warehouse
database.

## Lineage graphs

Each table page draws its lineage: the tables it is made from to the left, as far as they go, and
the tables made from it to the right. Arrows point from each source to what is made from it.

Tables open collapsed, showing their names; the page's own table opens with its columns. Click a
table's name to show or hide its columns (the columns lineage connects), or ↗ to open its page.
*Expand all* and *Collapse all* do every table at once.

| Line | Means |
|---|---|
| solid | the column is copied as it is |
| dashed | the column is derived: an expression, `CASE` or aggregate; the tooltip shows it, and a column may have several sources |
| dotted, box to box | the tables are linked, but not column by column: an ingestion script, or a task whose columns cannot be traced |

A column marked `ƒ` is made from no source column, such as `COUNT(*)` or a constant.

- **Click a column** to trace it: every path into it and out of it lights up, across every
  table shown, and each table on the way opens. Click it again, or *Clear trace*, to stop. A column's name in the table above the
  graph traces it too, and the link can be shared: `…/sales.orders.html#col=amount_usd`.
- **Pan and zoom:** drag the background to move around, or use the scroll bars. −, + and *Fit*
  zoom out, in, and to the whole graph; Ctrl or ⌘ with the mouse wheel zooms around the
  pointer. The frame grows with the graph, up to most of the window's height.
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

## Keeping it up to date

The site is plain files, so its run details, statuses, row counts, errors and last runs, are as
of when it was generated. Write it again on a schedule, nightly say, to keep them fresh; every
page says when it was generated and warns once its run details are more than a day old.

```yaml
Docs_site:
  prod:
    Schedule: "0 2 * * *"      # 02:00 every day: five cron fields, or @daily, @hourly, ...
    Output: /srv/etl-craft-docs  # optional: the folder generate-docs writes; default catalog/
```

**Under an orchestrator**, `etl-craft generate-yml --docs` writes the `etl_craft_docs` DAG,
which runs `etl-craft generate-docs` on `Schedule` (with no schedule when `Allow_schedule` is
false, like every other DAG):

```bash
etl-craft generate-yml --docs --output dags/etl_craft_docs.yml
```

**Without one**, schedule the command on the machine itself, from the project directory:

```text
0 2 * * *  cd /srv/etl-craft && etl-craft generate-docs      # crontab on Linux or macOS
schtasks /Create /SC DAILY /ST 02:00 /TN etl-craft-docs /TR "cmd /c cd /d C:\etl-craft && etl-craft generate-docs"
```

Writing the site again never leaves it half written: the new site is built beside the old one
and swapped in once it is complete, so a server publishing the folder keeps serving whole pages.

## The output folder

`generate-docs` writes `--output`, else `Docs_site.Output`, else `catalog/` in the project
directory. It writes only a folder that is new, empty, or one it wrote before, which it marks with
a `.etl-craft-catalog` file; that folder is replaced whole, so pages for removed tasks and tables
go too. A folder holding anything else is refused.
