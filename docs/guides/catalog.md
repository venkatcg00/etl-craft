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
| **DAGs** | every pipeline with its name, description, schedule, the pipelines it depends on, its last run, how many of its recent runs succeeded and how long they took on average; the ingestion scripts; and the SQL tasks whose columns cannot be traced |
| **Warehouse** | every database, then schema, then table, with the tasks that write and read each table, its pipelines and business rules; and the external sources ingestion scripts read |

Every task, table, pipeline, rule and script a page mentions is a link to its own page, so any
chain can be followed: DAGs › pipeline › task › the table it writes › the task that reads that
table › its pipeline, and so on.

| Page | Shows |
|---|---|
| Pipeline | its description (`CFG_PIPELINES.DESCRIPTION`), schedule, SLA, refresh type, last run, whether it is paused, the pipelines it depends on and that depend on it, its **DAG**, its tasks with what each waits for, writes and reads, and its **runs** |
| Run | one run of a pipeline: its status, run date (and whether it was a backfill), when it ran and how long it took, its SLA, each task's status, attempts, counts and message, what operators changed in it, the upstream runs it was **built from**, and the downstream runs that **used** it |
| Task | its pipeline, the tasks it waits for and that wait for it (with the dependency type), handler, the tables it reads, writes and checks, its business rules, documentation and documentation version, column mapping, parameters with the SELECT (a `SOURCE_SQL_FILE` is shown too), its last run with its counts and error, and its **runs** |
| Table | the tasks that write and read it, their pipelines, its business rules, its last write and row counts, its columns with where each is made from, and its lineage graph |
| Business rule | type, table, key column, wave, task, and its condition |
| Ingestion script | whether the file is there, the tasks that run it, and what they read and write |

## Runs and KPIs

A pipeline's page ends with its latest 30 runs:

- **KPIs**: how many of the finished runs succeeded, how many failed or were cancelled, SLA
  misses, and the average and longest run. A `SKIPPED` run did no work and one still
  `IN-PROGRESS` has not ended, so neither counts either way.
- **A bar per run**, oldest first: its height is how long the run took, its colour the run's
  status. Each bar opens that run's page.
- **A row per run**: its run date, status, start, duration, SLA, the upstream runs it was built
  from, and how many changes operators made to it.

A task's page does the same for the task: the share of its runs that succeeded, the average run
and the average rows written, a bar per run of the rows it wrote, and a row per run with its
attempts, counts and message.

Each run has a page of its own, so runs chain like everything else: a run page lists the
upstream runs it was built from and the downstream runs that used it, from the
[consumption log](dependencies.md#dependencies-on-other-pipelines), each a link to that run's
page. Follow them from any run to the first run of the data it holds, or to everything built
from it.

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

## Publishing the site

`etl-craft publish-docs` serves the site until it is stopped (Ctrl-C, or SIGTERM from a service
manager), so it can run as a long-lived service beside the nightly rebuild. It serves the folder
as it is at each request, so a rebuilt site is served as soon as it is swapped in.

**At a link, through ngrok.** Install the SDK on the machine that publishes
(`pip install 'etl-craft[publish]'`), put your ngrok authtoken in a variable, and name it:

```yaml
Docs_site:
  prod:
    Authtoken: NGROK_AUTHTOKEN         # the variable holding the token, never the token itself
    Domain: etl-docs.example.com       # optional: a domain reserved in your ngrok account
    Allowed_ips: [203.0.113.0/24]      # optional: only visitors from these ranges are served
```

```text
$ etl-craft publish-docs
publish-docs: serving /srv/etl-craft/catalog at https://etl-docs.example.com (Ctrl-C to stop)
```

- **Who can see it.** There is no login: anyone with the link can read the site, so share it
  like a document. The site is never listed or indexed: every response carries
  `X-Robots-Tag: noindex`, `robots.txt` disallows everything, pages send no referrer and cannot be
  framed, and folders are never listed. `Allowed_ips` limits it to your offices or VPN: every
  other visitor gets `403`. `publish-docs` enforces it itself, on any ngrok plan, from the
  visitor's address ngrok forwards. Teams behind a firewall allow the site's domain (the one
  `publish-docs` prints) through it.
- **A link that stays the same.** Without `Domain`, ngrok gives the account's own free domain.
  The URL is recorded in the Engine DB; if a later publish gets another one, links already
  shared would break, so `publish-docs` stops and names both. Set `Domain` to keep the first,
  or pass `--accept-new-url` to use the new one from then on.
- For the first few seconds after `publish-docs` starts, the link can show ngrok's own
  "endpoint is offline" page while ngrok routes to it; reload and the site appears.
- On ngrok's free plan, a browser first shows ngrok's own warning page once per visitor;
  a paid plan's domain does not.
- The variable only needs to be set on the machine that publishes; `doctor` says when it is
  missing there, and when the SDK is not installed.

**On this machine or network only**, without ngrok:

```bash
etl-craft publish-docs --local-only                         # http://127.0.0.1:<port>/
etl-craft publish-docs --local-only --host 0.0.0.0 --port 8080   # to the local network
```

## The output folder

`generate-docs` writes `--output`, else `Docs_site.Output`, else `catalog/` in the project
directory. It writes only a folder that is new, empty, or one it wrote before, which it marks with
a `.etl-craft-catalog` file; that folder is replaced whole, so pages for removed tasks and tables
go too. A folder holding anything else is refused.
