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
target or warehouse dialect changed; `--refresh` works every task out again. Ingestion scripts are
not traced: their SQL, if any, is inside the script.

## Documentation versions

A task's `DOCUMENTATION` parameter holds its description. `etl-craft docs-version` records a new
version in `AUD_TASK_DOCUMENTATION` for each task whose text changed since its last version, and
lists every documented task with its version; `--check` shows what would change and records
nothing. The version follows the text itself, so it cannot fall out of step with it.
