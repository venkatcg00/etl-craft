# The project directory

Everything a deployment needs lives in one directory, `etl-craft/`, which your team keeps under
version control next to its other code:

```
etl-craft/
├── craft-connector.yml     # connections and settings
├── .env                    # secrets, when Secrets.Source_type is file (keep it out of git)
├── engine.db               # the Engine DB, when it is SQLite at jdbc:sqlite:engine.db
├── sql_scripts/             # SELECTs for SQL tasks and SQL_SCRIPT files
│   └── sales/orders.sql
├── python_scripts/          # Python scripts that PYTHON tasks name
│   └── load_orders.py
├── config/                 # pipelines as CSV files, one per CFG_ table, for `etl-craft config apply`
│   └── pipelines.csv, tasks.csv, task_parameters.csv, ...
├── migrations/             # your own Engine DB migrations, applied by `etl-craft migrate`
│   └── 0001_add_sales_tables.sql
└── logs/                   # one log file per task attempt (Orchestration.Log_dir)
```

The directory holding `craft-connector.yml` is the project directory. Every relative path in the
file (`Secrets.Path`, a SQLite or DuckDB file in a `Jdbc_url`, `Orchestration.Log_dir`) is resolved from it, so every
command and task finds the same files wherever it runs from.

## Finding the project

A command uses `--config` when given, then `$ETL_CRAFT_CONFIG`. Otherwise it searches the current
directory and then each parent, the way git finds `.git`. In each directory it looks for:

- `craft-connector.yml`, when you are inside the project;
- `etl-craft/craft-connector.yml`, when you are in the directory that holds it, such as the root
  of your repository.

Finding both in the same directory is an error (exit status `3`): keep one, or pass `--config`.
With neither found anywhere, the error names `etl-craft/craft-connector.yml` in the current
directory.

## Files that tasks name

A task names its SQL file or script by its path from the project directory, like every other
path: `sql_scripts/sales/orders.sql` or `python_scripts/load_orders.py`. The path must start with
the handler's folder (`sql_scripts/` for `SOURCE_SQL_FILE` and `SQL_SCRIPT`, `python_scripts/`
for `PYTHON`) and stay inside it, with no `..`, and the file must exist and end in `.sql` or
`.py`. Otherwise the task fails before anything runs, with a
message that names the parameter, the value and the folder, and suggests close matches:

```
SOURCE_SQL_FILE='sql_scripts/sales/order.sql': no such file /srv/etl/etl-craft/sql_scripts/sales/order.sql — did you mean: sql_scripts/sales/orders.sql
```

## Configuration files

`etl-craft config apply` merges the CSV files in `config/` into the Engine DB's `CFG_` tables.
See [Configuration files](../guides/configuration-files.md).

## Project migrations

`etl-craft migrate` applies your migrations from `migrations/` when that folder exists. See
[Engine DB setup and upgrades](engine-db.md).
