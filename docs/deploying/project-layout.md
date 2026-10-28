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
the handler's folder (`sql_scripts/` for `SCRIPT_FILE` and `SQL_SCRIPT`, `python_scripts/`
for `PYTHON`) and stay inside it, with no `..`, and the file must exist and end in `.sql` or
`.py`. Otherwise the task fails before anything runs, with a
message that names the parameter, the value and the folder, and suggests close matches:

```
SCRIPT_FILE='sql_scripts/sales/order.sql': no such file /srv/etl/etl-craft/sql_scripts/sales/order.sql — did you mean: sql_scripts/sales/orders.sql
```

## Configuration files

`etl-craft config apply` merges the CSV files in `config/` into the Engine DB's `CFG_` tables.
See [Configuration files](../guides/configuration-files.md).

## Project migrations

`etl-craft migrate` applies your migrations from `migrations/` when that folder exists. See
[Engine DB setup and upgrades](engine-db.md).

## Several projects in one repository

Keep one folder per project. For example, `sales/` and `finance/` each contain their own
`craft-connector.yml`, `config/`, `sql_scripts/`, `python_scripts/` and `migrations/`.
Set `Orchestration.Project: sales` in the first file and `Orchestration.Project: finance` in
the second. The code comes from that setting, never from the folder name. Both projects may
use the same Engine DB, with separate warehouse connections.

```bash
etl-craft config plan --project-dir sales/
etl-craft config apply --project-dir sales/
```

Project commands require `--project` or `Orchestration.Project`; there is no implicit `default`.
If both are given they must agree. Deployment commands such as accounts, groups, pools and
`project list` need no project selection. An administrator creates a project, then grants its
team access explicitly. Deploying configuration requires project CAN_MANAGE.

Configuration files carry changes, so CI may stage only changed CSV files. Absent rows and files
are untouched; applying unchanged files is a no-op. The application has no git-aware or
multi-project apply command. A trusted deployment job can choose folders with git, for example:

```python
import os
import subprocess
from pathlib import Path

changed = (
    subprocess.check_output(["git", "diff", "--name-only", "-z", os.environ["BASE_SHA"], "HEAD"])
    .decode()
    .split("\0")
)
projects = set()
for filename in filter(None, changed):
    for folder in Path(filename).parents:
        if (folder / "craft-connector.yml").is_file():
            projects.add(folder)
            break
for folder in sorted(projects):
    for action in ("plan", "apply"):
        subprocess.run(["etl-craft", "config", action, "--project-dir", str(folder)], check=True)
```

This example applies each existing touched folder's change files, including unchanged files as
no-ops. A deleted project folder applies nothing. Disabling the project remains the explicit
administrator command `project disable --project CODE --reason REASON`.

Projects in an instance share a code trust boundary. Use separate instances for separate
security domains; project permissions control application access, not arbitrary task code.


## Environments and governance

An admin labels each project with an environment (`DEV`, `SIT`, `UAT`, `PROD`, `SANDBOX` or
`CUSTOM`), an optional environment name, and `STRICT` or `RELAXED` governance:

```sh
etl-craft project set --project sales --environment-type DEV --environment-name perf --governance RELAXED --reason "Development testing"
etl-craft project list --reason "Review environments"
```

These fields are administration data, never `craft-connector.yml` or CSV configuration.
Existing projects remain `CUSTOM`/`STRICT`; production requires `STRICT`. Omitted fields on
`project set` keep their stored values. Only deployment admins change them, always with a reason.

`STRICT` interventions require a reason. `RELAXED` permits empty reasons for marks, pauses,
holds/releases, stops/aborts, recover/restart, one-run pause permission, dependency overrides,
reruns/backfills, offset/watermark resets, item controls and data edits/unlocks. Authorization,
auditing and safety checks are unchanged; administrative reasons remain mandatory.

Direct SQL edits need PostgreSQL and a `RELAXED` project. They retain row identities, attribution,
constraints and history. The Engine DB is the source of truth; no file-drift report is needed.
On SQLite use files or commands: an ordinary client reports `no such function: etl_craft_actor`
(or another `etl_craft_...` function). Doctor reports this limitation as INFO for RELAXED SQLite
projects. SQLite file writers can bypass protection; use file permissions and PostgreSQL for
shared database access.

Projects may share a warehouse database and schema. Admin project reads and doctor show an
informational note when the current approved connection definitions match; nothing is blocked.
Projects without a published connection cannot be compared until their connections are known.

If an approved bundle or its connection settings cannot be read, project results report the
comparison as unavailable with a remedy. That diagnostic does not block an admin change.
