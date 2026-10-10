# Exit codes

Every `etl-craft` command ends with an exit status that says exactly what happened, so schedulers
and scripts can tell a failed run from a broken setup, and one kind of error from another. Each
error class has its own status; every error etl-craft raises on purpose belongs to the
`EtlCraftError` hierarchy in [`etl_craft.core.errors`](../api/etl_craft/core/errors.md).

| Status | Name | Meaning |
|---|---|---|
| `0` | `SUCCESS` | The command succeeded. `etl-craft run` exits `0` when the pipeline or task ends `SUCCESS` or records `SKIPPED`; a paused pipeline that starts nothing retains exit 0. |
| `1` | `FAILURE` | The work ran and did not succeed: a task or pipeline ended `FAILED` or `CANCELLED`, `validate` found problems, or a `doctor` check failed. |
| `2` | `USAGE` | The command line arguments are invalid (`UsageError`, or `argparse` itself). |
| `3` | `CONFIGURATION` | `craft-connector.yml` is missing or invalid, a secret variable is unset, a connection target has no matching dialect, or the Engine DB cannot be reached (`ConfigurationError`). |
| `4` | `METADATA` | A pipeline or task code does not resolve to an active `CFG_` row (`MetadataError`). |
| `5` | `GRAPH` | The dependency graph is invalid: a duplicate task, an unknown dependency type or a run condition that cannot hold (`GraphError`). |
| `6` | `SELF_DEPENDENCY` | A task depends on itself (`SelfDependencyError`). |
| `7` | `DEPENDENCY_CYCLE` | The dependencies form a cycle (`CycleError`). |
| `8` | `UNKNOWN_TASK` | A dependency names a task that is not active in the pipeline (`UnknownTaskError`). |
| `9` | `RUN_STATE` | The run log is in a state the requested run cannot proceed from, such as a single task with no run to bind to (`RunStateError`). |
| `10` | `RUN_REFUSED` | The run is not allowed in the configured mode, for example `--force` in remote mode (`RunRefusedError`). |
| `11` | `CONNECTION_TEST` | A connection failed its test before the run started (`ConnectionTestError`). |
| `12` | `ENGINE_DB` | The Engine DB cannot be initialised or changed (`EngineDbError`). |
| `13` | `MIGRATION` | A migration failed, or an applied migration file is missing or was edited (`MigrationError`). |
| `14` | `LOCK_TIMEOUT` | A cross-process lock was not acquired in time, such as a busy single-writer warehouse (`LockTimeoutError`). |
| `15` | `HANDLER` | A task handler is missing or failed; the task is recorded as `FAILED` (`HandlerError`). |
| `16` | `UNEXPECTED` | An error with no class of its own, including a bug. Its traceback is written to the log. |
| `17` | `CLONING` | Copying an Engine DB table into the warehouse failed; the message names the table and the database's error (`CloningError`). |
| `18` | `REMOTE_UNSUPPORTED` | In remote mode, a pipeline has a rule its orchestrator does not support, such as run condition `N` or a `HAS_DATA` dependency; the message names each pipeline, task and rule, with the remedy (`RemoteUnsupportedError`). |
| `19` | `INJECTED_FAULT` | A development fault was deliberately injected (`InjectedFaultError`). |
| `20` | `STALE_TRANSITION` | A run or attempt changed status or owner before this write, or another active attempt won admission (`StaleTransitionError`). Read its history and refresh the row before retrying. |
| `21` | `SQL_GUARD` | Correct the SQL input or target state before retrying (`SqlGuardError`). |
| `22` | `INCOMPLETE` | The run was left unfinished, including a pause during a run, an interrupted backfill or work owned by another process. `--init-only` deliberately initializes a run and retains exit 0. |
| `23` | `WAITING` | A single-task run recorded nothing because its dependencies are not met yet. Run it again once the upstreams satisfy its condition. |
| `24` | `RESOURCE_NOT_FOUND` | An exact execution resource or attempt log is unavailable. |
| `25` | `METADATA_FILE` | A file in the project's `config/` folder cannot be read, or its rows cannot be loaded; the message names the file, line, column and value. |
