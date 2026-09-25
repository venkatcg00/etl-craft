# Exit codes

Every `etl-craft` command ends with an exit status that says exactly what happened, so schedulers
and scripts can tell a failed run from a broken setup, and one kind of error from another. Each
error class has its own status; every error etl-craft raises on purpose belongs to the
`EtlCraftError` hierarchy in [`etl_craft.core.errors`](../api/etl_craft/core/errors.md).

| Status | Name | Meaning |
|---|---|---|
| `0` | `SUCCESS` | The command succeeded. `etl-craft run` exits `0` when the pipeline or task ends `SUCCESS` or `SKIPPED`. |
| `1` | `FAILURE` | The work ran and did not succeed: a task or pipeline ended `FAILED`, `validate` found problems, or a `doctor` check failed. |
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
