# Exit codes

Every `etl-craft` command ends with one of three exit statuses, so schedulers and scripts can
tell a failed run from a broken setup.

| Status | Meaning |
|---|---|
| `0` | Success. |
| `1` | The work ran and did not succeed: a task or pipeline failed, `validate` found problems, or a `doctor` check failed. |
| `2` | The command could not start: invalid arguments, or a missing or invalid `craft-connector.yml`. |

## Which errors map to which status

Every error etl-craft raises on purpose belongs to the `EtlCraftError` hierarchy in
[`etl_craft.core.errors`](../api/etl_craft/core/errors.md), and each family carries its exit
status.

| Error | Status | Raised when |
|---|---|---|
| `ConfigurationError` | `2` | `craft-connector.yml` is missing or invalid, a secret variable is unset, a connection target has no matching dialect, or the Engine DB cannot be reached |
| `UsageError` | `2` | the command line arguments are invalid |
| `MetadataError` | `1` | a pipeline or task code does not resolve to an active `CFG_` row |
| `GraphError` | `1` | the task or pipeline dependency graph is invalid, for example a cycle |
| `RunStateError` | `1` | the run log is in a state the requested run cannot proceed from |
| `RunRefusedError` | `1` | the run is not allowed in the configured mode, for example `--force` in remote mode |
| `ConnectionTestError` | `1` | a connection fails its test before the run starts |
| `EngineDbError`, `MigrationError`, `LockTimeoutError` | `1` | the Engine DB cannot be initialised or migrated, or a lock is not acquired in time |
| `HandlerError` | `1` | a task handler is missing or fails; the task is recorded as `FAILED` |

`etl-craft run` exits `0` when the pipeline or task ends `SUCCESS` or `SKIPPED`, and `1`
otherwise.
