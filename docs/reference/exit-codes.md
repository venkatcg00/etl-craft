# Exit codes

Every `etl-craft` command ends with one of three exit statuses, so schedulers and scripts can
tell a failed run from a broken setup.

| Status | Meaning |
|---|---|
| `0` | Success. |
| `1` | The work ran and did not succeed: a task or pipeline failed, `validate` found problems, or a `doctor` check failed. |
| `2` | The command could not start: invalid arguments, or a missing or invalid `craft-connector.yml`. |
