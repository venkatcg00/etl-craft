# Scoped operator controls

Use the exact run id returned by `etl-craft run --init-only`. These examples use run `42` and
control `17`; substitute the ids returned by your commands.

```bash
etl-craft pause --pipeline_code SALES_DAILY --run-id 42 --task_code transform --reason "inspect input"
etl-craft resume --pipeline_code SALES_DAILY --run-id 42 --task_code transform --reason "ready"
etl-craft cancel --pipeline_code SALES_DAILY --run-id 42 --stop --request-key drain-42 --reason "maintenance"
etl-craft status --request-id 17 --format json
```

The stop response pins affected attempt ids. Wait for `status=COMPLETED`; `REQUESTED` and
`ACKNOWLEDGED` retain actual pending process/query endings. `run_status` is independent.
Without `--stop`, cancellation requests an abort; server-side queries require a cancellation
adapter and otherwise refuse with a graceful-stop remedy. See the [control guide](../guides/run-control.md).
