# Scoped operator controls

Use the exact run id returned by `etl-craft run --init-only`. These examples use run `42` and
control `17`; substitute the ids returned by your commands.

```bash
etl-craft hold --pipeline-code SALES_DAILY --run-id 42 --task-code transform --reason "inspect input"
etl-craft release --pipeline-code SALES_DAILY --run-id 42 --task-code transform --reason "ready"
etl-craft stop --pipeline-code SALES_DAILY --run-id 42 --request-key drain-42 --reason "maintenance"
etl-craft control-request --control-id 17 --format json
```

The stop response pins affected attempt ids. Wait for `status=COMPLETED`; `REQUESTED` and
`ACKNOWLEDGED` retain actual pending process/query endings. `run_status` is independent.
`abort` stops task processes now; server-side queries require a cancellation adapter, so it
refuses there and names `stop` as the remedy. See the [control guide](../guides/run-control.md).

Prepare recovery of the same run, or a new restart that retains current checkpoints:

```bash
etl-craft run --pipeline-code SALES_DAILY --run-id 42 --recover --request-key repair-42 --reason "input fixed" --format json
etl-craft run --pipeline-code SALES_DAILY --run-id 42 --restart --request-key fresh-42 --reason "fresh context" --format json
etl-craft capabilities --pipeline-code SALES_DAILY --run-id 42 --format json
```

Use the returned `execution_run_id` with `run --run-id`; preparation completion is separate from
execution completion. Restart retains offsets/watermarks and can repeat handler writes. The
[control guide](../guides/run-control.md#inspect-capabilities-and-apply-an-explicit-selection)
includes the exact bulk JSON format and one-run permission under a definition pause.
