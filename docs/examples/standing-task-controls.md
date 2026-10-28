# Standing task pause example

Run these commands in the project folder, with `Orchestration.Project` explicitly configured.
Choose the pipeline and task codes from `etl-craft pipelines` and `etl-craft steps`.

```sh
etl-craft pause --pipeline-code SALES_DAILY --task-code load --reason "Source maintenance"
etl-craft pause --pipeline-code SALES_DAILY --task-code load --replace --outcome SKIPPED \
  --expires-at 2030-12-01T12:00:00+00:00 --reason "Skip during maintenance"
etl-craft resume --pipeline-code SALES_DAILY --task-code load --reason "Source restored"
```

Choose a future expiry with an explicit offset. Task resume removes the pause and its outcome;
it creates no runs. Runs admitted before a pause or replacement hold instead of inheriting a
new outcome. A scheduled pipeline resume requires a separate choice:

```sh
etl-craft resume --pipeline-code SALES_DAILY --then wait --reason "Wait for the next tick"
```

See [run control](../guides/run-control.md) for all three scheduled resume choices.
