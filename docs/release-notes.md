--8<-- "CHANGELOG.md"

## Moving the parallel-task limit from 0.3.0

Before upgrading, copy `Orchestration.Max_parallel_tasks` from `craft-connector.yml` into
`config/pipelines.csv` as `MAX_PARALLEL_TASKS` for each pipeline that needs that value, then
remove the YAML key. Config loading, validate, doctor and upgrade commands refuse the old key,
naming its file, its value and this remedy. Nothing runs with the old key silently ignored.

The Engine DB never stored the old YAML setting. Migrations do not read project YAML; upgraded
pipelines start with the package default of 8. Apply your edited config files after migrating.
The seeded `default` pool remains available. Administrators create additional pools with
`etl-craft pool add --name POOL --reason "Capacity"`.

An omitted task pool uses the seeded active `default` pool. Local process capacity remains bounded
by resource discovery and `Local_python_slots` / `Local_warehouse_slots`, independently of pipeline limits.
