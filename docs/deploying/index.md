# Deploying

Running etl-craft on a machine or under an orchestrator.

- [The project directory](project-layout.md): `etl-craft/`, with the config, SQL files,
  ingestion scripts, migrations and logs, and how commands find it.
- [Local mode](local-mode.md): etl-craft as the orchestrator, runs started on a schedule, and
  stepping in.
- [Running under an orchestrator](orchestrator.md): `generate-yml`, the DAG it writes, and the
  global DAG.
- [Checking a deployment: `doctor` and `setup`](doctor-and-setup.md): every check, and setting up
  or upgrading in one command.
- [Cloning into the warehouse](cloning.md): copying the Engine DB tables into the warehouse
  after every run, and `etl-craft clone`.
- [Engine DB setup and upgrades](engine-db.md): `init-db`, `migrate`, and your own migrations.
- [Operations](operations.md): backups, upgrades, logs and watching runs.
- [Security](security.md): secrets, least-privilege accounts, and what tasks can do.
