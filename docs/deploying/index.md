# Deploying

Running etl-craft on a machine or under an orchestrator.

- [The project directory](project-layout.md): `etl-craft/`, with the config, SQL files,
  ingestion scripts, migrations and logs, and how commands find it.
- [Running under an orchestrator](orchestrator.md): `generate-yml`, the DAG it writes, and the
  global DAG.
- [Engine DB setup and upgrades](engine-db.md): `init-db`, `migrate`, and your own migrations.

!!! note "Planned"
    - **Local mode**: etl-craft runs each pipeline's waves itself.
    - **Operations**: backups and upgrades.
    - **Security**: secrets, credentials and least-privilege roles.
