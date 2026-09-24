# Deploying

Running etl-craft on a machine or under an orchestrator.

- [Engine DB setup and upgrades](engine-db.md): `init-db`, `migrate`, and your own migrations.

!!! note "Planned"
    - **Local mode**: etl-craft runs each pipeline's waves itself.
    - **Orchestrator mode**: `generate-yml` emits one DAG description per pipeline, which you
      convert for your scheduler.
    - **Operations**: backups and upgrades.
    - **Security**: secrets, credentials and least-privilege roles.
