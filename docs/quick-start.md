# Quick Start

From nothing to five pipelines running, checked, traced and documented, in about ten minutes. You
run the Support Insights demo from the repository's `examples/demo`: two clients' support
interactions landed, parsed and built into a small data mart, with business rules, alerts, a
failure on purpose and a retry. The Engine DB is a SQLite file and the warehouse a DuckDB file,
both created beside the demo's `craft-connector.yml`.

The release tests run these steps, as written here, from the built package.

## What you need

- Python 3.11 or newer.
- Docker, or another way to run [Mailpit](https://mailpit.axllent.org/), for the demo's alert
  emails.

## 1. Install etl-craft and get the demo

```bash
git clone https://github.com/venkatcg00/etl-craft.git
cd etl-craft
python -m venv .venv && . .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install .
cd examples/demo
```

`etl-craft --version` checks the install. DuckDB and SQLite need nothing more; the other
warehouses are extras, such as `pip install ".[trino]"`.

## 2. Start Mailpit

```bash
docker run -d --name mailpit -p 51025:1025 -p 58025:8025 axllent/mailpit
```

The demo's `craft-connector.yml` sends its alerts to port `51025`; you read them at
<http://localhost:58025>.

## 3. Set up the databases

```bash
python prepare.py warehouse
etl-craft setup
python prepare.py metadata
etl-craft validate
```

- `prepare.py warehouse` creates the schemas the demo writes in `warehouse.duckdb`. etl-craft
  never creates warehouse schemas: your team does.
- `setup` runs every check `doctor` makes (the config, the Engine DB, the warehouse, the email
  relay, the project folders) and creates the Engine DB's tables in `engine.db`.
- `prepare.py metadata` loads the pipelines, their tasks, parameters, dependencies and business
  rules into `engine.db`, as `CFG_` rows. In your own project these rows are your pipelines,
  written and reviewed like code.
- `validate` checks all of it without running anything:
  `checked 5 pipeline(s) and 27 task(s): 0 failed, 0 warning(s)`.

## 4. Run the pipelines

```bash
etl-craft run --pipeline_code CLIENT_ALPHA
etl-craft run --pipeline_code CLIENT_BETA
etl-craft run --pipeline_code SUPPORT_DM      # fails at flaky_feed, on purpose
etl-craft run --pipeline_code SUPPORT_DM
```

Each run prints its tasks in dependency waves, each in a process of its own, then a summary:

```text
CLIENT_ALPHA: pipeline_run_id=1 SUCCESS
CLIENT_BETA: pipeline_run_id=2 SUCCESS
SUPPORT_DM: pipeline_run_id=3 FAILED — 1 task(s) did not succeed: flaky_feed (FAILED); skipped because of the failure: interactions, setup_fact, fact, quality, area_summary, source_counts, drop_source_counts
SUPPORT_DM: pipeline_run_id=4 SUCCESS
```

`SUPPORT_DM` waits for both clients' pipelines. Its `flaky_feed` is not ready the first time, so
the tasks that need it are skipped, the run fails and exits `1`, and the alerts in Mailpit say
so. The next run finds the feed and succeeds on the same client runs. Each task attempt has its
own log under `logs/`.

## 5. Look at what happened

```bash
etl-craft history --pipeline_code SUPPORT_DM
etl-craft graph --pipeline_code SUPPORT_DM
etl-craft lineage --table dm.support_fact --column rating --upstream
etl-craft generate-docs
```

- `history` lists the runs; `graph` the waves and dependencies.
- `lineage` traces a column back through every task and pipeline that wrote it:

    ```text
    dm.support_fact.rating
      <- pre_dm.interactions.rating  [copy]  (SUPPORT_DM.fact)
        <- prs.alpha_interactions.rating  [copy]  (SUPPORT_DM.interactions)
          <- lnd.client_alpha.rating  [CAST(a.rating AS INT)]  (CLIENT_ALPHA.parse)
          <- lnd.client_alpha.rating  [CAST(a.rating AS INT)]  (CLIENT_ALPHA.setup_prs)
        <- prs.beta_interactions.rating  [copy]  (SUPPORT_DM.interactions)
          <- lnd.client_beta.score  [CAST(b.score AS INT)]  (CLIENT_BETA.parse)
          <- lnd.client_beta.score  [CAST(b.score AS INT)]  (CLIENT_BETA.setup_prs)
      <- pre_dm.interactions.rating  [copy]  (SUPPORT_DM.setup_fact)
    ```

    Each table is written by two tasks: its `SETUP_TABLE` task creates it from the SELECT, and
    the task after it fills it.

- `generate-docs` writes the [catalog site](guides/catalog.md) to `catalog/`: open
  `catalog/index.html` for every pipeline's DAG, every table with its columns, lineage graphs,
  business rules and last runs.

The data is in `warehouse.duckdb`, in the `lnd`, `prs`, `ds`, `cdc`, `pre_dm` and `dm` schemas,
and after every run the Engine DB's tables are cloned into its `aud` schema.

## Next

- [Guides](guides/index.md): how dependencies, the SQL actions, business rules, ingestion
  scripts and alerts work, and how to step in on a run.
- [The project directory](deploying/project-layout.md) and
  [`craft-connector.yml` examples](examples/README.md): starting a project of your own.
- [Running under an orchestrator](deploying/orchestrator.md): the same pipelines as Airflow DAGs.
