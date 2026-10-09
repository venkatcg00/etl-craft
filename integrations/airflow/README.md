# etl-craft-airflow

A separate Airflow DAG factory for `etl_craft_yaml_version: 1` exports. Tested with
Apache Airflow **2.11.0** and **3.3.2** on Python 3.11; supported minor lines are 2.11.x
and 3.3.x. Other minor lines and YAML versions are refused.

Install this distribution in the scheduler's Airflow environment from the repository:

```bash
pip install ./integrations/airflow
# Or install Airflow too: pip install './integrations/airflow[airflow3]'
```

Install `etl-craft` separately in the worker's ETL environment. Put its executable on the
worker's PATH and set `ETL_CRAFT_CONFIG` to the project's connector file. Airflow's dependency
constraints do not apply to that environment. The factory never imports the ETL engine.

Generate YAML with `etl-craft generate-yml` into a folder next to your DAG module. In that module:

```python
from pathlib import Path
import pendulum
from etl_craft_airflow import load_dags

globals().update(
    load_dags(
        Path(__file__).parent / "etl_craft_yaml",
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    )
)
```

Choose the start date for your deployment; the metadata's `start_date` wins when present.
Only deploy trusted YAML: its Bash commands execute on your workers. Validation refuses
malformed YAML, unknown versions, missing dependencies, cycles and duplicate DAG ids.

Pipeline commands, environment, trigger rules, timezone, schedule, retries and graph are
preserved. Nonzero exit codes fail Bash tasks, including 99. Sensors use reschedule mode and
compare the same logical date by default; adjust the returned sensor's `execution_delta` or
`execution_date_fn` before registration if upstream schedules differ. Pipeline `sla_hours` and
`refresh_type` remain engine metadata; etl-craft enforces its SLA at finalization.

The global export uses `TriggerDagRunOperator`, waits for each child result, and preserves the
parent's run id and logical date. A retried or cleared trigger clears that exact child DAG run,
so its tasks replay under the same etl-craft run key. The docs export uses a Bash task.

The schema symlink points to the engine's canonical schema in this checkout. Both wheel and
sdist contain the resolved file and work without the repository.
