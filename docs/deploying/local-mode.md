# Local mode

In local mode (`Orchestration.Mode: local`) etl-craft is the orchestrator. `etl-craft run
--pipeline_code X` runs the whole pipeline in dependency waves, each task in a process of its
own, at most `Orchestration.Max_parallel_tasks` at once, and checks every rule itself: run
conditions, dependency types, and dependencies on other pipelines. Nothing else is needed but a
way to start runs on a schedule.

## Starting runs on a schedule

Any scheduler that runs a command will do. Run the commands from the project directory, or pass
`--config`, so they find `craft-connector.yml`:

```cron
# crontab: the clients at 05:00, the data mart after them, the catalog at 02:00
0 5 * * *  cd /srv/etl-craft && .venv/bin/etl-craft run --pipeline_code CLIENT_ALPHA
0 5 * * *  cd /srv/etl-craft && .venv/bin/etl-craft run --pipeline_code CLIENT_BETA
30 5 * * * cd /srv/etl-craft && .venv/bin/etl-craft run --pipeline_code SUPPORT_DM
0 2 * * *  cd /srv/etl-craft && .venv/bin/etl-craft generate-docs
```

On Windows, Task Scheduler runs the same commands with `.venv\Scripts\etl-craft.exe`, starting
in the project directory. A systemd timer works the same way.

- **Order between pipelines comes from the metadata**, not the schedule. A pipeline that depends
  on another waits for a running upstream (up to `Orchestration.Gate_wait_minutes`, an hour unless set) and judges its last finished run, so
  `SUPPORT_DM` above starts only on fresh client runs, and is recorded `SKIPPED` otherwise. See
  [Dependencies on other pipelines](../guides/dependencies.md#dependencies-on-other-pipelines).
- **Start each pipeline from one scheduler entry.** Only one run of a pipeline is in progress at
  a time; running it again resumes that run, but do not start two `run` processes for the same
  pipeline at once.
- **The exit status says what happened**: `0` for `SUCCESS` or `SKIPPED`, `1` for a run that
  failed or was cancelled, and one status per kind of error (see
  [Exit codes](../reference/exit-codes.md)), so the scheduler can alert on it. The pipeline's
  own [alert tasks](../guides/email-alerts.md) and SLA emails say more.

## When a run needs a hand

A run that was stopped (Ctrl-C, `SIGTERM`, a reboot) stays `IN-PROGRESS`, and the next `run`
resumes it without repeating finished tasks. To mark a task or a run, record a stand-in upstream
run, cancel a run, run a task again or without its dependencies, or relax dependency gates in an
environment where some upstreams never run, see [Stepping in](../guides/run-control.md).

## Moving to an orchestrator

The same metadata runs under an orchestrator in remote mode, which then holds every rule; see
[Running under an orchestrator](orchestrator.md). A few rules have no equivalent in an
orchestrator's DAG, and remote mode names them.
