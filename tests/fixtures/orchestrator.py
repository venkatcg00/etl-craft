"""A small orchestrator that runs the DAGs ``generate-yml`` writes, as Airflow would.

It reads a DAG's ``tasks``: each step's ``bash_command``, the steps it ``depends_on`` and its
``trigger_rule``. A step is decided once every step it depends on is: it runs when its trigger
rule is met, and is otherwise ``skipped`` (``upstream_failed`` when a failure is why), which
counts as settled for the steps after it. A step that fails is run again up to
``default_args.retries`` times, as Airflow's retries do.

A ``sensor`` step asks ``sense`` for the state of the DAG run or task it waits for, and succeeds
when that state is one of its ``allowed_states``; otherwise it fails, as a sensor that times out
does. A DAG run succeeds when every leaf step succeeded or was skipped, as Airflow decides.
"""

from __future__ import annotations

import shlex
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from typing import Any

RUN_DATE_TEMPLATE = "{{ data_interval_end | ds }}"
"""The Airflow template a remote DAG passes the run date with; this orchestrator puts today."""

SUCCESS, FAILED, SKIPPED, UPSTREAM_FAILED = "success", "failed", "skipped", "upstream_failed"


@dataclass
class DagRun:
    """What happened to each step, and how many times each was run."""

    states: dict[str, str] = field(default_factory=dict)
    tries: dict[str, int] = field(default_factory=dict)
    leaves: list[str] = field(default_factory=list)

    @property
    def state(self) -> str:
        """``success`` when every leaf step succeeded or was skipped, else ``failed``."""
        settled = all(self.states[leaf] in (SUCCESS, SKIPPED) for leaf in self.leaves)
        return SUCCESS if settled else FAILED


def _rule_met(rule: str, upstream: list[str]) -> bool | None:
    """Return whether ``rule`` lets a step run: ``None`` skips it, ``False`` upstream_failed."""
    failed = [s in (FAILED, UPSTREAM_FAILED) for s in upstream]
    succeeded = [s == SUCCESS for s in upstream]
    if rule == "all_success":
        return True if all(succeeded) else (False if any(failed) else None)
    if rule == "all_failed":
        return True if all(failed) else None
    if rule == "all_done":
        return True
    if rule == "one_success":
        return True if any(succeeded) else (False if any(failed) else None)
    if rule == "one_failed":
        return True if any(failed) else None
    if rule == "one_done":
        return True if any(s in (SUCCESS, FAILED) for s in upstream) else None
    raise AssertionError(f"a trigger rule this orchestrator does not know: {rule}")


def run_dag(
    dag: dict[str, Any],
    execute: Callable[[list[str]], int],
    sense: Callable[[dict[str, Any]], str | None] | None = None,
) -> DagRun:
    """Run every step of ``dag``; ``execute`` runs one command and returns its exit status.

    ``sense`` returns the state a sensor step waits on, or ``None`` when there is none yet.
    """
    steps: dict[str, dict[str, Any]] = dag["tasks"]
    retries = int(dag.get("default_args", {}).get("retries", 0))
    upstream = {name for step in steps.values() for name in step["depends_on"]}
    run = DagRun(leaves=[name for name in steps if name not in upstream])
    while len(run.states) < len(steps):
        ready = [
            name
            for name, step in steps.items()
            if name not in run.states and all(d in run.states for d in step["depends_on"])
        ]
        assert ready, f"the DAG cannot progress: {sorted(set(steps) - set(run.states))}"
        for name in ready:
            step = steps[name]
            met = _rule_met(step["trigger_rule"], [run.states[d] for d in step["depends_on"]])
            if met is None:
                run.states[name] = SKIPPED
                continue
            if met is False:
                run.states[name] = UPSTREAM_FAILED
                continue
            if "sensor" in step:
                assert sense is not None, f"{name} is a sensor, and nothing answers it"
                run.tries[name] = 1
                seen = sense(step["sensor"])
                run.states[name] = SUCCESS if seen in step["sensor"]["allowed_states"] else FAILED
                continue
            command = shlex.split(
                step["bash_command"].replace(RUN_DATE_TEMPLATE, date.today().isoformat())
            )
            for attempt in range(1, retries + 2):
                run.tries[name] = attempt
                if execute(command) == 0:
                    run.states[name] = SUCCESS
                    break
            else:
                run.states[name] = FAILED
    return run
