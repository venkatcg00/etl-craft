"""Measure completed workflow execution separately from runner scheduling delays."""

from __future__ import annotations

import base64
import json
import subprocess
from datetime import datetime
from typing import Any

import yaml


def api(path: str) -> Any:
    result = subprocess.run(["gh", "api", path], check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def measure(
    run: dict[str, Any], jobs: list[dict[str, Any]], workflow: dict[str, Any]
) -> dict[str, Any]:
    """Use the workflow's actual needs graph and the longest matrix member per job."""
    definitions = workflow["jobs"]
    durations: dict[str, float] = {}
    waits = []
    for job in jobs:
        if job["status"] != "completed" or not job["completed_at"]:
            raise ValueError(f"Job {job['name']} has not completed; measure after the run finishes")
        matches = []
        for key, definition in definitions.items():
            name = definition.get("name", key)
            prefix = name.split("${{", 1)[0]
            if ("${{" in name and job["name"].startswith(prefix)) or job["name"] == name:
                matches.append(key)
        if len(matches) != 1:
            raise ValueError(f"Job {job['name']} matches {matches}; use unambiguous workflow names")
        duration = (timestamp(job["completed_at"]) - timestamp(job["started_at"])).total_seconds()
        key = matches[0]
        durations[key] = max(durations.get(key, 0), duration)
        waits.append(
            {
                "name": job["name"],
                "execution_seconds": duration,
                "queue_seconds": max(
                    0, (timestamp(job["started_at"]) - timestamp(job["created_at"])).total_seconds()
                ),
            }
        )
    if set(durations) != set(definitions):
        raise ValueError("Workflow jobs are missing; measure a complete run with every job")
    chains: dict[str, float] = {}

    def chain(key: str, visiting: frozenset[str] = frozenset()) -> float:
        if key in visiting:
            raise ValueError(f"Workflow needs cycle at {key}")
        if key not in chains:
            needs = definitions[key].get("needs", [])
            if isinstance(needs, str):
                needs = [needs]
            chains[key] = durations[key] + max(
                (chain(parent, visiting | {key}) for parent in needs), default=0
            )
        return chains[key]

    execution = max(chain(key) for key in definitions)
    wall = (
        max(timestamp(job["completed_at"]) for job in jobs) - timestamp(run["created_at"])
    ).total_seconds()
    return {
        "run_id": run["id"],
        "conclusion": run["conclusion"],
        "execution_seconds": execution,
        "queue_seconds": sum(job["queue_seconds"] for job in waits),
        "workflow_queue_seconds": max(
            0,
            (
                min(timestamp(job["created_at"]) for job in jobs) - timestamp(run["created_at"])
            ).total_seconds(),
        ),
        "longest_job_queue_seconds": max(job["queue_seconds"] for job in waits),
        "wall_seconds": wall,
        "slowest_jobs": sorted(waits, key=lambda job: job["execution_seconds"], reverse=True)[:5],
    }


def timing(run_id: str) -> int:
    """Read the run and its version of the workflow, including all paginated jobs."""
    run = api(f"repos/{{owner}}/{{repo}}/actions/runs/{run_id}")
    jobs = []
    page = 1
    while True:
        batch = api(
            f"repos/{{owner}}/{{repo}}/actions/runs/{run_id}/attempts/{run['run_attempt']}/jobs?per_page=100&page={page}"
        )
        jobs.extend(batch["jobs"])
        if len(jobs) >= batch["total_count"]:
            break
        if not batch["jobs"]:
            raise ValueError("GitHub returned incomplete job pages; fetch the run again")
        page += 1
    source = api(f"repos/{{owner}}/{{repo}}/contents/{run['path']}?ref={run['head_sha']}")
    workflow = yaml.safe_load(base64.b64decode(source["content"]))
    print(json.dumps(measure(run, jobs, workflow), indent=2))
    return 0
