"""Run explicit benchmark deployment commands and collect a release budget report."""

import argparse
import json
import math
import os
import signal
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.request import Request

from benchmarks.budgets import buckets, evaluate, histogram, percentile, quantile, samples
from etl_craft.config.loader import load_project
from etl_craft.config.remote import server_url
from etl_craft.core.http_client import open_request


def command(config: Path, project: str, *args: str) -> str:
    """Apply metadata through the same CLI used by teams; retain diagnostics on failure."""
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "etl_craft",
            "--config",
            str(config),
            "--project",
            project,
            *args,
        ],
        check=True,
        timeout=1800,
        stdout=subprocess.PIPE,
        text=True,
    )
    return result.stdout


def prepare(root: Path) -> Path:
    """Apply and publish every generated project before starting schedules."""
    projects = sorted(root.glob("bench_*/craft-connector.yml"))
    if not projects:
        raise SystemExit(f"{root}: no generated projects; run benchmarks.generate first")
    for config in projects:
        project = load_project(config)
        current = json.loads(
            command(
                config,
                project,
                "project",
                "list",
                "--reason",
                "Benchmark setup",
                "--format",
                "json",
            )
        )
        if not any(row["project"] == project for row in current["projects"]):
            command(config, project, "project", "add", "--reason", "Benchmark setup")
        command(config, project, "config", "apply")
        command(config, project, "bundle", "publish")
    return projects[0]


def start(argv: list[str], root: Path, output: Path) -> subprocess.Popen:
    """Launch argv without a shell, using the team's existing credential environment."""
    if not argv or not all(isinstance(value, str) for value in argv):
        raise SystemExit(
            "deployment command: expected a nonempty JSON argv array; fix the manifest"
        )
    with output.open("wb") as stream:
        return subprocess.Popen(
            argv, cwd=root, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True
        )


def metrics_document(url: str, timeout: float) -> str:
    """Use the shared API URL/redirect rules for both readiness and measurement reads."""
    address = server_url(url.removesuffix("/metrics")) + "/metrics"
    with open_request(Request(address), timeout=timeout) as response:
        return response.read().decode()


def ready(manifest: dict, processes: list[subprocess.Popen], agents: int) -> None:
    """Wait for leadership or agent registration without racing process startup."""
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if any(process.poll() is not None for process in processes):
            raise SystemExit("benchmark process exited during startup; inspect process logs")
        try:
            current = list(samples(metrics_document(manifest["metrics"], 2)))
            leader = any(
                name == "etl_craft_controller_leader" and value == 1 for name, _, value in current
            )
            active = sum(value for name, _, value in current if name == "etl_craft_active_agents")
            if leader and active >= agents:
                return
        except OSError:
            pass  # Readiness may race bind; the deadline refuses an unavailable listener.
        time.sleep(0.1)
    raise SystemExit(f"benchmark startup needs leader and {agents} agents; inspect process logs")


def collect(
    manifest: dict,
    processes: list[subprocess.Popen],
    seconds: float,
    interval: float,
    ceiling: int,
    required_active: int = 0,
) -> dict:
    """Scrape cumulative histograms and sample gauges; fail on unavailable observations."""
    gauges = {"queue": [], "cpu": [], "connections": [], "memory": [], "active": []}
    deadline = time.monotonic() + seconds
    last = ""
    previous_ticks, previous_active = {}, -1
    ticks = Counter()
    qualified = 0
    while time.monotonic() < deadline:
        started = time.monotonic()
        if any(process.poll() is not None for process in processes):
            raise SystemExit(
                "benchmark process exited; expected controller and agents alive; "
                "inspect process logs"
            )
        last = metrics_document(manifest["metrics"], 10)
        current = list(samples(last))
        if not any(
            name == "etl_craft_controller_leader" and value == 1 for name, _, value in current
        ):
            raise SystemExit(
                "metrics endpoint is not the leader; select the controller's direct address"
            )
        free = {
            (labels["pool"], labels["slot_kind"])
            for name, labels, value in current
            if name == "etl_craft_agent_slots" and labels["status"] == "free" and value > 0
        }
        ages = [
            value
            for name, labels, value in current
            if name == "etl_craft_queue_oldest_age_seconds"
            and (labels["pool"], labels["slot_kind"]) in free
        ]
        # A successful scrape with idle slots and no queued work is a measured zero queue age.
        if free:
            gauges["queue"].append(max(ages, default=0))
        gauges["active"].append(
            sum(
                value
                for name, labels, value in current
                if name == "etl_craft_runs" and labels["status"] == "IN-PROGRESS"
            )
        )
        current_ticks = buckets(last, "etl_craft_controller_tick_duration_seconds")
        if gauges["active"][-1] >= required_active and previous_active >= required_active:
            if any(current_ticks.get(bound, 0) < count for bound, count in previous_ticks.items()):
                raise SystemExit(
                    "tick counter reset; expected one controller process; inspect restart logs"
                )
            for bound, count in current_ticks.items():
                ticks[bound] += count - previous_ticks.get(bound, 0)
            qualified += 1
        previous_ticks, previous_active = current_ticks, gauges["active"][-1]
        stats = read_probe(manifest, processes[0].pid, ceiling)
        for key, field in (
            ("cpu", "postgres_cpu_percent"),
            ("connections", "connections"),
            ("memory", "controller_memory_bytes"),
        ):
            gauges[key].append(stats[field])
        time.sleep(max(0, interval - (time.monotonic() - started)))
    return {
        "admission_p99_seconds": histogram(last, "etl_craft_admission_latency_seconds", 0.99),
        "queue_free_p99_seconds": percentile(gauges["queue"], 0.99),
        "postgres_cpu_percent": max(gauges["cpu"], default=None),
        "connections": max(gauges["connections"], default=None),
        "controller_memory_bytes": max(gauges["memory"], default=None),
        "tick_p95_seconds": quantile(ticks, 0.95),
        "tick_required_active": required_active,
        "tick_qualified_intervals": qualified,
        "peak_active_runs": max(gauges["active"], default=0),
        "samples": len(gauges["cpu"]),
    }


def read_probe(manifest: dict, pid: int, ceiling: int) -> dict:
    """Require finite host measurements and the deployment's allowed session ceiling."""
    probe = subprocess.run(
        manifest["probe"],
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
        env={**os.environ, "BENCHMARK_CONTROLLER_PID": str(pid)},
    )
    stats = json.loads(probe.stdout)
    if ceiling > int(stats["max_connections"]) * 0.8:
        raise SystemExit(
            f"connection ceiling={ceiling}; expected at most 80% of PostgreSQL "
            f"max_connections={stats['max_connections']}; lower the benchmark ceiling"
        )
    for field in ("postgres_cpu_percent", "connections", "controller_memory_bytes"):
        value = float(stats[field])
        if not math.isfinite(value) or value < 0:
            raise SystemExit(
                f"probe {field}={value}; expected finite nonnegative measurement; fix the probe"
            )
        stats[field] = value
    return stats


def report(path: Path, values: dict, ceiling: int, seconds: float, scale: dict) -> bool:
    """Write measurements with missing values failed and qualification kept explicit."""
    results = evaluate(values, ceiling)
    lines = [
        "# Controller load benchmark",
        "",
        f"Scale: {json.dumps(scale, sort_keys=True)}",
        f"Duration: {seconds:g}s; samples: {values['samples']}; "
        f"peak active runs: {values['peak_active_runs']:g}.",
        "",
        f"Tick intervals: {values['tick_qualified_intervals']} with at least "
        f"{values['tick_required_active']} active runs at both scrapes.",
        "",
        "| Budget | Measured | Upper bound | Result |",
        "| --- | --- | --- | --- |",
    ]
    for result in results:
        limit = result.limit
        lines.append(
            f"| {result.name} | {result.value} | {limit:g} | "
            f"{'PASS' if result.passed else 'FAIL'} |"
        )
    lines += [
        "",
        "Histogram percentiles are conservative bucket upper bounds; gauge percentiles use",
        "nearest-rank samples. CPU/memory/connections are the maximum sampled values.",
        "Queue age is sampled only for matching pools and slot kinds advertising free slots.",
        "Missing evidence fails.",
        "",
        "This report is a harness check unless run for 24 hours on the documented reference",
        "cluster with at least 200 active runs. The reference gate is deferred until after S8.",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return all(result.passed for result in results)


def seed(root: Path, manifest: dict, scale: dict, count: int) -> None:
    """Admit independent roots with bounded CLI concurrency so 200 runs overlap."""
    projects = sorted(root.glob("bench_*/craft-connector.yml"))
    requests = []
    for index in range(min(count, len(projects) * min(2, scale["pipelines"]))):
        path = projects[index % len(projects)]
        requests.append((path, f"P_{index // len(projects):03d}"))

    def admit(request):
        path, pipeline = request
        command(
            path,
            load_project(path),
            "run",
            "--pipeline-code",
            pipeline,
            "--init-only",
            "--api",
            manifest["metrics"].removesuffix("/metrics"),
        )

    with ThreadPoolExecutor(max_workers=min(8, len(requests))) as workers:
        list(workers.map(admit, requests))


def main() -> None:
    """Start the controller and agents, run schedules and always stop owned launch processes."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--deployment", type=Path, required=True)
    parser.add_argument("--seconds", type=float, default=86400)
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--connection-ceiling", type=int, required=True)
    parser.add_argument("--seed-runs", type=int, default=200)
    args = parser.parse_args()
    if min(args.seconds, args.interval, args.connection_ceiling, args.seed_runs) <= 0 or not all(
        math.isfinite(value) for value in (args.seconds, args.interval)
    ):
        parser.error("duration, interval, connection ceiling and seed runs must be positive")
    root = args.root.resolve()
    manifest = json.loads(args.deployment.read_text(encoding="utf-8"))
    if not manifest["agents"]:
        parser.error("deployment requires at least one agent command")
    prepare(root)
    processes = []
    try:
        controller = start(manifest["controller"], root, root / "controller.log")
        processes.append(controller)
        ready(manifest, processes, 0)
        for index, argv in enumerate(manifest["agents"]):
            processes.append(start(argv, root, root / f"agent-{index}.log"))
        ready(manifest, processes, len(manifest["agents"]))
        scale = json.loads((root / "scale.json").read_text(encoding="utf-8"))
        seed(root, manifest, scale, args.seed_runs)
        for process in processes:
            if process.poll() is not None:
                raise SystemExit("a benchmark process exited; inspect controller/agent logs")
        required_active = 200 if scale["projects"] >= 100 and scale["pipelines"] >= 100 else 0
        values = collect(
            manifest,
            processes,
            args.seconds,
            args.interval,
            args.connection_ceiling,
            required_active,
        )
        if not report(args.report, values, args.connection_ceiling, args.seconds, scale):
            raise SystemExit("benchmark budget failed; inspect the generated report")
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)


if __name__ == "__main__":
    main()
