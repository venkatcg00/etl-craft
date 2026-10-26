"""Measure a Linux controller and a Docker PostgreSQL deployment without another dependency."""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from etl_craft.config import load_config
from etl_craft.engine.connection import engine_db


def main() -> None:
    """Return normalized Docker CPU, live DB connections and the controller's RSS."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--postgres-container", required=True)
    parser.add_argument("--postgres-cpus", type=float, default=4)
    parser.add_argument("--controller-pid-file", type=Path)
    args = parser.parse_args()
    if args.postgres_cpus <= 0:
        parser.error("postgres-cpus must be positive")
    pid = (
        args.controller_pid_file.read_text(encoding="utf-8").strip()
        if args.controller_pid_file
        else os.environ["BENCHMARK_CONTROLLER_PID"]
    )
    status = Path(f"/proc/{int(pid)}/status").read_text(encoding="utf-8")
    rss = next(
        int(line.split()[1]) * 1024 for line in status.splitlines() if line.startswith("VmRSS:")
    )
    measured = subprocess.check_output(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.CPUPerc}}",
            args.postgres_container,
        ],
        text=True,
    ).strip()
    cpu = float(measured.removesuffix("%")) / args.postgres_cpus
    engine = engine_db(load_config(args.config))
    try:
        with engine.connect() as conn:
            row = conn.exec_driver_sql(
                "SELECT COUNT(*) AS connections,current_setting('max_connections')::int AS maximum "
                "FROM pg_stat_activity"
            ).one()
        sys.stdout.write(
            json.dumps(
                {
                    "postgres_cpu_percent": cpu,
                    "connections": row.connections,
                    "max_connections": row.maximum,
                    "controller_memory_bytes": rss,
                }
            )
            + "\n"
        )
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
