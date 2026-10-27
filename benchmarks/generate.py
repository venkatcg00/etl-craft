"""Generate ordinary isolated projects for the controller load benchmark."""

import argparse
import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path

import yaml

from etl_craft.engine.repository.config_rows import TABLES

FAKE = '''"""Synthetic benchmark work through the ordinary Python task contract."""
import random
import time
from etl_craft.scripting import ScriptResult

def run(task):
    options = task.input_params
    time.sleep(float(options["seconds"]))
    if random.random() < float(options["failure_rate"]):
        raise OSError("configured benchmark failure")
    return ScriptResult(1)
'''


@dataclass(frozen=True)
class Scale:
    """Reference scale, with smaller dimensions available for harness checks."""

    projects: int = 100
    pipelines: int = 100
    tasks: int = 10
    seconds: float = 1
    failure_rate: float = 0


def generate(root: Path, template: Path, scale: Scale) -> list[Path]:
    """Write CSVs and execution files; apply and publish them with existing commands."""
    if (
        min(scale.projects, scale.pipelines, scale.tasks) < 1
        or not math.isfinite(scale.seconds)
        or scale.seconds < 0
    ):
        raise SystemExit("benchmark scale: expected positive counts and nonnegative task seconds")
    if not 0 <= scale.failure_rate <= 1:
        raise SystemExit("benchmark failure_rate: expected a fraction between 0 and 1")
    root.mkdir(parents=True, exist_ok=False)
    raw = yaml.safe_load(template.read_text(encoding="utf-8"))
    projects = []
    for index in range(scale.projects):
        folder = root / f"bench_{index:03d}"
        folder.mkdir()
        (folder / "craft-connector.yml").write_text(yaml.safe_dump(raw, sort_keys=False))
        (folder / "python_scripts").mkdir()
        (folder / "python_scripts/work.py").write_text(FAKE)
        config = folder / "config"
        config.mkdir()
        rows = project_rows(index, scale)
        for table in TABLES:
            with (config / table.file).open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(
                    stream, fieldnames=[column.name for column in table.columns]
                )
                writer.writeheader()
                writer.writerows(rows.get(table.file, []))
        projects.append(folder)
    (root / "scale.json").write_text(json.dumps(vars(scale), indent=2) + "\n")
    return projects


def project_rows(index: int, scale: Scale) -> dict[str, list[dict]]:
    """Use forward-only dependencies and spread daily schedules across all 1440 minutes."""
    rows = {table.file: [] for table in TABLES}
    for pipeline in range(scale.pipelines):
        code = f"P_{pipeline:03d}"
        minute = (index * scale.pipelines + pipeline) * 1440 // (scale.projects * scale.pipelines)
        rows["pipelines.csv"].append(
            {
                "PIPELINE_CODE": code,
                "PIPELINE_NAME": code,
                "REFRESH_TYPE": "FULL",
                "RUN_SCHEDULE": f"{minute % 60} {minute // 60} * * *",
                "SCHEDULE_TIMEZONE": "UTC",
                "OVERLAP_POLICY": "QUEUE",
                "ACTIVE_FLAG": "Y",
            }
        )
        if pipeline >= 2:
            rows["pipeline_dependencies.csv"].append(
                {
                    "PIPELINE_CODE": code,
                    "DEPENDS_ON_PIPELINE_CODE": f"P_{pipeline - 2:03d}",
                    "DEPENDENCY_TYPE": "SUCCESS",
                    "ACTIVE_FLAG": "Y",
                    "CONSUME_REPAIRS": "Y",
                }
            )
        for task in range(scale.tasks):
            task_code = f"T_{task:02d}"
            identity = {"PIPELINE_CODE": code, "TASK_CODE": task_code, "ACTIVE_FLAG": "Y"}
            rows["tasks.csv"].append({**identity, "TASK_TYPE": "ETL", "HANDLER": "PYTHON"})
            for name, value in {
                "SCRIPT_FILE": "python_scripts/work.py",
                "INPUT_PARAMS": json.dumps(
                    {"seconds": scale.seconds, "failure_rate": scale.failure_rate}
                ),
            }.items():
                rows["task_parameters.csv"].append(
                    {
                        **identity,
                        "PARAMETER_NAME": name,
                        "PARAMETER_VALUE": value,
                    }
                )
            if task:
                rows["task_dependencies.csv"].append(
                    {
                        **identity,
                        "DEPENDS_ON_PIPELINE_CODE": code,
                        "DEPENDS_ON_TASK_CODE": f"T_{task - 1:02d}",
                        "DEPENDENCY_TYPE": "SUCCESS",
                        "CONSUME_REPAIRS": "Y",
                    }
                )
    return rows


def main() -> None:
    """Generate metadata only; never connect to or change a database."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--config", type=Path, required=True)
    for name, default in (("projects", 100), ("pipelines", 100), ("tasks", 10)):
        parser.add_argument(f"--{name}", type=int, default=default)
    parser.add_argument("--task-seconds", type=float, default=1)
    parser.add_argument("--failure-rate", type=float, default=0)
    args = parser.parse_args()
    generate(
        args.root.resolve(),
        args.config,
        Scale(
            args.projects,
            args.pipelines,
            args.tasks,
            args.task_seconds,
            args.failure_rate,
        ),
    )


if __name__ == "__main__":
    main()
