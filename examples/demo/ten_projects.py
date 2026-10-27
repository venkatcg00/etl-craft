"""Run the same small pipeline in ten projects sharing the configured Engine DB."""

from __future__ import annotations

import csv
import shutil
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from etl_craft.cli import main

HERE = Path(__file__).resolve().parent


def command(config: Path, *arguments: str) -> None:
    """Keep metadata, authorization and execution on the package's public CLI path."""
    code = main(["--config", str(config), *arguments])
    if code:
        raise SystemExit(code)


def run_demo(config: Path) -> tuple[str, ...]:
    """Preserve deployment pools and existing projects while creating ten demo namespaces."""
    projects = tuple(f"demo_team_{number:02d}" for number in range(1, 11))
    with TemporaryDirectory(prefix="etl-craft-ten-projects-") as temporary:
        root = Path(temporary)
        template = root / "template"
        command(config, "--project", "default", "config", "export", "--config-dir", str(template))
        for project in projects:
            command(
                config,
                "--project",
                project,
                "project",
                "add",
                "--max-running-tasks",
                "1",
                "--reason",
                "Ten-project demonstration",
            )
            folder = root / project
            shutil.copytree(template, folder)
            wanted = {
                "pipelines.csv": [
                    {
                        "PIPELINE_CODE": "POOL_DEMO",
                        "PIPELINE_NAME": "Pool demo",
                        "REFRESH_TYPE": "FULL",
                        "ACTIVE_FLAG": "Y",
                    }
                ],
                "tasks.csv": [
                    {
                        "PIPELINE_CODE": "POOL_DEMO",
                        "TASK_CODE": "load",
                        "TASK_TYPE": "ETL",
                        "HANDLER": "PYTHON",
                        "ACTIVE_FLAG": "Y",
                    }
                ],
                "task_parameters.csv": [
                    {
                        "PIPELINE_CODE": "POOL_DEMO",
                        "TASK_CODE": "load",
                        "PARAMETER_NAME": "SCRIPT_FILE",
                        "PARAMETER_VALUE": "python_scripts/pool_demo.py",
                        "ACTIVE_FLAG": "Y",
                    }
                ],
            }
            for path in folder.glob("*.csv"):
                with path.open(encoding="utf-8", newline="") as stream:
                    fields = next(csv.reader(stream))
                with path.open("w", encoding="utf-8", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
                    writer.writeheader()
                    writer.writerows(wanted.get(path.name, []))
            command(config, "--project", project, "config", "apply", "--config-dir", str(folder))
            command(config, "--project", project, "run", "--pipeline-code", "POOL_DEMO")
    return projects


if __name__ == "__main__":
    if len(sys.argv) > 2:
        raise SystemExit("usage: python ten_projects.py [craft-connector.yml]")
    run_demo(Path(sys.argv[1]).absolute() if len(sys.argv) == 2 else HERE / "craft-connector.yml")
