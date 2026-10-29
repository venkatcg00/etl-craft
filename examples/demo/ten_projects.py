"""Run the same small pipeline in ten projects sharing the configured Engine DB."""

from __future__ import annotations

import csv
import sys
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml

from etl_craft.cli import main
from etl_craft.config import load_config
from etl_craft.config.loader import read_config
from etl_craft.dialects.engine.sqlite import resolve_sqlite_path

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
        original = load_config(config)
        source = read_config(config)
        for project in projects:
            folder = root / project
            folder.mkdir()
            written = deepcopy(source)
            orchestration = written.setdefault("Orchestration", {})
            orchestration["Project"] = project
            orchestration["Log_dir"] = str(original.log_dir)
            for block in orchestration.values():
                if isinstance(block, dict) and "Project" in block:
                    block["Project"] = project
            engine = written["Engine"]
            profile = engine.get(original.engine.name, engine)
            profile["Jdbc_url"] = (
                "jdbc:sqlite:" + resolve_sqlite_path(original.engine.jdbc_url, config)
                if original.engine.jdbc_url.startswith("jdbc:sqlite:")
                else original.engine.jdbc_url
            )
            if original.source.path is not None:
                written["Secrets"]["Path"] = str(original.source.path)
            project_file = folder / "craft-connector.yml"
            project_file.write_text(yaml.safe_dump(written, sort_keys=False))
            for name in ("config", "python_scripts", "sql_scripts", "migrations"):
                (folder / name).mkdir()
            (folder / "python_scripts/pool_demo.py").write_bytes(
                (HERE / "python_scripts/pool_demo.py").read_bytes()
            )
            command(
                project_file,
                "--project",
                project,
                "project",
                "add",
                "--max-running-tasks",
                "1",
                "--reason",
                "Ten-project demonstration",
            )
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
                    },
                    {
                        "PIPELINE_CODE": "POOL_DEMO",
                        "TASK_CODE": "load",
                        "PARAMETER_NAME": "SCRIPT_KIND",
                        "PARAMETER_VALUE": "INGESTION",
                        "ACTIVE_FLAG": "Y",
                    },
                ],
            }
            for name, rows in wanted.items():
                with (folder / "config" / name).open("w", encoding="utf-8", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
                    writer.writeheader()
                    writer.writerows(rows)
            command(project_file, "config", "plan")
            command(project_file, "config", "apply")
            command(project_file, "run", "--pipeline-code", "POOL_DEMO")
    return projects


if __name__ == "__main__":
    if len(sys.argv) > 2:
        raise SystemExit("usage: python ten_projects.py [craft-connector.yml]")
    run_demo(Path(sys.argv[1]).absolute() if len(sys.argv) == 2 else HERE / "craft-connector.yml")
