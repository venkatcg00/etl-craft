"""Select locally affected Python tests without overlooking files loaded as data."""

import argparse
import subprocess
import sys
from pathlib import PurePosixPath

ROOTS = ("examples/", "templates/", "docs/examples/", "tests/fixtures/schemas/")
SERVICE_MARKERS = (
    "engine_sqlite or engine_postgres or warehouse_duckdb or warehouse_postgres "
    "or warehouse_duckdb_iceberg or warehouse_trino_iceberg"
)


def needs_full_suites(paths: list[str]) -> bool:
    """Testmon cannot track SQL, manifests, migrations, or shipped project data."""
    return any(
        path.startswith(ROOTS)
        or (path.startswith("release/") and path.endswith(".toml"))
        or ("src/etl_craft/dialects/" in path and PurePosixPath(path).suffix in {".sql", ".toml"})
        for path in paths
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", default="auto")
    args = parser.parse_args()
    paths = []
    for command in (
        ["git", "diff", "--name-only", "origin/main", "--"],
        ["git", "ls-files", "--others", "--exclude-standard"],
    ):
        paths.extend(subprocess.check_output(command, text=True).splitlines())
    # A mixed testmon selection can include services; keep its memory use bounded too.
    workers = "4" if args.workers == "auto" else args.workers
    if needs_full_suites(paths):
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "-n",
                workers,
                "--dist",
                "worksteal",
                "-m",
                SERVICE_MARKERS,
            ],
            check=False,
        )
        if result.returncode:
            return result.returncode
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "--testmon",
            "-n",
            workers,
            "--dist",
            "worksteal",
        ],
        check=False,
    ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
