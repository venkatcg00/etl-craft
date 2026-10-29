"""Run CI partitions and verify that every test and matrix result is accounted for."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from release_gate import check_outcomes, git, node_ids, sha256_of

ROOT = Path(__file__).resolve().parents[1]
LOCAL_WAREHOUSES = ("duckdb", "postgres", "duckdb_iceberg", "trino_iceberg")
WAREHOUSE_TESTS = (
    "warehouse_duckdb or warehouse_postgres or warehouse_duckdb_iceberg or warehouse_trino_iceberg"
)
SERVICE_MARKERS = {
    "engine": f"engine_postgres and not ({WAREHOUSE_TESTS})",
    "warehouse-local": "warehouse_duckdb or warehouse_postgres or harness or connections",
    "warehouse-iceberg": "warehouse_duckdb_iceberg or warehouse_trino_iceberg",
}
SHARDS = {
    "coverage": 1,
    "measured-coverage": 3,
    "engine": 5,
    "warehouse-local": 1,
    "warehouse-iceberg": 2,
    "e2e": 1,
}
VALIDATIONS = {"package-ubuntu-latest", "package-macos-latest"}


def combinations() -> dict[str, list[dict[str, str]]]:
    """Plan the approved pull-request platform, service and installed-demo combinations."""
    coverage = [
        {"profile": f"coverage-{version}-{runner}", "python": version, "os": runner}
        for version in ("3.11", "3.12", "3.13")
        for runner in ("ubuntu-latest",)
    ]
    service = [{"profile": profile} for profile in SERVICE_MARKERS]
    e2e = [
        {"profile": f"e2e-{warehouse}", "warehouse": warehouse} for warehouse in LOCAL_WAREHOUSES
    ]
    return {
        "coverage": coverage,
        "macos": [
            {"profile": "coverage-3.13-macos-latest", "python": "3.13", "os": "macos-latest"}
        ],
        "service": service,
        "e2e": e2e,
    }


def matrix(serial: bool = False) -> dict[str, list[dict[str, str | int]]]:
    """Expand each combination into independently isolated jobs."""
    return {
        family: [
            row | {"index": index, "count": count}
            for row in rows
            for count in [
                1
                if serial
                else SHARDS[
                    "measured-coverage"
                    if row["profile"] == "coverage-3.11-ubuntu-latest"
                    else "coverage"
                    if family == "macos"
                    else family
                    if family != "service"
                    else row["profile"]
                ]
            ]
            for index in range(1 if family == "e2e" else count)
        ]
        for family, rows in combinations().items()
    }


PRIMARY = "coverage-3.11-ubuntu-latest"


def selection(profile: str) -> str:
    """Select complete ordinary suites once, plus explicit platform compatibility checks."""
    if profile == PRIMARY:
        return f"(unit or engine_sqlite or docs) and not ({WAREHOUSE_TESTS})"
    if profile.startswith("coverage-"):
        return "unit or engine_sqlite"
    if profile == "e2e-duckdb":
        return "e2e_pip or e2e_uv or package"
    return "e2e_pip or e2e_uv" if profile.startswith("e2e-") else SERVICE_MARKERS[profile]


def assigned_profile(node: str, markers: list[str]) -> str:
    """Give each collected ordinary case one authoritative owner; cloud stays release-only."""
    present = set(markers)
    if present & {"cloud_databricks", "cloud_snowflake"}:
        return "release"
    if "package" in present:
        return "e2e-duckdb"
    if present & {"e2e_pip", "e2e_uv"}:
        warehouse = next(
            (name for name in LOCAL_WAREHOUSES if node.endswith(f"-{name}]")), "duckdb"
        )
        return f"e2e-{warehouse}"
    if present & {"warehouse_duckdb_iceberg", "warehouse_trino_iceberg"}:
        return "warehouse-iceberg"
    if present & {"warehouse_duckdb", "warehouse_postgres", "connections", "harness"}:
        return "warehouse-local"
    if present & {"unit", "engine_sqlite", "docs"}:
        return PRIMARY
    return "engine" if "engine_postgres" in present else "unassigned"


def check_collections(groups: dict[str, list[dict[str, Any]]]) -> list[str]:
    """Compare authoritative selections with the unfiltered collection, including new tests."""
    if PRIMARY not in groups:
        return ["missing authoritative complete collection"]
    catalog = groups[PRIMARY][0].get("ci", {}).get("catalog")
    if not isinstance(catalog, dict) or not catalog:
        return ["missing authoritative unfiltered collection"]
    expected: dict[str, set[str]] = {}
    for node, markers in catalog.items():
        if not isinstance(node, str) or not isinstance(markers, list):
            return ["invalid unfiltered collection"]
        profile = assigned_profile(node, markers)
        if profile != "release":
            expected.setdefault(profile, set()).add(node)
    problems = []
    for profile, nodes in expected.items():
        reports = groups.get(profile, [])
        found = node_ids(reports[0].get("ci", {}).get("expected")) if reports else None
        if found != nodes:
            problems.append(
                f"{profile}: complete suite has missing or duplicated ownership: "
                f"{sorted(nodes ^ (found or set()))}"
            )
    for profile, reports in groups.items():
        if profile.startswith("coverage-") and profile != PRIMARY:
            wanted = {
                node
                for node, marks in catalog.items()
                if "unit" in marks
                or (
                    "engine_sqlite" in marks
                    and node.startswith("tests/integration/engine/test_transitions.py::")
                )
            }
            if node_ids(reports[0].get("ci", {}).get("expected")) != wanted:
                problems.append(f"{profile}: incomplete platform compatibility selection")
    return problems


def run(profile: str, index: int, count: int, output: Path) -> int:
    """Run a complete marker selection, partitioning only after its collection finishes."""
    if os.environ.get("PYTEST_ADDOPTS", "").strip():
        print("unset PYTEST_ADDOPTS before recording complete CI collections", file=sys.stderr)
        return 2
    output.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "-p",
        "no:cacheprovider",
        "-o",
        "addopts=",
        "--strict-markers",
        "--strict-config",
        "-n",
        "logical",
        "--dist",
        "worksteal",
    ]
    command += ["-m", selection(profile)]
    if profile in {PRIMARY, "warehouse-local"}:
        command += ["--cov", "--cov-report=", "--cov-fail-under=0"]
    elif profile == MEASURED_SERVICE:
        # Only PostgreSQL reaches parts of the supervision modules: measure just those.
        command += [f"--cov={_module(path)}" for path in SUPERVISION_MODULES]
        command += ["--cov-report=", "--cov-fail-under=0"]
    command += [f"--evidence={output / 'results.json'}"]
    command += [f"--ci-shard={index}/{count}"]
    if wheel := os.environ.get("ETL_CRAFT_TEST_WHEEL"):
        command += [f"--evidence-wheel={wheel}"]
    env = {
        **os.environ,
        "ETL_CRAFT_CI_PROFILE": profile,
        "COVERAGE_FILE": str(output / "coverage.data"),
    }
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    print(
        f"{profile}: shard {index}/{count}, exit {result.returncode}\n{result.stdout}",
        end="",
        flush=True,
    )
    return result.returncode


def check_profile(profile: str, reports: list[dict[str, Any]], count: int) -> list[str]:
    """Require identical full collections and one result for each assigned test."""
    problems: list[str] = []
    indices = [report.get("ci", {}).get("index") for report in reports]
    if sorted(indices, key=str) != sorted(range(count), key=str):
        return [f"{profile}: expected exactly shards 0..{count - 1}, found {indices!r}"]
    expected = node_ids(reports[0].get("ci", {}).get("expected"))
    if expected is None:
        return [f"{profile}: invalid complete collection"]
    seen: set[str] = set()
    commits = set()
    for report in reports:
        details = report.get("ci", {})
        marker = selection(profile)
        if report.get("marker") != marker:
            problems.append(f"{profile}: unexpected test selection {report.get('marker')!r}")
        if details.get("catalog") != reports[0].get("ci", {}).get("catalog"):
            problems.append(f"{profile}: unfiltered collections disagree")
        if details.get("count") != count or node_ids(details.get("expected")) != expected:
            problems.append(f"{profile}: shard collection/count disagrees with its peers")
        if details.get("profile") != profile:
            problems.append(f"{profile}: mislabeled shard")
        if profile.startswith("e2e-") and details.get("warehouse") != profile.removeprefix("e2e-"):
            problems.append(f"{profile}: wrong warehouse combination")
        if not profile.startswith("coverage-") and details.get("services_required") is not True:
            problems.append(f"{profile}: service requirements were not enforced")
        selected = node_ids(report.get("collected"))
        problems.extend(
            f"{profile}: {problem}"
            for problem in check_outcomes(report, allow_skips=False, require_pass=False)
        )
        if selected is None:
            continue
        if seen & selected:
            problems.append(f"{profile}: duplicate test IDs across shards")
        seen |= selected
        if report.get("exit_status") != 0:
            problems.append(f"{profile}: failed shard {details.get('index')}")
        if report.get("dirty") is not False or not report.get("commit"):
            problems.append(f"{profile}: missing clean source provenance")
        commits.add(report.get("commit"))
        if profile.startswith("coverage-"):
            version = profile.split("-", 2)[1]
            expected_platform = "darwin" if profile.endswith("macos-latest") else "linux"
            if not str(report.get("python", "")).startswith(version + ".") or (
                report.get("platform_key") != expected_platform
            ):
                problems.append(f"{profile}: wrong Python/platform combination")
    if seen != expected:
        problems.append(f"{profile}: missing or unexpected test IDs: {sorted(seen ^ expected)!r}")
    if len(commits) != 1:
        problems.append(f"{profile}: shards used different source commits")
    if not any(
        isinstance(report.get("counts"), dict) and report["counts"].get("passed", 0)
        for report in reports
    ):
        problems.append(f"{profile}: no tests passed across the complete combination")
    return problems


def verify(directory: Path, wheel: Path, serial: bool = False) -> list[str]:
    """Fail closed when a profile, artifact, shard or coverage result is missing."""
    groups: dict[str, list[dict[str, Any]]] = {}
    paths: dict[str, list[Path]] = {}
    problems = []
    head = git(ROOT, "rev-parse", "HEAD").stdout.strip()
    wheel_hash = sha256_of(wheel)
    for path in directory.rglob("results.json"):
        report = json.loads(path.read_text(encoding="utf-8"))
        profile = report.get("ci", {}).get("profile")
        groups.setdefault(profile, []).append(report)
        paths.setdefault(profile, []).append(path)
        if report.get("commit") != head:
            problems.append(f"{path}: expected source commit {head}")
        if str(profile).startswith("e2e-") and report.get("wheel_sha256") != wheel_hash:
            problems.append(f"{path}: wrong installed-wheel hash")
    expected = {
        str(row["profile"]): int(row["count"]) for rows in matrix(serial).values() for row in rows
    }
    if set(groups) != set(expected):
        missing = sorted(set(groups) ^ set(expected), key=str)
        problems.append(f"CI profiles missing/unexpected: {missing}")
    for profile, count in expected.items():
        if profile in groups:
            problems.extend(check_profile(profile, groups[profile], count))
    validations = {}
    for path in directory.rglob("validation.json"):
        report = json.loads(path.read_text(encoding="utf-8"))
        name = report.get("name")
        if name in validations or report.get("commit") != head:
            problems.append(f"{path}: duplicate or wrong-source validation")
        validations[name] = report
    if set(validations) != VALIDATIONS:
        problems.append("package matrix results are missing or unexpected")
    if not problems:
        problems.extend(check_collections(groups))
    if not problems:
        for profile in expected:
            if profile == "coverage-3.11-ubuntu-latest":
                measured = (
                    paths[profile]
                    + paths.get(MEASURED_SERVICE, [])
                    + paths.get("warehouse-local", [])
                )
                problems.extend(coverage_report(profile, measured, directory))
    return problems


SUPERVISION_MODULES = (
    "src/etl_craft/execution/reconcile.py",
    "src/etl_craft/controller/supervisor.py",
    "src/etl_craft/controller/leadership.py",
    "src/etl_craft/api/serve.py",
)
"""Modules that must each reach 90% coverage, beyond the project total."""
MEASURED_SERVICE = "engine"
"""The service profile whose coverage of the supervision modules joins every combination's."""


def _module(path: str) -> str:
    """Return the import name of a module under ``src/``."""
    return path.removeprefix("src/").removesuffix(".py").replace("/", ".")


def coverage_report(profile: str, reports: list[Path], directory: Path) -> list[str]:
    """Combine each complete Python/platform run, then require 90% overall and per module.

    ``reports`` also holds the Engine DB service shards, which measure only the supervision
    modules, so code that only PostgreSQL reaches counts toward them.
    """
    files = [path.parent / "coverage.data" for path in reports]
    missing = [str(path) for path in files if not path.is_file()]
    if missing:
        return [f"{profile}: missing coverage artifacts {missing}"]
    target = directory / "combined" / profile
    target.mkdir(parents=True, exist_ok=True)
    # The coverage tool itself is never measured, even when it runs under a measured process.
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"COVERAGE_PROCESS_START", "COVERAGE_PROCESS_CONFIG"}
    } | {"COVERAGE_FILE": str(target / ".coverage")}
    commands = [
        ["combine", "--keep", *map(str, files)],
        ["report", "--fail-under=90"],
    ]
    for arguments in commands:
        result = subprocess.run(
            [sys.executable, "-m", "coverage", *arguments], cwd=ROOT, env=env, check=False
        )
        if result.returncode:
            return [f"{profile}: coverage {' '.join(arguments[:1])} failed"]
    problems = []
    for module in SUPERVISION_MODULES:
        result = subprocess.run(
            [sys.executable, "-m", "coverage", "report", f"--include={module}", "--fail-under=90"],
            cwd=ROOT,
            env=env,
            check=False,
        )
        if result.returncode:
            problems.append(f"{profile}: {module} is below 90% coverage; add tests for it")
    return problems


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    timing_parser = commands.add_parser("timing")
    timing_parser.add_argument("run_id")
    plan = commands.add_parser("matrix")
    plan.add_argument("--serial", action="store_true")
    plan.add_argument("--output", type=Path, required=True)
    for kind in ("run", "run-all"):
        execute = commands.add_parser(kind)
        execute.add_argument(
            "profile",
            choices=sorted(row["profile"] for rows in combinations().values() for row in rows),
        )
        if kind == "run":
            execute.add_argument("index", type=int)
        execute.add_argument("count", type=int)
        execute.add_argument("output", type=Path)
    check = commands.add_parser("verify")
    check.add_argument("directory", type=Path)
    check.add_argument("wheel", type=Path)
    check.add_argument("--serial", action="store_true")
    record = commands.add_parser("record")
    record.add_argument("name", choices=sorted(VALIDATIONS))
    record.add_argument("output", type=Path)
    args = parser.parse_args(argv)
    if args.command == "timing":
        from ci_timing import timing

        return timing(args.run_id)
    if args.command == "matrix":
        with args.output.open("a", encoding="utf-8") as handle:
            for family, rows in matrix(args.serial).items():
                handle.write(f"{family}={json.dumps({'include': rows})}\n")
        return 0
    if args.command == "run":
        return run(args.profile, args.index, args.count, args.output)
    if args.command == "run-all":
        if args.count < 1:
            parser.error(f"CI worker count {args.count} must be positive; set count to at least 1")
        with ThreadPoolExecutor(max_workers=args.count) as workers:
            futures = [
                workers.submit(run, args.profile, index, args.count, args.output / str(index))
                for index in range(args.count)
            ]
            codes = [future.result() for future in futures]
        return int(any(code != 0 for code in codes))
    if args.command == "record":
        args.output.mkdir(parents=True, exist_ok=True)
        report = {"name": args.name, "commit": git(ROOT, "rev-parse", "HEAD").stdout.strip()}
        (args.output / "validation.json").write_text(json.dumps(report), encoding="utf-8")
        return 0
    jobs = json.loads(os.environ.get("ETL_CRAFT_CI_JOBS", "{}"))
    required_jobs = {"prepare", "static", "test", "service", "e2e", "package"}
    if set(jobs) != required_jobs or any(job.get("result") != "success" for job in jobs.values()):
        print("CI jobs are missing, failed, cancelled or skipped", file=sys.stderr)
        return 1
    problems = verify(args.directory, args.wheel, args.serial)
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
