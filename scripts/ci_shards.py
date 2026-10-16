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
SERVICE_MARKERS = {
    "engine": "engine_sqlite or engine_postgres",
    "warehouse-duckdb": "warehouse_duckdb",
    "warehouse-postgres": "warehouse_postgres",
    "warehouse-iceberg": "warehouse_duckdb_iceberg or warehouse_trino_iceberg",
    "connections": "harness or connections",
}
SHARDS = {
    "coverage": 4,
    "engine": 6,
    "warehouse-duckdb": 1,
    "warehouse-postgres": 1,
    "warehouse-iceberg": 3,
    "connections": 1,
    "e2e": 4,
}
VALIDATIONS = {"package-ubuntu-latest", "package-macos-latest", "airflow-2.11.0", "airflow-3.3.2"}


def combinations() -> dict[str, list[dict[str, str]]]:
    """Keep the existing Python/platform, service and installed-demo combinations."""
    coverage = [
        {"profile": f"coverage-{version}-{runner}", "python": version, "os": runner}
        for version in ("3.11", "3.12", "3.13")
        for runner in ("ubuntu-latest", "macos-latest")
    ]
    service = [{"profile": profile} for profile in SERVICE_MARKERS]
    e2e = [
        {"profile": f"e2e-{warehouse}", "warehouse": warehouse} for warehouse in LOCAL_WAREHOUSES
    ]
    return {"coverage": coverage, "service": service, "e2e": e2e}


def matrix(serial: bool = False) -> dict[str, list[dict[str, str | int]]]:
    """Expand each combination into independently isolated jobs."""
    return {
        family: [
            row | {"index": index, "count": count}
            for row in rows
            for count in [
                1 if serial else SHARDS[family if family != "service" else row["profile"]]
            ]
            for index in range(1 if family == "e2e" else count)
        ]
        for family, rows in combinations().items()
    }


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
    ]
    if profile.startswith("coverage-"):
        command += ["--cov", "--cov-report=", "--cov-fail-under=0"]
    else:
        marker = "e2e_pip or e2e_uv" if profile.startswith("e2e-") else SERVICE_MARKERS[profile]
        command += ["-m", marker]
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
        marker = (
            None
            if profile.startswith("coverage-")
            else ("e2e_pip or e2e_uv" if profile.startswith("e2e-") else SERVICE_MARKERS[profile])
        )
        if report.get("marker") != marker:
            problems.append(f"{profile}: unexpected test selection {report.get('marker')!r}")
        if details.get("count") != count or node_ids(details.get("expected")) != expected:
            problems.append(f"{profile}: shard collection/count disagrees with its peers")
        if details.get("profile") != profile:
            problems.append(f"{profile}: mislabeled shard")
        if profile.startswith("e2e-") and details.get("warehouse") != profile.removeprefix("e2e-"):
            problems.append(f"{profile}: wrong warehouse combination")
        if not profile.startswith("coverage-") and details.get("services_required") is not True:
            problems.append(f"{profile}: service requirements were not enforced")
        selected = node_ids(report.get("collected"))
        allow_skips = profile.startswith(("coverage-", "e2e-"))
        problems.extend(
            f"{profile}: {problem}"
            for problem in check_outcomes(report, allow_skips=allow_skips, require_pass=False)
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
        problems.append("package/Airflow matrix results are missing or unexpected")
    if not problems:
        for profile in expected:
            if profile.startswith("coverage-"):
                problems.extend(coverage_report(profile, paths[profile], directory))
    return problems


def coverage_report(profile: str, reports: list[Path], directory: Path) -> list[str]:
    """Combine each complete Python/platform run before enforcing its unchanged 90% gate."""
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
    return []


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
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
    required_jobs = {"prepare", "static", "test", "service", "e2e", "package", "airflow"}
    if set(jobs) != required_jobs or any(job.get("result") != "success" for job in jobs.values()):
        print("CI jobs are missing, failed, cancelled or skipped", file=sys.stderr)
        return 1
    problems = verify(args.directory, args.wheel, args.serial)
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
