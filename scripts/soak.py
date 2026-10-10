"""Run the demo under ``etl-craft server`` for days, killing the server at random, and check it.

Usage::

    python scripts/soak.py setup DIR --wheel WHEEL [--every MINUTES]
    python scripts/soak.py run DIR [--days 7] [--kill-minutes 20:180]
    python scripts/soak.py check DIR [--final]

``setup`` installs the wheel into ``DIR/venv``, copies ``examples/demo`` to ``DIR/demo``, prepares
its warehouse and metadata, and schedules ``CLIENT_ALPHA``, ``CLIENT_BETA`` and ``SUPPORT_DM``
every ``--every`` minutes, a third of the interval apart. ``run`` keeps ``etl-craft server``
running in ``DIR/demo``, sends it SIGKILL at random times and starts it again, appends a check to
``DIR/report.jsonl`` every half hour, and after ``--days`` stops the server and runs the final
check. Started again after an interruption, it continues until the same end.

The soak passes when nothing is lost or doubled and every task state has an explanation:

- every schedule tick due since the soak began has exactly one run;
- no run stays queued or in progress longer than ``STUCK_MINUTES``;
- no run ends ``FAILED`` except the one whose ``flaky_feed`` ran first, which fails on purpose;
- no task run has more than one successful attempt, and no attempt keeps an expired lease;
- (final) no ``APPEND_TABLE`` target holds rows twice, or rows of a task run that did not succeed;
- (final) ``etl-craft explain`` answers for every task state reached, up to three runs each.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SCHEDULED = ("CLIENT_ALPHA", "CLIENT_BETA", "SUPPORT_DM")
STUCK_MINUTES = 120
CHECK_EVERY = timedelta(minutes=30)
TICK_GRACE = timedelta(minutes=3)
"""How late the server may create a due tick's run before the tick counts as lost."""


def _ts(instant: datetime) -> str:
    """Format ``instant`` as the Engine DB stores timestamps on SQLite, for text comparison."""
    return instant.astimezone(UTC).isoformat(sep=" ", timespec="microseconds")


def _bin(soak: Path, name: str) -> str:
    return str(soak / "venv" / "bin" / name)


def _env(soak: Path) -> dict[str, str]:
    return {
        **os.environ,
        "PATH": f"{soak / 'venv' / 'bin'}{os.pathsep}{os.environ['PATH']}",
        "ETL_CRAFT_ACTOR": "soak",
    }


def _call(soak: Path, *argv: str) -> None:
    subprocess.run(argv, cwd=soak / "demo", env=_env(soak), check=True)


def setup(soak: Path, wheel: Path, every: int) -> None:
    """Install ``wheel``, copy and prepare the demo, and schedule its three pipelines."""
    if (soak / "demo").exists():
        raise SystemExit(f"{soak / 'demo'} exists; choose an empty folder")
    soak.mkdir(parents=True, exist_ok=True)
    subprocess.run(["uv", "venv", str(soak / "venv"), "--python", "3.11"], check=True)
    subprocess.run(
        ["uv", "pip", "install", "--python", _bin(soak, "python"), str(wheel)], check=True
    )
    shutil.copytree(REPO / "examples" / "demo", soak / "demo")
    _call(soak, _bin(soak, "python"), "prepare.py", "warehouse")
    _call(soak, _bin(soak, "etl-craft"), "setup")
    _call(soak, _bin(soak, "python"), "prepare.py", "metadata")
    offsets = [0, every // 3, 2 * every // 3]
    lines = [
        f"UPDATE CFG_PIPELINES SET RUN_SCHEDULE = '{offset}-59/{every} * * * *' "
        f"WHERE PIPELINE_CODE = '{code}';"
        for code, offset in zip(SCHEDULED, offsets, strict=True)
    ]
    (soak / "demo" / "migrations" / "0900_soak_schedules.sql").write_text(
        "-- The soak's schedules.\n" + "\n".join(lines) + "\n", encoding="utf-8"
    )
    _call(soak, _bin(soak, "etl-craft"), "migrate")
    _call(soak, _bin(soak, "etl-craft"), "validate")
    print(f"soak prepared in {soak}; start it with: python scripts/soak.py run {soak}")


def _state(soak: Path, days: float) -> dict[str, str | int]:
    path = soak / "soak.json"
    if path.exists():
        state: dict[str, str | int] = json.loads(path.read_text(encoding="utf-8"))
        return state
    now = datetime.now(UTC)
    state = {
        "started_at": now.isoformat(),
        "ends_at": (now + timedelta(days=days)).isoformat(),
        "kills": 0,
        "server_starts": 0,
    }
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    return state


def _save(soak: Path, state: dict[str, str | int]) -> None:
    (soak / "soak.json").write_text(json.dumps(state, indent=2), encoding="utf-8")


def _start_server(soak: Path, state: dict[str, str | int]) -> subprocess.Popen[bytes]:
    state["server_starts"] = int(state["server_starts"]) + 1
    _save(soak, state)
    logs = soak / "server-logs"
    logs.mkdir(exist_ok=True)
    with (logs / f"server-{state['server_starts']:04d}.log").open("wb") as output:
        return subprocess.Popen(
            [_bin(soak, "etl-craft"), "server"],
            cwd=soak / "demo",
            env=_env(soak),
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )


def _record(soak: Path, entry: dict[str, object]) -> None:
    with (soak / "report.jsonl").open("a", encoding="utf-8") as report:
        report.write(json.dumps(entry, default=str) + "\n")


def run(soak: Path, days: float, kill_minutes: tuple[float, float]) -> int:
    """Keep the server running until the soak ends, killing it at random; return 0 on a pass."""
    state = _state(soak, days)
    ends_at = datetime.fromisoformat(str(state["ends_at"]))
    server = _start_server(soak, state)
    next_kill = time.monotonic() + random.uniform(*kill_minutes) * 60
    next_check = datetime.now(UTC) + CHECK_EVERY
    try:
        while datetime.now(UTC) < ends_at:
            time.sleep(10)
            if server.poll() is not None:
                _record(
                    soak,
                    {"at": datetime.now(UTC), "event": "server exited", "code": server.returncode},
                )
                server = _start_server(soak, state)
            if time.monotonic() >= next_kill:
                server.kill()
                server.wait()
                state["kills"] = int(state["kills"]) + 1
                _save(soak, state)
                _record(soak, {"at": datetime.now(UTC), "event": "kill -9", "pid": server.pid})
                time.sleep(random.uniform(1, 30))
                server = _start_server(soak, state)
                next_kill = time.monotonic() + random.uniform(*kill_minutes) * 60
            if datetime.now(UTC) >= next_check:
                _record(soak, check(soak, final=False))
                next_check = datetime.now(UTC) + CHECK_EVERY
    finally:
        if server.poll() is None:
            server.send_signal(signal.SIGTERM)
            try:
                server.wait(timeout=180)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
    result = check(soak, final=True)
    _record(soak, result)
    (soak / "soak-result.json").write_text(json.dumps(result, indent=2, default=str))
    print(json.dumps(result, indent=2, default=str))
    return 0 if result["passed"] else 1


def _expected_ticks(expr: str, zone: str, start: datetime, end: datetime) -> list[datetime]:
    from etl_craft.core.cron import next_after

    ticks: list[datetime] = []
    instant = start
    while True:
        instant = next_after(expr, instant, zone)
        if instant > end:
            return ticks
        ticks.append(instant.astimezone(UTC))


def check(soak: Path, *, final: bool) -> dict[str, object]:
    """Check the Engine DB (and, at the end, the warehouse and explanations)."""
    state = json.loads((soak / "soak.json").read_text(encoding="utf-8"))
    started = datetime.fromisoformat(state["started_at"])
    now = datetime.now(UTC)
    db = sqlite3.connect(f"file:{soak / 'demo' / 'engine.db'}?mode=ro", uri=True, timeout=60)
    db.row_factory = sqlite3.Row
    problems: list[str] = []
    runs: dict[str, dict[str, int]] = {}
    flaky_run = db.execute(
        "SELECT t.PIPELINE_RUN_ID FROM AUD_TASK_RUN_LOG t "
        "JOIN CFG_TASKS c ON c.TASK_ID = t.TASK_ID "
        "WHERE c.TASK_CODE = 'flaky_feed' ORDER BY t.TASK_RUN_ID LIMIT 1"
    ).fetchone()
    first_flaky = flaky_run[0] if flaky_run else None
    for row in db.execute(
        "SELECT PIPELINE_ID AS id, PIPELINE_CODE AS code, RUN_SCHEDULE AS schedule, "
        "COALESCE(SCHEDULE_TIMEZONE, 'UTC') AS zone FROM CFG_PIPELINES "
        "WHERE RUN_SCHEDULE IS NOT NULL"
    ).fetchall():
        keys: dict[datetime, int] = {}
        for (key,) in db.execute(
            "SELECT RUN_KEY FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_ID = ? "
            "AND RUN_KEY LIKE 'schedule:%'",
            (row["id"],),
        ):
            instant = datetime.fromisoformat(key.removeprefix("schedule:")).astimezone(UTC)
            keys[instant] = keys.get(instant, 0) + 1
        expected = _expected_ticks(row["schedule"], row["zone"], started, now - TICK_GRACE)
        lost = [tick for tick in expected if tick not in keys]
        doubled = [tick for tick, count in keys.items() if count > 1]
        if lost:
            problems.append(f"{row['code']}: {len(lost)} tick(s) without a run, first {lost[0]}")
        if doubled:
            problems.append(f"{row['code']}: {len(doubled)} tick(s) with several runs")
        runs[row["code"]] = dict(
            db.execute(
                "SELECT STATUS, COUNT(*) FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_ID = ? "
                "GROUP BY STATUS",
                (row["id"],),
            ).fetchall()
        )
        runs[row["code"]]["ticks_due"] = len(expected)
    stuck = db.execute(
        "SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG WHERE STATUS IN ('QUEUED', 'IN-PROGRESS') "
        "AND START_DATE < ?",
        (_ts(now - timedelta(minutes=STUCK_MINUTES)),),
    ).fetchone()[0]
    if stuck:
        problems.append(f"{stuck} run(s) unfinished for over {STUCK_MINUTES} minutes")
    failed = db.execute(
        "SELECT r.PIPELINE_RUN_ID, p.PIPELINE_CODE FROM AUD_PIPELINES_RUN_LOG r "
        "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = r.PIPELINE_ID "
        "WHERE r.STATUS = 'FAILED' AND r.PIPELINE_RUN_ID != COALESCE(?, -1)",
        (first_flaky,),
    ).fetchall()
    if failed:
        problems.append(
            f"{len(failed)} run(s) ended FAILED: "
            + ", ".join(f"{code} {run_id}" for run_id, code in failed[:10])
        )
    doubled_attempts = db.execute(
        "SELECT COUNT(*) FROM (SELECT TASK_RUN_ID FROM AUD_TASK_ATTEMPTS WHERE STATUS = 'SUCCESS' "
        "GROUP BY TASK_RUN_ID HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    if doubled_attempts:
        problems.append(f"{doubled_attempts} task run(s) with several successful attempts")
    expired = db.execute(
        "SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS WHERE STATUS IN ('CLAIMED', 'RUNNING') "
        "AND LEASE_EXPIRES_AT < ?",
        (_ts(now - timedelta(minutes=10)),),
    ).fetchone()[0]
    if expired:
        problems.append(f"{expired} attempt(s) holding a lease that expired 10+ minutes ago")
    attempts = dict(db.execute("SELECT STATUS, COUNT(*) FROM AUD_TASK_ATTEMPTS GROUP BY STATUS"))
    result: dict[str, object] = {
        "at": now,
        "final": final,
        "kills": state["kills"],
        "server_starts": state["server_starts"],
        "runs": runs,
        "attempts": attempts,
    }
    if final:
        problems += _check_appends(soak, db)
        problems += _check_explanations(soak, db)
    db.close()
    result["problems"] = problems
    result["passed"] = not problems
    return result


def _targets(db: sqlite3.Connection, action: str) -> dict[int, str]:
    """Map each task with SQL_ACTION ``action`` to its TARGET_OBJECT."""
    rows = db.execute(
        "SELECT t.TASK_ID, MAX(CASE WHEN p.PARAMETER_NAME = 'TARGET_OBJECT' "
        "THEN p.PARAMETER_VALUE END) FROM CFG_TASKS t "
        "JOIN CFG_TASK_PARAMETERS p ON p.TASK_ID = t.TASK_ID GROUP BY t.TASK_ID "
        "HAVING MAX(CASE WHEN p.PARAMETER_NAME = 'SQL_ACTION' THEN p.PARAMETER_VALUE END) = ?",
        (action,),
    ).fetchall()
    return dict(rows)


def _check_appends(soak: Path, db: sqlite3.Connection) -> list[str]:
    """No append target holds rows twice, or rows of a task run that did not succeed.

    A target no ``DELETE_ROWS`` task touches holds exactly each task run's inserted rows; one a
    ``DELETE_ROWS`` task also writes may hold fewer, never more.
    """
    import duckdb

    pruned = set(_targets(db, "DELETE_ROWS").values())
    problems = []
    warehouse = duckdb.connect(str(soak / "demo" / "warehouse.duckdb"), read_only=True)
    try:
        for task_id, target in _targets(db, "APPEND_TABLE").items():
            stored = dict(
                warehouse.execute(
                    f"SELECT TASK_RUN_ID, COUNT(*) FROM {target} "
                    "WHERE TASK_RUN_ID IS NOT NULL GROUP BY TASK_RUN_ID"
                ).fetchall()
            )
            inserted = dict(
                db.execute(
                    "SELECT TASK_RUN_ID, INSERT_COUNT FROM AUD_TASK_RUN_LOG "
                    "WHERE TASK_ID = ? AND STATUS = 'SUCCESS'",
                    (task_id,),
                ).fetchall()
            )
            extra = [run for run, count in stored.items() if count > (inserted.get(run) or 0)]
            missing = (
                []
                if target in pruned
                else [run for run, count in inserted.items() if stored.get(run, 0) != (count or 0)]
            )
            if extra:
                problems.append(f"{target}: {len(extra)} task run(s) with extra rows")
            if missing:
                problems.append(f"{target}: {len(missing)} task run(s) with missing rows")
    finally:
        warehouse.close()
    return problems


def _check_explanations(soak: Path, db: sqlite3.Connection) -> list[str]:
    """``explain`` answers for every task state reached, up to three runs each."""
    samples: dict[tuple[str, str, str], list[int]] = {}
    for run_id, pipeline, task, status in db.execute(
        "SELECT t.PIPELINE_RUN_ID, p.PIPELINE_CODE, c.TASK_CODE, t.STATUS "
        "FROM AUD_TASK_RUN_LOG t JOIN CFG_TASKS c ON c.TASK_ID = t.TASK_ID "
        "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = c.PIPELINE_ID ORDER BY t.PIPELINE_RUN_ID"
    ):
        runs = samples.setdefault((pipeline, task, status), [])
        if len(runs) < 3:
            runs.append(run_id)
    problems = []
    for (pipeline, task, status), runs in sorted(samples.items()):
        for run_id in runs:
            done = subprocess.run(
                [
                    _bin(soak, "etl-craft"),
                    "explain",
                    "--run-id",
                    str(run_id),
                    "--pipeline_code",
                    pipeline,
                    "--task_code",
                    task,
                    "--format",
                    "json",
                ],
                cwd=soak / "demo",
                env=_env(soak),
                capture_output=True,
                text=True,
                check=False,
            )
            try:
                document = json.loads(done.stdout)
                explained = bool(document["state"]) and bool(document["next_action"])
            except (ValueError, KeyError, TypeError):
                explained = False
            if done.returncode != 0 or not explained:
                problems.append(
                    f"explain {pipeline}.{task} in run {run_id} ({status}): exit "
                    f"{done.returncode}, {done.stderr.strip()[-200:]}"
                )
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description="Soak the demo under etl-craft server.")
    verbs = parser.add_subparsers(dest="verb", required=True)
    prepare = verbs.add_parser("setup")
    prepare.add_argument("dir", type=Path)
    prepare.add_argument("--wheel", type=Path, required=True)
    prepare.add_argument("--every", type=int, default=15, help="minutes between ticks")
    soak = verbs.add_parser("run")
    soak.add_argument("dir", type=Path)
    soak.add_argument("--days", type=float, default=7)
    soak.add_argument("--kill-minutes", default="20:180", help="MIN:MAX minutes between kills")
    inspect = verbs.add_parser("check")
    inspect.add_argument("dir", type=Path)
    inspect.add_argument("--final", action="store_true")
    args = parser.parse_args()
    folder = args.dir.resolve()
    if args.verb == "setup":
        setup(folder, args.wheel.resolve(), args.every)
        return 0
    if args.verb == "run":
        low, _, high = args.kill_minutes.partition(":")
        return run(folder, args.days, (float(low), float(high)))
    result = check(folder, final=args.final)
    print(json.dumps(result, indent=2, default=str))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
