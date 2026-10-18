"""Measure the CPU and memory this process may use, and size every role from them.

Discovery reads the process's CPU affinity and the CPU quota and memory limit of its cgroup and
every ancestor (cgroup v2, else v1); the tightest applies. Host totals are context, used only
when nothing tighter is found. A measurement that cannot be read is reported unavailable with
the reason, never as zero.

Sizing is opt-in through a declared ``DeploymentProfile``. Each profile reserves a share of the
allocation for whatever else runs on the host, counts the controller (the overseer and the
HTTP server, one process) once, and divides the rest into task slots by per-kind cost
estimates. Configured settings stay ceilings: a derived size never exceeds them, and a size is
never rounded up past what the allocation holds.
"""

from __future__ import annotations

import math
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import psutil

from etl_craft.core.enums import DeploymentProfile
from etl_craft.core.errors import ConfigurationError

MIB = 1024 * 1024
GIB = 1024 * MIB
CGROUP_ROOT = "/sys/fs/cgroup"


@dataclass(frozen=True)
class Measurement:
    """One measured quantity: CPUs or bytes, where it was read, and the limit or the reason."""

    value: float | None
    source: str
    detail: str


@dataclass(frozen=True)
class Allocation:
    """The CPU and memory this process may use, with the host totals for context."""

    cpus: Measurement
    memory: Measurement
    host_cpus: int | None
    host_memory: int | None
    scope: str
    observed_at: datetime


@dataclass(frozen=True)
class Host:
    """What discovery reads; tests supply their own instead of this machine's."""

    read: Callable[[str], str | None]
    affinity: Callable[[], int | None]
    host_cpus: int | None
    host_memory: int | None


def system_host() -> Host:
    """Read this machine: its files, its scheduler affinity and psutil's memory total."""
    return Host(_read_file, _affinity, os.cpu_count(), psutil.virtual_memory().total)


def _read_file(path: str) -> str | None:
    try:
        with open(path, encoding="utf-8") as handle:
            return handle.read()
    except OSError:
        return None


def _affinity() -> int | None:
    getter = getattr(os, "sched_getaffinity", None)
    return None if getter is None else len(getter(0))


def discover(host: Host | None = None, now: datetime | None = None) -> Allocation:
    """Measure the allocation; unreadable limits are unavailable, with their reasons."""
    host = host or system_host()
    paths = _cgroup_paths(host.read("/proc/self/cgroup"))
    scope = paths.get("v2") or paths.get("cpu") or paths.get("memory") or "host"
    return Allocation(
        _cpus(host, paths),
        _memory(host, paths),
        host.host_cpus,
        host.host_memory,
        scope,
        now or datetime.now(UTC),
    )


def _cgroup_paths(text: str | None) -> dict[str, str]:
    """Map each hierarchy to its cgroup path: ``v2``, or the v1 ``cpu`` and ``memory``."""
    paths: dict[str, str] = {}
    for line in (text or "").splitlines():
        parts = line.split(":", 2)
        if len(parts) != 3:
            continue
        hierarchy, controllers, path = parts
        if hierarchy == "0" and controllers == "":
            paths["v2"] = path
        for name in controllers.split(","):
            if name in {"cpu", "memory"}:
                paths[name] = path
    return paths


def _ancestors(path: str) -> list[str]:
    """``path`` and every parent up to the root: an inherited limit applies as well."""
    parts = [part for part in path.split("/") if part]
    return ["/" + "/".join(parts[:end]) if end else "" for end in range(len(parts), -1, -1)]


def _tightest(
    host: Host, files: list[str], parse: Callable[[str, str], float | None]
) -> tuple[float, str] | None:
    """Find the smallest limit any of ``files`` sets, and the file that set it."""
    found: tuple[float, str] | None = None
    for file in files:
        text = host.read(file)
        try:
            value = None if text is None else parse(text, file)
        except (ValueError, ZeroDivisionError) as error:
            raise ConfigurationError(
                f"{file}: {text!r} is not a cgroup limit; expected a number, a quota and period, "
                "or max. Check the container runtime's cgroup mount"
            ) from error
        if value is not None and (found is None or value < found[0]):
            found = (value, file)
    return found


def _cpus(host: Host, paths: dict[str, str]) -> Measurement:
    candidates: list[Measurement] = []
    affinity = host.affinity()
    if affinity is not None:
        candidates.append(Measurement(float(affinity), "CPU affinity", f"{affinity} CPUs"))
    if "v2" in paths:
        files = [f"{CGROUP_ROOT}{d}/cpu.max" for d in _ancestors(paths["v2"])]
        quota = _tightest(host, files, lambda text, _: _v2_quota(text))
    else:
        quota = _tightest(
            host,
            [f"{CGROUP_ROOT}/cpu{d}/cpu.cfs_quota_us" for d in _ancestors(paths.get("cpu", "/"))],
            lambda text, file: _v1_quota(host, text, file),
        )
    if quota is not None:
        candidates.append(Measurement(quota[0], quota[1], f"quota of {quota[0]:g} CPUs"))
    if candidates:
        return min(candidates, key=lambda m: m.value or 0.0)
    if host.host_cpus is not None:
        count = float(host.host_cpus)
        return Measurement(count, "host CPU count", f"{host.host_cpus} CPUs; no tighter limit")
    return Measurement(None, "unavailable", "no affinity, cgroup quota or host CPU count")


def _v2_quota(text: str) -> float | None:
    fields = text.split()
    if len(fields) != 2 or fields[0] == "max":
        return None
    return int(fields[0]) / int(fields[1])


def _v1_quota(host: Host, text: str, file: str) -> float | None:
    quota = int(text.strip())
    period = host.read(file.replace("cpu.cfs_quota_us", "cpu.cfs_period_us"))
    if quota <= 0 or period is None:
        return None
    return quota / int(period.strip())


def _memory(host: Host, paths: dict[str, str]) -> Measurement:
    if "v2" in paths:
        files = [f"{CGROUP_ROOT}{d}/memory.max" for d in _ancestors(paths["v2"])]
    else:
        root = f"{CGROUP_ROOT}/memory"
        files = [f"{root}{d}/memory.limit_in_bytes" for d in _ancestors(paths.get("memory", "/"))]
    limit = _tightest(host, files, lambda text, _: _bytes(text, host.host_memory))
    if limit is not None:
        return Measurement(limit[0], limit[1], f"limit of {limit[0] / MIB:.0f} MB")
    if host.host_memory is not None:
        total = float(host.host_memory)
        return Measurement(total, "host memory", f"{total / MIB:.0f} MB; no tighter limit")
    return Measurement(None, "unavailable", "no cgroup memory limit or host memory total")


def _bytes(text: str, host_memory: int | None) -> float | None:
    """Parse a memory limit; ``max``, or a v1 value no smaller than the host, sets none."""
    text = text.strip()
    if text == "max":
        return None
    value = int(text)
    return None if host_memory is not None and value >= host_memory else float(value)


@dataclass(frozen=True)
class Policy:
    """A profile's reserve for the rest of the host and its server thread ceiling."""

    cpu_share: float
    min_cpu_reserve: float
    memory_share: float
    min_memory_reserve: int
    max_server_threads: int
    runs_tasks: bool = True


POLICIES = {
    DeploymentProfile.LAPTOP: Policy(0.5, 1.0, 0.5, 2 * GIB, 4),
    DeploymentProfile.SERVER: Policy(0.1, 0.5, 0.1, 1 * GIB, 40),
    DeploymentProfile.SERVER_WORKERS: Policy(0.1, 0.5, 0.1, 1 * GIB, 40, runs_tasks=False),
}
CONTROLLER_CPUS = 0.5
CONTROLLER_MEMORY = 256 * MIB
TASK_COST = {"warehouse": (0.25, 128 * MIB), "ingestion": (1.0, 512 * MIB)}
"""Per task process: CPUs and memory. Warehouse tasks mostly wait on the warehouse."""
SERVER_THREADS_PER_CPU = 4
SUPERVISION_CONNECTIONS = 2
DEFAULT_POOL_SIZE = 5
POOL_OVERFLOW = 10
"""SQLAlchemy's queue pool defaults; a sized role changes only the pool size."""
TASKS_PER_SUPERVISION_CONNECTION = 8


@dataclass(frozen=True)
class Ceilings:
    """Settings the team wrote explicitly; ``None`` where it left the default."""

    max_parallel_tasks: int | None = None
    ingestion_slots: int | None = None
    warehouse_slots: int | None = None


@dataclass(frozen=True)
class Size:
    """One derived size and the binding constraint that set it."""

    value: int
    reason: str


@dataclass(frozen=True)
class Sizing:
    """Every role's size for one profile and allocation, with the fallbacks applied."""

    profile: DeploymentProfile
    reserve_cpus: float | None
    reserve_memory: float | None
    task_slots: Size
    ingestion_slots: Size
    warehouse_slots: Size
    server_threads: Size
    engine_pool: Size
    fallbacks: tuple[str, ...]


NO_CEILINGS = Ceilings()


def size(
    allocation: Allocation, profile: DeploymentProfile, ceilings: Ceilings = NO_CEILINGS
) -> Sizing:
    """Size task slots, server threads and the Engine DB pool for ``profile``."""
    policy = POLICIES[profile]
    fallbacks: list[str] = []
    cpus = allocation.cpus.value
    if cpus is None:
        cpus = 1.0
        fallbacks.append("CPU allocation unavailable: sized as one CPU")
    memory = allocation.memory.value
    if memory is None:
        fallbacks.append("no usable memory bound is known: task slots are not limited by memory")
    reserve_cpus = max(policy.min_cpu_reserve, cpus * policy.cpu_share)
    reserve_memory = (
        None if memory is None else max(policy.min_memory_reserve, memory * policy.memory_share)
    )
    free_cpus = cpus - reserve_cpus - CONTROLLER_CPUS
    free_memory = (
        None
        if memory is None or reserve_memory is None
        else (memory - reserve_memory - CONTROLLER_MEMORY)
    )
    if policy.runs_tasks:
        total, ingestion, warehouse = _slots(free_cpus, free_memory, ceilings)
    else:
        none = Size(0, f"{profile}: the controller runs no tasks; workers do")
        total, ingestion, warehouse = none, none, none
    threads = (
        Size(1, "CPU allocation unavailable: at most one server thread")
        if allocation.cpus.value is None
        else Size(
            max(1, min(policy.max_server_threads, math.floor(cpus * SERVER_THREADS_PER_CPU))),
            f"{SERVER_THREADS_PER_CPU} per CPU, at most {policy.max_server_threads} for {profile}",
        )
    )
    pool = SUPERVISION_CONNECTIONS + math.ceil(total.value / TASKS_PER_SUPERVISION_CONNECTION)
    return Sizing(
        profile,
        reserve_cpus,
        reserve_memory,
        total,
        ingestion,
        warehouse,
        threads,
        Size(
            pool + threads.value,
            f"{SUPERVISION_CONNECTIONS} + 1 per {TASKS_PER_SUPERVISION_CONNECTION} task slots "
            f"+ 1 per server thread",
        ),
        tuple(fallbacks),
    )


def _slots(
    free_cpus: float, free_memory: float | None, ceilings: Ceilings
) -> tuple[Size, Size, Size]:
    """Slots per kind from what is left after the reserve and the controller."""
    fits = {}
    for kind, (cpu_cost, memory_cost) in TASK_COST.items():
        by_cpu = max(0, math.floor(free_cpus / cpu_cost))
        if free_memory is not None and math.floor(free_memory / memory_cost) < by_cpu:
            fits[kind] = Size(
                max(0, math.floor(free_memory / memory_cost)),
                f"memory: {max(free_memory, 0) / MIB:.0f} MB free at {memory_cost // MIB} MB each",
            )
        else:
            fits[kind] = Size(by_cpu, f"CPU: {max(free_cpus, 0):g} free at {cpu_cost:g} each")
    total = _ceiling(fits["warehouse"], ceilings.max_parallel_tasks, "Max_parallel_tasks")
    ingestion = _ceiling(fits["ingestion"], ceilings.ingestion_slots, "Local_ingestion_slots")
    warehouse = _ceiling(total, ceilings.warehouse_slots, "Local_warehouse_slots")
    if ingestion.value > total.value:
        ingestion = Size(total.value, "no more than the task slots")
    return total, ingestion, warehouse


def _ceiling(derived: Size, ceiling: int | None, setting: str) -> Size:
    if ceiling is not None and ceiling < derived.value:
        return Size(ceiling, f"Orchestration.{setting} ceiling")
    return derived


def describe(measurement: Measurement, unit: str) -> str:
    """One line for a log or a report: the value, or why it is unavailable."""
    if measurement.value is None:
        return f"unavailable ({measurement.detail})"
    value = measurement.value if unit == "CPUs" else measurement.value / MIB
    shown = f"{value:g} CPUs" if unit == "CPUs" else f"{value:.0f} MB"
    return f"{shown} from {measurement.source} ({measurement.detail})"
