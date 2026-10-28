"""Partition CI collections and attach complete collection identities to existing evidence."""

from __future__ import annotations

import json
import math
import os
import platform
from pathlib import Path

import psutil
import pytest

TIMINGS = Path(__file__).resolve().parents[2] / "scripts" / "ci-durations.json"


def partitions(nodes: list[str], count: int, timings: dict[str, float]) -> list[list[str]]:
    """Balance measured durations; new tests still run with a one-second estimate."""
    if count < 1 or len(nodes) != len(set(nodes)):
        raise pytest.UsageError("CI collection needs unique test IDs and a positive shard count")
    groups: list[list[str]] = [[] for _ in range(count)]
    loads = [0.0] * count

    def weight(node: str) -> float:
        value = timings.get(node, 1.0)
        if not math.isfinite(value) or value < 0:
            raise pytest.UsageError(f"CI duration for {node!r} must be finite and nonnegative")
        return max(value, 0.01)

    for node in sorted(nodes, key=lambda node: (-weight(node), node)):
        index = min(range(count), key=lambda i: (loads[i], len(groups[i]), i))
        groups[index].append(node)
        loads[index] += weight(node)
    return [sorted(group) for group in groups]


def pytest_addoption(parser: pytest.Parser) -> None:
    """Keep partition selection explicit so nested pytest processes are not partitioned."""
    parser.getgroup("ci").addoption("--ci-shard", metavar="INDEX/COUNT")


def pytest_configure(config: pytest.Config) -> None:
    """Enable sharding only for an explicitly configured CI invocation."""
    selection = config.getoption("--ci-shard")
    if selection is not None:
        config.pluginmanager.register(Shard(config, selection), "ci-shard")


class Shard:
    """Keep the unpartitioned collection alongside one shard's outcome evidence."""

    def __init__(self, config: pytest.Config, selection: str) -> None:
        try:
            self.index, self.count = map(int, selection.split("/"))
            self.profile = os.environ["ETL_CRAFT_CI_PROFILE"]
        except (ValueError, KeyError) as error:
            raise pytest.UsageError("CI sharding needs an index, count and profile") from error
        if not 0 <= self.index < self.count or not self.profile:
            raise pytest.UsageError("CI shard index must be within its positive shard count")
        evidence = config.getoption("--evidence")
        if not evidence:
            raise pytest.UsageError("CI sharding requires --evidence=PATH for completeness checks")
        self.config = config
        self.path = Path(evidence)
        self.expected: list[str] = []

    @pytest.hookimpl(trylast=True)
    def pytest_collection_modifyitems(
        self, config: pytest.Config, items: list[pytest.Item]
    ) -> None:
        """Apply the partition after ordinary marker and expression selection."""
        self.expected = sorted(item.nodeid for item in items)
        family = self.profile.split("-", 1)[0]
        if family == "coverage":
            family += "-macos" if self.profile.endswith("macos-latest") else "-linux"
        models = json.loads(TIMINGS.read_text(encoding="utf-8"))
        model = models["warehouse"] if family == "warehouse" else models[family]
        timings = {
            f"{module}::{node}": duration
            for module, cases in model.items()
            for node, duration in cases.items()
        }
        selected = set(partitions(self.expected, self.count, timings)[self.index])
        kept = [item for item in items if item.nodeid in selected]
        removed = [item for item in items if item.nodeid not in selected]
        items[:] = kept
        config.hook.pytest_deselected(items=removed)

    @pytest.hookimpl(optionalhook=True)
    def pytest_testnodedown(self, node, error) -> None:
        """Take the complete pre-partition collection from each worker."""
        expected = node.workeroutput.get("ci_expected")
        if not error and isinstance(expected, list):
            if self.expected and self.expected != expected:
                raise pytest.UsageError("CI workers disagree on the complete test collection")
            self.expected = expected

    @pytest.hookimpl(trylast=True)
    def pytest_sessionfinish(self) -> None:
        """Extend the existing recorder after it has persisted the shard outcomes."""
        if hasattr(self.config, "workerinput"):
            self.config.workeroutput["ci_expected"] = self.expected
            return
        evidence = json.loads(self.path.read_text(encoding="utf-8"))
        evidence["ci"] = {
            "profile": self.profile,
            "index": self.index,
            "count": self.count,
            "expected": self.expected,
            "warehouse": os.environ.get("ETL_CRAFT_E2E_WAREHOUSE"),
            "cache_mode": os.environ.get("ETL_CRAFT_CI_CACHE_MODE"),
            "cache_hit": os.environ.get("ETL_CRAFT_CI_CACHE_HIT"),
            "services_required": os.environ.get("ETL_CRAFT_REQUIRE_SERVICES") == "1",
            "runner": {
                "platform": platform.platform(),
                "cpu_count": os.cpu_count(),
                "memory_bytes": psutil.virtual_memory().total,
            },
        }
        self.path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
