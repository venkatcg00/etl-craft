"""The release budgets, evaluated from fixed samples or a real benchmark."""

import math
from dataclasses import dataclass

LIMITS = {
    "admission_p99_seconds": 2,
    "queue_free_p99_seconds": 5,
    "postgres_cpu_percent": 60,
    "connections": None,
    "controller_memory_bytes": 4 * 1024**3,
    "tick_p95_seconds": 1,
}


@dataclass(frozen=True)
class Result:
    """A missing measurement is a failed budget, never a zero."""

    name: str
    value: float | None
    limit: float

    @property
    def passed(self) -> bool:
        """Require a finite measurement below the bound."""
        if self.value is None or not math.isfinite(self.value):
            return False
        return self.value <= self.limit if self.name == "connections" else self.value < self.limit


def evaluate(values: dict[str, float | None], connections: int) -> list[Result]:
    """Use strict upper bounds; connection ceiling is inclusive by definition."""
    return [
        Result(name, values.get(name), connections if limit is None else limit)
        for name, limit in LIMITS.items()
    ]


def percentile(values: list[float], fraction: float) -> float | None:
    """Nearest-rank percentile of collected gauge samples, without assuming missing zeros."""
    return sorted(values)[math.ceil(len(values) * fraction) - 1] if values else None


def buckets(body: str, family: str) -> dict[float, float]:
    """Sum cumulative buckets across a family's finite label dimensions."""
    result: dict[float, float] = {}
    for name, labels, value in samples(body):
        if name == family + "_bucket":
            bound = float(labels["le"])
            result[bound] = result.get(bound, 0) + value
    return result


def quantile(counts: dict[float, float], fraction: float) -> float | None:
    """Return a conservative upper bucket; absent observations never become zero."""
    total = counts.get(math.inf, 0)
    if not total:
        return None
    return next(bound for bound in sorted(counts) if counts[bound] >= total * fraction)


def histogram(body: str, family: str, fraction: float) -> float | None:
    """Conservative percentile upper bucket across all project labels."""
    return quantile(buckets(body, family), fraction)


def samples(body: str):
    """Read the exporter grammar, including escaped label values."""
    import re

    label = re.compile(r'([a-z_]+)="((?:[^"\\]|\\.)*)"')
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        metric, value = line.rsplit(" ", 1)
        name, _, encoded = metric.partition("{")
        labels = dict(label.findall(encoded))
        yield name, labels, float(value)
