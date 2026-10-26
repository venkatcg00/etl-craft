"""Thread-safe process measurements and Prometheus text exposition."""

import threading
from dataclasses import dataclass, field

Labels = tuple[tuple[str, str], ...]
BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0)


def escaped(value: str) -> str:
    """Escape precisely the three special characters in a label value."""
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


@dataclass
class Family:
    """One metric family with fixed help, type and label names."""

    help: str
    kind: str
    values: dict[Labels, float] = field(default_factory=dict)
    observations: dict[Labels, list[float]] = field(default_factory=dict)


class Metrics:
    """Keep bounded bucket counts, never individual observations or execution identities."""

    def __init__(self) -> None:
        """Start a process registry with no observations or leadership."""
        self.lock = threading.RLock()
        self.scrape_lock = threading.Lock()
        self.families: dict[str, Family] = {}
        self.leader = False
        self.heartbeat: float | None = None

    def define(self, name: str, help: str, kind: str) -> None:
        """Declare a family once, including before its first event."""
        with self.lock:
            self.families.setdefault(name, Family(help, kind))

    def add(self, name: str, labels: Labels, value: float = 1) -> None:
        """Add a committed event or observe a duration with cumulative buckets."""
        with self.lock:
            family = self.families[name]
            if family.kind == "histogram":
                counts = family.observations.setdefault(labels, [0.0] * (len(BUCKETS) + 2))
                for index, bound in enumerate(BUCKETS):
                    counts[index] += value <= bound
                counts[-2] += value
                counts[-1] += 1
            else:
                family.values[labels] = family.values.get(labels, 0) + value

    def render(self, gauges: dict[str, dict[Labels, float]]) -> str:
        """Render one consistent snapshot, with one HELP and TYPE per family."""
        lines: list[str] = []
        with self.lock:
            for name, family in sorted(self.families.items()):
                full = "etl_craft_" + name
                lines.extend((f"# HELP {full} {family.help}", f"# TYPE {full} {family.kind}"))
                for labels, value in sorted(gauges.get(name, family.values).items()):
                    lines.append(sample(full, labels, value))
                for labels, counts in sorted(family.observations.items()):
                    for bound, count in zip(BUCKETS, counts, strict=False):
                        lines.append(sample(full + "_bucket", (*labels, ("le", str(bound))), count))
                    lines.append(sample(full + "_bucket", (*labels, ("le", "+Inf")), counts[-1]))
                    lines.append(sample(full + "_sum", labels, counts[-2]))
                    lines.append(sample(full + "_count", labels, counts[-1]))
        return "\n".join(lines) + "\n"


def sample(name: str, labels: Labels, value: float) -> str:
    """Encode a numeric sample with deterministic label order."""
    suffix = ",".join(f'{key}="{escaped(value)}"' for key, value in labels)
    return name + ("{" + suffix + "}" if suffix else "") + f" {value:g}"
