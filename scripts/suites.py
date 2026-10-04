"""The release suites defined in release/required-suites.toml, and where their evidence lives."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SUITES_FILE = REPO_ROOT / "release" / "required-suites.toml"
EVIDENCE_ROOT = Path("release") / "evidence"


@dataclass(frozen=True)
class Suite:
    """One required suite: its name, the pytest marker that selects it, and how it runs."""

    name: str
    marker: str
    description: str
    wheel: bool = False
    where: str = "ci"
    platform_only: dict[str, tuple[str, ...]] = field(default_factory=dict)

    def selection(self, platform: str) -> list[str]:
        """Exclude only the explicitly declared tests for other platforms."""
        all_nodes = {node for nodes in self.platform_only.values() for node in nodes}
        selected = set(self.platform_only.get(platform, ()))
        return [f"--deselect={node}" for node in sorted(all_nodes - selected)]


def load_suites(path: Path = SUITES_FILE) -> dict[str, Suite]:
    """Read the suite definitions, keyed by suite name, in file order."""
    with path.open("rb") as handle:
        raw = tomllib.load(handle)
    return {
        name: Suite(
            name=name,
            marker=str(spec["marker"]),
            description=str(spec.get("description", "")),
            wheel=bool(spec.get("wheel", False)),
            where=str(spec.get("where", "ci")),
            platform_only={
                platform: tuple(nodes) for platform, nodes in spec.get("platform_only", {}).items()
            },
        )
        for name, spec in raw["suites"].items()
    }


def project_version(root: Path = REPO_ROOT) -> str:
    """Return the version declared in the project's pyproject.toml."""
    with (root / "pyproject.toml").open("rb") as handle:
        return str(tomllib.load(handle)["project"]["version"])


def evidence_dir(version: str, root: Path = REPO_ROOT) -> Path:
    """Return the directory holding the evidence files for ``version``."""
    return root / EVIDENCE_ROOT / version
