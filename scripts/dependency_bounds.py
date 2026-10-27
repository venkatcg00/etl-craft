"""Emit direct dependency lower bounds for the installed-package gate."""

import re
import sys
import tomllib
from pathlib import Path


def lower_bounds(project: dict[str, object]) -> list[str]:
    dependencies = project["dependencies"]
    extras = project["optional-dependencies"]
    assert isinstance(dependencies, list) and isinstance(extras, dict)
    requirements = [*dependencies, *(item for group in extras.values() for item in group)]
    for requirement in requirements:
        if requirement.startswith("etl-craft["):
            continue
        match = re.fullmatch(r"([a-zA-Z0-9_-]+)(?:\[[^]]+\])?>=([0-9.]+),<[^,]+", requirement)
        if match is None:
            raise SystemExit(
                f"dependency {requirement!r}: expected explicit lower and upper bounds"
            )
    return sorted(
        {requirement for requirement in requirements if not requirement.startswith("etl-craft[")}
    )


if __name__ == "__main__":
    project = tomllib.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))["project"]
    print("\n".join(lower_bounds(project)))
