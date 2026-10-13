"""Hold the code in src/etl_craft to its quality baseline.

Usage:
  python scripts/check_quality.py              compare the code with scripts/quality-baseline.json
  python scripts/check_quality.py --update     rewrite the baseline from the code
  python scripts/check_quality.py --compare-to REF [--allow-growth]
                                               refuse a baseline that grew against REF's

Every metric gives each file a score:

- size and complexity metrics sum how far the file's functions (or the module) are over the
  limit, so a new offender or a worse one raises the score and splitting a function lowers it;
- the other metrics count how often the file does something the project avoids.

The check fails when a file's score rises above the baseline, or a file without a baseline entry
gets a score, and also when the baseline allows more than the code has, so every improvement is
kept. ``--compare-to`` is the pull-request check: the committed baseline may only shrink, unless
the owner allows growth with the ``quality-exception`` label.
"""

from __future__ import annotations

import argparse
import ast
import io
import json
import re
import subprocess
import sys
import tokenize
import tomllib
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BASELINE = Path("scripts/quality-baseline.json")
PACKAGE = Path("src/etl_craft")
FUNCTION_LINES = 80
MODULE_LINES = 800

RUFF_RULES = {
    "C901": "complexity",
    "PLR0913": "arguments",
    "PLR0912": "branches",
    "PLR0915": "statements",
}
METRICS = {
    "complexity": "functions over the complexity limit (ruff C901): split them by responsibility",
    "arguments": "functions with too many parameters (ruff PLR0913): group them in a dataclass",
    "branches": "functions with too many branches (ruff PLR0912): split them",
    "statements": "functions with too many statements (ruff PLR0915): split them",
    "function-lines": f"functions over {FUNCTION_LINES} lines: split them by responsibility",
    "module-lines": f"modules over {MODULE_LINES} lines: split the module by concern",
    "broad-except": "broad except clauses that do not raise: catch the specific error",
    "builtin-raise": "raises of built-in exceptions: raise a core.errors class",
    "enum-literal": "strings equal to a core.enums value: use the enum member",
    "inline-sql": "SQL text outside the dialects layer: add a catalog query and use "
    "engine.queries.statement",
    "dialect-branch": "dialect.name checks outside the dialects layer: add a dialect method",
    "private-import": "imports of another module's private names: share the helper properly",
    "type-ignore": "type: ignore comments: fix the type",
    "noqa": "noqa comments: fix the finding",
    "no-cover": "pragma: no cover comments: test the code",
    "cast": "typing.cast calls: narrow the type instead",
    "any-signature": "bare Any in function signatures: use a precise type",
}
# Enums whose values are status-like words that should never appear as bare strings.
ENUMS = (
    "DependencyType",
    "EmailFlavour",
    "Handler",
    "InterventionAction",
    "RefreshType",
    "RunStatus",
    "SlaStatus",
    "SqlAction",
)
BUILTIN_ERRORS = frozenset(
    {
        "ArithmeticError",
        "AssertionError",
        "AttributeError",
        "Exception",
        "FileExistsError",
        "FileNotFoundError",
        "IOError",
        "IndexError",
        "KeyError",
        "LookupError",
        "OSError",
        "PermissionError",
        "RuntimeError",
        "TypeError",
        "ValueError",
        "ZeroDivisionError",
    }
)
# Packages that run Engine DB SQL; its text belongs in the dialects layer's query catalog.
ENGINE_SQL_PACKAGES = (
    "api",
    "cli",
    "config",
    "core",
    "engine",
    "execution",
    "overseer",
    "services",
)
RUFF_VALUE = re.compile(r"\((\d+) > (\d+)\)")
SQL_START = re.compile(
    r"\s*(SELECT|INSERT|UPDATE|DELETE|WITH|CREATE|ALTER|DROP|MERGE|TRUNCATE|PRAGMA|SET|BEGIN|"
    r"LOCK|COMMENT|GRANT|REVOKE|LISTEN|NOTIFY|VACUUM|ANALYZE|EXPLAIN|CALL|DO|SHOW|COPY)\b",
    re.IGNORECASE,
)
DEPENDENCY_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*")


@dataclass
class Score:
    """One file's score for one metric, with the findings that make it up."""

    value: int = 0
    details: list[str] = field(default_factory=list)

    def add(self, amount: int, detail: str) -> None:
        """Add ``amount`` to the score for the finding ``detail``."""
        self.value += amount
        self.details.append(detail)


Measurement = dict[str, dict[str, Score]]


@dataclass
class Baseline:
    """The scores the code may have, and its core dependencies."""

    metrics: dict[str, dict[str, int]]
    dependencies: list[str]

    @classmethod
    def parse(cls, text: str) -> Baseline:
        """Read a baseline document."""
        document = json.loads(text)
        if not isinstance(document, dict):
            raise SystemExit("quality baseline: expected a JSON object")
        metrics = {
            str(metric): {str(path): int(value) for path, value in scores.items()}
            for metric, scores in document.get("metrics", {}).items()
        }
        dependencies = [str(name) for name in document.get("dependencies", [])]
        return cls(metrics, dependencies)

    def render(self) -> str:
        """Write the baseline as sorted, diff-friendly JSON."""
        document = {
            "dependencies": sorted(self.dependencies),
            "metrics": {
                metric: dict(sorted(scores.items()))
                for metric, scores in sorted(self.metrics.items())
                if scores
            },
        }
        return json.dumps(document, indent=2) + "\n"


def _functions(tree: ast.AST) -> Iterator[tuple[str, ast.FunctionDef | ast.AsyncFunctionDef]]:
    """Yield every function and method with its dotted name inside the module."""

    def visit(
        node: ast.AST, prefix: str
    ) -> Iterator[tuple[str, ast.FunctionDef | ast.AsyncFunctionDef]]:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
                name = f"{prefix}{child.name}"
                yield name, child
                yield from visit(child, f"{name}.")
            elif isinstance(child, ast.ClassDef):
                yield from visit(child, f"{prefix}{child.name}.")
            else:
                yield from visit(child, prefix)

    yield from visit(tree, "")


def _enclosing(tree: ast.AST) -> dict[int, str]:
    """Map each line inside a function to the innermost function's dotted name."""
    names: dict[int, str] = {}
    for name, node in _functions(tree):
        for line in range(node.lineno, (node.end_lineno or node.lineno) + 1):
            names[line] = name
    return names


def _docstrings(tree: ast.AST) -> set[int]:
    """Return the ids of the constant nodes that are docstrings."""
    found: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            first = node.body[0] if node.body else None
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                found.add(id(first.value))
    return found


def _raises(handler: ast.ExceptHandler) -> bool:
    """Whether the handler's own body raises (nested functions do not count)."""
    pending: list[ast.AST] = list(handler.body)
    while pending:
        node = pending.pop()
        if isinstance(node, ast.Raise):
            return True
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda | ast.ClassDef):
            pending.extend(ast.iter_child_nodes(node))
    return False


def _is_broad(node: ast.expr | None) -> bool:
    if node is None:
        return True
    if isinstance(node, ast.Tuple):
        return any(_is_broad(element) for element in node.elts)
    return isinstance(node, ast.Name) and node.id in {"Exception", "BaseException"}


def _raised_builtin(node: ast.Raise) -> str | None:
    target = node.exc.func if isinstance(node.exc, ast.Call) else node.exc
    if isinstance(target, ast.Name) and target.id in BUILTIN_ERRORS:
        return target.id
    return None


def _called(node: ast.Call) -> str:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""


def _runs_sql_text(node: ast.Call) -> bool:
    """Whether the call hands literal SQL to SQLAlchemy (``text`` or ``exec_driver_sql``)."""
    func = node.func
    sqlalchemy_text = (isinstance(func, ast.Name) and func.id == "text") or (
        isinstance(func, ast.Attribute)
        and func.attr == "text"
        and isinstance(func.value, ast.Name)
        and func.value.id in {"sqlalchemy", "sa"}
    )
    driver_sql = isinstance(func, ast.Attribute) and func.attr == "exec_driver_sql"
    if not (sqlalchemy_text or driver_sql) or not node.args:
        return False
    first = node.args[0]
    if isinstance(first, ast.JoinedStr) and first.values:
        first = first.values[0]
    return (
        isinstance(first, ast.Constant)
        and isinstance(first.value, str)
        and SQL_START.match(first.value) is not None
    )


def _is_any(node: ast.expr | None) -> bool:
    if isinstance(node, ast.Name):
        return node.id == "Any"
    return isinstance(node, ast.Attribute) and node.attr == "Any"


class _FileScan:
    """Measure one module for every AST and comment metric."""

    def __init__(self, path: str, source: str, enum_values: frozenset[str]) -> None:
        self.path = path
        self.source = source
        self.tree = ast.parse(source)
        self.names = _enclosing(self.tree)
        self.enum_values = enum_values
        self.package = path.removeprefix(f"{PACKAGE.as_posix()}/").split("/")[0]
        self.scores: dict[str, Score] = {}

    def where(self, node: ast.AST) -> str:
        line = getattr(node, "lineno", 0)
        return f"{self.names.get(line, '<module>')}:{line}"

    def add(self, metric: str, amount: int, detail: str) -> None:
        self.scores.setdefault(metric, Score()).add(amount, detail)

    def run(self) -> dict[str, Score]:
        lines = len(self.source.splitlines())
        if lines > MODULE_LINES:
            self.add("module-lines", lines - MODULE_LINES, f"{lines} lines (limit {MODULE_LINES})")
        for name, function in _functions(self.tree):
            length = (function.end_lineno or function.lineno) - function.lineno + 1
            if length > FUNCTION_LINES:
                detail = f"{name}:{function.lineno} {length} lines (limit {FUNCTION_LINES})"
                self.add("function-lines", length - FUNCTION_LINES, detail)
            arguments = [*function.args.posonlyargs, *function.args.args, *function.args.kwonlyargs]
            annotations = [argument.annotation for argument in arguments]
            if any(_is_any(annotation) for annotation in [*annotations, function.returns]):
                self.add("any-signature", 1, f"{name}:{function.lineno}")
        self._nodes()
        self._comments()
        return self.scores

    def _nodes(self) -> None:
        docstrings = _docstrings(self.tree)
        formatted = {
            id(value)
            for node in ast.walk(self.tree)
            if isinstance(node, ast.JoinedStr)
            for value in node.values
        }
        for node in ast.walk(self.tree):
            if isinstance(node, ast.ExceptHandler) and _is_broad(node.type) and not _raises(node):
                self.add("broad-except", 1, self.where(node))
            elif isinstance(node, ast.Raise) and (error := _raised_builtin(node)):
                self.add("builtin-raise", 1, f"{self.where(node)} {error}")
            elif isinstance(node, ast.Call):
                self._call(node)
            elif (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and node.value in self.enum_values
                and id(node) not in docstrings
                and id(node) not in formatted
                and self.path != f"{PACKAGE.as_posix()}/core/enums.py"
            ):
                self.add("enum-literal", 1, f"{self.where(node)} {node.value!r}")
            elif (
                isinstance(node, ast.Attribute)
                and node.attr == "name"
                and isinstance(node.value, ast.Attribute)
                and node.value.attr == "dialect"
                and self.package != "dialects"
            ):
                self.add("dialect-branch", 1, self.where(node))
            elif isinstance(node, ast.ImportFrom) and (node.module or "").startswith("etl_craft"):
                private = [
                    a.name
                    for a in node.names
                    if a.name.startswith("_") and not a.name.startswith("__")
                ]
                if private:
                    self.add(
                        "private-import", len(private), f"{self.where(node)} {', '.join(private)}"
                    )

    def _call(self, node: ast.Call) -> None:
        if _called(node) == "cast":
            self.add("cast", 1, self.where(node))
        elif self.package in ENGINE_SQL_PACKAGES and _runs_sql_text(node):
            self.add("inline-sql", 1, self.where(node))

    def _comments(self) -> None:
        tokens = tokenize.generate_tokens(io.StringIO(self.source).readline)
        for token in tokens:
            if token.type != tokenize.COMMENT:
                continue
            line = token.start[0]
            where = f"{self.names.get(line, '<module>')}:{line}"
            text = token.string
            if re.search(r"type:\s*ignore", text):
                self.add("type-ignore", 1, where)
            if re.search(r"\bnoqa\b", text, re.IGNORECASE):
                self.add("noqa", 1, where)
            if "pragma: no cover" in text:
                self.add("no-cover", 1, where)


def _enum_values() -> frozenset[str]:
    from etl_craft.core import enums

    return frozenset(member.value for name in ENUMS for member in getattr(enums, name))


def _ruff(root: Path, files: Sequence[Path]) -> list[dict[str, object]]:
    command = [
        sys.executable,
        "-m",
        "ruff",
        "check",
        "--no-cache",
        "--exit-zero",
        "--output-format",
        "json",
        "--select",
        ",".join(RUFF_RULES),
        *(str(path.relative_to(root)) for path in files),
    ]
    result = subprocess.run(command, cwd=root, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise SystemExit(f"ruff failed:\n{result.stderr}")
    findings = json.loads(result.stdout or "[]")
    if not isinstance(findings, list):
        raise SystemExit("ruff returned unexpected output")
    return findings


def dependencies(root: Path) -> list[str]:
    """Return the normalized names of the package's core dependencies."""
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    names = set()
    for spec in project.get("project", {}).get("dependencies", []):
        match = DEPENDENCY_NAME.match(spec)
        if match:
            names.add(match.group(0).lower().replace("_", "-"))
    return sorted(names)


def measure(root: Path) -> Measurement:
    """Score every module under src/etl_craft for every metric."""
    files = sorted((root / PACKAGE).rglob("*.py"))
    enum_values = _enum_values()
    measurement: Measurement = {metric: {} for metric in METRICS}
    trees: dict[str, dict[int, str]] = {}
    for file in files:
        path = file.relative_to(root).as_posix()
        scan = _FileScan(path, file.read_text(encoding="utf-8"), enum_values)
        for metric, score in scan.run().items():
            measurement[metric][path] = score
        trees[path] = scan.names
    for finding in _ruff(root, files):
        code = str(finding["code"])
        match = RUFF_VALUE.search(str(finding["message"]))
        location = finding["location"]
        if code not in RUFF_RULES or match is None or not isinstance(location, dict):
            continue
        path = Path(str(finding["filename"])).resolve().relative_to(root.resolve()).as_posix()
        line = int(location["row"])
        value, limit = int(match.group(1)), int(match.group(2))
        detail = f"{trees.get(path, {}).get(line, '<module>')}:{line} {value} (limit {limit})"
        measurement[RUFF_RULES[code]].setdefault(path, Score()).add(value - limit, detail)
    return measurement


def baseline_of(measurement: Mapping[str, Mapping[str, Score]], deps: list[str]) -> Baseline:
    """Return the baseline that matches ``measurement`` exactly."""
    metrics = {
        metric: {path: score.value for path, score in scores.items() if score.value}
        for metric, scores in measurement.items()
    }
    return Baseline(metrics, deps)


def compare(current: Baseline, allowed: Baseline) -> tuple[list[str], list[str]]:
    """Return the scores above the baseline and the baseline entries the code no longer needs."""
    worse: list[str] = []
    stale: list[str] = []
    for metric in sorted(set(current.metrics) | set(allowed.metrics)):
        now, limit = current.metrics.get(metric, {}), allowed.metrics.get(metric, {})
        for path in sorted(set(now) | set(limit)):
            value, allowance = now.get(path, 0), limit.get(path, 0)
            if value > allowance:
                worse.append(f"{metric}: {path} scores {value}, baseline {allowance}")
            elif value < allowance:
                stale.append(f"{metric}: {path} scores {value}, baseline {allowance}")
    for name in sorted(set(current.dependencies) - set(allowed.dependencies)):
        worse.append(f"dependencies: new core dependency {name}")
    for name in sorted(set(allowed.dependencies) - set(current.dependencies)):
        stale.append(f"dependencies: {name} is no longer a core dependency")
    return worse, stale


def _explain(worse: list[str], measurement: Measurement) -> Iterator[str]:
    for line in worse:
        yield f"  {line}"
        metric, _, rest = line.partition(": ")
        path = rest.split(" ", 1)[0]
        if metric in METRICS:
            yield f"      {METRICS[metric]}"
            for detail in measurement.get(metric, {}).get(path, Score()).details:
                yield f"      - {detail}"


def check(root: Path, baseline_path: Path) -> int:
    """Compare the code with the committed baseline; return the exit status."""
    measurement = measure(root)
    current = baseline_of(measurement, dependencies(root))
    if not baseline_path.is_file():
        print(f"no quality baseline at {baseline_path}; run `make quality-baseline`")
        return 1
    worse, stale = compare(current, Baseline.parse(baseline_path.read_text(encoding="utf-8")))
    if worse:
        print("quality: the change makes these scores worse than the baseline:")
        print("\n".join(_explain(worse, measurement)))
        print(
            "Fix the code. If the change cannot fit, stop and ask the owner: only the owner's "
            "quality-exception label lets a pull request grow scripts/quality-baseline.json."
        )
        return 1
    if stale:
        print("quality: the code is better than the baseline; keep the improvement:")
        print("\n".join(f"  {line}" for line in stale))
        print("Run `make quality-baseline` and commit scripts/quality-baseline.json.")
        return 1
    print("quality: every score is within the baseline")
    return 0


def compare_to(root: Path, baseline_path: Path, ref: str, *, allow_growth: bool) -> int:
    """Refuse a committed baseline that grew against the one at ``ref``."""
    shown = subprocess.run(
        ["git", "show", f"{ref}:{baseline_path.relative_to(root).as_posix()}"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if shown.returncode != 0:
        print(f"quality: {ref} has no baseline to compare with")
        return 0
    grown, _ = compare(
        Baseline.parse(baseline_path.read_text(encoding="utf-8")), Baseline.parse(shown.stdout)
    )
    if not grown:
        print(f"quality: the baseline does not grow against {ref}")
        return 0
    print(f"quality: the baseline grows against {ref}:")
    print("\n".join(f"  {line}" for line in grown))
    if allow_growth:
        print("Allowed: the pull request carries the owner's quality-exception label.")
        return 0
    print("Only the owner's quality-exception label allows this; fix the code instead.")
    return 1


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description="Hold src/etl_craft to its quality baseline.")
    parser.add_argument("--update", action="store_true", help="rewrite the baseline from the code")
    parser.add_argument("--compare-to", metavar="REF", help="refuse growth against REF's baseline")
    parser.add_argument("--allow-growth", action="store_true", help="report growth, do not fail")
    parser.add_argument("--root", type=Path, default=REPO_ROOT, help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the requested check and return its exit status."""
    args = parse_args(sys.argv[1:] if argv is None else argv)
    root: Path = args.root.resolve()
    baseline_path = root / BASELINE
    if args.compare_to:
        return compare_to(root, baseline_path, args.compare_to, allow_growth=args.allow_growth)
    if args.update:
        baseline = baseline_of(measure(root), dependencies(root))
        baseline_path.write_text(baseline.render(), encoding="utf-8")
        print(f"quality: wrote {baseline_path.relative_to(root)}")
        return 0
    return check(root, baseline_path)


if __name__ == "__main__":
    raise SystemExit(main())
