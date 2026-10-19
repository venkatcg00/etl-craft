"""The agent instructions name only paths, names and make targets that exist."""

import importlib
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = REPO_ROOT / "src" / "etl_craft"
AGENTS = REPO_ROOT / "AGENTS.md"
SKILLS = REPO_ROOT / ".agents" / "skills"
CLAUDE_SKILLS = REPO_ROOT / ".claude" / "skills"
# Codex reads at most 32 KiB of AGENTS.md text; keep room for nested files.
AGENTS_BUDGET = 24 * 1024
LAYERS = (
    "core|config|dialects|engine|warehouse|scripting|handlers|execution|services|controller|api|cli"
)
DOTTED_NAME = re.compile(rf"^(?:etl_craft\.)?(?:{LAYERS})(?:\.[A-Za-z_]\w*)+$")
FENCE = re.compile(r"^```.*?^```", re.MULTILINE | re.DOTALL)
INLINE = re.compile(r"`([^`\n]+)`")
MAKE = re.compile(r"\bmake((?: [a-z][a-z-]*)+)")


def _documents() -> list[Path]:
    return [AGENTS, *sorted(SKILLS.glob("*/SKILL.md"))]


def _inline_spans(path: Path) -> list[str]:
    return INLINE.findall(FENCE.sub("", path.read_text(encoding="utf-8")))


def _code(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    return [*INLINE.findall(text), *(block for block in FENCE.findall(text))]


def _looks_like_path(span: str) -> bool:
    """A repository path names a directory; bare names such as ``schema.sql`` recur per dialect."""
    if any(char in span for char in " <>*$(){}=|,'\""):
        return False
    if span.startswith("/") and "/" not in span[1:]:
        return False  # a slash command such as ``/roadmap-item``
    return "/" in span.rstrip("/")


def _resolves(name: str) -> bool:
    dotted = name if name.startswith("etl_craft.") else f"etl_craft.{name}"
    parts = dotted.split(".")
    for split in range(len(parts), 1, -1):
        try:
            target: object = importlib.import_module(".".join(parts[:split]))
        except ModuleNotFoundError:
            continue
        for attribute in parts[split:]:
            if not hasattr(target, attribute):
                return False
            target = getattr(target, attribute)
        return True
    return False


def test_agents_md_fits_the_codex_instruction_budget():
    size = len(AGENTS.read_bytes())
    assert size <= AGENTS_BUDGET, f"AGENTS.md is {size} bytes; keep it under {AGENTS_BUDGET}"


@pytest.mark.parametrize("document", _documents(), ids=lambda path: path.parent.name)
def test_every_path_named_exists(document):
    missing = [
        span
        for span in _inline_spans(document)
        if _looks_like_path(span)
        and not (REPO_ROOT / span).exists()
        and not (PACKAGE_ROOT / span).exists()
    ]
    assert missing == [], f"{document.relative_to(REPO_ROOT)} names missing paths: {missing}"


@pytest.mark.parametrize("document", _documents(), ids=lambda path: path.parent.name)
def test_every_python_name_named_exists(document):
    names = [span.split("(")[0] for span in _inline_spans(document)]
    missing = [name for name in names if DOTTED_NAME.match(name) and not _resolves(name)]
    assert missing == [], f"{document.relative_to(REPO_ROOT)} names missing objects: {missing}"


@pytest.mark.parametrize(
    "document",
    [*_documents(), REPO_ROOT / "CONTRIBUTING.md"],
    ids=lambda path: path.parent.name or path.name,
)
def test_every_make_target_named_exists(document):
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    targets = set(re.findall(r"^([a-z][a-z-]*):", makefile, re.MULTILINE))
    named = {
        target
        for code in _code(document)
        for match in MAKE.findall(code)
        for target in match.split()
    }
    assert named - targets == set(), f"{document.name} names unknown make targets"


def test_claude_md_imports_agents_md():
    lines = (REPO_ROOT / "CLAUDE.md").read_text(encoding="utf-8").splitlines()
    assert "@AGENTS.md" in lines


@pytest.mark.parametrize("skill", sorted(SKILLS.glob("*/SKILL.md")), ids=lambda p: p.parent.name)
def test_each_skill_has_a_name_and_description(skill):
    text = skill.read_text(encoding="utf-8")
    match = re.match(r"^---\n(.*?)\n---\n", text, re.DOTALL)
    assert match, f"{skill} has no frontmatter"
    fields = dict(line.split(": ", 1) for line in match.group(1).splitlines() if ": " in line)
    assert fields.get("name") == skill.parent.name
    assert len(fields["name"]) <= 64
    assert fields.get("description", "").strip()


def test_claude_code_sees_exactly_the_shared_skills():
    shared = {path.name for path in SKILLS.iterdir() if (path / "SKILL.md").is_file()}
    linked = {path.name for path in CLAUDE_SKILLS.iterdir()}
    assert linked == shared
    for name in linked:
        link = CLAUDE_SKILLS / name
        assert link.is_symlink(), f"{link} must link to .agents/skills/{name}"
        assert link.resolve() == (SKILLS / name).resolve()
