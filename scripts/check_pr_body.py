"""Check that a pull request description fills in every section of the template.

Usage: ``PR_BODY=... python scripts/check_pr_body.py [--template PATH]``

The sections are the ``## `` headings of ``.github/pull_request_template.md``. Each must be
present with text other than the template's comments; "None" is an answer, an empty section is
not. In the "Roadmap item" section every checklist point (the item's "Done when") must be
ticked.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections.abc import Sequence
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = REPO_ROOT / ".github" / "pull_request_template.md"
CHECKLIST_SECTION = "Roadmap item"
HEADING = re.compile(r"^## +(.+?)\s*$", re.MULTILINE)
COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
UNTICKED = re.compile(r"^\s*[-*] \[ \] *(.*)$", re.MULTILINE)


def sections(markdown: str) -> dict[str, str]:
    """Map each ``## `` heading to its text, without HTML comments."""
    text = COMMENT.sub("", markdown.replace("\r\n", "\n"))
    found: dict[str, str] = {}
    matches = list(HEADING.finditer(text))
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        found.setdefault(match.group(1), text[match.end() : end].strip())
    return found


def problems(body: str, template: str) -> list[str]:
    """Return what the description is missing, in template order."""
    expected = list(sections(template))
    found = sections(body)
    issues = []
    for heading in expected:
        if heading not in found:
            issues.append(f'missing section "## {heading}"')
        elif not found[heading]:
            issues.append(f'section "## {heading}" is empty; write "None" if nothing applies')
    for point in UNTICKED.findall(found.get(CHECKLIST_SECTION, "")):
        issues.append(f'"Done when" point not met: {point.strip() or "(empty item)"}')
    return issues


def main(argv: Sequence[str] | None = None) -> int:
    """Check the description in ``PR_BODY``; return 0, 1 for problems, or 2 for a usage error."""
    parser = argparse.ArgumentParser(description="Check a pull request description.")
    parser.add_argument("--template", type=Path, default=TEMPLATE)
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    template = args.template.read_text(encoding="utf-8")
    if CHECKLIST_SECTION not in sections(template):
        print(f'{args.template} has no "## {CHECKLIST_SECTION}" section', file=sys.stderr)
        return 2
    issues = problems(os.environ.get("PR_BODY", ""), template)
    if not issues:
        print("The pull request description fills in every section.")
        return 0
    print("The pull request description is incomplete:")
    print("\n".join(f"  - {issue}" for issue in issues))
    print("Edit the description; this check runs again on every edit.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
