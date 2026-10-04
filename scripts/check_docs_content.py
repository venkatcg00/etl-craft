"""Check that the built Python API has useful entry points and rendered public contracts."""

from __future__ import annotations

import sys
from html.parser import HTMLParser
from pathlib import Path


class Article(HTMLParser):
    """Read the article content, excluding navigation links elsewhere on the page."""

    def __init__(self) -> None:
        """Start with no article content or links."""
        super().__init__()
        self.inside = False
        self.links: list[str] = []
        self.text: list[str] = []
        self.anchors: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Capture links only inside the rendered article."""
        if tag == "article":
            self.inside = True
        if self.inside and (anchor := dict(attrs).get("id")):
            self.anchors.add(anchor)
        if self.inside and tag == "a":
            href = dict(attrs).get("href")
            if href:
                self.links.append(href)

    def handle_endtag(self, tag: str) -> None:
        """Stop collecting at the end of the article."""
        if tag == "article":
            self.inside = False

    def handle_data(self, data: str) -> None:
        """Collect the article's visible text."""
        if self.inside:
            self.text.append(data)


def problems(site: Path) -> list[str]:
    """Return missing content and links, rather than accepting a successful but empty build."""
    failures = []
    checks = {
        "api/etl_craft/index.html": ("Python API", "ScriptTask", "ScriptResult", "Offset"),
        "api/etl_craft/scripting/index.html": ("ScriptTask", "ScriptResult", "Offset", "row_count"),
    }
    for relative, terms in checks.items():
        page = site / relative
        try:
            markup = page.read_text(encoding="utf-8")
        except OSError as error:
            failures.append(f"{page}: cannot read API reference ({error})")
            continue
        article = Article()
        article.feed(markup)
        content = " ".join(article.text)
        for term in terms:
            if term not in content:
                failures.append(f"{page}: missing Python API content {term!r}")
        if relative.endswith("scripting/index.html"):
            for name in ("ScriptTask", "ScriptResult", "Offset"):
                if f"etl_craft.scripting.{name}" not in article.anchors:
                    failures.append(f"{page}: missing rendered Python contract {name!r}")
        if relative.endswith("etl_craft/index.html"):
            for target in ("scripting/", "config/", "core/errors/"):
                if not any(link.endswith(target) for link in article.links):
                    failures.append(f"{page}: missing Python API link {target!r}")
    return failures


def main(args: list[str]) -> int:
    if len(args) != 1:
        print("usage: check_docs_content.py <built-site>", file=sys.stderr)
        return 2
    failures = problems(Path(args[0]))
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
