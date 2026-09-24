"""Generate one API reference page per public module under src/etl_craft, and their navigation.

Run by the mkdocs-gen-files plugin during `mkdocs build`; the pages exist only in the build.
"""

from pathlib import Path

import mkdocs_gen_files

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

nav = mkdocs_gen_files.Nav()
for source in sorted((SRC / "etl_craft").rglob("*.py")):
    parts = source.relative_to(SRC).with_suffix("").parts
    if parts[-1] == "__init__":
        parts = parts[:-1]
        page = Path(*parts, "index.md")
    else:
        page = Path(*parts).with_suffix(".md")
    if any(part.startswith("_") for part in parts):
        continue

    nav[parts] = page.as_posix()
    with mkdocs_gen_files.open(Path("api", page), "w", encoding="utf-8") as handle:
        handle.write(f"::: {'.'.join(parts)}\n")
    mkdocs_gen_files.set_edit_path(Path("api", page), Path("..") / source.relative_to(ROOT))

with mkdocs_gen_files.open("api/SUMMARY.md", "w", encoding="utf-8") as handle:
    handle.writelines(nav.build_literate_nav())
