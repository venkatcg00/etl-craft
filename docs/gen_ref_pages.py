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
        if parts == ("etl_craft",):
            handle.write(
                "# Python API\n\n"
                "Use these references alongside the guides: signatures, types and docstrings "
                "come from the installed source. The ingestion-script contract is the entry "
                "point for writing Python tasks. Other modules describe the engine's current "
                "implementation; their interfaces may change before 1.0.0.\n\n"
                "## Start here\n\n"
                "- [Ingestion script types](scripting.md): `ScriptTask`, `ScriptResult` and "
                "`Offset`, with the data a task receives and returns.\n"
                "- [Writing ingestion scripts](../../guides/ingestion-scripts.md): examples, "
                "offsets, warehouse access and logging.\n"
                "- [Configuration](config/index.md): loading the project configuration "
                "and resolving its settings.\n"
                "- [Core errors](core/errors.md): named errors and their exit statuses.\n\n"
                "## Packages\n\n"
            )
        if source.stem == "__init__":
            children = sorted(
                child
                for child in source.parent.iterdir()
                if not child.name.startswith("_")
                and (child.suffix == ".py" or (child / "__init__.py").is_file())
            )
            for child in children:
                target = f"{child.name}/index.md" if child.is_dir() else f"{child.stem}.md"
                handle.write(f"- [{child.stem}]({target})\n")
            handle.write("\n")
        handle.write(f"::: {'.'.join(parts)}\n")
    mkdocs_gen_files.set_edit_path(Path("api", page), Path("..") / source.relative_to(ROOT))

with mkdocs_gen_files.open("api/SUMMARY.md", "w", encoding="utf-8") as handle:
    handle.writelines(nav.build_literate_nav())
