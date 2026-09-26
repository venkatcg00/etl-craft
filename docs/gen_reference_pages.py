"""Generate the configuration, task parameter and Engine DB schema references from the code.

Run by the mkdocs-gen-files plugin during `mkdocs build`; the pages exist only in the build.
The pages themselves are rendered by scripts/reference_pages.py, which the tests also check.
"""

import sys
from pathlib import Path

import mkdocs_gen_files

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import reference_pages

PAGES = {
    "reference/configuration.md": (
        reference_pages.configuration_page,
        "../src/etl_craft/config/loader.py",
    ),
    "reference/task-parameters.md": (
        reference_pages.task_parameters_page,
        "../src/etl_craft/handlers/registry.py",
    ),
    "reference/engine-db-schema.md": (
        reference_pages.schema_page,
        "../src/etl_craft/dialects/engine/postgres/schema.sql",
    ),
}

for page, (render, source) in PAGES.items():
    with mkdocs_gen_files.open(page, "w", encoding="utf-8") as handle:
        handle.write(render())
    mkdocs_gen_files.set_edit_path(page, source)
