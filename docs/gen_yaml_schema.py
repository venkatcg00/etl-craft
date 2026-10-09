"""Publish the schema used to validate generated DAG exports."""

from importlib.resources import files

import mkdocs_gen_files

with mkdocs_gen_files.open("reference/dag-yaml-v1.json", "w", encoding="utf-8") as handle:
    handle.write(files("etl_craft").joinpath("schemas/dag-yaml-v1.json").read_text())
