"""Generate reference/cli.md from the command line's own parser, so it matches the release.

Run by the mkdocs-gen-files plugin during `mkdocs build`; the page exists only in the build.
"""

import argparse
import os

import mkdocs_gen_files

from etl_craft.cli import build_parser

# argparse wraps help text to the terminal width; a fixed width keeps the page stable.
os.environ["COLUMNS"] = "100"
parser = build_parser()
sections = [
    "# Command line\n",
    "Every command accepts the global options, before or after the command name. Results are "
    "written to standard output; errors (`error: ...`) and log records go to standard error. "
    "The [exit codes](exit-codes.md) page lists what each exit status means.\n",
    f"```text\n{parser.format_help()}```\n",
]
for action in parser._actions:
    if isinstance(action, argparse._SubParsersAction):
        for name, subparser in action.choices.items():
            sections.append(f"## `{name}`\n")
            sections.append(f"```text\n{subparser.format_help()}```\n")

with mkdocs_gen_files.open("reference/cli.md", "w", encoding="utf-8") as handle:
    handle.write("\n".join(sections))
mkdocs_gen_files.set_edit_path("reference/cli.md", "../src/etl_craft/cli/__init__.py")
