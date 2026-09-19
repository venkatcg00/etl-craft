"""Standalone, metadata-driven ETL orchestration engine."""

import sys


def main() -> int:
    """Entry point for the `etl-craft` console script — delegates to cli.main."""
    from etl_craft.cli import main as cli_main

    return cli_main(sys.argv[1:])
