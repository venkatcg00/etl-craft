"""The ``etl-craft`` command line."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from etl_craft import __version__


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser."""
    parser = argparse.ArgumentParser(
        prog="etl-craft",
        description="Metadata-driven ETL orchestration engine.",
    )
    parser.add_argument("--version", action="version", version=f"etl-craft {__version__}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command line and return its exit code."""
    parser = build_parser()
    parser.parse_args(argv)
    parser.print_usage()
    return 2
