"""``etl-craft config``: load the project's config files into the Engine DB, or write them."""

from __future__ import annotations

import argparse
from pathlib import Path

from etl_craft.cli.commands.common import Command, command_context, configure_output
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.config_files import ConfigSync
from etl_craft.services.operations.config_files import apply_config, export_config, plan_config


def _directory(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config-dir",
        type=Path,
        metavar="PATH",
        help="the config files (default: config/ in the project directory)",
    )
    configure_output(parser)


def _configure(parser: argparse.ArgumentParser) -> None:
    verbs = parser.add_subparsers(dest="config_verb", required=True)
    # Without abbreviations, --config after a verb is refused rather than read as --config-dir.
    _directory(
        verbs.add_parser(
            "plan",
            help="show what apply would change and validate it, changing nothing",
            allow_abbrev=False,
        )
    )
    _directory(
        verbs.add_parser(
            "apply", help="make the CFG_ tables match the config files", allow_abbrev=False
        )
    )
    export = verbs.add_parser(
        "export", help="write the CFG_ rows as config files", allow_abbrev=False
    )
    _directory(export)
    export.add_argument(
        "--force", action="store_true", help="overwrite config files that already exist"
    )


def _render(out: Output, done: ConfigSync, verb: str) -> None:
    out.line(f"config {done.revision} from {done.directory}: {len(done.changes)} change(s)")
    for change in done.changes:
        details = [
            f"{column.column}: {column.before!r} -> {column.after!r}"
            for column in change.columns
            if change.operation != "retire"
        ]
        if change.reason:
            details.append(change.reason)
        out.line(
            f"  {change.operation:<10} {change.file:<26} {change.row}"
            + (f"  ({'; '.join(details)})" if details else "")
        )
    for finding in done.findings:
        out.line(f"[{finding.status.value:<4}] {finding.where}: {finding.message}")
    if done.failed:
        out.line("validate failed the result, so nothing was changed")
    elif verb == "plan":
        out.line("plan only: nothing was changed")
    else:
        out.line(f"applied {len(done.changes)} change(s)")


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        if args.config_verb == "export":
            written = export_config(ctx, args.config_dir, overwrite=args.force)
            if args.output_format == "json":
                out.document(written)
            else:
                counts = ", ".join(f"{name} {count}" for name, count in written.rows.items())
                out.line(f"wrote {written.directory}: {counts} row(s)")
            return ExitCode.SUCCESS
        if args.config_verb == "plan":
            done = plan_config(ctx, args.config_dir)
        else:
            done = apply_config(ctx, args.config_dir)
    out.result(done, args.output_format, text=lambda: _render(out, done, args.config_verb))
    return ExitCode.FAILURE if done.failed else ExitCode.SUCCESS


COMMAND = Command(
    name="config",
    help="Load the project's config files into the CFG_ tables, plan that, or export them.",
    configure=_configure,
    run=_run,
)
