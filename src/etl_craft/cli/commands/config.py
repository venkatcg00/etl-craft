"""``etl-craft config``: load the project's config files into the Engine DB, or write them."""

from __future__ import annotations

import argparse
from pathlib import Path

from etl_craft.cli.commands.common import Command, call, configure_output, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName, RunStatus
from etl_craft.core.errors import ExitCode
from etl_craft.services.config_files import ConfigExport, ConfigSync
from etl_craft.services.operations.config_files import ConfigDirectory, ConfigExportInput


def _directory(parser: argparse.ArgumentParser, *, project: bool = True) -> None:
    if project:
        parser.add_argument(
            "--project-dir", type=Path, help="Project folder holding craft-connector.yml."
        )
    parser.add_argument(
        "--config-dir",
        type=Path,
        metavar="PATH",
        help="The config files (default: config/ in the project directory).",
    )
    configure_output(parser)


def _configure(parser: argparse.ArgumentParser) -> None:
    verbs = parser.add_subparsers(dest="config_verb", required=True)
    # Without abbreviations, --config after a verb is refused rather than read as --config-dir.
    _directory(
        verbs.add_parser(
            "plan",
            help="Show what apply would change and validate it, changing nothing.",
            allow_abbrev=False,
        )
    )
    _directory(
        verbs.add_parser("apply", help="Merge the submitted config changes.", allow_abbrev=False)
    )
    export = verbs.add_parser(
        "export", help="Write the CFG_ rows as config files.", allow_abbrev=False
    )
    _directory(export, project=False)
    export.add_argument(
        "--force", action="store_true", help="Overwrite config files that already exist."
    )
    export.add_argument(
        "--as-of",
        metavar="WHEN",
        help="The rows as they stood at a pipeline_run_id's start or an ISO 8601 time "
        "(UTC unless it names a zone), rebuilt from the change history.",
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
    for access in done.tag_access_changes:
        out.line(f"  access {access.pipeline}:")
        for grant in access.grants:
            match = "starts matching" if grant.starts_matching else "stops matching"
            out.line(f"    {grant.principal} tag={grant.tag!r} {grant.permission}: {match}")
        if access.policy_change is not None:
            out.line(f"    {access.policy_change}")
    for finding in done.findings:
        out.line(f"[{finding.status.value:<4}] {finding.where}: {finding.message}")
    for checked in done.refreshes:
        out.line(f"{checked.pipeline_code}: refresh {checked.status}")
        for refresh_finding in checked.findings:
            out.line(
                f"  {refresh_finding.file}: {refresh_finding.message}; {refresh_finding.remedy}"
            )
    if done.failed:
        out.line("validate failed the result, so nothing was changed")
    elif verb == "plan":
        out.line("plan only: nothing was changed")
    else:
        out.line(f"applied {len(done.changes)} change(s)")


def _run(args: argparse.Namespace, out: Output) -> int:
    if args.config_verb == "export":
        written = call(args, ActionName.CONFIG_EXPORT, request_from(args, ConfigExportInput))
        assert isinstance(written, ConfigExport)
        if args.output_format == "json":
            out.document(written)
        else:
            counts = ", ".join(f"{name} {count}" for name, count in written.rows.items())
            when = f" as of {written.as_of}" if written.as_of else ""
            out.line(f"wrote {written.directory}{when}: {counts} row(s)")
        return ExitCode.SUCCESS
    name = ActionName.CONFIG_PLAN if args.config_verb == "plan" else ActionName.CONFIG_APPLY
    done = call(args, name, request_from(args, ConfigDirectory))
    assert isinstance(done, ConfigSync)
    out.result(done, args.output_format, text=lambda: _render(out, done, args.config_verb))
    return (
        ExitCode.FAILURE
        if done.failed or any(r.status == RunStatus.FAILED for r in done.refreshes)
        else ExitCode.SUCCESS
    )


COMMAND = Command(
    name="config",
    help="Load the project's config files into the CFG_ tables, plan that, or export them.",
    configure=_configure,
    run=_run,
    actions=(ActionName.CONFIG_PLAN, ActionName.CONFIG_APPLY, ActionName.CONFIG_EXPORT),
)
