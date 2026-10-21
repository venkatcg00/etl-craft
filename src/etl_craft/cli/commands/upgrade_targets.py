"""``upgrade-targets``: add execution identities and correct legacy SCD active state."""

import argparse

from etl_craft.cli.commands.common import Command, call, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName, SqlAction
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.maintenance import TargetUpgrades, UpgradeInput


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--action",
        type=SqlAction,
        choices=[
            kind for kind in SqlAction if kind not in {SqlAction.DROP_TABLE, SqlAction.CREATE_VIEW}
        ],
        help="Filter by SQL action; defaults to configured SQL and python targets.",
    )
    parser.add_argument(
        "--target",
        help="One schema.table (or database.schema.table); defaults to all selected targets.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate targets and show additions without writing warehouse rows.",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    done = call(args, ActionName.UPGRADE_TARGETS, request_from(args, UpgradeInput))
    assert isinstance(done, TargetUpgrades)
    results = done.targets
    if not results:
        out.line("No active configured targets to upgrade")
    for result in results:
        if not result.changed:
            out.line(f"{result.target}: audit columns already upgraded")
        elif result.dry_run:
            columns = ", ".join(result.columns) or "none"
            out.line(f"{result.target}: would upgrade audit columns; additions: {columns}")
            assert result.sql is not None
            out.line(result.sql)
        else:
            columns = ", ".join(result.columns) or "none"
            out.line(f"{result.target}: upgraded audit columns; additions: {columns}")
    return ExitCode.SUCCESS


COMMAND = Command(
    "upgrade-targets",
    "Upgrade execution identities and legacy SCD active state on existing targets.",
    configure=_configure,
    run=_run,
    actions=(ActionName.UPGRADE_TARGETS,),
)
