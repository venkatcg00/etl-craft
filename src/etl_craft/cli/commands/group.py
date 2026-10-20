"""Administrator controls for permission group identities and memberships."""

import argparse

from etl_craft.cli.commands.common import Command, call, configure_output, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.services.operations import groups
from etl_craft.services.operations.requests import NoInput

VERBS = {
    "list": (ActionName.GROUP_LIST, NoInput),
    "add": (ActionName.GROUP_ADD, groups.GroupAdd),
    "disable": (ActionName.GROUP_DISABLE, groups.GroupName),
    "add-member": (ActionName.GROUP_ADD_MEMBER, groups.GroupMember),
    "remove-member": (ActionName.GROUP_REMOVE_MEMBER, groups.GroupMember),
}


def _configure(parser: argparse.ArgumentParser) -> None:
    verbs = parser.add_subparsers(dest="group_verb", required=True)
    for name in VERBS:
        sub = verbs.add_parser(name)
        if name != "list":
            sub.add_argument("--name", required=True)
        if name == "add":
            sub.add_argument("--description")
        if name in ("add-member", "remove-member"):
            sub.add_argument("--account", required=True)
        configure_output(sub)


def _run(args: argparse.Namespace, out: Output) -> int:
    action, request = VERBS[args.group_verb]
    done = call(args, action, request_from(args, request))
    assert isinstance(done, groups.Groups)
    out.result(
        done,
        args.output_format,
        text=lambda: out.rows(
            (g.name, g.active_flag, g.description, ", ".join(g.members)) for g in done.groups
        ),
    )
    return 0


COMMAND = Command(
    "group",
    "Manage permission groups (admin only).",
    _run,
    _configure,
    actions=tuple(action for action, _ in VERBS.values()),
)
