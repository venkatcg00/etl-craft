"""Administrator announcements, shown as a banner on every page of the web UI."""

import argparse
from datetime import datetime

from etl_craft.cli.commands.common import Command, call, configure_output, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.enums_ui import AnnouncementLevel
from etl_craft.services.operations import announcements

VERBS = {
    "list": (ActionName.ANNOUNCE_LIST, announcements.AnnouncementFilter),
    "add": (ActionName.ANNOUNCE_ADD, announcements.AnnouncementAdd),
    "end": (ActionName.ANNOUNCE_END, announcements.AnnouncementEnd),
}


def _configure(parser: argparse.ArgumentParser) -> None:
    verbs = parser.add_subparsers(dest="announce_verb", required=True)
    listing = verbs.add_parser("list", help="Announcements showing now.")
    listing.add_argument("--all", action="store_true", help="Every announcement, newest first.")
    add = verbs.add_parser("add", help="Post an announcement (admin only).")
    add.add_argument("--message", required=True, help="The text, at most 500 characters.")
    add.add_argument(
        "--level", type=AnnouncementLevel, choices=tuple(AnnouncementLevel), default="INFO"
    )
    add.add_argument(
        "--starts-at", type=datetime.fromisoformat, help="ISO time with offset (default: now)."
    )
    add.add_argument(
        "--ends-at", type=datetime.fromisoformat, help="ISO time with offset (default: open)."
    )
    add.add_argument("--reason", required=True)
    end = verbs.add_parser("end", help="Take an announcement down now (admin only).")
    end.add_argument("--announcement-id", type=int, required=True)
    end.add_argument("--reason", required=True)
    for sub in (listing, add, end):
        configure_output(sub)


def _run(args: argparse.Namespace, out: Output) -> int:
    action, request = VERBS[args.announce_verb]
    done = call(args, action, request_from(args, request))
    assert isinstance(done, announcements.Announcements)

    def render() -> None:
        if not done.announcements:
            out.empty("no announcements")
            return
        out.rows([("ID", "LEVEL", "STARTS_AT", "ENDS_AT", "ENDED_AT", "MESSAGE")])
        out.rows(
            (a.announcement_id, a.level, a.starts_at, a.ends_at, a.ended_at, a.message)
            for a in done.announcements
        )

    out.result(done, args.output_format, text=render)
    return 0


COMMAND = Command(
    "announce",
    "Post, list and end announcements shown on every page of the web UI.",
    _run,
    _configure,
    actions=tuple(action for action, _ in VERBS.values()),
)
