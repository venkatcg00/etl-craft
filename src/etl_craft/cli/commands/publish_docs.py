"""``etl-craft publish-docs``: serve the catalog site until stopped, locally or through ngrok."""

from __future__ import annotations

import argparse
import signal
import threading
from pathlib import Path

from etl_craft.cli.commands.common import Command, command_context, on_host, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.docs_publish import published
from etl_craft.services.operations.actions import admitted
from etl_craft.services.operations.documents import DEFAULT_FOLDER, PublishInput, docs_folder


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--output",
        type=Path,
        metavar="DIR",
        help=f"The site's folder (default: Docs_site.Output, else {DEFAULT_FOLDER}/ in the "
        "project directory).",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Serve on this machine or network only, without ngrok.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Local or organization-hosted listen address (default: 127.0.0.1).",
    )
    parser.add_argument(
        "--port", type=int, default=0, help="The local port (default: any free one)."
    )
    parser.add_argument(
        "--accept-new-url",
        action="store_true",
        help="Accept and record a publication URL different from the one recorded.",
    )


def wait_until_stopped() -> None:
    """Block until Ctrl-C or SIGTERM."""
    stop = threading.Event()
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, lambda signum, frame: stop.set())
    try:
        while not stop.wait(1):
            pass
    except KeyboardInterrupt:
        pass


def _run(args: argparse.Namespace, out: Output) -> int:
    on_host(args, ActionName.PUBLISH_DOCS)
    request = request_from(args, PublishInput)
    with command_context(args) as ctx, admitted(ctx, ActionName.PUBLISH_DOCS, request):
        out.line("publish-docs: this whole-project static catalog does not enforce pipeline grants")
        folder = docs_folder(ctx.config, request.output)
        with published(
            ctx.engine,
            ctx.config,
            folder,
            local_only=request.local_only,
            host=request.host,
            port=request.port,
            accept_new_url=request.accept_new_url,
        ) as site:
            out.line(f"publish-docs: serving {folder} at {site.url} (Ctrl-C to stop)")
            out.stdout.flush()
            ctx.engine.dispose()
            wait_until_stopped()
    out.line("publish-docs: stopped")
    return ExitCode.SUCCESS


COMMAND = Command(
    name="publish-docs",
    help="Serve the catalog locally, through ngrok, or at Docs_site.External_url.",
    configure=_configure,
    run=_run,
    actions=(ActionName.PUBLISH_DOCS,),
)
