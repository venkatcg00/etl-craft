"""``etl-craft publish-docs``: serve the catalog site until stopped, locally or through ngrok."""

from __future__ import annotations

import argparse
import signal
import threading
from pathlib import Path

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.commands.generate_docs import DEFAULT_FOLDER
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.docs_publish import published


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--output",
        type=Path,
        metavar="DIR",
        help=f"the site's folder (default: Docs_site.Output, else {DEFAULT_FOLDER}/ in the "
        "project directory)",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="serve on this machine or network only, without ngrok",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="with --local-only, the address to listen on (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port", type=int, default=0, help="the local port (default: any free one)"
    )
    parser.add_argument(
        "--accept-new-url",
        action="store_true",
        help="publish even though ngrok gives another URL than the one recorded, and record it",
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
    config = load_command_config(args)
    folder = args.output or config.docs_site.output or config.project_dir / DEFAULT_FOLDER
    engine = connect_engine_db(config)
    try:
        with published(
            engine,
            config,
            folder,
            local_only=args.local_only,
            host=args.host,
            port=args.port,
            accept_new_url=args.accept_new_url,
        ) as site:
            out.line(f"publish-docs: serving {folder} at {site.url} (Ctrl-C to stop)")
            out.stdout.flush()
            wait_until_stopped()
    finally:
        engine.dispose()
    out.line("publish-docs: stopped")
    return ExitCode.SUCCESS


COMMAND = Command(
    name="publish-docs",
    help="Serve the catalog site until stopped, at an ngrok link or on this machine.",
    configure=_configure,
    run=_run,
)
