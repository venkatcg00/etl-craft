"""Publish a bounded project through the developer action, including API-only CI runners."""

import argparse
from pathlib import Path

from etl_craft.cli.commands.common import Command, call, configure_output
from etl_craft.cli.output import Output
from etl_craft.config import resolve_config_path
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.engine.project_files import collect
from etl_craft.services.operations.bundles import BundleView, PublishBundle


def configure(parser: argparse.ArgumentParser) -> None:
    """Resolve the project directory without loading its Engine credentials."""
    parser.add_argument("operation", choices=["publish"])
    parser.add_argument(
        "--project-dir", type=Path, help="Project directory containing craft-connector.yml."
    )
    configure_output(parser)


def run(args: argparse.Namespace, out: Output) -> int:
    """Read upload inputs, then let the action validate and publish the manifest."""
    if args.project_dir is not None and args.config is not None:
        raise UsageError(
            "bundle publish: --project-dir and --config both given; expected one source; choose one"
        )
    path = (
        (args.project_dir / "craft-connector.yml")
        if args.project_dir is not None
        else resolve_config_path(args.config)
    )
    args.config = path
    result = call(args, ActionName.BUNDLE_PUBLISH, PublishBundle(files=collect(path.parent)))
    assert isinstance(result, BundleView)
    out.result(
        result,
        args.output_format,
        text=lambda: out.line(f"Published bundle {result.bundle_id}: {len(result.files)} files"),
    )
    return ExitCode.SUCCESS


COMMAND = Command(
    "bundle", "Publish immutable project files.", run, configure, (ActionName.BUNDLE_PUBLISH,)
)
