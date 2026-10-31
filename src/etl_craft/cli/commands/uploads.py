"""Stream a local stewardship file, or read its recorded upload history."""

import argparse
import mimetypes
from contextlib import AbstractContextManager
from pathlib import Path

from etl_craft.cli.client import ApiClient
from etl_craft.cli.commands.common import (
    Command,
    call,
    command_context,
    command_project,
    configure_output,
)
from etl_craft.cli.output import Output
from etl_craft.cli.prompts import CONSOLE
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.serialization import from_json
from etl_craft.services.operations.uploads import UploadReceipt, UploadRequest, UploadSubmission


def configure(parser: argparse.ArgumentParser) -> None:
    """Choose an explicitly assigned pipeline and one local file."""
    parser.add_argument("--pipeline-code", "--pipeline_code", required=True)
    parser.add_argument("file", type=Path)
    configure_output(parser)


def configure_history(parser: argparse.ArgumentParser) -> None:
    """Choose the pipeline whose upload history is visible."""
    parser.add_argument("--pipeline-code", "--pipeline_code", required=True)
    configure_output(parser)


def upload(args: argparse.Namespace, out: Output) -> int:
    """Read the caller's local file incrementally; the service makes every decision."""
    if CONSOLE.get() is not None:
        raise UsageError(
            "upload FILE cannot read files on the server through the console; "
            "use the local CLI with --api or submit a file through the Uploads page"
        )
    request = UploadRequest(pipeline_code=args.pipeline_code)
    if args.api:
        result = ApiClient(args.api, command_project(args, required=True)).upload(
            request.pipeline_code, args.file
        )
    else:
        with command_context(args) as ctx:
            receiver = invoke(ctx, ActionName.UPLOAD, request)
            assert isinstance(receiver, AbstractContextManager)
            with receiver as receipt:
                assert isinstance(receipt, UploadReceipt)
                receipt.begin_file(
                    args.file.name,
                    mimetypes.guess_type(args.file.name)[0] or "application/octet-stream",
                )
                with args.file.open("rb") as stream:
                    while chunk := stream.read(65536):
                        receipt.write(chunk)
            result = receipt.result
    out.result(
        from_json(UploadSubmission, result), args.output_format, text=lambda: out.line(str(result))
    )
    return ExitCode.SUCCESS


def history(args: argparse.Namespace, out: Output) -> int:
    """Render the same upload records as the API."""
    result = call(args, ActionName.UPLOADS, UploadRequest(pipeline_code=args.pipeline_code))
    out.result(result, args.output_format, text=lambda: out.line(str(result)))
    return ExitCode.SUCCESS


UPLOAD = Command("upload", "Upload one stewardship file.", upload, configure, (ActionName.UPLOAD,))
HISTORY = Command(
    "uploads", "List stewardship uploads.", history, configure_history, (ActionName.UPLOADS,)
)
