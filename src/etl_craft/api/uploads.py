"""Decode one bounded multipart file directly into the shared upload receipt."""

from contextlib import AbstractContextManager
from email import policy
from email.message import Message
from email.parser import BytesParser

from fastapi import Request
from fastapi.responses import JSONResponse

from etl_craft.core.enums import ActionName
from etl_craft.core.enums_uploads import MultipartState
from etl_craft.core.errors import UsageError
from etl_craft.services.operations.actions import ACTIONS, invoke
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.serialization import from_json, to_json
from etl_craft.services.operations.uploads import UploadReceipt, UploadRequest, UploadSubmission

MAX_HEADERS = 8192


class MultipartFile:
    """Retain only a bounded header or a possible boundary prefix between chunks."""

    def __init__(self, content_type: str, receipt: UploadReceipt) -> None:
        """Require a bounded ASCII boundary before receiving any file bytes."""
        message = Message()
        message["Content-Type"] = content_type
        boundary = message.get_boundary()
        if (
            message.get_content_type() != "multipart/form-data"
            or not boundary
            or not boundary.isascii()
            or len(boundary) > 70
            or any(ord(c) < 32 or ord(c) > 126 for c in boundary)
        ):
            raise UsageError(
                f"upload {receipt.upload_id}: invalid multipart boundary; expected "
                "multipart/form-data with a 1..70 byte boundary; send one corrected file"
            )
        self.boundary = b"--" + boundary.encode("ascii")
        self.receipt = receipt
        self.buffer = bytearray()
        self.state = MultipartState.INITIAL
        self.received = 0

    def feed(self, data: bytes) -> None:
        """Consume small transport slices without retaining the uploaded body."""
        self.received += len(data)
        maximum = self.receipt.ctx.config.uploads.max_file_mb * 1024 * 1024 + 2 * MAX_HEADERS
        if self.received > maximum:
            raise UsageError(
                f"upload {self.receipt.upload_id}: multipart body exceeds {maximum} bytes; "
                "expected one file within Uploads.Max_file_mb; upload a smaller corrected file"
            )
        self.buffer.extend(data)
        while True:
            before = (self.state, len(self.buffer))
            if self.state == MultipartState.INITIAL:
                self._initial()
            elif self.state == MultipartState.HEADERS:
                self._headers()
            elif self.state == MultipartState.BODY:
                self._body()
            elif len(self.buffer) > 2 or bytes(self.buffer) not in {b"", b"\r", b"\r\n"}:
                raise UsageError(
                    f"upload {self.receipt.upload_id}: unexpected multipart epilogue; "
                    "expected a closing boundary; send one corrected file"
                )
            if before == (self.state, len(self.buffer)):
                return

    def _initial(self) -> None:
        size = len(self.boundary) + 2
        if len(self.buffer) < size:
            return
        if self.buffer[: len(self.boundary)] != self.boundary:
            raise UsageError("multipart opening boundary differs; send one corrected file")
        suffix = bytes(self.buffer[len(self.boundary) : size])
        if suffix not in {b"\r\n", b"--"}:
            raise UsageError("multipart boundary suffix is invalid; send one corrected file")
        del self.buffer[:size]
        self.state = MultipartState.END if suffix == b"--" else MultipartState.HEADERS

    def _headers(self) -> None:
        stop = self.buffer.find(b"\r\n\r\n")
        if stop < 0:
            if len(self.buffer) > MAX_HEADERS:
                raise UsageError("multipart headers exceed 8192 bytes; send one corrected file")
            return
        if stop > MAX_HEADERS:
            raise UsageError("multipart headers exceed 8192 bytes; send one corrected file")
        message = BytesParser(policy=policy.default).parsebytes(
            bytes(self.buffer[:stop]) + b"\r\n\r\n"
        )
        if (
            message.defects
            or len(message.get_all("Content-Disposition", [])) != 1
            or len(message.get_all("Content-Type", [])) > 1
            or message.get_content_disposition() != "form-data"
            or message.get_param("name", header="Content-Disposition") != "file"
            or message.get_filename() is None
        ):
            raise UsageError(
                f"upload {self.receipt.upload_id}: malformed or non-file part; expected "
                "one multipart file field named file; send one corrected file"
            )
        self.receipt.begin_file(
            message.get_filename() or "", message.get("Content-Type", "application/octet-stream")
        )
        del self.buffer[: stop + 4]
        self.state = MultipartState.BODY

    def _body(self) -> None:
        marker = b"\r\n" + self.boundary
        stop = self.buffer.find(marker)
        if stop < 0:
            count = max(0, len(self.buffer) - len(marker) - 2)
            if count:
                self.receipt.write(bytes(self.buffer[:count]))
                del self.buffer[:count]
            return
        after = stop + len(marker)
        if len(self.buffer) < after + 2:
            if stop:
                self.receipt.write(bytes(self.buffer[:stop]))
                del self.buffer[:stop]
            return
        suffix = bytes(self.buffer[after : after + 2])
        if suffix not in {b"--", b"\r\n"}:
            self.receipt.write(bytes(self.buffer[: stop + 2]))
            del self.buffer[: stop + 2]
            return
        self.receipt.write(bytes(self.buffer[:stop]))
        del self.buffer[: after + 2]
        self.state = MultipartState.END if suffix == b"--" else MultipartState.HEADERS

    def finish(self) -> None:
        """Refuse truncation rather than queueing an incomplete file."""
        if self.state != MultipartState.END or bytes(self.buffer) not in {b"", b"\r\n"}:
            raise UsageError(
                f"upload {self.receipt.upload_id}: multipart ended in {self.state}; "
                "expected a complete closing boundary; send one corrected file"
            )


async def receive(caller: OperationContext, request: Request) -> JSONResponse:
    """Parse only transport data; the operation owns size checks, files and admission."""
    from etl_craft.api.routes import parse_request

    parsed = parse_request(ACTIONS[ActionName.UPLOAD], dict(request.query_params))
    assert isinstance(parsed, UploadRequest)
    receiver = invoke(caller, ActionName.UPLOAD, parsed)
    assert isinstance(receiver, AbstractContextManager)
    with receiver as receipt:
        assert isinstance(receipt, UploadReceipt)
        parser = MultipartFile(request.headers.get("Content-Type", ""), receipt)
        async for chunk in request.stream():
            for start in range(0, len(chunk), 16384):
                parser.feed(chunk[start : start + 16384])
        parser.finish()
    return JSONResponse(to_json(from_json(UploadSubmission, receipt.result)))
