"""Bounded stewardship readers; workbook containers are checked before importing Excel."""

from __future__ import annotations

import codecs
import csv
import importlib
import struct
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from xml.parsers import expat

from etl_craft.core.enums_uploads import UploadFormat
from etl_craft.core.errors import HandlerError
from etl_craft.core.uploads import UploadFile

MAX_ENTRIES = 10000
MAX_EXPANDED_BYTES = 200 * 1024 * 1024


@dataclass(frozen=True)
class UploadReader:
    """Open a checked file incrementally using the pipeline's declared format."""

    upload: UploadFile
    file_format: UploadFormat
    encoding: str = "utf-8"
    sheet_name: str = ""

    def rows(self) -> Iterator[tuple[object, ...]]:
        """Yield text fields or saved workbook values without loading a whole file."""
        if self.file_format == UploadFormat.XLSX:
            yield from self._workbook()
        else:
            try:
                csv.field_size_limit(max(csv.field_size_limit(), self.upload.size_bytes))
                with self.upload.path.open(encoding=self.encoding, newline="") as stream:
                    for row in csv.reader(stream, delimiter="|", strict=True):
                        yield tuple(row)
            except (UnicodeError, LookupError, csv.Error) as error:
                raise HandlerError(
                    f"upload {self.upload.upload_id} {self.upload.file_name!r}: invalid "
                    f"{self.encoding} pipe-delimited text ({error}); expected RFC 4180 "
                    "quoting with | separators; upload a corrected file"
                ) from error

    def _workbook(self) -> Iterator[tuple[object, ...]]:
        excel = _excel(self.upload)
        workbook = excel.load_workbook(self.upload.path, read_only=True, data_only=True)
        formulas = excel.load_workbook(self.upload.path, read_only=True, data_only=False)
        try:
            if self.sheet_name and self.sheet_name not in workbook.sheetnames:
                raise HandlerError(
                    f"upload {self.upload.upload_id}: sheet={self.sheet_name!r}; expected "
                    f"one of {workbook.sheetnames}; choose SHEET_NAME or upload a corrected file"
                )
            sheet = workbook[self.sheet_name] if self.sheet_name else workbook.worksheets[0]
            formula_sheet = formulas[sheet.title]
            sheet.reset_dimensions()
            formula_sheet.reset_dimensions()
            for row, expressions in zip(sheet.iter_rows(), formula_sheet.iter_rows(), strict=True):
                for cell, expression in zip(row, expressions, strict=True):
                    if expression.data_type == "f" and cell.value is None:
                        raise HandlerError(
                            f"upload {self.upload.upload_id}, sheet {sheet.title!r}, "
                            f"cell {expression.coordinate}: formula has no saved value; "
                            "expected a cached result; recalculate, save and upload "
                            "a corrected file"
                        )
                yield tuple(cell.value for cell in row)
        finally:
            workbook.close()
            formulas.close()


def check_file(
    upload: UploadFile, file_format: str, limit_bytes: int, encoding: str = "utf-8"
) -> None:
    """Check declared size, extension and contents before any team script executes."""
    expected = "." + file_format.lower()
    suffix = Path(upload.file_name).suffix.lower()
    where = f"upload {upload.upload_id} {upload.file_name!r}"
    if upload.size_bytes > limit_bytes:
        raise HandlerError(
            f"{where}: size={upload.size_bytes} bytes; expected at most {limit_bytes}; "
            "upload a corrected file below MAX_FILE_MB"
        )
    if suffix != expected or suffix in {".xls", ".xlsm"}:
        raise HandlerError(
            f"{where}: extension={suffix!r}; expected {expected}; upload a corrected file"
        )
    with upload.path.open("rb") as stream:
        prefix = stream.read(8192)
    if file_format == UploadFormat.XLSX:
        check_workbook(upload)
        _excel(upload)
        return
    try:
        decoded = codecs.getincrementaldecoder(encoding)(errors="strict").decode(
            prefix, final=upload.size_bytes <= len(prefix)
        )
    except (LookupError, UnicodeError) as error:
        raise HandlerError(
            f"{where}: content does not match ENCODING={encoding!r}; expected decodable "
            "pipe-delimited text; correct ENCODING or upload a corrected file"
        ) from error
    if prefix.startswith((b"PK\x03\x04", b"\xd0\xcf\x11\xe0")) or "\0" in decoded:
        raise HandlerError(
            f"{where}: binary contents; expected {file_format} pipe-delimited text; "
            "upload a corrected file"
        )


def check_workbook(upload: UploadFile) -> None:
    """Bound ZIP expansion and reject macros and unsaved formulas before openpyxl."""
    where = f"upload {upload.upload_id} {upload.file_name!r}"
    try:
        _zip_limits(upload)
        with zipfile.ZipFile(upload.path) as archive:
            entries = archive.infolist()
            if len({entry.filename.casefold() for entry in entries}) != len(entries):
                raise HandlerError(
                    f"{where}: duplicate workbook parts; expected unique ZIP entries; "
                    "upload a corrected workbook"
                )
            expanded = sum(entry.file_size for entry in entries)
            if len(entries) > MAX_ENTRIES or expanded > MAX_EXPANDED_BYTES:
                raise HandlerError(
                    f"{where}: ZIP entries={len(entries)}, expanded bytes={expanded}; "
                    f"expected at most {MAX_ENTRIES} and {MAX_EXPANDED_BYTES}; "
                    "upload a smaller corrected workbook"
                )
            for entry in entries:
                if entry.filename.endswith(".xml") or entry.filename.endswith(".rels"):
                    _safe_xml(archive, entry.filename, where)
            names = {entry.filename for entry in entries}
            if "[Content_Types].xml" not in names or "xl/workbook.xml" not in names:
                raise HandlerError(
                    f"{where}: ZIP is not an XLSX workbook; expected workbook parts; "
                    "upload a corrected file"
                )
            if any(name.lower().endswith("vbaproject.bin") for name in names):
                raise HandlerError(
                    f"{where}: vbaProject.bin found; expected a macro-free XLSX workbook; "
                    "remove macros and upload a corrected file"
                )
    except (zipfile.BadZipFile, expat.ExpatError, RuntimeError, NotImplementedError) as error:
        raise HandlerError(
            f"{where}: invalid workbook contents ({error}); expected XLSX; upload a corrected file"
        ) from error


def _safe_xml(archive: zipfile.ZipFile, name: str, where: str) -> None:
    """Reject a DTD in any XML encoding before any reader can expand its entities."""
    parser = expat.ParserCreate()

    def refuse_doctype(name: str, system: str | None, public: str | None, subset: int) -> None:
        raise HandlerError(
            f"{where}: XML DTD in {name}; expected workbook XML without DTDs or entities; "
            "upload a corrected file"
        )

    parser.StartDoctypeDeclHandler = refuse_doctype
    with archive.open(name) as stream:
        while chunk := stream.read(65536):
            parser.Parse(chunk, False)
        parser.Parse(b"", True)


def _excel(upload: UploadFile) -> ModuleType:
    """Check the optional reader before the team script runs."""
    try:
        return importlib.import_module("openpyxl")
    except ImportError as error:
        raise HandlerError(
            f"upload {upload.upload_id}: XLSX reader is unavailable; expected the excel "
            "extra; install it with pip install etl-craft[excel]"
        ) from error


def _zip_limits(upload: UploadFile) -> None:
    """Count central-directory records without allocating a ZipInfo for every hostile entry."""
    with upload.path.open("rb") as stream:
        if stream.read(4) != b"PK\x03\x04":
            raise HandlerError(
                f"upload {upload.upload_id}: contents are not XLSX ZIP data; expected "
                "a macro-free workbook; upload a corrected file"
            )
        stream.seek(0, 2)
        size = stream.tell()
        stream.seek(max(0, size - 65557))
        tail = stream.read(65557)
        end = tail.rfind(b"PK\x05\x06")
        if end < 0 or len(tail) < end + 22:
            raise zipfile.BadZipFile("missing end-of-central-directory record")
        _, disk, start_disk, on_disk, total, directory_size, _, comment = struct.unpack(
            "<4s4H2IH", tail[end : end + 22]
        )
        if disk or start_disk or on_disk != total or total > MAX_ENTRIES:
            raise HandlerError(
                f"upload {upload.upload_id}: ZIP entries={total}, disk={disk}; expected "
                f"one disk and at most {MAX_ENTRIES} entries; upload a smaller corrected workbook"
            )
        end_position = size - len(tail) + end
        position = end_position - directory_size
        if position < 0 or end + 22 + comment != len(tail):
            raise zipfile.BadZipFile("invalid central-directory bounds")
        stream.seek(position)
        count = expanded = 0
        while stream.tell() < end_position:
            header = stream.read(46)
            if len(header) != 46 or header[:4] != b"PK\x01\x02":
                raise zipfile.BadZipFile("invalid central-directory entry")
            count += 1
            expanded += struct.unpack_from("<I", header, 24)[0]
            if count > MAX_ENTRIES or expanded > MAX_EXPANDED_BYTES:
                raise HandlerError(
                    f"upload {upload.upload_id}: ZIP entries={count}, expanded bytes={expanded}; "
                    f"expected at most {MAX_ENTRIES} entries and {MAX_EXPANDED_BYTES} bytes; "
                    "upload a smaller corrected workbook"
                )
            lengths = struct.unpack_from("<3H", header, 28)
            stream.seek(sum(lengths), 1)
        if stream.tell() != end_position or count != total:
            raise zipfile.BadZipFile("central-directory count or size differs")
