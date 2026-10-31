"""Streamed upload verification and hostile workbook boundaries."""

import hashlib
import re
import zipfile

import pytest

from etl_craft.core.errors import HandlerError
from etl_craft.core.upload_copy import verified_upload
from etl_craft.core.upload_reader import UploadReader, check_file
from etl_craft.core.uploads import UploadFile, UploadInfo

pytestmark = pytest.mark.unit


def file_info(path, name=None):
    data = path.read_bytes()
    return UploadFile(
        1, "alice", name or path.name, len(data), hashlib.sha256(data).hexdigest(), path
    )


def test_copy_verifies_stream_before_publishing(tmp_path):
    expected = UploadInfo(7, "alice", "input.csv", 4, hashlib.sha256(b"a|b\n").hexdigest())
    copied = verified_upload(tmp_path, expected, iter([b"a|", b"b\n"]))
    assert copied.path.read_bytes() == b"a|b\n"
    assert copied.account == "alice"


def test_hash_mismatch_publishes_no_script_input(tmp_path):
    expected = UploadInfo(7, "alice", "input.csv", 4, hashlib.sha256(b"a|b\n").hexdigest())
    with pytest.raises(HandlerError) as failure:
        verified_upload(tmp_path, expected, iter([b"a|c\n"]))
    for value in ("upload 7", expected.sha256, hashlib.sha256(b"a|c\n").hexdigest(), "restore"):
        assert value in str(failure.value)
    assert not (tmp_path / "7/content").exists()
    assert not list((tmp_path / "7").iterdir())


def test_text_reader_preserves_quoted_pipes_and_encoding(tmp_path):
    path = tmp_path / "data.txt"
    path.write_bytes('id|name\n1|"André|team"\n'.encode("latin-1"))
    upload = file_info(path)
    check_file(upload, "TXT", 1000, "latin-1")
    assert list(UploadReader(upload, "TXT", "latin-1").rows()) == [
        ("id", "name"),
        ("1", "André|team"),
    ]


@pytest.mark.parametrize("name", ["input.xls", "input.xlsm", "input.csv"])
def test_binary_or_unsupported_extension_is_refused(tmp_path, name):
    path = tmp_path / name
    path.write_bytes(b"PK\x03\x04\0binary")
    with pytest.raises(HandlerError, match="corrected file"):
        check_file(file_info(path), "CSV", 1000)


def test_macro_part_is_refused_before_excel_import(tmp_path):
    path = tmp_path / "data.xlsx"
    with zipfile.ZipFile(path, "w") as archive:
        for name in ("[Content_Types].xml", "xl/workbook.xml"):
            archive.writestr(name, "<root/>")
        archive.writestr("xl/vbaProject.bin", b"macro")
    with pytest.raises(HandlerError, match=r"vbaProject\.bin"):
        check_file(file_info(path), "XLSX", 10000)


def test_utf16_entity_definition_is_refused(tmp_path):
    path = tmp_path / "data.xlsx"
    xml = (
        '<?xml version="1.0" encoding="utf-16"?><!DOCTYPE root [<!ENTITY x "bad">]><root>&x;</root>'
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("[Content_Types].xml", xml.encode("utf-16"))
        archive.writestr("xl/workbook.xml", "<root/>")
    with pytest.raises(HandlerError, match="DTD"):
        check_file(file_info(path), "XLSX", 10000)


def test_unsaved_formula_names_sheet_and_cell(tmp_path):
    import openpyxl

    path = tmp_path / "formula.xlsx"
    workbook = openpyxl.Workbook()
    workbook.active.title = "Corrections"
    workbook.active["B2"] = "=1+1"
    workbook.save(path)
    workbook.close()
    upload = file_info(path)
    check_file(upload, "XLSX", 10000)
    with pytest.raises(HandlerError) as failure:
        list(UploadReader(upload, "XLSX", sheet_name="Corrections").rows())
    for value in ("Corrections", "B2", "no saved value", "recalculate"):
        assert value in str(failure.value)


def test_workbook_expansion_is_bounded_before_excel_import(tmp_path, monkeypatch):
    import etl_craft.core.upload_reader as reader

    path = tmp_path / "oversize.xlsx"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", "<root/>" * 50)
        archive.writestr("xl/workbook.xml", "<root/>")
    monkeypatch.setattr(reader, "MAX_EXPANDED_BYTES", 50)
    with pytest.raises(HandlerError, match=r"expanded bytes=.*expected"):
        check_file(file_info(path), "XLSX", 10000)


def test_xlsx_missing_extra_fails_before_team_script(tmp_path, monkeypatch):
    import etl_craft.core.upload_reader as reader

    path = tmp_path / "data.xlsx"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("[Content_Types].xml", "<root/>")
        archive.writestr("xl/workbook.xml", "<root/>")

    def missing(name):
        raise ImportError(name)

    monkeypatch.setattr(reader.importlib, "import_module", missing)
    with pytest.raises(HandlerError, match=r"pip install etl-craft\[excel\]"):
        check_file(file_info(path), "XLSX", 10000)


@pytest.mark.parametrize("sheet", ["", "Corrections"])
def test_workbook_reader_uses_selected_sheet_and_saved_formula_values(tmp_path, sheet):
    import openpyxl

    path = tmp_path / "saved.xlsx"
    workbook = openpyxl.Workbook()
    workbook.active.title = "Corrections"
    workbook.active.append(["id", "amount"])
    workbook.active.append([1, "=1+1"])
    workbook.save(path)
    workbook.close()
    with zipfile.ZipFile(path) as archive:
        parts = [(item, archive.read(item)) for item in archive.infolist()]
    with zipfile.ZipFile(path, "w") as archive:
        for item, data in parts:
            if item.filename == "xl/worksheets/sheet1.xml":
                data = re.sub(rb"<f>1\+1</f><v\s*(?:/>|></v>)", b"<f>1+1</f><v>2</v>", data)
                assert b"<v>2</v>" in data
            archive.writestr(item, data)
    upload = file_info(path)
    check_file(upload, "XLSX", 100000)
    assert list(UploadReader(upload, "XLSX", sheet_name=sheet).rows()) == [
        ("id", "amount"),
        (1, 2),
    ]


def test_text_fields_are_bounded_by_the_file_ceiling_not_csvs_small_default(tmp_path):
    path = tmp_path / "large.csv"
    value = "x" * 200000
    path.write_text("id|value\n1|" + value + "\n")
    upload = file_info(path)
    check_file(upload, "CSV", 1024 * 1024)
    assert list(UploadReader(upload, "CSV").rows()) == [("id", "value"), ("1", value)]
