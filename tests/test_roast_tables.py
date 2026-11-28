"""Shared table helpers of the roasters (`roast.tables`, `roast.attempt`)."""
import csv
import io
import json
import logging
import math
import re
import sys
import zipfile
from collections import Counter
from pathlib import Path

import pytest
from roast_utils import LAYOUTS, read_tsv

from brainspresso.actions import Fail, WriteText
from brainspresso.bids.keys import KeyAliases
from brainspresso.bids.layout import make_layout
from brainspresso.roast import attempt, make_context, run_roast, tables
from brainspresso.roast.tables import (
    NANS,
    Sheet,
    Table,
    TableError,
    clean,
    integer,
    numeric_columns,
    read_csv,
    read_xls,
    read_xlsx,
    sidecar_name,
    table_action,
    xls_date,
)

IXI_XLS = (Path(tables.__file__).parent.parent / "datasets" / "ixi"
           / "templates" / "IXI.xls")
KEYS = KeyAliases({"meta": {}, "raw": {"anat": {"T1w"}}},
                  bids={"T1w": {"suffix": ["T1w"]}})


@pytest.fixture(autouse=True)
def forget_warnings():
    """Each test warns as a new process would."""
    tables._warned.clear()
    yield
    tables._warned.clear()


# ----------------------------------------------------------------------
#   Cells
# ----------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    (None, "n/a"), (math.nan, "n/a"), ("", "n/a"), ("  ", "n/a"),
    *((nan, "n/a") for nan in sorted(NANS)),
    (30.0, "30"), (0.0, "0"), (-0.0, "0"), (0.5, "0.5"), (1.306, "1.306"),
    (1e16, "1e+16"), (math.inf, "inf"), (7, "7"), (True, "True"),
    ("30.0", "30.0"),                       # (text is kept as it is)
    (" a\tb ", "a b"), ("a\r\n\r\nb", "a b"), ("a \n b", "a b"),
    ('say "no"', "say 'no'"), ("x", "x"),
])
def test_clean(value, expected):
    assert clean(value) == expected


def test_clean_counts_and_options():
    counts = Counter()
    assert clean('a\t"b"', counts=counts) == "a 'b'"
    assert clean('"', quotes="`", counts=counts) == "`"
    assert clean("plain", counts=counts) == "plain"
    assert counts == {"quotes": 2, "breaks": 1}
    assert clean("NA", nans=()) == "NA"
    assert clean("-9999", nans=NANS | {"-9999"}) == "n/a"


def test_integer():
    assert [integer(v) for v in (1, 1.0, "1", " 2.0 ", "1.5", "", None,
                                 "x")] == [1, 1, 1, 2, None, None, None,
                                           None]


# ----------------------------------------------------------------------
#   CSV
# ----------------------------------------------------------------------

def test_read_csv_encodings(tmp_path, caplog):
    """UTF-8 (with or without BOM) first, then Windows-1252 (logged)"""
    bom = tmp_path / "bom.csv"
    bom.write_bytes("﻿ID,Name\n1,Zoë\n".encode())
    sheet = read_csv(bom)
    assert sheet.header == ["ID", "Name"]
    assert sheet.rows == [(2, ["1", "Zoë"])]
    cp = tmp_path / "cp.csv"
    cp.write_bytes("ID,Name\n1,Fran\xe7ais \u20ac\n".encode("cp1252"))
    with caplog.at_level(logging.INFO):
        assert read_csv(cp).rows[0][1][1] == "Fran\xe7ais €"
    assert "cp.csv: not UTF-8, read as cp1252" in caplog.text
    with pytest.raises(TableError, match=r"^cp\.csv: cannot be read "
                                         r"\(UnicodeDecodeError"):
        read_csv(cp, encodings=("utf-8-sig",))


def test_read_csv_rows(tmp_path):
    """Blank rows dropped; rows numbered by the physical line they start
    on (a quoted line break spans two); short rows read as empty past
    their end"""
    path = tmp_path / "t.csv"
    path.write_bytes(b'ID,Note\n1,"a\nb"\n\n , \n2\n')
    sheet = read_csv(path)
    assert sheet.rows == [(2, ["1", "a\nb"]), (6, ["2"])]
    assert sheet.cell(sheet.rows[1][1], 1) == ""
    assert sheet.cell(sheet.rows[1][1], None, default=None) is None


def test_read_csv_crlf(tmp_path):
    """Files of Windows tools: CRLF rows, CRLF (or a lone CR) inside a
    quoted cell; cells are read as stored, `clean` makes each break one
    space"""
    path = tmp_path / "t.csv"
    path.write_bytes(b'ID,Note\r\n1,"a\r\nb"\r\n2,"c\rd"\r\n\r\n'
                     b'3,"e\r\n\r\n f"\r\n')
    sheet = read_csv(path)
    assert sheet.header == ["ID", "Note"]
    assert sheet.rows == [(2, ["1", "a\r\nb"]), (4, ["2", "c\rd"]),
                          (7, ["3", "e\r\n\r\n f"])]
    counts = Counter()
    assert [clean(row[1], counts=counts) for _, row in sheet.rows] == [
        "a b", "c d", "e f"]
    assert counts == {"breaks": 3}
    table = Table({"participant_id": "ID", "note": "Note"})
    assert list(table.rows([path]))[1:] == [
        ["sub-1", "a b"], ["sub-2", "c d"], ["sub-3", "e f"]]


def test_read_csv_file_object_and_options():
    sheet = read_csv(io.BytesIO(b"a\tb\n1\t\"2\n"), delimiter="\t",
                     name="x.tsv", quoting=csv.QUOTE_NONE)
    assert (sheet.name, sheet.path) == ("x.tsv", None)
    assert sheet.rows == [(2, ["1", '"2'])]
    with pytest.raises(TableError, match=r"^x\.csv: cannot be read "
                                         r"\(csv\.Error"):
        read_csv(io.BytesIO(b'a\n"b"c\n'), name="x.csv", strict=True)


def test_row_warning_names_the_physical_line(tmp_path, caplog):
    """After a cell with line breaks, a warning names the line of the
    row in the file"""
    path = _csv(tmp_path / "a.csv", 'SUB,NOTE\n1,"x\ny\nz"\n,lost\n')
    with caplog.at_level(logging.WARNING):
        list(Table({"participant_id": "SUB", "note": "NOTE"}).rows([path]))
    assert "a.csv, line 5: row skipped (no subject)" in caplog.text


def test_read_csv_no_encoding():
    with pytest.raises(ValueError, match="no encoding"):
        read_csv(io.BytesIO(b"a\n"), name="x.csv", encodings=())


def test_read_csv_missing(tmp_path):
    with pytest.raises(TableError, match=r"^no\.csv: cannot be read "
                                         r"\(FileNotFoundError"):
        read_csv(tmp_path / "no.csv")


def test_header_lookup(tmp_path, caplog):
    """No case, no whitespace, aliases, first match wins"""
    sheet = Sheet("t.csv", ["Subject ID", "AGE", "age", "M/F"], [])
    assert sheet.find("subjectid") == 0
    assert sheet.find(" s u b j e c t i d ") == 0
    assert sheet.find("Age") == 1               # (duplicated: the first)
    assert sheet.find(("Sex", "m/f")) == 3
    assert sheet.find("sex") is None
    assert sheet.find("sex", default=7) == 7
    assert sheet.require(("ID", "Subject ID")) == 0
    with pytest.raises(TableError, match=r"^t\.csv: no column 'Sex' or "
                                         r"'Gender'$"):
        sheet.require(("Sex", "Gender"))
    with caplog.at_level(logging.WARNING):
        assert sheet.locate(["subject id", "Sex", "Age"]) == [0, 1, 1]
    assert "t.csv: columns not found by name, read by position: 'Sex' " \
        "(column 2: 'AGE')" in caplog.text


# ----------------------------------------------------------------------
#   Spreadsheets
# ----------------------------------------------------------------------

def _xlsx(path, rows):
    openpyxl = pytest.importorskip("openpyxl")
    book = openpyxl.Workbook()
    for row in rows:
        book.active.append(row)
    book.save(path)
    return path


def test_read_xlsx(tmp_path):
    """Values as stored; formatted empty trailing rows are blank"""
    path = _xlsx(tmp_path / "t.xlsx", [["ID", "Age"], ["a", 30], ["b", 0.5],
                                       [None, None]])
    openpyxl = pytest.importorskip("openpyxl")
    book = openpyxl.load_workbook(path)
    book.active["A10"].number_format = "0.00"   # a formatted empty cell
    book.save(path)
    sheet = read_xlsx(path)
    assert sheet.header == ["ID", "Age"]
    assert sheet.rows == [(2, ["a", 30]), (3, ["b", 0.5])]


def test_read_xlsx_wrong_dimension(tmp_path):
    """Every row is read, whatever dimension the file stores"""
    path = _xlsx(tmp_path / "t.xlsx", [["ID"], ["a"], ["b"]])
    with zipfile.ZipFile(path) as z:
        files = {n: z.read(n) for n in z.namelist()}
    sheet = "xl/worksheets/sheet1.xml"
    # (the tag is written `<dimension ref="..."/>` with lxml, `... />`
    # without; inserted after `<sheetPr>` if absent)
    stale = b'<dimension ref="A1:A1"/>'
    files[sheet], count = re.subn(rb'<dimension\s[^>]*/>', stale,
                                  files[sheet])
    if not count:
        files[sheet], count = re.subn(
            rb'(</sheetPr>|<sheetPr\s[^>]*/>)', rb'\1' + stale,
            files[sheet], count=1)
    assert count == 1 and stale in files[sheet]
    wrong = tmp_path / "wrong.xlsx"
    with zipfile.ZipFile(wrong, "w") as z:
        for name, data in files.items():
            z.writestr(name, data)
    assert [row for _, row in read_xlsx(wrong).rows] == [["a"], ["b"]]


def test_read_xlsx_errors(tmp_path, monkeypatch):
    path = tmp_path / "t.xlsx"
    path.write_bytes(b"not a workbook")
    pytest.importorskip("openpyxl")
    with pytest.raises(TableError, match=r"^t\.xlsx: cannot be read \("):
        read_xlsx(path)
    monkeypatch.setitem(sys.modules, "openpyxl", None)
    with pytest.raises(TableError, match=r"^t\.xlsx: openpyxl is needed to "
                                         r"read it: pip install "
                                         r"brainspresso\[oasis2\]$"):
        read_xlsx(path, extra="oasis2")


def test_read_xls():
    """The IXI spreadsheet shipped with brainspresso"""
    pytest.importorskip("xlrd")
    sheet = read_xls(IXI_XLS)
    assert sheet.header[:2] == ["IXI_ID", "SEX_ID (1=m, 2=f)"]
    assert sheet.find("height") == 2
    line, row = sheet.rows[1]
    assert line == 3 and row[0] == 2.0 and clean(row[2]) == "164"
    assert xls_date(row[10], sheet.datemode) == "2005-11-18"


def test_read_xls_errors(tmp_path, monkeypatch):
    path = tmp_path / "t.xls"
    path.write_bytes(b"not a workbook")
    pytest.importorskip("xlrd")
    with pytest.raises(TableError, match=r"^t\.xls: cannot be read \("):
        read_xls(path)
    monkeypatch.setitem(sys.modules, "xlrd", None)
    with pytest.raises(TableError, match=r"pip install brainspresso\[ixi\]"):
        read_xls(path, extra="ixi")


# ----------------------------------------------------------------------
#   Mapping (`Table`)
# ----------------------------------------------------------------------

def _csv(path, text):
    path.write_bytes(text.encode())      # (no "\r\n" on Windows)
    return path


def test_table_rows(tmp_path, caplog):
    """By name in each file; bad rows skipped with one warning each;
    a subject listed twice: the first file, its baseline row"""
    table = Table({"participant_id": "SUB", "age": ("AGE", "AGE_AT_SCAN"),
                   "sex": "SEX"}, {"sex": {"1": "M", "2": "F"}},
                  baseline="AGE")
    a = _csv(tmp_path / "a.csv", "SUB,AGE,SEX\n01,30.0,1\n02,40,2\n"
                                 "02,35,1\n,1,1\nx-y,2,2\n")
    b = _csv(tmp_path / "b.csv", "sex,sub\n2,03\n1,01\n")
    with caplog.at_level(logging.WARNING):
        rows = list(table.rows([a, b], "participants.tsv"))
        again = list(table.rows([a, b], "participants.tsv"))
    assert rows == again == [
        ["participant_id", "age", "sex"],
        ["sub-01", "30.0", "M"], ["sub-02", "35", "M"],
        ["sub-03", "n/a", "F"]]
    text = caplog.text
    assert text.count("a.csv, line 4: row skipped (subject 02 listed "
                      "twice)") == 1
    assert text.count("a.csv, line 5: row skipped (no subject)") == 1
    assert text.count("a.csv, line 6: row skipped (subject 'x-y' is not "
                      "a label)") == 1
    assert text.count("b.csv, line 3: row skipped (subject 01 listed "
                      "twice)") == 1


def test_table_cells(tmp_path, caplog):
    """Cells cleaned (warned once per table), absent column warned once,
    a file without the subject column skipped"""
    table = Table({"participant_id": "SUB", "note": "NOTE", "x": "X"},
                  nans=NANS | {"-9999"})
    a = _csv(tmp_path / "a.csv",
             'SUB,NOTE\n1,"say ""hi""\tnow"\n2,-9999\n3,"a\nb"\n')
    c = _csv(tmp_path / "c.csv", "ID\n4\n")
    with caplog.at_level(logging.WARNING):
        for _ in range(2):
            rows = list(table.rows([a, c], "t.tsv"))
    assert rows[1:] == [["sub-1", "say 'hi' now", "n/a"],
                        ["sub-2", "n/a", "n/a"], ["sub-3", "a b", "n/a"]]
    assert caplog.text.count("t.tsv: '\"' replaced by \"'\" in 1 cells, "
                             "tabs or line breaks replaced by a space in "
                             "2 cells (BIDS tables are not quoted)") == 1
    assert caplog.text.count("t.tsv: no CSV file has a column for x "
                             "(n/a): a.csv, c.csv") == 1
    assert caplog.text.count("c.csv: no column SUB: skipped") == 2


# ----------------------------------------------------------------------
#   Writing (`table_action`) and trimming
# ----------------------------------------------------------------------

def test_numeric_columns():
    """Numeric: no `Levels`, and a numeric `Format` or `Units` that are not
    a date or time format"""
    sidecar = {
        "age": {"Description": "Age", "Units": "year"},
        "count": {"Format": "integer"},
        "score": {"Format": "number", "Units": "point"},
        "icd9": {"Description": "Diagnosis code"},
        "sex": {"Levels": {"M": "male"}, "Units": "n/a"},
        "dob": {"Units": "YYYY-MM-DD"},
        "acq_time": {"Units": "hh:mm:ss"},
        "when": {"Units": "ISO 8601"},
        "version": {"Format": "string", "Units": "v"},
        "length": {"Units": "mm"},
        "Description": "not a column",
    }
    assert numeric_columns(sidecar) == {"age", "count", "score", "length"}
    assert numeric_columns(None) == set()


def test_clean_numeric():
    assert [clean(v, numeric=True) for v in (
        "70.0", " 0.50 ", "1e3", "-0", "+5", "007", "295.70", "x", "inf",
        "1.2.3", 30.0)] == [
        "70", "0.5", "1000", "0", "5", "7", "295.7", "x", "inf", "1.2.3",
        "30"]
    assert clean("70.0") == "70.0"               # (not numeric: as stored)


@pytest.mark.parametrize("sidecar", ["dict", "file", "resource"])
def test_table_action_numeric_columns(tmp_path, sidecar):
    """Numbers stored as text: shortest form in a numeric column only (a
    code column keeps `295.70`)"""
    layout = make_layout("flat", tmp_path / "DS")
    raw = layout.raw
    meta = {"age": {"Units": "year"}, "icd9": {"Description": "code"}}
    found = {"dict": meta, "file": tmp_path / "p.json", "resource": True}
    (tmp_path / "p.json").write_text(json.dumps(meta))
    templates = tmp_path / "templates"
    templates.mkdir()
    (templates / "participants.json").write_text(json.dumps(meta))
    rows = [["participant_id", "age", "icd9"],
            ["sub-01", "70.0", "295.70"], ["sub-02", "0.50", "n/a"]]

    def roaster(ctx):
        yield table_action(ctx, raw / "participants.tsv", rows, [],
                           sidecar=found[sidecar])

    ctx = make_context(layout, KEYS, templates=templates)
    assert not run_roast(roaster, ctx, display=False).errors
    assert read_tsv(layout.original(raw / "participants.tsv")) == [
        ["participant_id", "age", "icd9"], ["sub-01", "70", "295.70"],
        ["sub-02", "0.5", "n/a"]]


def test_sidecar_name(tmp_path):
    ctx = make_context(make_layout("study", tmp_path / "DS"), KEYS)
    raw = ctx.layout.raw
    assert [sidecar_name(ctx, raw / p) for p in (
        "participants.tsv", "phenotype/srs.tsv",
        "sub-01/sub-01_sessions.tsv",
        "sub-01/ses-1/sub-01_ses-1_scans.tsv")] == [
        "participants.json", "phenotype/srs.json", "sessions.json",
        "scans.json"]


@pytest.mark.parametrize("kind", LAYOUTS)
def test_table_action_pristine_and_trimmed(tmp_path, kind, caplog):
    """Written to `.brainspresso/orig/` (cells cleaned, warned once), then
    trimmed to the subjects on disk byte for byte"""
    layout = make_layout(kind, tmp_path / "DS")
    raw = layout.raw
    rows = [["participant_id", "age", "note"],
            ["sub-01", 30.0, 'say "hi"'], ["sub-02", 0.5, "a\tb"],
            ["sub-03", None, "x"]]

    def roaster(ctx):
        yield table_action(ctx, raw / "participants.tsv", rows, [])
        yield WriteText("{}", raw / "sub-01" / "anat" / "sub-01_T1w.json")
        yield WriteText("{}", raw / "sub-02" / "anat" / "sub-02_T1w.json")

    with caplog.at_level(logging.WARNING):
        report = run_roast(roaster, make_context(layout, KEYS),
                           display=False)
    assert not report.errors
    pristine = layout.original(raw / "participants.tsv").read_text()
    assert pristine == ("participant_id\tage\tnote\n"
                        "sub-01\t30\tsay 'hi'\n"
                        "sub-02\t0.5\ta b\n"
                        "sub-03\tn/a\tx\n")
    visible = (raw / "participants.tsv").read_text()
    assert visible == pristine.replace("sub-03\tn/a\tx\n", "")
    assert caplog.text.count("participants.tsv: '\"' replaced by \"'\" in "
                             "1 cells, tabs or line breaks replaced by a "
                             "space in 1 cells") == 1


def test_table_action_reads_when_run(tmp_path):
    """A reader that fails is an error of its table only"""
    layout = make_layout("flat", tmp_path / "DS")
    raw = layout.raw

    def fails():
        raise TableError("x.csv: no column 'ID'")

    def roaster(ctx):
        yield table_action(ctx, raw / "participants.tsv", fails, [])
        yield table_action(ctx, raw / "phenotype" / "a.tsv",
                           lambda: [["participant_id"], ["sub-01"]], [],
                           tag="pheno", pristine=False)

    report = run_roast(roaster, make_context(layout, KEYS), display=False)
    assert [(tag, name) for tag, name, _ in report.errors] == [
        ("meta", "participants.tsv")]
    assert "x.csv: no column 'ID'" in report.errors[0][2]
    assert read_tsv(raw / "phenotype" / "a.tsv") == [["participant_id"],
                                                    ["sub-01"]]
    assert not layout.original(raw / "participants.tsv").exists()


# ----------------------------------------------------------------------
#   `attempt`
# ----------------------------------------------------------------------

def test_attempt():
    def reader(value, *, fail=None, name=None, tag=None):
        if fail:
            raise fail
        return value, name, tag

    # (the reader's own `name` and `tag` pass through)
    assert attempt(reader, 1, name="n", tag="t", fail_name="t.tsv",
                   fail_tag="meta") == ((1, "n", "t"), None)
    result, error = attempt(reader, 1, fail=TableError("x.csv: bad"),
                            fail_name="t.tsv", fail_tag="meta")
    assert result is None and isinstance(error, Fail)
    assert (error.message, error.name, error.tag) == ("x.csv: bad",
                                                      "t.tsv", "meta")
    _, error = attempt(reader, 1, fail=OSError("gone"), fail_name="t.tsv",
                       fail_tag="pheno", fail_what="x.tar.gz")
    assert error.message == "x.tar.gz: cannot be read (OSError: gone)"
    _, error = attempt(reader, 1, fail=OSError("gone"), fail_name="t.tsv",
                       fail_tag="pheno")
    assert error.message == "t.tsv: cannot be read (OSError: gone)"
    with pytest.raises(KeyError):
        attempt(reader, 1, fail=KeyError("bug"), fail_name="t.tsv",
                fail_tag="meta")
    with pytest.raises(KeyError):
        attempt(reader, 1, fail=KeyError("bug"), fail_name="t.tsv",
                fail_tag="meta", fail_errors=(OSError,))
