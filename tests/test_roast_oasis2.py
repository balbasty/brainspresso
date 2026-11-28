"""Tests of the OASIS-II roast step."""
import json
import os
import re
import sys
import zipfile

import nibabel as nib
import numpy as np
import pytest
from cli_utils import bx
from roast_utils import (
    LAYOUTS,
    SCOPE_SCENARIOS,
    analyze_pair,
    check_keys_rerun,
    check_scope,
    init_layout,
    layout_of,
    make_tar,
    open_spy,
    raw_file,
    raw_tree,
    read_tsv,
)

from brainspresso.actions.state import read_task_states
from brainspresso.datasets.oasis.ii import roaster
from brainspresso.datasets.oasis.ii.dataset import OASIS2, PartOptions
from brainspresso.datasets.oasis.ii.roaster import roast
from brainspresso.datasets.options import KeyOptions
from brainspresso.errors import UsageError

openpyxl = pytest.importorskip("openpyxl")

XLSX = "oasis_longitudinal_demographics.xlsx"
HEADER = ["Subject ID", "MRI ID", "Group", "Visit", "MR Delay", "M/F",
          "Hand", "Age", "EDUC", "SES", "MMSE", "CDR", "eTIV", "nWBV", "ASF"]
ROWS = [
    ["OAS2_0001", "OAS2_0001_MR1", "Nondemented", 1, 0, "M", "R", 87, 14,
     2, 27, 0, 1987, 0.696, 0.883],
    ["OAS2_0001", "OAS2_0001_MR2", "Nondemented", 2, 457, "M", "R", 88, 14,
     2, 30.0, 0.0, 2004, 0.681, 0.876],
    ["OAS2_0002", "OAS2_0002_MR1", "Demented", 1, 0, "F", "R", 75, 12,
     None, None, 0.5, 1678, 0.736, 1.046],
]
META = {"README", "dataset_description.json", "participants.json",
        "participants.tsv", "sessions.json"}
SESSIONS = {"sub-0001/sub-0001_sessions.tsv",
            "sub-0002/sub-0002_sessions.tsv"}
SCANS = [(1, 1, 1), (1, 1, 2), (1, 2, 1), (2, 1, 1)]


def write_xlsx(src, rows=ROWS):
    wb = openpyxl.Workbook()
    ws = wb.active
    for row in [HEADER, *rows]:
        ws.append(row)
    wb.save(src / XLSX)


def part(src, number=1, scans=SCANS, top=True):
    """A part (`top=False`: without the top folder)"""
    m = {}
    for sub, ses, run in scans:
        hdr, img = analyze_pair()
        base = f"OAS2_{sub:04d}_MR{ses}/RAW/mpr-{run}.nifti"
        if top:
            base = f"OAS2_RAW_PART{number}/{base}"
        m[base + ".hdr"] = hdr
        m[base + ".img"] = img
    if (2, 1, 1) in scans:
        # a stray file (a duplicate of mpr-1) is skipped
        m["OAS2_RAW_PART1/OAS2_0002_MR1/RAW/3906-3.nifti.hdr"] = hdr
        m["OAS2_RAW_PART1/OAS2_0002_MR1/RAW/3906-3.nifti.img"] = img
    return make_tar(src / f"OAS2_RAW_PART{number}.tar.gz", m)


def make_source(root):
    """Write the harvested files of OASIS-II (one part)."""
    src = root / "sourcedata"
    src.mkdir(parents=True, exist_ok=True)
    write_xlsx(src)
    part(src)
    return root


@pytest.fixture(params=LAYOUTS)
def root(tmp_path, request):
    root = tmp_path / "OASIS-2"
    init_layout(root, request.param)
    return make_source(root)


def scans(sub, ses, run, ext=("nii.gz", "json")):
    return {
        f"sub-{sub:04d}/ses-{ses}/anat/"
        f"sub-{sub:04d}_ses-{ses}_run-{run}_T1w.{e}" for e in ext
    }


RAW1 = scans(1, 1, 1) | scans(1, 1, 2) | scans(1, 2, 1)
RAW2 = scans(2, 1, 1)
RAW = RAW1 | RAW2


def test_age_units():
    """`age` in years: BIDS unit "year" (not "years")"""
    for name in ('participants.json', 'sessions.json'):
        meta = json.loads((roaster.TEMPLATES / name).read_text())
        assert meta["age"]["Units"] == "year"


def test_roast_raw(root):
    report = roast(root, keys={"raw"}, parts=[1], display=False)
    assert not report.errors
    assert raw_tree(root) == RAW
    f = raw_file(root, "sub-0001/ses-1/anat/sub-0001_ses-1_run-2_T1w.nii.gz")
    img = nib.load(f)
    assert img.shape == (2, 2, 2)
    assert np.array_equal(np.asarray(img.dataobj).ravel(), np.arange(8))
    assert np.allclose(img.affine, roaster.AFFINE_RAW)
    tpl = json.loads((roaster.TEMPLATES / "T1w.json").read_text())
    assert json.loads(raw_file(
        root, "sub-0002/ses-1/anat/sub-0002_ses-1_run-1_T1w.json"
    ).read_text()) == tpl


def test_part_without_top_folder(root):
    """Members `OAS2_<sub>_MR<n>/RAW/...` (no `OAS2_RAW_PART<N>/`)"""
    part(root / "sourcedata", top=False, scans=[(1, 1, 1)])
    roast(root, keys={"raw"})
    assert raw_tree(root) == scans(1, 1, 1)


@pytest.mark.parametrize("prefix", ["./", "./data/OASIS2/", "/"])
def test_member_prefixes(root, prefix):
    """Any prefix before `OAS2_<sub>_MR<n>/RAW/` (`./` included)"""
    hdr, img = analyze_pair()
    base = f"{prefix}OAS2_0001_MR1/RAW/mpr-1.nifti"
    make_tar(root / "sourcedata" / "OAS2_RAW_PART1.tar.gz",
             {base + ".hdr": hdr, base + ".img": img})
    report = roast(root, keys={"raw"}, display=False)
    assert not report.errors
    assert raw_tree(root) == scans(1, 1, 1)


def test_part_without_scans(root):
    """A part without any recognised scan is an error"""
    make_tar(root / "sourcedata" / "OAS2_RAW_PART1.tar.gz",
             {"OAS2_RAW_PART1/README.txt": b"hello"})
    report = roast(root, keys={"raw"}, display=False)
    assert report.counts == {"parts": {"error": 1}}
    # (no scan of the selected subjects: not an error)
    part(root / "sourcedata")
    report = roast(root, keys={"raw"}, subs=[9], display=False)
    assert not report.errors and raw_tree(root) == set()


def test_sessions(root):
    roast(root, keys={"raw", "meta"}, sessions=["1"])
    assert raw_tree(root) == META | SESSIONS | (RAW - scans(1, 2, 1))
    rows = read_tsv(raw_file(root, "sub-0001/sub-0001_sessions.tsv"))
    assert [r[0] for r in rows] == ["session_id", "ses-1"]


def test_json_only_and_exclude(root):
    roast(root, keys={"raw"}, parts=[1], json="only", exclude_subs=[1])
    assert raw_tree(root) == {p for p in RAW2 if p.endswith(".json")}


def test_subject_filter(root):
    """Known bug of the legacy roaster (`_raw_skip_path` lacked a
    `return`: subject filters were ignored)"""
    roast(root, keys={"raw"}, parts=[1], subs=[2])
    assert raw_tree(root) == RAW2


@pytest.mark.parametrize("subs", [["sub-0002"], ["0002"], ["2"], [2],
                                  ["2:3"]])
def test_subject_values(root, subs):
    """`roast(subs=)` takes the values of `--subs`"""
    roast(root, keys={"raw"}, subs=subs)
    assert raw_tree(root) == RAW2


def test_subject_file(root, tmp_path):
    (tmp_path / "subs.txt").write_text("sub-0001\n")
    roast(root, keys={"raw"}, exclude_subs=[str(tmp_path / "subs.txt")])
    assert raw_tree(root) == RAW2


def test_meta(root):
    roast(root, keys={"meta", "raw"})
    assert raw_tree(root) == META | SESSIONS | RAW
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert rows == [["participant_id", "sex", "handedness", "age"],
                    ["sub-0001", "M", "R", "87"],
                    ["sub-0002", "F", "R", "75"]]
    rows = read_tsv(raw_file(root, "sub-0001/sub-0001_sessions.tsv"))
    assert rows[0] == roaster.SESSIONS_HEADER
    assert rows[1:] == [
        ["ses-1", "1", "0", "N", "87", "14", "2", "27", "0", "1987",
         "0.696", "0.883"],
        ["ses-2", "2", "457", "N", "88", "14", "2", "30", "0", "2004",
         "0.681", "0.876"]]
    rows = read_tsv(raw_file(root, "sub-0002/sub-0002_sessions.tsv"))
    # missing values are n/a; CDR 0.5 is a level of sessions.json
    assert rows[1] == ["ses-1", "1", "0", "D", "75", "12", "n/a", "n/a",
                       "0.5", "1678", "0.736", "1.046"]
    levels = json.loads((roaster.TEMPLATES / "sessions.json").read_text())
    assert set(levels) == set(roaster.SESSIONS_HEADER[1:])
    assert {r[8] for r in [*read_tsv(raw_file(
        root, "sub-0001/sub-0001_sessions.tsv"))[1:], rows[1]]} \
        <= set(levels["cdr"]["Levels"])


def test_meta_trimmed(root):
    """The participants and sessions tables list what is on disk; their
    pristine copies (`.brainspresso/orig/`) everything"""
    roast(root, keys={"meta"})
    assert raw_tree(root) == META
    table = raw_file(root, "participants.tsv")
    assert len(read_tsv(table)) == 1
    original = layout_of(root).original(table)
    assert [r[0] for r in read_tsv(original)] == [
        "participant_id", "sub-0001", "sub-0002"]
    sessions = layout_of(root).original(
        raw_file(root, "sub-0001/sub-0001_sessions.tsv"))
    assert len(read_tsv(sessions)) == 3

    part(root / "sourcedata", scans=[(1, 2, 1), (2, 1, 1)])
    roast(root, keys={"raw"}, subs=[1])
    assert raw_tree(root) == META | scans(1, 2, 1) | {
        "sub-0001/sub-0001_sessions.tsv"}
    assert [r[0] for r in read_tsv(table)] == ["participant_id", "sub-0001"]
    rows = read_tsv(raw_file(root, "sub-0001/sub-0001_sessions.tsv"))
    assert [r[0] for r in rows] == ["session_id", "ses-2"]


def test_session_from_mri_id(root):
    """The session is the number of the MRI ID (the folder of the
    scans), not the visit; participants: their first visit"""
    write_xlsx(root / "sourcedata", [
        ["OAS2_0001", "OAS2_0001_MR3", "Converted", 2, 400, "F", "L", 71,
         16, 1, 29, 0.5, 1500, 0.7, 1.1],
        ["OAS2_0001", "OAS2_0001_MR1", "Converted", 1, 0, "F", "L", 70,
         16, 1, 30, 0, 1500, 0.7, 1.1],
    ])
    roast(root, keys={"meta"})
    rows = read_tsv(layout_of(root).original(
        raw_file(root, "sub-0001/sub-0001_sessions.tsv")))
    assert [r[:4] for r in rows[1:]] == [["ses-1", "1", "0", "C"],
                                         ["ses-3", "2", "400", "C"]]
    rows = read_tsv(layout_of(root).original(
        raw_file(root, "participants.tsv")))
    assert rows[1] == ["sub-0001", "F", "L", "70"]


def test_bad_rows(root, caplog):
    """Blank and formatted rows are skipped; short or bad ones with a
    warning; unknown groups are n/a"""
    src = root / "sourcedata"
    write_xlsx(src, [
        *ROWS,
        [None] * 15,
        ["  ", None, None],
        ["OAS2_0003", "OAS2_0003_MR1", "Unknown", 1, 0, "M", "R", 60, 12,
         1, 30, 0, 1500, 0.7, 1.1],
        ["OAS2_0003_MR2", "?", "Demented", "x", 0, "M", "R", 61, 12,
         1, 30, 0, 1500, 0.7, 1.1],
        ["OAS2_0004", "bad", "Demented", None, 0, "M", "R", 61, 12,
         1, 30, 0, 1500, 0.7, 1.1],
    ])
    report = roast(root, keys={"meta"}, display=False)
    assert not report.errors
    rows = read_tsv(layout_of(root).original(
        raw_file(root, "participants.tsv")))
    assert [r[0] for r in rows] == ["participant_id", "sub-0001",
                                    "sub-0002", "sub-0003"]
    rows = read_tsv(layout_of(root).original(
        raw_file(root, "sub-0003/sub-0003_sessions.tsv")))
    assert rows[1][3] == "n/a"
    assert "unknown group 'Unknown'" in caplog.text
    assert "subject ID 'OAS2_0003_MR2' (skipped)" in caplog.text
    assert "no session in MRI ID 'bad'" in caplog.text



def test_short_rows_padded(root, caplog):
    """A sheet with fewer columns: rows padded with n/a"""
    src = root / "sourcedata"
    wb = openpyxl.Workbook()
    wb.active.append(HEADER[:3])
    wb.active.append(["OAS2_0001", "OAS2_0001_MR1", "Demented"])
    wb.save(src / XLSX)
    report = roast(root, keys={"meta"}, display=False)
    assert not report.errors
    assert "columns not found by name, read by position: 'Visit'" \
        in caplog.text
    rows = read_tsv(layout_of(root).original(
        raw_file(root, "sub-0001/sub-0001_sessions.tsv")))
    assert rows[1] == ["ses-1", "n/a", "n/a", "D", *["n/a"] * 8]


def test_trailing_empty_cells_and_stale_dimension(root):
    """Empty trailing cells are n/a; rows past a stale `<dimension>`
    are read"""
    src = root / "sourcedata"
    write_xlsx(src, [
        *ROWS,
        ["OAS2_0003", "OAS2_0003_MR1", "Demented", 1, 0, "F", "L", 70,
         12, 2, 28, 1, None, None, None],
    ])
    path = src / XLSX
    with zipfile.ZipFile(path) as z:
        files = {n: z.read(n) for n in z.namelist()}
    sheet = "xl/worksheets/sheet1.xml"
    # (the tag is written `<dimension ref="..."/>` with lxml, `... />`
    # without; inserted after `<sheetPr>` if absent)
    stale = b'<dimension ref="A1:C2"/>'
    files[sheet], count = re.subn(rb'<dimension\s[^>]*/>', stale,
                                  files[sheet])
    if not count:
        files[sheet], count = re.subn(
            rb'(</sheetPr>|<sheetPr\s[^>]*/>)', rb'\1' + stale,
            files[sheet], count=1)
    assert count == 1 and stale in files[sheet]
    with zipfile.ZipFile(path, "w") as z:
        for name, data in files.items():
            z.writestr(name, data)
    report = roast(root, keys={"meta"}, display=False)
    assert not report.errors
    rows = read_tsv(layout_of(root).original(
        raw_file(root, "participants.tsv")))
    assert [r[0] for r in rows[1:]] == ["sub-0001", "sub-0002", "sub-0003"]
    rows = read_tsv(layout_of(root).original(
        raw_file(root, "sub-0003/sub-0003_sessions.tsv")))
    assert rows[1] == ["ses-1", "1", "0", "D", "70", "12", "2", "28", "1",
                       "n/a", "n/a", "n/a"]


def test_columns_by_name(root):
    """Columns are found by name (case and spaces ignored)"""
    order = [1, 0, *range(2, 15)][::-1]
    write_xlsx(root / "sourcedata", [[row[i] for i in order]
                                     for row in ROWS])
    book = openpyxl.load_workbook(root / "sourcedata" / XLSX)
    for cell, i in zip(book.active[1], order):
        cell.value = HEADER[i].upper().replace(" ", "  ")
    book.save(root / "sourcedata" / XLSX)
    roast(root, keys={"meta"})
    rows = read_tsv(layout_of(root).original(
        raw_file(root, "sub-0002/sub-0002_sessions.tsv")))
    assert rows[1] == ["ses-1", "1", "0", "D", "75", "12", "n/a", "n/a",
                       "0.5", "1678", "0.736", "1.046"]


def test_mri_id_checks(root, caplog):
    """An MRI ID of another subject is skipped; an MRI ID that cannot
    be parsed: the session is the visit (with a warning)"""
    write_xlsx(root / "sourcedata", [
        ["OAS2_0001", "OAS2_0002_MR1", "Demented", 1, 0, "F", "R", 70, 12,
         2, 28, 1, 1500, 0.7, 1.1],
        ["OAS2_0001", "??", "Demented", 2, 300, "F", "R", 71, 12,
         2, 28, 1, 1500, 0.7, 1.1],
    ])
    report = roast(root, keys={"meta"}, display=False)
    assert not report.errors
    assert "MRI ID 'OAS2_0002_MR1' is not of subject 'OAS2_0001' " \
        "(skipped)" in caplog.text
    assert "MRI ID '??': session from the visit (ses-2)" in caplog.text
    rows = read_tsv(layout_of(root).original(
        raw_file(root, "sub-0001/sub-0001_sessions.tsv")))
    assert [r[0] for r in rows] == ["session_id", "ses-2"]


def test_missing_openpyxl(root, monkeypatch):
    monkeypatch.setitem(sys.modules, "openpyxl", None)
    report = roast(root, keys={"meta"}, display=False)
    assert report.counts["meta"]["error"] == 1
    assert any("pip install brainspresso[oasis2]" in str(e)
               for e in report.errors)


def test_missing_spreadsheet(root):
    (root / "sourcedata" / XLSX).unlink()
    report = roast(root, keys={"meta"}, display=False)
    assert report.counts["meta"] == {"done": 4, "error": 1}
    assert raw_tree(root) == META - {"participants.tsv"}


def test_unreadable_spreadsheet(root):
    (root / "sourcedata" / XLSX).write_bytes(b"not a spreadsheet")
    report = roast(root, keys={"meta"}, display=False)
    assert report.counts["meta"]["error"] == 1


def test_spreadsheet_edit_updates_tables(root):
    roast(root, keys={"meta", "raw"}, display=False)
    xlsx = root / "sourcedata" / XLSX
    book = openpyxl.load_workbook(xlsx)
    book.active["F2"] = "F"
    book.save(xlsx)
    report = roast(root, keys={"meta", "raw"}, display=False)
    assert not report.errors
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert rows[1][:2] == ["sub-0001", "F"]


# ----------------------------------------------------------------------
#   Parts
# ----------------------------------------------------------------------

def test_missing_part(root, caplog):
    """A missing part named with `--parts` is an error, other missing
    parts are skipped with a warning"""
    report = roast(root, keys={"raw"}, parts=[2], display=False)
    assert raw_tree(root) == set()
    assert report.counts == {"parts": {"error": 1}}
    report = roast(root, keys={"raw"}, display=False)
    assert not report.errors
    assert raw_tree(root) == RAW
    assert "OAS2_RAW_PART2.tar.gz not found (skipped)" in caplog.text


def test_two_parts(root):
    part(root / "sourcedata", 2, scans=[(3, 1, 1)])
    report = roast(root, keys={"raw"}, display=False)
    assert not report.errors
    assert raw_tree(root) == RAW | scans(3, 1, 1)
    states = read_task_states(layout_of(root).state)
    assert {"roast/parts/part1", "roast/parts/part2"} <= set(states)


def test_unreadable_part(root):
    """A truncated part is an error; the other part is roasted"""
    src = root / "sourcedata"
    part(src, 2, scans=[(3, 1, 1)])
    data = (src / "OAS2_RAW_PART1.tar.gz").read_bytes()
    (src / "OAS2_RAW_PART1.tar.gz").write_bytes(data[:len(data) // 2])
    report = roast(root, keys={"raw"}, display=False)
    assert report.counts["parts"]["error"] == 1
    assert raw_tree(root) >= scans(3, 1, 1)


def test_unreadable_part_is_closed(root, monkeypatch):
    closed = []
    real = roaster.TarArchive

    class Spy(real):
        def getmembers(self):
            raise EOFError("truncated")

        def __exit__(self, *args):
            closed.append(self.path.name)
            return super().__exit__(*args)

    monkeypatch.setattr(roaster, "TarArchive", Spy)
    report = roast(root, keys={"raw"}, parts=[1], display=False)
    assert report.counts == {"parts": {"error": 1}}
    assert closed == ["OAS2_RAW_PART1.tar.gz"]


def test_members_read_in_archive_order(root, monkeypatch):
    """One forward pass: members are extracted in archive order"""
    read = []
    real = roaster.TarArchive

    class Spy(real):
        def extractfile(self, member):
            read.append(self.getmember(member).offset)
            return super().extractfile(member)

    monkeypatch.setattr(roaster, "TarArchive", Spy)
    report = roast(root, keys={"raw"}, display=False)
    assert not report.errors
    assert len(read) == 2 * len(SCANS) and read == sorted(read)


def test_missing_member(root):
    """A missing `.hdr` fails its own conversion only"""
    m = {}
    for sub, ses, run in [(1, 1, 1), (2, 1, 1)]:
        hdr, img = analyze_pair()
        base = f"OAS2_RAW_PART1/OAS2_{sub:04d}_MR{ses}/RAW/mpr-{run}.nifti"
        m[base + ".img"] = img
        if sub == 1:
            m[base + ".hdr"] = hdr
    make_tar(root / "sourcedata" / "OAS2_RAW_PART1.tar.gz", m)
    report = roast(root, keys={"raw"}, display=False)
    assert report.counts["raw"] == {"done": 3, "error": 1}
    assert raw_tree(root) == scans(1, 1, 1) | scans(2, 1, 1, ("json",))

    # `--sidecars only`: not an error (no data)
    report = roast(root, keys={"raw"}, json="only", display=False)
    assert not report.errors


def test_missing_sidecar_template(root, monkeypatch):
    """A missing sidecar template is an error only if sidecars are
    written"""
    monkeypatch.setattr(roaster.RoastContext, "resource",
                        lambda self, name: None)
    report = roast(root, keys={"raw"}, json="no", display=False)
    assert not report.errors
    assert raw_tree(root) == {p for p in RAW if p.endswith(".nii.gz")}
    report = roast(root, keys={"raw"}, display=False)
    assert report.counts["raw"]["error"] == len(SCANS)


@pytest.mark.parametrize("parts", [[0], [3], [1, 4]])
def test_parts_out_of_range(root, parts):
    with pytest.raises(UsageError, match="parts are 1 and 2"):
        roast(root, keys={"raw"}, parts=parts)
    with pytest.raises(UsageError, match="parts are 1 and 2"):
        OASIS2().execute("roast", out=root,
                         parts=PartOptions(parts=tuple(parts)))


def test_parts_named_all(root):
    """`--parts 1 2`: the missing part is an error"""
    ds = OASIS2()
    groups = {"keys": KeyOptions(keys=("raw",)),
              "parts": PartOptions(parts=(1, 2))}
    report = ds.execute("roast", out=root, **groups)
    assert report.counts["parts"] == {"error": 1}
    report = ds.execute("roast", out=root, keys=KeyOptions(keys=("raw",)))
    assert not report.errors


def test_meta_opens_no_part(root, monkeypatch):
    opened = []
    real = roaster.TarArchive

    def opener(path, *a, **k):
        opened.append(path.name)
        return real(path, *a, **k)

    monkeypatch.setattr(roaster, "TarArchive", opener)
    report = roast(root, keys={"meta"}, display=False)
    assert not report.errors and opened == []


@pytest.mark.parametrize("scenario", SCOPE_SCENARIOS)
def test_scope_of_parts(root, monkeypatch, scenario):
    """`--subs` narrowing and `meta` skip the parts, a new subject
    re-reads the part that holds it only (`roast.scope`)"""
    part(root / "sourcedata", number=2, scans=[(3, 1, 1)])
    opened = open_spy(monkeypatch, roaster)
    check_scope(
        scenario, root, lambda **kw: roast(root, display=False, **kw),
        opened, a=1, b=3, label_a="0001", label_b="0003",
        archives={
            "OAS2_RAW_PART1.tar.gz": ({"0001", "0002"}, "roast/parts/part1"),
            "OAS2_RAW_PART2.tar.gz": ({"0003"}, "roast/parts/part2")},
        keys={"meta", "raw"}, key="raw", absent=9)


def test_keys_of_parts(root):
    """Adding a key that a part shard reads re-runs it"""
    check_keys_rerun(root, lambda **kw: roast(root, display=False, **kw),
                     {"meta", "raw"}, {"roast/parts/": ("raw",)})


def test_rerun_skips_parts(root, monkeypatch):
    """Under `skip`, an unchanged part is not read again; a changed part
    or template runs it again"""
    report = roast(root, keys={"raw"}, display=False)
    assert report.counts["raw"] == {"done": 8}
    calls = []
    real = roaster.roast_part

    def spy(ctx, path):
        calls.append(path.name)
        return real(ctx, path)

    monkeypatch.setattr(roaster, "roast_part", spy)
    report = roast(root, keys={"raw"}, display=False)
    assert calls == [] and report.counts == {"raw": {"skipped": 8}}

    tarball = root / "sourcedata" / "OAS2_RAW_PART1.tar.gz"
    mtime = tarball.stat().st_mtime - 3600
    os.utime(tarball, (mtime, mtime))
    roast(root, keys={"raw"}, display=False)
    assert len(calls) == 1

    calls.clear()
    (root / "sourcedata" / "T1w.json").write_text('{"EchoTime": 1}')
    roast(root, keys={"raw"}, display=False)
    assert len(calls) == 1
    roast(root, keys={"raw"}, ifexists="overwrite", display=False)
    sidecar = raw_file(root, "sub-0001/ses-1/anat/sub-0001_ses-1_run-1_"
                             "T1w.json")
    assert json.loads(sidecar.read_text()) == {"EchoTime": 1}


def test_roast_parts_in_processes(root):
    part(root / "sourcedata", 2, scans=[(3, 1, 1)])
    report = roast(root, jobs=2, display=False)
    assert not report.errors
    assert raw_tree(root) == META | SESSIONS | RAW | scans(3, 1, 1)


# ----------------------------------------------------------------------
#   Command line
# ----------------------------------------------------------------------

def cli(root, *args):
    bx("oasis2", "roast", "--out", root, *args)


def test_cli_keys_and_subjects(root):
    cli(root, "--keys", "raw", "--subs", "1")
    assert raw_tree(root) == RAW1
    # (the sessions tables of excluded subjects are not written)
    cli(root, "--exclude-keys", "raw", "--exclude-subs", "sub-0001")
    assert raw_tree(root) == META | RAW1
    cli(root, "--subs", "2:3")
    assert raw_tree(root) == META | RAW | {"sub-0002/sub-0002_sessions.tsv"}
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert [r[0] for r in rows] == ["participant_id", "sub-0001", "sub-0002"]


def test_cli_subject_file(root, tmp_path):
    (tmp_path / "subs.txt").write_text("0002\n")
    cli(root, "--keys", "raw", "--subs", str(tmp_path / "subs.txt"))
    assert raw_tree(root) == RAW2


def test_cli_sidecars(root):
    cli(root, "--keys", "raw", "--sidecars", "no")
    assert raw_tree(root) == {p for p in RAW if p.endswith(".nii.gz")}
    cli(root, "--keys", "raw", "--sidecars", "only")
    assert raw_tree(root) == RAW


def test_cli_sessions(root):
    cli(root, "--keys", "raw", "--sessions", "ses-2")
    assert raw_tree(root) == scans(1, 2, 1)


def test_cli_parts(root):
    cli(root, "--keys", "raw", "--parts", "2")
    assert raw_tree(root) == set()
    part(root / "sourcedata", 2, scans=[(3, 1, 1)])
    cli(root, "--keys", "raw", "--parts", "2")
    assert raw_tree(root) == scans(3, 1, 1)


@pytest.mark.parametrize("layout", LAYOUTS)
def test_cli_layout(tmp_path, layout):
    root = tmp_path / "OASIS-2"
    part(root / "sourcedata", scans=[(1, 1, 1)])
    cli(root, "--keys", "raw", "--layout", layout)
    assert layout_of(root).kind == layout
    assert raw_tree(root) == scans(1, 1, 1)
    assert (root / "rawbids").is_dir() == (layout == "study")
