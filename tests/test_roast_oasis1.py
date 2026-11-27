"""Characterization tests for the OASIS-I roast step."""
import json

import nibabel as nib
import numpy as np
import pytest
from roast_utils import (
    LAYOUTS,
    analyze_pair,
    init_layout,
    layout_of,
    make_tar,
    raw_file,
    raw_tree,
    read_tsv,
)

openpyxl = pytest.importorskip("openpyxl")
from brainspresso.datasets.oasis.i.bidsifier import Bidsifier  # noqa: E402

DRV = "oasis-processed"


def drv(root):
    """Root of the processed-images derivative dataset"""
    return layout_of(root).derivative(DRV).root


def _analyze(members, base, data=None):
    hdr, img = analyze_pair(data=data)
    members[base + ".hdr"] = hdr
    members[base + ".img"] = img


@pytest.fixture(params=LAYOUTS)
def root(tmp_path, request):
    root = tmp_path / "OASIS-1"
    init_layout(root, request.param)
    src = root / "sourcedata"
    src.mkdir(parents=True)
    m = {}
    for id in (1, 2):
        s = f"disc1/OAS1_{id:04d}_MR1"
        for run in (1, 2):
            _analyze(m, f"{s}/RAW/OAS1_{id:04d}_MR1_mpr-{run}_anon")
        _analyze(m, f"{s}/PROCESSED/MPRAGE/SUBJ_111/"
                    f"OAS1_{id:04d}_MR1_mpr_n4_anon_sbj_111")
        for sfx in ("gfc", "masked_gfc"):
            _analyze(m, f"{s}/PROCESSED/MPRAGE/T88_111/"
                        f"OAS1_{id:04d}_MR1_mpr_n4_anon_111_t88_{sfx}")
        _analyze(m, f"{s}/FSL_SEG/"
                    f"OAS1_{id:04d}_MR1_mpr_n4_anon_111_t88_masked_gfc_fseg",
                 data=np.array([0, 1, 2, 3] * 2, dtype=np.int16)
                 .reshape(2, 2, 2))
    # repeat session is skipped
    _analyze(m, "disc1/OAS1_0002_MR2/RAW/OAS1_0002_MR2_mpr-1_anon")
    make_tar(src / "oasis_cross-sectional_disc1.tar.gz", m)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["ID", "M/F", "Hand", "Age", "Educ", "SES", "MMSE", "CDR",
               "eTIV", "nWBV", "ASF", "Delay"])
    ws.append(["OAS1_0001_MR1", "F", "R", 74, 2, 3, 29, 0, 1344, 0.743,
               1.306, "N/A"])
    ws.append(["OAS1_0002_MR1", "M", "R", 55, "N/A", "", 30, 0, 1147,
               0.81, 1.531, "N/A"])
    ws.append(["OAS1_0002_MR2", "M", "R", 55, 4, 1, 30, 0, 1147,
               0.81, 1.531, 1])
    wb.save(src / "oasis_cross-sectional.xlsx")
    return root


def raw_runs(id):
    return {
        f"sub-{id:04d}/anat/sub-{id:04d}_run-{r}_T1w.{e}"
        for r in (1, 2) for e in ("nii.gz", "json")
    }


def test_roast_raw(root):
    Bidsifier(root, keys={"raw"}, discs=[1]).run()
    assert raw_tree(root) == raw_runs(1) | raw_runs(2)
    img = nib.load(raw_file(root, "sub-0001/anat/sub-0001_run-1_T1w.nii.gz"))
    assert img.shape == (2, 2, 2)
    assert np.array_equal(np.asarray(img.dataobj).ravel(), np.arange(8))
    tpl = json.loads((Bidsifier.TPLDIR / "T1w.json").read_text())
    assert json.loads(raw_file(
        root, "sub-0001/anat/sub-0001_run-1_T1w.json").read_text()) == tpl


def test_roast_raw_filters(root):
    Bidsifier(root, keys={"raw"}, discs=[1], exclude_subs=[1],
              json="only").run()
    assert raw_tree(root) == {p for p in raw_runs(2) if p.endswith("json")}


def test_missing_disc(root):
    Bidsifier(root, keys={"raw"}, discs=[2]).run()
    assert raw_tree(root) == set()


def test_meta(root):
    Bidsifier(root, keys={"meta"}).run()
    assert raw_tree(root) == {"README", "dataset_description.json",
                              "participants.json", "participants.tsv"}
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert [r[0] for r in rows] == ["participant_id", "sub-0001", "sub-0002"]
    assert rows[1][:4] == ["sub-0001", "F", "R", "74"]
    assert rows[2][4] == "n/a"


@pytest.mark.xfail(strict=True, reason="empty xlsx cells are read as None "
                   "and written as '' instead of 'n/a'")
def test_meta_empty_cell(root):
    Bidsifier(root, keys={"meta"}).run()
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert rows[2][5] == "n/a"


@pytest.mark.xfail(strict=True, reason="PARTICIPANTS_HEADER misses a comma: "
                   "'handedness' 'age' -> 'handednessage'")
def test_meta_header(root):
    Bidsifier(root, keys={"meta"}).run()
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert rows[0] == ["participant_id", "sex", "handedness", "age", "educ",
                       "ses", "mmse", "cdr", "etiv", "nwbv", "asf"]
    assert len(rows[0]) == len(rows[1])


def test_avg(root):
    Bidsifier(root, keys={"avg"}, discs=[1], subs=[1]).run()
    assert raw_tree(root) == set()
    folder = drv(root)
    files = {p.relative_to(folder).as_posix()
             for p in folder.rglob("*") if p.is_file()}
    assert files == {"sub-0001/anat/sub-0001_res-1mm_T1w.nii.gz",
                     "sub-0001/anat/sub-0001_res-1mm_T1w.json"}
    meta = json.loads(
        (folder / "sub-0001/anat/sub-0001_res-1mm_T1w.json").read_text())
    assert meta["BiasCorrected"] == "N4"
    assert len(meta["Sources"]) == 2
    assert nib.load(
        folder / "sub-0001/anat/sub-0001_res-1mm_T1w.nii.gz"
    ).shape == (2, 2, 2)


@pytest.mark.xfail(strict=True, reason="avg/tal 'Sources' miss the '_' "
                   "between sub-XXXX and run-X")
def test_avg_sources(root):
    Bidsifier(root, keys={"avg"}, discs=[1], subs=[1], json="only").run()
    meta = json.loads((drv(root) / "sub-0001/anat/"
                       "sub-0001_res-1mm_T1w.json").read_text())
    assert meta["Sources"][0] == \
        "bids:raw:sub-0001/anat/sub-0001_run-1_T1w.nii.gz"


@pytest.mark.xfail(strict=True, reason="do_tal checks for the member name "
                   "without '.img' extension, so it is never found")
def test_tal(root):
    Bidsifier(root, keys={"tal"}, discs=[1], subs=[1]).run()
    anat = drv(root) / "sub-0001/anat"
    assert (anat / "sub-0001_space-Talairach_res-1mm_T1w.nii.gz").exists()
    assert (anat / "sub-0001_space-Talairach_res-1mm_desc-skullstripped"
                   "_T1w.nii.gz").exists()


@pytest.mark.xfail(strict=True, reason="tar2nii(relabel=True) saves the "
                   "relabelled volume to dst instead of niipath")
def test_fsl_relabel(root):
    Bidsifier(root, keys={"fsl"}, discs=[1], subs=[1]).run()
    f = (drv(root) / "sub-0001/anat"
         / "sub-0001_space-Talairach_res-1mm_dseg.nii.gz")
    dat = np.asarray(nib.load(f).dataobj).ravel()
    assert list(dat[:4]) == [0, 2, 3, 1]
