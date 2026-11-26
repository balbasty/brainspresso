"""Characterization tests for the OASIS-II roast step."""
import json

import nibabel as nib
import numpy as np
import pytest
from roast_utils import (
    LAYOUTS,
    analyze_pair,
    init_layout,
    make_tar,
    raw_file,
    raw_tree,
    read_tsv,
)

openpyxl = pytest.importorskip("openpyxl")
from brainspresso.datasets.OASIS.II.bidsifier import Bidsifier  # noqa: E402

HEADER = ["Subject ID", "MRI ID", "Group", "Visit", "MR Delay", "M/F",
          "Hand", "Age", "EDUC", "SES", "MMSE", "CDR", "eTIV", "nWBV", "ASF"]
ROWS = [
    ["OAS2_0001", "OAS2_0001_MR1", "Nondemented", 1, 0, "M", "R", 87, 14,
     2, 27, 0, 1987, 0.696, 0.883],
    ["OAS2_0001", "OAS2_0001_MR2", "Nondemented", 2, 457, "M", "R", 88, 14,
     2, 30, 0, 2004, 0.681, 0.876],
    ["OAS2_0002", "OAS2_0002_MR1", "Demented", 1, 0, "F", "R", 75, 12,
     3, 23, 0.5, 1678, 0.736, 1.046],
]


@pytest.fixture(params=LAYOUTS)
def root(tmp_path, request):
    root = tmp_path / "OASIS-2"
    init_layout(root, request.param)
    src = root / "sourcedata"
    src.mkdir(parents=True)
    wb = openpyxl.Workbook()
    ws = wb.active
    for row in [HEADER, *ROWS]:
        ws.append(row)
    wb.save(src / "oasis_longitudinal_demographics.xlsx")
    m = {}
    for sub, ses, run in [(1, 1, 1), (1, 1, 2), (1, 2, 1), (2, 1, 1)]:
        hdr, img = analyze_pair()
        base = f"OAS2_RAW_PART1/OAS2_{sub:04d}_MR{ses}/RAW/mpr-{run}.nifti"
        m[base + ".hdr"] = hdr
        m[base + ".img"] = img
    m["OAS2_RAW_PART1/OAS2_0002_MR1/RAW/3906-3.nifti.hdr"] = hdr
    m["OAS2_RAW_PART1/OAS2_0002_MR1/RAW/3906-3.nifti.img"] = img
    make_tar(src / "OAS2_RAW_PART1.tar.gz", m)
    return root


def scans(sub, ses, run, ext=("nii.gz", "json")):
    return {
        f"sub-{sub:04d}/ses-{ses}/anat/"
        f"sub-{sub:04d}_ses-{ses}_run-{run}_T1w.{e}" for e in ext
    }


RAW = (scans(1, 1, 1) | scans(1, 1, 2) | scans(1, 2, 1) | scans(2, 1, 1)
       | {"sub-0001/sub-0001_sessions.tsv", "sub-0002/sub-0002_sessions.tsv"})


def test_roast_raw(root):
    Bidsifier(root, keys={"raw"}, parts=[1]).run()
    assert raw_tree(root) == RAW
    f = raw_file(root, "sub-0001/ses-1/anat/sub-0001_ses-1_run-2_T1w.nii.gz")
    img = nib.load(f)
    assert img.shape == (2, 2, 2)
    assert np.array_equal(np.asarray(img.dataobj).ravel(), np.arange(8))
    assert np.allclose(img.affine, Bidsifier.AFFINE_RAW)
    tpl = json.loads((Bidsifier.TPLDIR / "T1w.json").read_text())
    assert json.loads(raw_file(
        root, "sub-0002/ses-1/anat/sub-0002_ses-1_run-1_T1w.json"
    ).read_text()) == tpl
    rows = read_tsv(raw_file(root, "sub-0001/sub-0001_sessions.tsv"))
    assert rows[0] == Bidsifier.SESSION_HEADER
    assert [r[:3] for r in rows[1:]] == [["ses-1", "0", "N"],
                                         ["ses-2", "457", "N"]]


def test_json_only_and_exclude(root):
    Bidsifier(root, keys={"raw"}, parts=[1], json="only",
              exclude_subs=[1]).run()
    assert "sub-0002/ses-1/anat/sub-0002_ses-1_run-1_T1w.json" \
        in raw_tree(root)
    assert not any(p.endswith(".nii.gz") for p in raw_tree(root))


@pytest.mark.xfail(strict=True, reason="_raw_skip_path evaluates `True` "
                   "instead of `return True`, so subject filters are ignored")
def test_subject_filter(root):
    Bidsifier(root, keys={"raw"}, parts=[1], subs=[2]).run()
    assert not any(p.startswith("sub-0001") for p in raw_tree(root))


def test_meta(root):
    Bidsifier(root, keys={"meta"}).run()
    assert raw_tree(root) == {"README", "dataset_description.json",
                              "participants.json", "participants.tsv",
                              "sessions.json"}
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert rows == [["participant_id", "sex", "handedness", "age"],
                    ["sub-0001", "M", "R", "87"],
                    ["sub-0002", "F", "R", "75"]]


def test_missing_part(root):
    Bidsifier(root, keys={"raw"}, parts=[2]).run()
    assert raw_tree(root) == set()
