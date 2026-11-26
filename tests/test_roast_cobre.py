"""Characterization tests for the COBRE roast (bidsify) step."""
import json

import nibabel as nib
import numpy as np
import pytest

from brainspresso.datasets.COBRE.bidsifier import Bidsifier
from roast_utils import make_tar, nii_bytes, raw_file, raw_tree, read_tsv

CSV = (
    ",Current Age,Gender,Handedness,Subject Type,Diagnosis\n"
    "0040000,20,Male,Right,Patient,295.3 Schizophrenia\n"
    "0040001,30,Female,Left,Control,None\n"
    "0040002,Disenrolled,Disenrolled,Disenrolled,Disenrolled,Disenrolled\n"
)


@pytest.fixture
def root(tmp_path):
    root = tmp_path / "COBRE"
    src = root / "sourcedata"
    src.mkdir(parents=True)
    (src / "COBRE_phenotypic_data.csv").write_text(CSV)
    make_tar(src / "COBRE_scan_data.tgz", {
        "COBRE/0040000/session_1/anat_1/mprage.nii.gz": nii_bytes(),
        "COBRE/0040000/session_1/rest_1/rest.nii.gz": nii_bytes((2, 2, 2, 3)),
        "COBRE/0040001/session_1/anat_1/mprage.nii.gz": nii_bytes(),
    }, dirs=["COBRE", "COBRE/0040000"])
    return root


RAW = {
    "sub-0040000/anat/sub-0040000_T1w.nii.gz",
    "sub-0040000/anat/sub-0040000_T1w.json",
    "sub-0040000/func/sub-0040000_task-rest_bold.nii.gz",
    "sub-0040000/func/sub-0040000_task-rest_bold.json",
    "sub-0040001/anat/sub-0040001_T1w.nii.gz",
    "sub-0040001/anat/sub-0040001_T1w.json",
}
META = {
    "README", "dataset_description.json",
    "participants.json", "participants.tsv",
}


def test_roast_all(root):
    Bidsifier(root, keys={"meta", "T1w", "func"}).run()
    assert raw_tree(root) == RAW | META
    img = nib.load(raw_file(root, "sub-0040000/func/"
                                  "sub-0040000_task-rest_bold.nii.gz"))
    assert img.shape == (2, 2, 2, 3)
    assert np.allclose(img.affine, np.diag([2., 2., 2., 1.]))
    assert json.loads(
        raw_file(root, "sub-0040000/anat/sub-0040000_T1w.json").read_text()
    ) == json.loads((Bidsifier.TPLDIR / "T1w.json").read_text())


def test_participants(root):
    Bidsifier(root, keys={"meta"}).run()
    assert raw_tree(root) == META
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert rows[0] == ["participant_id", "age", "sex", "handedness",
                       "patient", "icd9", "icd9_subtype"]
    assert rows[1] == ["0040000", "20", "M", "R", "Y", "295.3",
                       "Schizophrenia"]
    assert rows[2][:5] == ["0040001", "30", "F", "L", "N"]
    assert [r[0] for r in rows[1:]] == ["0040000", "0040001", "0040002"]


@pytest.mark.xfail(strict=True, reason="participant_id lacks 'sub-' prefix")
def test_participants_sub_prefix(root):
    Bidsifier(root, keys={"meta"}).run()
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert rows[1][0] == "sub-0040000"


def test_participants_disenrolled(root):
    Bidsifier(root, keys={"meta"}).run()
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert rows[3] == ["0040002"] + ["n/a"] * 6


def test_keys_and_subject_filters(root):
    Bidsifier(root, keys={"T1w"}, exclude_subs=[40001]).run()
    assert raw_tree(root) == {
        "sub-0040000/anat/sub-0040000_T1w.nii.gz",
        "sub-0040000/anat/sub-0040000_T1w.json",
    }


def test_json_only_and_no(root, tmp_path):
    Bidsifier(root, keys={"T1w", "func"}, json="only", subs=[40000]).run()
    assert raw_tree(root) == {p for p in RAW
                              if p.endswith(".json") and "0040000" in p}
    Bidsifier(root, keys={"T1w"}, json="no", subs=[40001]).run()
    assert "sub-0040001/anat/sub-0040001_T1w.nii.gz" in raw_tree(root)
    assert "sub-0040001/anat/sub-0040001_T1w.json" not in raw_tree(root)


def test_missing_archive(tmp_path):
    root = tmp_path / "COBRE"
    (root / "sourcedata").mkdir(parents=True)
    Bidsifier(root, keys={"T1w", "func"}).run()
    assert raw_tree(root) == set()
