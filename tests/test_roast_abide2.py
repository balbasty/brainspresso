"""Characterization tests for the ABIDE-II roast (bidsify) step."""
import json

import nibabel as nib
import pytest

from brainspresso.datasets.ABIDE.II.bidsifier import Bidsifier
from roast_utils import make_tar, nii_bytes, raw_file, raw_tree, read_tsv

PHEADER = Bidsifier.ParticipantsMapper.header
SRS = list(Bidsifier.SRSMapper.header.values())
BVAL = b"0 1000 1000\n"


def subject_members(id, rest=True):
    b = f"ABIDEII-NYU_1/{id}/session_1/"
    m = {b + "anat_1/anat.nii.gz": nii_bytes()}
    if rest:
        m[b + "rest_1/rest.nii.gz"] = nii_bytes((2, 2, 2, 3))
        m[b + "dti_1/dti.nii.gz"] = nii_bytes((2, 2, 2, 3))
        m[b + "dti_1/dti.bval"] = BVAL
        m[b + "dti_1/dti.bvec"] = b"0 1 0\n0 0 1\n0 0 0\n"
    return m


def write_csv(path, cols, rows):
    path.write_text("\n".join(
        ",".join(r.get(c, "") for c in cols) for r in [
            {c: c for c in cols}, *rows]) + "\n")


@pytest.fixture
def root(tmp_path):
    root = tmp_path / "ABIDE-2"
    src = root / "sourcedata"
    src.mkdir(parents=True)
    # Real column is CURRENT_MED_STATUS; the mapper expects a typo'd
    # name, so provide both.
    cols = list(dict.fromkeys(PHEADER.values())) + ["CURRENT_MED_STATUS"]
    write_csv(src / "Phenotypic_V1_0b.csv", cols, [
        {"SUB_ID": "29001", "SITE_ID": "ABIDE-II_KKI_1", "SEX": "1",
         "DX_GROUP": "2", "AGE_AT_SCAN": "10.5", "FIQ": "-9999",
         "HANDEDNESS_CATEGORY": "Ambi", "FIQ_TEST_TYPE": "WISC-IV",
         "CURRENT_MED_`STATUS": "1"},
        {"SUB_ID": "29002", "SITE_ID": "ABIDE-II_NYU_2", "SEX": "2",
         "DX_GROUP": "1", "AGE_AT_SCAN": "12"},
    ])
    write_csv(src / "ABIDEII-NYU_1.csv", ["SUB_ID", *SRS], [
        {"SUB_ID": "29001", "SRS_VERSION": "1", "SRS_TOTAL_RAW": "42"},
    ])
    return root


@pytest.fixture
def tar_files(root):
    """Archive with file members only (no directory entries)."""
    return make_tar(root / "sourcedata" / "ABIDEII-NYU_1.tar.gz",
                    {**subject_members(29001), **subject_members(29002)})


def scans(id):
    b = f"sub-{id}/ses-01"
    return {
        f"{b}/anat/sub-{id}_ses-01_T1w.nii.gz",
        f"{b}/anat/sub-{id}_ses-01_T1w.json",
        f"{b}/func/sub-{id}_ses-01_task-rest_bold.nii.gz",
        f"{b}/func/sub-{id}_ses-01_task-rest_bold.json",
        f"{b}/dwi/sub-{id}_ses-01_dwi.nii.gz",
        f"{b}/dwi/sub-{id}_ses-01_dwi.json",
        f"{b}/dwi/sub-{id}_ses-01_dwi.bval",
        f"{b}/dwi/sub-{id}_ses-01_dwi.bvec",
    }


def raw_only(root):
    return {p for p in raw_tree(root) if p.startswith("sub-")}


def test_roast_raw(root, tar_files):
    Bidsifier(root, keys={"T1w", "rest", "dti"}).run()
    assert raw_tree(root) == scans(29001) | scans(29002)
    assert raw_file(
        root, "sub-29001/ses-01/dwi/sub-29001_ses-01_dwi.bval"
    ).read_bytes() == BVAL
    img = nib.load(raw_file(
        root, "sub-29002/ses-01/func/sub-29002_ses-01_task-rest_bold.nii.gz"))
    assert img.shape == (2, 2, 2, 3)
    tpl = json.loads((Bidsifier.TPLDIR / "NYU/1/T1w.json").read_text())
    assert json.loads(raw_file(
        root, "sub-29001/ses-01/anat/sub-29001_ses-01_T1w.json"
    ).read_text()) == tpl


def test_json_only_and_no(root, tar_files):
    Bidsifier(root, keys={"T1w", "rest", "dti"}, json="only").run()
    assert raw_tree(root) == {
        p for p in scans(29001) | scans(29002) if p.endswith(".json")}
    (root / "rawdata").rename(root / "old")
    Bidsifier(root, keys={"T1w", "rest", "dti"}, json="no").run()
    assert raw_only(root) == {
        p for p in scans(29001) | scans(29002) if not p.endswith(".json")}


@pytest.mark.xfail(strict=True, reason="_make_raw uses `return` instead of "
                   "`continue`: a directory entry ends the archive")
def test_roast_raw_with_dir_entries(root):
    make_tar(root / "sourcedata" / "ABIDEII-NYU_1.tar.gz",
             subject_members(29001),
             dirs=["ABIDEII-NYU_1", "ABIDEII-NYU_1/29001"])
    Bidsifier(root, keys={"T1w", "rest", "dti"}).run()
    assert raw_tree(root) == scans(29001)


@pytest.mark.xfail(strict=True, reason="_make_raw uses `return` instead of "
                   "`continue`: a filtered-out subject ends the archive")
def test_subject_filter(root, tar_files):
    Bidsifier(root, keys={"T1w", "rest", "dti"}, subs={29002}).run()
    assert raw_tree(root) == scans(29002)


@pytest.mark.xfail(strict=True, reason="_run builds rawkeys from allleaves, "
                   "ignoring keys/exclude_keys")
def test_key_filter(root, tar_files):
    Bidsifier(root, keys={"T1w"}).run()
    assert raw_tree(root) == {
        p for p in scans(29001) | scans(29002) if "/anat/" in p}


def test_participants(root):
    Bidsifier(root, keys={"meta"}).run()
    tree = raw_tree(root)
    assert {"README", "dataset_description.json", "participants.json",
            "participants.tsv", "phenotype/srs.json"} <= tree
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert rows[0] == list(PHEADER)
    assert [r[0] for r in rows[1:]] == ["29001", "29002"]
    row = dict(zip(rows[0], rows[1]))
    assert {k: row[k] for k in ("site", "sample", "sex", "age", "handedness",
                                "group", "fiq", "fiq_type", "med_status",
                                "bmi")} == {
        "site": "KKI", "sample": "KKI", "sex": "M", "age": "10.5",
        "handedness": "B", "group": "C", "fiq": "n/a", "fiq_type": "WISC-4",
        "med_status": "Y", "bmi": "n/a"}
    assert dict(zip(rows[0], rows[2]))["site"] == "NYU"


def test_phenotype(root):
    # Phenotype tables are built from *all* sourcedata/*.csv, so the
    # participants CSV (which lacks SRS columns) must be absent.
    (root / "sourcedata" / "Phenotypic_V1_0b.csv").unlink()
    Bidsifier(root, keys={"pheno"}).run()
    assert "participants.tsv" not in raw_tree(root)
    assert json.loads(raw_file(root, "phenotype/srs.json").read_text()) \
        == json.loads((Bidsifier.TPLDIR / "phenotype/srs.json").read_text())
    srs = read_tsv(raw_file(root, "phenotype/srs.tsv"))
    assert srs[0] == list(Bidsifier.SRSMapper.header)
    assert srs[1][:4] == ["29001", "C", "n/a", "42"]


@pytest.mark.xfail(strict=True, reason="participant_id lacks 'sub-' prefix")
def test_participants_sub_prefix(root):
    Bidsifier(root, keys={"meta"}).run()
    assert read_tsv(raw_file(root, "participants.tsv"))[1][0] == "sub-29001"


@pytest.mark.xfail(strict=True, reason="ParticipantsMapper reads column "
                   "'CURRENT_MED_`STATUS' (stray backtick)")
def test_participants_med_status_column(root):
    src = root / "sourcedata" / "Phenotypic_V1_0b.csv"
    cols = [c for c in dict.fromkeys(PHEADER.values()) if "`" not in c]
    write_csv(src, cols + ["CURRENT_MED_STATUS"], [
        {"SUB_ID": "29001", "SITE_ID": "ABIDE-II_KKI_1",
         "CURRENT_MED_STATUS": "0"}])
    Bidsifier(root, keys={"meta"}).run()
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert dict(zip(rows[0], rows[1]))["med_status"] == "N"
