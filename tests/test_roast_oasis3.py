"""Characterization tests for the OASIS-III roast (bidsify) step."""
import json

import nibabel as nib
import pytest

from brainspresso.datasets.OASIS.III.bidsifier import Bidsifier
from roast_utils import (
    LAYOUTS, init_layout, make_tar, nii_bytes, raw_file, raw_tree, read_tsv,
)

DEMO = (
    "OASISID,GENDER,HAND,AgeatEntry,AgeatDeath,EDUC,SES,racecode,AIAN,NHPI,"
    "ASIAN,AA,WHITE,daddem,momdem,APOE\n"
    "OAS30001,2,R,65.1,,12,2,5,0,0,0,0,1,0,1,33\n"
    "OAS30002,1,L,70.0,80.5,16,,2,0,0,1,0,0,5,0,34\n"
)
CDR = (
    "OASISID,OASIS_session_label,days_to_visit,age at visit,CDRTOT,dx1,"
    "dx1_code\n"
    "OAS30001,OAS30001_UDSb4_d0000,0,65.1,0,Cognitively normal,1\n"
    "OAS30001,OAS30001_UDSb4_d0339,339,66.0,0.5,AD Dementia,2\n"
    "OAS30002,OAS30002_UDSb4_d0000,0,70.0,,.,3\n"
)
SIDECAR = {"MagneticFieldStrength": 3}

T1 = "sub-0001/ses-d0129/anat/sub-0001_ses-d0129_T1w"
T2 = "sub-0002/ses-d0000/anat/sub-0002_ses-d0000_T2w"
META = {"README", "dataset_description.json", "participants.json",
        "participants.tsv", "sessions.json"}
PHENO = {"phenotype/UDS_b4_cdr.tsv", "phenotype/UDS_b4_cdr.json"}


@pytest.fixture(params=LAYOUTS)
def root(tmp_path, request):
    root = tmp_path / "OASIS-3"
    init_layout(root, request.param)
    src = root / "sourcedata"
    d = src / "OASIS3_data_files"
    make_tar(d / "demo.tar.gz", {"demo/demo.csv": DEMO.encode()})
    make_tar(d / "UDSb4.tar.gz", {"b4/cdr.csv": CDR.encode()})
    for sub, ses, mod in [(1, "d0129", "T1w"), (2, "d0000", "T2w")]:
        b = f"OAS3{sub:04d}_MR_{ses}/anat1/NIFTI/sub-OAS3{sub:04d}_sess-{ses}"
        make_tar(src / f"OAS3{sub:04d}_MR_{ses}" / "anat1.tar.gz", {
            f"{b}_{mod}.json": json.dumps(SIDECAR).encode(),
            f"{b}_{mod}.nii.gz": nii_bytes(),
        })
    return root


def test_roast_all(root):
    Bidsifier(root, keys={"meta", "pheno", "T1w", "T2w"}).run()
    assert raw_tree(root) == META | PHENO | {
        f"{T1}.json", f"{T1}.nii.gz", f"{T2}.json", f"{T2}.nii.gz",
        "sub-0001/sessions.tsv", "sub-0002/sessions.tsv",
    }
    assert json.loads(raw_file(root, f"{T1}.json").read_text()) == SIDECAR
    assert nib.load(raw_file(root, f"{T2}.nii.gz")).shape == (2, 2, 2)

    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert rows[0] == list(Bidsifier.PARTICIPANTS_HEADER_MAP)
    assert rows[1] == ["sub-0001", "F", "R", "65.1", "n/a", "12", "2", "W",
                       "N", "N", "N", "N", "Y", "N", "Y", "33"]
    assert rows[2][:8] == ["sub-0002", "M", "L", "70.0", "80.5", "16",
                           "n/a", "A"]
    assert rows[2][13] == "U"

    rows = read_tsv(raw_file(root, "phenotype/UDS_b4_cdr.tsv"))
    assert rows[0] == ["participant_id", "session_id", "delay", "age",
                       "CDRTOT", "DX1", "DX1_CODE"]
    assert rows[1] == ["sub-0001", "ses-d0000", "0", "65.1", "0",
                       "Cognitively normal", "1"]
    assert rows[3][4:6] == ["n/a", "n/a"]
    cdr = json.loads(raw_file(root, "phenotype/UDS_b4_cdr.json").read_text())
    assert {"MeasurementToolMetadata", "delay", "age", "CDRTOT"} <= set(cdr)

    # closest CDR visit to day 129 is day 0
    assert read_tsv(raw_file(root, "sub-0001/sessions.tsv")) == [
        ["session_id", "pathology", "age"], ["ses-d0129", "1", "65.4532"],
    ]


def test_meta_only(root):
    # "meta" implies "pheno"
    Bidsifier(root, keys={"meta"}).run()
    assert raw_tree(root) == META | PHENO


def test_json_no(root):
    Bidsifier(root, keys={"T1w"}, json="no").run()
    # sessions.tsv is only updated when sidecars are written
    assert raw_tree(root) == {f"{T1}.nii.gz"}


def test_subject_filters(root):
    Bidsifier(root, keys={"meta", "pheno", "T1w", "T2w"},
              exclude_subs={2}).run()
    assert raw_tree(root) - META - PHENO == {
        f"{T1}.json", f"{T1}.nii.gz", "sub-0001/sessions.tsv"}
    Bidsifier(root, keys={"T2w"}, subs={2}, json="no").run()
    assert f"{T2}.nii.gz" in raw_tree(root)


def test_key_selects_modality(root):
    Bidsifier(root, keys={"T2w"}, json="no").run()
    assert raw_tree(root) == {f"{T2}.nii.gz"}


@pytest.mark.xfail(strict=True, reason="sessions file is written as "
                   "sub-XXXX/sessions.tsv instead of sub-XXXX_sessions.tsv")
def test_sessions_file_name(root):
    Bidsifier(root, keys={"meta", "pheno", "T1w"}).run()
    assert "sub-0001/sub-0001_sessions.tsv" in raw_tree(root)


@pytest.mark.xfail(strict=True, raises=TypeError, reason="exclude_keys is "
                   "built with set(*map(...)), which fails for >1 key")
def test_exclude_two_keys(root):
    Bidsifier(root, keys={"T1w"}, exclude_keys={"fs", "pup"}, json="no").run()
    assert raw_tree(root) == {f"{T1}.nii.gz"}
