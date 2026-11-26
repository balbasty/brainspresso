"""Characterization tests for the ABIDE-I roast from NITRC tarballs."""
import json

import nibabel as nib
import pytest

from brainspresso.datasets.ABIDE.I.bidsifier_nitrc import Bidsifier
from roast_utils import (
    LAYOUTS, init_layout, make_tar, nii_bytes, raw_file, raw_tree, read_tsv,
)

PHENOS = Bidsifier.PHENOTYPES
COLS = list(dict.fromkeys(
    c for m in [Bidsifier.ParticipantsMapper, *PHENOS.values()]
    for c in m.header.values()))


def write_csv(path, rows):
    path.write_text("\n".join(
        ",".join(r.get(c, "") for c in COLS) for r in [
            {c: c for c in COLS}, *rows]) + "\n")


@pytest.fixture(params=LAYOUTS)
def root(tmp_path, request):
    root = tmp_path / "ABIDE-1"
    init_layout(root, request.param)
    src = root / "sourcedata"
    src.mkdir(parents=True)
    make_tar(src / "Caltech.tgz", {
        "Caltech/0051456/session_1/anat_1/mprage.nii.gz": nii_bytes(),
        "Caltech/0051456/session_1/rest_1/rest.nii.gz":
            nii_bytes((2, 2, 2, 3)),
        "Caltech/0051457/session_1/anat_1/mprage.nii.gz": nii_bytes(),
        "Caltech/0051457/session_1/anat_1/notes.txt": b"x",
    }, dirs=["Caltech", "Caltech/0051456"])
    make_tar(src / "MaxMun_a.tgz", {
        "MaxMun_a/0051330/session_1/anat_1/mprage.nii.gz": nii_bytes(),
    })
    write_csv(src / "phenotypic_CALTECH.csv", [
        {"SUB_ID": "51456", "SITE_ID": "CALTECH", "SEX": "1",
         "AGE_AT_SCAN": "55.4", "DX_GROUP": "1", "FIQ": "-9999",
         "FIQ_TEST_TYPE": "WAIS_III", "ADI_R_SOCIAL_TOTAL_A": "20",
         "ADI_R_RSRCH_RELIABLE": "1"},
        {"SUB_ID": "51457", "SITE_ID": "CALTECH", "SEX": "2",
         "AGE_AT_SCAN": "22.9", "DX_GROUP": "2"},
    ])
    write_csv(src / "phenotypic_MAX_MUN.csv", [
        {"SUB_ID": "51330", "SITE_ID": "MAX_MUN", "SEX": "1"},
    ])
    return root


RAW = {
    "sub-51456/anat/sub-51456_T1w.nii.gz",
    "sub-51456/anat/sub-51456_T1w.json",
    "sub-51456/func/sub-51456_task-rest_bold.nii.gz",
    "sub-51456/func/sub-51456_task-rest_bold.json",
    "sub-51457/anat/sub-51457_T1w.nii.gz",
    "sub-51457/anat/sub-51457_T1w.json",
    "sub-51330/anat/sub-51330_T1w.nii.gz",
    "sub-51330/anat/sub-51330_T1w.json",
}
META = {"README", "dataset_description.json",
        "participants.json", "participants.tsv"}
PHENO = {f"phenotype/{p}.{e}" for p in PHENOS for e in ("tsv", "json")}


def test_roast_raw(root):
    Bidsifier(root, keys={"T1w", "rest"}).run()
    assert raw_tree(root) == RAW
    img = nib.load(raw_file(root,
                            "sub-51456/func/sub-51456_task-rest_bold.nii.gz"))
    assert img.shape == (2, 2, 2, 3)
    tpl = json.loads((Bidsifier.TPLDIR / "MaxMun/T1w.json").read_text())
    assert json.loads(raw_file(
        root, "sub-51330/anat/sub-51330_T1w.json").read_text()) == tpl


def test_roast_filters(root):
    Bidsifier(root, keys={"T1w"}, exclude_subs={51457}, json="no").run()
    assert raw_tree(root) == {"sub-51456/anat/sub-51456_T1w.nii.gz",
                              "sub-51330/anat/sub-51330_T1w.nii.gz"}
    Bidsifier(root, keys={"rest"}, subs={51456}, json="only").run()
    assert "sub-51456/func/sub-51456_task-rest_bold.json" in raw_tree(root)
    assert "sub-51456/func/sub-51456_task-rest_bold.nii.gz" \
        not in raw_tree(root)


def test_roast_all(root):
    Bidsifier(root).run()
    assert raw_tree(root) == RAW | META | PHENO


def test_meta(root):
    Bidsifier(root, keys={"meta"}).run()
    assert raw_tree(root) == META | PHENO
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert rows[0] == list(Bidsifier.ParticipantsMapper.header)
    assert [r[0] for r in rows[1:3]] == ["51456", "51457"]
    row = dict(zip(rows[0], rows[1]))
    assert {k: row[k] for k in ("site", "sample", "sex", "age", "group",
                                "fiq", "fiq_type")} == {
        "site": "Caltech", "sample": "Caltech", "sex": "M", "age": "55.4",
        "group": "A", "fiq": "n/a", "fiq_type": "WAIS-III"}

    adir = read_tsv(raw_file(root, "phenotype/adir.tsv"))
    assert adir[0] == list(PHENOS["adir"].header)
    assert dict(zip(adir[0], adir[1]))["social"] == "20"
    assert dict(zip(adir[0], adir[1]))["reliable"] == "Y"
    assert json.loads(raw_file(root, "phenotype/adir.json").read_text()) \
        == json.loads((Bidsifier.TPLDIR / "phenotype/adir.json").read_text())


@pytest.mark.xfail(strict=True, reason="participant_id lacks 'sub-' prefix")
def test_participants_sub_prefix(root):
    Bidsifier(root, keys={"meta"}).run()
    ids = [r[0] for r in read_tsv(raw_file(root, "participants.tsv"))[1:]]
    assert ids == ["sub-51456", "sub-51457", "sub-51330"]


@pytest.mark.xfail(strict=True, reason="_phenotype_paths globs "
                   "phenotypic_MAXMUN*.csv, missing phenotypic_MAX_MUN.csv")
def test_participants_maxmun(root):
    Bidsifier(root, keys={"meta"}).run()
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert [r[0] for r in rows[1:]] == ["51456", "51457", "51330"]
