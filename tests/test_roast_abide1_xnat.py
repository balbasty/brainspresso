"""ABIDE-I roast of the NITRC-IR archives (`--source xnat`, engine)."""
import gzip
import json
import logging

import nibabel as nib
import numpy as np
import pytest
from cli_utils import bx
from roast_utils import (
    LAYOUTS,
    init_layout,
    layout_of,
    make_tar,
    nii_bytes,
    raw_file,
    raw_tree,
    read_tsv,
)

from brainspresso.datasets.indi.abide.i import roaster, sources, tables
from brainspresso.datasets.indi.abide.i.roaster import FS_NAME, roast
from brainspresso.datasets.indi.abide.i.sites import TEMPLATES

COLS = list(dict.fromkeys(
    c for t in [tables.PARTICIPANTS, *tables.PHENOTYPES.values()]
    for c in t.columns.values()))
PHENO = {f"phenotype/{p}.{e}" for p in tables.PHENOTYPES
         for e in ("tsv", "json")}
META = {"README", "dataset_description.json",
        "participants.json", "participants.tsv"}


def bold_bytes(tr=None):
    """Gzipped 4D NIfTI-1, with TR `tr` (s) if given."""
    img = nib.Nifti1Image(np.zeros((2, 2, 2, 3), dtype=np.int16), np.eye(4))
    if tr is not None:
        img.header.set_zooms((1., 1., 1., tr))
        img.header.set_xyzt_units("mm", "sec")
    return gzip.compress(img.to_bytes())


def mgz_bytes(value=1) -> bytes:
    img = nib.MGHImage(np.full((2, 2, 2), value, np.float32), np.eye(4))
    return gzip.compress(img.to_bytes())


def scan(label, scan_id, name):
    """Member of a server-built archive of a scan."""
    return f"{label}/scans/{scan_id}/resources/NIfTI/files/{name}"


def add_scan(src, label, scan_id, data, name="mprage.nii.gz"):
    make_tar(src / label / f"{scan_id}.tar.gz", {
        scan(label, scan_id, name): data,
        f"{label}/scans/{scan_id}/resources/SNAPSHOTS/files/x.gif": b"gif",
    })


def add_freesurfer(src, label, name):
    lead = (f"{label}/assessors/{name}/DATA/files/{label}")
    make_tar(src / label / f"{name}.tar.gz", {
        f"{lead}/mri/aseg.mgz": mgz_bytes(),
        f"{lead}/stats/aseg.stats": b"# not bidsifiable",
        f"{label}/assessors/{name}/SNAPSHOTS/files/x.gif": b"gif",
    })


def make_source(root):
    """Write the harvested NITRC-IR files of ABIDE-I."""
    src = root / "sourcedata"
    src.mkdir(parents=True, exist_ok=True)
    rows = [
        {"SUB_ID": "51456", "SITE_ID": "CALTECH", "SEX": "1"},
        {"SUB_ID": "51330", "SITE_ID": "MAX_MUN", "SEX": "2"},
        {"SUB_ID": "51201", "SITE_ID": "UCLA_1", "SEX": "1"},
        {"SUB_ID": "50642", "SITE_ID": "CMU", "SEX": "1"},
        {"SUB_ID": "50003", "SITE_ID": "PITT", "SEX": "1"},
    ]
    (src / "Phenotypic_V1_0b.csv").write_text("\n".join(
        ",".join(r.get(c, "") for c in COLS)
        for r in [{c: c for c in COLS}, *rows]) + "\n")
    # 7-digit labels; a QA assessor; FreeSurfer outputs
    add_scan(src, "Caltech_0051456", "anat", nii_bytes())
    add_scan(src, "Caltech_0051456", "rest", bold_bytes(2.0), "rest.nii.gz")
    make_tar(src / "Caltech_0051456" / "Caltech_0051456-man_qa.tar.gz",
             {"Caltech_0051456/assessors/qa/files/qa.csv": b"x"})
    add_freesurfer(src, "Caltech_0051456", "Caltech_0051456-freesurfer")
    # a sample in the label (`int(label.split('_')[1])` failed)
    add_scan(src, "MaxMun_a_0051330", "anat", nii_bytes())
    # a sample glued to the site (`glob('UCLA_*')` missed it), 5 digits,
    # a high-resolution anatomical
    add_scan(src, "UCLA1_51201", "anat", nii_bytes())
    add_scan(src, "UCLA1_51201", "anat_2", nii_bytes(value=2))
    # no sample: the common CMU bold template has no TR (header's)
    add_scan(src, "CMU_0050642", "rest", bold_bytes(1.5), "rest.nii.gz")
    # FreeSurfer outputs, under another plausible label
    add_scan(src, "Pitt_0050003", "anat", nii_bytes())
    add_freesurfer(src, "Pitt_0050003", "Pitt_0050003-fs")
    # scan parameters (not a subject)
    (src / "Leuven-1").mkdir()
    (src / "Leuven-1" / "anat.pdf").write_bytes(b"%PDF")
    return root


@pytest.fixture(params=LAYOUTS)
def root(tmp_path, request):
    root = tmp_path / "ABIDE-1"
    init_layout(root, request.param)
    return make_source(root)


RAW = {
    "sub-51456/anat/sub-51456_T1w.nii.gz",
    "sub-51456/anat/sub-51456_T1w.json",
    "sub-51456/func/sub-51456_task-rest_bold.nii.gz",
    "sub-51456/func/sub-51456_task-rest_bold.json",
    "sub-51330/anat/sub-51330_T1w.nii.gz",
    "sub-51330/anat/sub-51330_T1w.json",
    "sub-51201/anat/sub-51201_T1w.nii.gz",
    "sub-51201/anat/sub-51201_T1w.json",
    "sub-51201/anat/sub-51201_acq-hires_T1w.nii.gz",
    "sub-51201/anat/sub-51201_acq-hires_T1w.json",
    "sub-50642/func/sub-50642_task-rest_bold.nii.gz",
    "sub-50642/func/sub-50642_task-rest_bold.json",
    "sub-50003/anat/sub-50003_T1w.nii.gz",
    "sub-50003/anat/sub-50003_T1w.json",
}


def tpl(path):
    return json.loads((TEMPLATES / path).read_text())


def sidecar(root, path):
    return json.loads(raw_file(root, path).read_text())


def xnat(root, **options):
    return roast(root, source="xnat", display=False, **options)


def test_raw(root):
    report = xnat(root, keys={"T1w", "rest"})
    assert not report.errors, report.errors
    assert raw_tree(root) == RAW
    assert sidecar(root, "sub-51330/anat/sub-51330_T1w.json") == \
        tpl("MaxMun_a/T1w.json")
    assert sidecar(root, "sub-51201/anat/sub-51201_T1w.json") == \
        tpl("UCLA_1/T1w.json")
    assert sidecar(root, "sub-51201/anat/sub-51201_acq-hires_T1w.json") == \
        tpl("UCLA_1/acq-hires_T1w.json")
    assert sidecar(root, "sub-50642/func/sub-50642_task-rest_bold.json") \
        == {**tpl("CMU/bold.json"), "RepetitionTime": 1.5}
    img = nib.load(raw_file(root,
                            "sub-51201/anat/sub-51201_acq-hires_T1w.nii.gz"))
    assert img.get_fdata().max() == 2


def test_tr_from_header_sidecars_only(root):
    """`--sidecars only`: the TR is read from the member (no image)"""
    report = xnat(root, keys={"rest"}, subs=[50642], json="only")
    assert not report.errors
    assert raw_tree(root) == {"sub-50642/func/sub-50642_task-rest_bold.json"}
    assert sidecar(root, "sub-50642/func/sub-50642_task-rest_bold.json")[
        "RepetitionTime"] == 1.5


def test_tr_missing_is_an_error(root, caplog):
    add_scan(root / "sourcedata", "CMU_0050643", "rest", bold_bytes(),
             "rest.nii.gz")
    with caplog.at_level(logging.ERROR):
        report = xnat(root, keys={"rest"}, subs=[50643])
    assert raw_tree(root) == {"sub-50643/func/sub-50643_task-rest_bold.nii.gz"}
    assert len(report.errors) == 1
    assert "has no RepetitionTime (REQUIRED)" in caplog.text


def test_meta(root):
    report = xnat(root, keys={"meta"})
    assert not report.errors
    assert raw_tree(root) == META | PHENO
    rows = read_tsv(layout_of(root).original(
        raw_file(root, "participants.tsv")))
    assert [r[:3] for r in rows[1:]] == [
        ["sub-51456", "Caltech", "Caltech"],
        ["sub-51330", "MaxMun", "MaxMun"],
        ["sub-51201", "UCLA", "UCLA1"],
        ["sub-50642", "CMU", "CMU"],
        ["sub-50003", "Pitt", "Pitt"],
    ]


def test_all(root):
    report = xnat(root)
    assert not report.errors, report.errors
    assert raw_tree(root) == RAW | META | PHENO
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert len(rows) == 6


@pytest.mark.parametrize("label", ["Caltech_0051456", "Pitt_0050003"])
def test_freesurfer(root, label):
    """FreeSurfer archives are found (the legacy roast looked them up
    with the OASIS-3 pattern) under both plausible labels"""
    sub = f"sub-{label[-5:]}"
    report = xnat(root, keys={"fs"}, subs=[label[-5:]])
    assert not report.errors, report.errors
    drv = layout_of(root).derivative(FS_NAME)
    unpacked = drv.source / sub
    assert (unpacked / "mri" / "aseg.mgz").exists()
    assert not (unpacked / "stats").exists()        # only with fs-all
    anat = drv.root / sub / "anat"
    seg = f"{sub}_atlas-Aseg_dseg"
    assert {p.name for p in anat.iterdir()} == {f"{seg}.nii.gz",
                                                f"{seg}.json"}
    assert (drv.root / "atlas-Aseg_dseg.tsv").exists()
    assert raw_tree(root) == set()                  # no raw key

    xnat(root, keys={"fs-all"}, subs=[label[-5:]])
    assert (unpacked / "stats" / "aseg.stats").exists()


def test_freesurfer_sidecars_only(root):
    report = xnat(root, keys={"fs"}, subs=[51456], json="only")
    assert not report.errors
    drv = layout_of(root).derivative(FS_NAME)
    assert not drv.source.exists()
    assert {p.name for p in (drv.root / "sub-51456/anat").iterdir()} == {
        "sub-51456_atlas-Aseg_dseg.json"}


def test_freesurfer_missing(tmp_path):
    root = tmp_path / "ABIDE-1"
    src = root / "sourcedata"
    add_scan(src, "Caltech_0051456", "anat", nii_bytes())
    report = xnat(root, keys={"fs", "T1w"}, json="no")
    assert raw_tree(root) == {"sub-51456/anat/sub-51456_T1w.nii.gz"}
    assert len(report.errors) == 1
    assert "FreeSurfer" in report.errors[0][2]


def test_derivative_keys_write_no_raw_data(root):
    """`--keys qa-man` (an assessor that is not converted) writes no rest
    scan (the legacy `rawkeys` did, from the leaves of the wrong tree)"""
    report = xnat(root, keys={"qa-man"})
    assert not report.errors
    assert raw_tree(root) == set()
    assert not layout_of(root).derivatives.exists()


def test_subject_folders():
    """`<site>[_<sample>]_<id>` folders with archives, ids of 5 or 7
    digits (the legacy globs missed `UCLA1_...` and 7-digit labels)"""
    match = roaster.SUBJECT_FOLDER.fullmatch
    for name, site, label in [
        ("Caltech_0051456", "Caltech", "51456"),
        ("Caltech_51456", "Caltech", "51456"),
        ("MaxMun_a_0051330", "MaxMun_a", "51330"),
        ("UCLA1_0051201", "UCLA1", "51201"),
        ("Leuven-1_0050682", "Leuven-1", "50682"),
    ]:
        found = match(name)
        assert found and found["site"] == site
        assert tables.participant_label(found["id"]) == label
    assert not match("Leuven-1") and not match("UCLA1")


def test_subjects_filters_and_shards(root, monkeypatch):
    opened = []
    real = roaster.TarArchive

    def opener(path):
        opened.append(path.parent.name + "/" + path.name)
        return real(path)

    monkeypatch.setattr(roaster, "TarArchive", opener)
    xnat(root, keys={"T1w"}, subs=["sub-51330", "0051201"])
    assert raw_tree(root) == {p for p in RAW
                              if "51330" in p or "51201" in p}
    assert sorted(opened) == ["MaxMun_a_0051330/anat.tar.gz",
                              "UCLA1_51201/anat.tar.gz",
                              "UCLA1_51201/anat_2.tar.gz"]
    # unchanged: skipped by signature
    xnat(root, keys={"T1w"}, subs=["sub-51330", "0051201"])
    assert len(opened) == 3


def test_corrupt_archive(root):
    (root / "sourcedata" / "Caltech_0051456" / "rest.tar.gz").write_bytes(
        b"garbage")
    report = xnat(root, keys={"T1w", "rest"})
    assert RAW - raw_tree(root) == {
        "sub-51456/func/sub-51456_task-rest_bold.nii.gz",
        "sub-51456/func/sub-51456_task-rest_bold.json"}
    assert len(report.errors) == 1
    assert "rest.tar.gz" in report.errors[0][2]


def test_archive_without_image(root):
    make_tar(root / "sourcedata" / "Olin_0050102" / "anat.tar.gz", {
        "Olin_0050102/scans/anat/resources/DICOM/files/1.dcm": b"x"})
    report = xnat(root, keys={"T1w"}, subs=[50102])
    assert raw_tree(root) == set()
    assert len(report.errors) == 1
    assert "no NIfTI image" in report.errors[0][2]


def test_no_subject_folder(tmp_path):
    root = tmp_path / "ABIDE-1"
    (root / "sourcedata").mkdir(parents=True)
    report = xnat(root, keys={"T1w", "meta"})
    assert len(report.errors) == 3   # participants, phenotypes, images


def test_jobs(root):
    report = xnat(root, jobs=2)
    assert not report.errors, report.errors
    assert raw_tree(root) == RAW | META | PHENO
    assert (layout_of(root).derivative(FS_NAME).root /
            "sub-50003/anat/sub-50003_atlas-Aseg_dseg.nii.gz").exists()


def test_cli(tmp_path):
    root = make_source(tmp_path / "ABIDE-1")
    bx("abide1", "roast", "--out", root, "--source", "xnat", "--keys",
       "T1w", "fs", "--subs", "51456", "--sidecars", "no")
    assert raw_tree(root) == {"sub-51456/anat/sub-51456_T1w.nii.gz"}
    drv = layout_of(root).derivative(FS_NAME)
    assert (drv.root / "sub-51456/anat/"
            "sub-51456_atlas-Aseg_dseg.nii.gz").exists()


@pytest.mark.parametrize("record, keys", [
    ({"label": "Caltech_0051456-freesurfer"}, ("fs", "fs-all")),
    ({"label": "Caltech_0051456-fs"}, ("fs", "fs-all")),
    ({"label": "Caltech_0051456_FS", "xsiType": "fs:fsData"},
     ("fs", "fs-all")),
    ({"label": "Caltech_0051456-man_qa"}, ("qa-man",)),
    ({"label": "Caltech_0051456-min_proc"}, ("proc-min",)),
])
def test_harvest_assessor_keys(record, keys):
    """FreeSurfer assessors are harvested with `fs` or `fs-all`"""
    assert sources.assessor_keys(record) == keys
    images = sources.XNATImages()
    for key in keys:
        assert images.keep_assessor({}, {}, record, lambda k, key=key:
                                    k == key)


@pytest.mark.parametrize("scan_id, key", [
    ("anat", "anat"), ("mprage", "anat"), ("anat_2", "anat"),
    ("hires", "anat"), ("rest", "rest"), ("rest_1", "rest"),
    ("dwi", "dwi"),
])
def test_harvest_scan_keys(scan_id, key):
    """Scans are harvested under the names that the roast converts"""
    assert sources.scan_key({"ID": scan_id}) == key
