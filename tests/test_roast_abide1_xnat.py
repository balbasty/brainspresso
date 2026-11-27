"""ABIDE-I roast of the NITRC-IR archives (`--source xnat`, engine)."""
import gzip
import json
import logging
from pathlib import Path, PurePosixPath

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
    scoped_records,
)

from brainspresso.cli import main
from brainspresso.datasets.indi.abide.i import roaster, sources, tables
from brainspresso.datasets.indi.abide.i.dataset import ABIDE1
from brainspresso.datasets.indi.abide.i.roaster import FS_NAME, roast
from brainspresso.datasets.indi.abide.i.sites import TEMPLATES
from brainspresso.sources.base import RemoteEntry
from brainspresso.utils.archive import TarArchive

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
    "sub-51201/anat/sub-51201_acq-hires_T2w.nii.gz",
    "sub-51201/anat/sub-51201_acq-hires_T2w.json",
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
    report = xnat(root, keys={"T1w", "T2w", "rest"})
    assert not report.errors, report.errors
    assert raw_tree(root) == RAW
    assert sidecar(root, "sub-51330/anat/sub-51330_T1w.json") == \
        tpl("MaxMun_a/T1w.json")
    assert sidecar(root, "sub-51201/anat/sub-51201_T1w.json") == \
        tpl("UCLA_1/T1w.json")
    assert sidecar(root, "sub-51201/anat/sub-51201_acq-hires_T2w.json") == \
        tpl("UCLA_1/acq-hires_T2w.json")
    assert sidecar(root, "sub-50642/func/sub-50642_task-rest_bold.json") \
        == {**tpl("CMU/bold.json"), "RepetitionTime": 1.5}
    img = nib.load(raw_file(root,
                            "sub-51201/anat/sub-51201_acq-hires_T2w.nii.gz"))
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


FS_OUT = {
    f"sub-{sub}/anat/sub-{sub}_atlas-Aseg_dseg.{ext}"
    for sub in ("51456", "50003") for ext in ("nii.gz", "json")
} | {f"atlas-{name}_dseg.tsv" for name in (
    "Aseg", "AsegDesikanKilliany", "DesikanKilliany", "Destrieux")}


def fs_tree(root):
    """Files of the FreeSurfer derivatives (not its `sourcedata/`)."""
    drv = layout_of(root).derivative(FS_NAME).root
    return {p.relative_to(drv).as_posix() for p in drv.rglob("*")
            if p.is_file() and p.relative_to(drv).parts[0] not in (
                "sourcedata", "dataset_description.json")}


def test_all(root):
    report = xnat(root)
    assert not report.errors, report.errors
    assert report.status == "complete"
    assert raw_tree(root) == RAW | META | PHENO
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert len(rows) == 6
    assert fs_tree(root) == FS_OUT
    # `fs-all` only when named: only the files that BIDS describes
    unpacked = layout_of(root).derivative(FS_NAME).source
    assert {p.relative_to(unpacked).as_posix()
            for p in unpacked.rglob("*") if p.is_file()} == {
        "sub-51456/mri/aseg.mgz", "sub-50003/mri/aseg.mgz"}


def test_default_keys_sidecars_only(root):
    """Default keys, `--sidecars only`: nothing is unpacked"""
    report = xnat(root, json="only")
    assert not report.errors, report.errors
    assert not layout_of(root).derivative(FS_NAME).source.exists()
    assert fs_tree(root) == {p for p in FS_OUT
                             if not p.endswith(".nii.gz")}
    report = xnat(root, keys={"fs-all"}, json="only", subs=[51456])
    assert not report.errors
    assert not layout_of(root).derivative(FS_NAME).source.exists()


@pytest.mark.parametrize("exclude", [["fs"], ["fs", "fs-all"],
                                     ["derivatives"]])
def test_exclude_freesurfer(root, exclude):
    """`--exclude-keys fs` also excludes its add-on `fs-all`"""
    report = xnat(root, exclude_keys=exclude)
    assert not report.errors
    assert not layout_of(root).derivative(FS_NAME).root.exists()
    assert raw_tree(root) == RAW | META | PHENO


def test_exclude_freesurfer_cli(tmp_path):
    root = make_source(tmp_path / "ABIDE-1")
    bx("abide1", "roast", "--out", root, "--source", "xnat",
       "--exclude-keys", "fs", "--subs", "50003")
    assert not layout_of(root).derivative(FS_NAME).root.exists()
    assert "sub-50003/anat/sub-50003_T1w.nii.gz" in raw_tree(root)
    # `fs-all` alone implies `fs`
    bx("abide1", "roast", "--out", root, "--source", "xnat",
       "--keys", "fs-all", "--subs", "50003")
    unpacked = layout_of(root).derivative(FS_NAME).source / "sub-50003"
    assert (unpacked / "stats" / "aseg.stats").exists()
    assert (layout_of(root).derivative(FS_NAME).root / "sub-50003/anat/"
            "sub-50003_atlas-Aseg_dseg.nii.gz").exists()


@pytest.mark.parametrize("keys, exclude, fetched", [
    ((), (), True), ((), ("fs",), False), ((), ("fs-all",), True),
    (("fs",), (), True), (("fs-all",), (), True), (("raw",), (), False),
    ((), ("derivatives",), False),
])
def test_harvest_exclude_freesurfer(keys, exclude, fetched):
    """The harvest follows the same rule (`fs-all` goes with `fs`)"""
    wants = ABIDE1().wants("harvest", keys, exclude)
    record = {"label": "Caltech_0051456-freesurfer"}
    assert sources.XNATImages().keep_assessor({}, {}, record, wants) \
        is fetched


def test_no_freesurfer_archive_completes(root, caplog):
    """A default roast of a harvest without FreeSurfer archives is
    complete; naming `fs` makes the missing archives an error. Only the
    selected subjects are looked at."""
    with caplog.at_level(logging.INFO):
        report = xnat(root, subs=[51330])
    assert report.status == "complete" and not report.errors
    assert "no FreeSurfer archive" in caplog.text
    assert not layout_of(root).derivative(FS_NAME).root.exists()
    for keys in ({"fs"}, {"fs-all"}, {"T1w", "fs"}):
        report = xnat(root, keys=keys, subs=[51330])
        assert len(report.errors) == 1, keys
        assert "FreeSurfer" in report.errors[0][2]


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
    for name in ("x_123456", "20240101", "0051456", "NYU_0051456_2",
                 "Caltech_005145", "backup_2024-01-01"):
        assert not match(name), name


def test_other_folders_skipped(root, caplog):
    """Folders with archives that are not ABIDE-I subjects are skipped,
    with a warning (unknown site, id of another width, experiments)"""
    for name in ("x_123456", "20240101", "NYU_0051456_2", "Nowhere_0051111"):
        add_scan(root / "sourcedata", name, "anat", nii_bytes())
    with caplog.at_level(logging.WARNING):
        report = xnat(root, keys={"T1w"}, json="no")
    assert not report.errors
    assert {p.split("/")[0] for p in raw_tree(root)} == {
        "sub-51456", "sub-51330", "sub-51201", "sub-50003"}
    for name in ("x_123456", "20240101", "NYU_0051456_2", "Nowhere_0051111"):
        assert f"{name}/: not a NITRC-IR subject folder" in caplog.text
    assert "Leuven-1/" not in caplog.text       # (no archive)


def test_freesurfer_names_agree(tmp_path):
    """The harvest saves FreeSurfer assessors under names that the roast
    recognises (a label known by its type only gets a marker)"""
    images = sources.XNATImages()
    for record, name in [
        ({"label": "Caltech_0051456-freesurfer"},
         "Caltech_0051456-freesurfer"),
        ({"label": "Caltech_0051456_fs"}, "Caltech_0051456_fs"),
        ({"label": "Caltech_0051456-fs"}, "Caltech_0051456-fs"),
        ({"label": "Caltech_0051456_v51", "xsiType": "fs:fsData"},
         "Caltech_0051456_v51-freesurfer"),
    ]:
        assert sources.assessor_keys(record) == ("fs", "fs-all")
        entry = RemoteEntry(path=f"x/{record['label']}.tar.gz", meta={
            "subject": "Caltech_0051456", "id": record["label"],
            "type": "assessor", "record": record})
        path = images.local_path(entry)
        assert path == f"Caltech_0051456/{name}.tar.gz"
        assert roaster.is_freesurfer(path)
    for label in ("Caltech_0051456-man_qa", "x-fsfast", "x_fsl"):
        assert not roaster.is_freesurfer(label)
        assert sources.assessor_keys({"label": label}) != ("fs", "fs-all")
    scan = RemoteEntry(path="x/anat.tar.gz", meta={
        "subject": "Caltech_0051456", "id": "anat", "type": "scan",
        "record": {"ID": "anat"}})
    assert images.local_path(scan) == "Caltech_0051456/anat.tar.gz"


def test_freesurfer_tree_root(tmp_path, caplog):
    """Wrapper folders named like FreeSurfer folders do not re-root the
    tree; a second tree, a template subject and `..` are skipped"""
    root = tmp_path / "ABIDE-1"
    src = root / "sourcedata"
    label = "Pitt_0050003"
    lead = f"{label}/assessors/stats/tmp/label/files/{label}"
    make_tar(src / label / f"{label}-fs.tar.gz", {
        f"{label}/assessors/stats/files/fsaverage/mri/aseg.mgz":
            mgz_bytes(5),
        f"{lead}/mri/aseg.mgz": mgz_bytes(),
        f"{lead}/stats/aseg.stats": b"stats",
        f"{lead}/mri/orig/001.mgz": mgz_bytes(),
        f"{lead}/mri/../../../../../../EVIL.txt": b"evil",
        f"{label}/other/{label}/surf/lh.white": b"x",
    })
    with caplog.at_level(logging.WARNING):
        report = xnat(root, keys={"fs-all"})
    assert not report.errors, report.errors
    unpacked = layout_of(root).derivative(FS_NAME).source / "sub-50003"
    assert {p.relative_to(unpacked).as_posix()
            for p in unpacked.rglob("*") if p.is_file()} == {
        "mri/aseg.mgz", "stats/aseg.stats", "mri/orig/001.mgz"}
    assert nib.load(unpacked / "mri/aseg.mgz").get_fdata().max() == 1
    assert not list(tmp_path.rglob("EVIL.txt"))
    assert caplog.text.count("FreeSurfer tree") == 2    # fsaverage, other
    tree = roaster.FreeSurferTree
    assert tree.split("a/stats/sub/mri/x.mgz") == (
        ("a", "stats", "sub"), PurePosixPath("mri/x.mgz"))
    assert tree.split("/abs/sub/mri/x.mgz") is None
    assert tree.split("sub/mri.2mm/x.mgz") is None


def test_freesurfer_rerun_skipped(root, monkeypatch):
    """A re-run skips the subjects whose archives did not change, those
    with a FreeSurfer archive too (its own shard)"""
    xnat(root)
    opened = []
    real = TarArchive.__init__

    def spy(self, path):
        opened.append(Path(path).name)
        real(self, path)

    monkeypatch.setattr(TarArchive, "__init__", spy)
    report = xnat(root)
    assert not report.errors
    assert opened == []
    # a new FreeSurfer archive re-runs its shard only
    add_freesurfer(root / "sourcedata", "Pitt_0050003", "Pitt_0050003-fs")
    xnat(root)
    assert opened == ["Pitt_0050003-fs.tar.gz"]


def test_scope_of_subject_shards(root, monkeypatch):
    """Toggling `meta`, fewer keys or subjects: no archive is opened
    (each shard declares its keys and its subject, `roast.scope`)"""
    report = xnat(root)
    assert not report.errors
    records = scoped_records(root)
    assert records["roast/fs/sub-50003"]["scopes"][0]["keys"] == ["fs"]
    opened = []
    real = TarArchive.__init__

    def spy(self, path):
        opened.append(Path(path).name)
        real(self, path)

    monkeypatch.setattr(TarArchive, "__init__", spy)
    for options in ({"exclude_keys": ["meta"]}, {"keys": ["T1w", "fs"]},
                    {"subs": ["51456"], "keys": ["meta", "rest"]}):
        assert not xnat(root, **options).errors
    assert opened == []
    # (`fs-all` is a key that the FreeSurfer shards had not written)
    xnat(root, keys=["fs-all"])
    assert sorted(opened) == ["Caltech_0051456-freesurfer.tar.gz",
                              "Pitt_0050003-fs.tar.gz"]


def test_subjects_filters_and_shards(root, monkeypatch):
    opened = []
    real = roaster.TarArchive

    def opener(path):
        opened.append(path.parent.name + "/" + path.name)
        return real(path)

    monkeypatch.setattr(roaster, "TarArchive", opener)
    xnat(root, keys={"T1w"}, subs=["sub-51330", "0051201"])
    assert raw_tree(root) == {p for p in RAW if "hires" not in p
                              and ("51330" in p or "51201" in p)}
    assert sorted(opened) == ["MaxMun_a_0051330/anat.tar.gz",
                              "UCLA1_51201/anat.tar.gz"]
    # unchanged: skipped by signature
    xnat(root, keys={"T1w"}, subs=["sub-51330", "0051201"])
    assert len(opened) == 2
    # the high-resolution anatomical is `T2w` (`acq-hires_T2w`)
    xnat(root, keys={"T2w"})
    assert opened[2:] == ["UCLA1_51201/anat_2.tar.gz"]


def test_corrupt_archive(root):
    (root / "sourcedata" / "Caltech_0051456" / "rest.tar.gz").write_bytes(
        b"garbage")
    report = xnat(root, keys={"anat", "rest"})
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
    ("anat", "T1w"), ("mprage", "T1w"), ("anat_1", "T1w"),
    ("anat_2", "T2w"), ("hires", "T2w"), ("anat_hires", "T2w"),
    ("rest", "rest"), ("rest_1", "rest"), ("dwi", "dwi"),
])
def test_harvest_scan_keys(scan_id, key):
    """Scans are harvested under the names that the roast converts"""
    assert sources.scan_key({"ID": scan_id}) == key


@pytest.mark.parametrize("path, keys, kept", [
    ("UCLA_1/0051201/session_1/anat_2/mprage.nii.gz", ["T2w"], True),
    ("UCLA_1/0051201/session_1/anat_2/mprage.nii.gz", ["T1w"], False),
    ("UCLA_1/0051201/session_1/anat_1/mprage.nii.gz", ["T1w"], True),
    ("UCLA_1/0051201/session_1/anat_1/mprage.nii.gz", ["T2w"], False),
    ("Pitt/0050003/session_1/anat_2/mprage.nii.gz", ["T1w"], True),
])
def test_harvest_s3_hires_key(path, keys, kept):
    """The bucket's UCLA `anat_2` is harvested with `T2w`"""
    wants = ABIDE1().wants("harvest", keys)
    entry = RemoteEntry(path=f"RawData/{path}")
    assert sources.S3().keep_raw(entry, None, wants) is kept


# ----------------------------------------------------------------------
#   Review round 2 of #33
# ----------------------------------------------------------------------

def test_keys_as_a_string(root):
    """`roast(keys="fs")`: one key, not its letters"""
    report = xnat(root, keys="fs", subs=[50003])
    assert not report.errors, report.errors
    assert fs_tree(root) == {p for p in FS_OUT if "51456" not in p}
    report = xnat(root, exclude_keys="fs", subs=[50003])
    assert not report.errors
    assert raw_tree(root) >= {"sub-50003/anat/sub-50003_T1w.nii.gz"}
    assert roaster.roast_options("fs") == {"fs_named": True}


@pytest.mark.parametrize("keys, named", [
    ((), False), (("T1w",), False), (("fs",), True), (("fs-all",), True),
    (("all",), True), (("derivatives",), True), (("proc",), True),
    (("FS",), True), (("Derivatives",), True), (("qa",), False),
])
def test_fs_named_through_the_hierarchy(keys, named):
    """A missing FreeSurfer archive is an error when `--keys` names `fs`,
    `fs-all` or a group above them, in any case"""
    assert roaster.roast_options(keys) == {"fs_named": named}


def test_fs_named_by_a_group_fails_without_archives(root):
    for keys in ({"all"}, {"derivatives"}, {"proc"}):
        report = xnat(root, keys=keys, subs=[51330])
        assert [e[0] for e in report.errors] == ["fs"], keys


@pytest.mark.parametrize("spec", ["roast", "harvest"])
def test_add_on_any_case(spec):
    """`--keys FS-ALL` implies `fs`; `--exclude-keys FS` excludes
    `fs-all` (names through the hierarchy, any case)"""
    keys = ABIDE1().keys_for(spec)
    select, _ = keys.effective(["FS-ALL"])
    assert keys.selects(select, "fs") and keys.selects(select, "fs-all")
    _, exclude = keys.effective((), ["FS"])
    assert keys.selects(exclude, "fs-all")
    # a group that holds both needs no add-on
    assert keys.effective(["derivatives"])[0] == ("derivatives",)
    _, exclude = keys.effective((), ["proc"])
    assert exclude[0] == "proc" and exclude.count("fs-all") <= 1


def test_add_on_any_case_cli(tmp_path):
    root = make_source(tmp_path / "ABIDE-1")
    bx("abide1", "roast", "--out", root, "--source", "xnat", "--keys",
       "FS-ALL", "--subs", "50003")
    drv = layout_of(root).derivative(FS_NAME)
    assert (drv.source / "sub-50003/stats/aseg.stats").exists()
    assert (drv.root / "sub-50003/anat/"
            "sub-50003_atlas-Aseg_dseg.nii.gz").exists()


def test_fs_all_unpacks_the_whole_root(tmp_path):
    """`fs-all`: every member under the subject's root, FreeSurfer
    folders or not (`tmp/`, `trash/`, `bem/`, `src/`, root files)"""
    root = tmp_path / "ABIDE-1"
    label = "Pitt_0050003"
    lead = f"{label}/assessors/a/files/{label}"
    make_tar(root / "sourcedata" / label / f"{label}-fs.tar.gz", {
        f"{lead}/mri/aseg.mgz": mgz_bytes(),
        f"{lead}/tmp/x.log": b"tmp",
        f"{lead}/trash/y": b"trash",
        f"{lead}/bem/z": b"bem",
        f"{lead}/src/w": b"src",
        f"{lead}/README": b"readme",
        f"{lead}/scripts/recon-all.log": b"log",
    })
    report = xnat(root, keys={"fs-all"})
    assert not report.errors, report.errors
    unpacked = layout_of(root).derivative(FS_NAME).source / "sub-50003"
    assert {p.relative_to(unpacked).as_posix()
            for p in unpacked.rglob("*") if p.is_file()} == {
        "mri/aseg.mgz", "tmp/x.log", "trash/y", "bem/z", "src/w", "README",
        "scripts/recon-all.log"}
    # `fs` alone: only the files that BIDS describes
    tree = roaster.FreeSurferTree("x", "50003")
    assert tree.relpath(f"{lead}/mri/aseg.mgz") == PurePosixPath(
        "mri/aseg.mgz")
    assert tree.relpath(f"{lead}/tmp/x.log") == PurePosixPath("tmp/x.log")
    assert tree.relpath(f"{lead}/../x") is None


def test_freesurfer_root_warnings(caplog):
    """A template tree first: "template subject"; a root that does not
    name the subject: a warning"""
    with caplog.at_level(logging.WARNING):
        tree = roaster.FreeSurferTree("a.tar.gz", "50003")
        assert tree.relpath("files/fsaverage/mri/aseg.mgz") is None
        assert "fsaverage/ skipped (template subject)" in caplog.text
        assert tree.relpath("files/Pitt_0050003/mri/aseg.mgz") is not None
        assert "does not name subject" not in caplog.text
        other = roaster.FreeSurferTree("b.tar.gz", "50003")
        assert other.relpath("files/Pitt_0050004/mri/aseg.mgz") is not None
        assert "Pitt_0050004/ does not name subject 50003" in caplog.text


def test_help_wording(capsys):
    try:
        main(["abide1", "roast", "--help"])
    except SystemExit:
        pass
    out = " ".join(capsys.readouterr().out.split())
    assert "not selected by default" in out
