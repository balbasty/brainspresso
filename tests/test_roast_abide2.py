"""ABIDE-II roast of the NITRC tarballs (engine)."""
import gzip
import io
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import nibabel as nib
import numpy as np
import pytest
from cli_utils import bx
from roast_utils import (
    LAYOUTS,
    SCOPE_SCENARIOS,
    check_keys_rerun,
    check_scope,
    init_layout,
    layout_of,
    make_tar,
    nii_bytes,
    open_spy,
    raw_file,
    raw_tree,
    read_tsv,
)

from brainspresso.bids.compost import retrim_tables
from brainspresso.bids.layout import Layout
from brainspresso.bids.tables import is_phenotype, trim_tables
from brainspresso.datasets.indi.abide.ii import roaster, sites, tables
from brainspresso.datasets.indi.abide.ii.dataset import ABIDE2
from brainspresso.datasets.indi.abide.ii.roaster import roast
from brainspresso.datasets.indi.abide.ii.sites import TEMPLATES
from brainspresso.datasets.indi.abide.scans import TemplateError
from brainspresso.datasets.indi.abide.tables import read_csv
from brainspresso.datasets.indi.engine import format_tsv
from brainspresso.roast import make_context

PHENOS = tables.PHENOTYPES
PARTICIPANTS = tables.PARTICIPANTS
COLS = list(dict.fromkeys(
    t.names(c)[0] for t in [PARTICIPANTS, *PHENOS.values()]
    for c in t.columns))
BVAL = b"0 1000 1000\n"
BVEC = b"0 1 0\n0 0 1\n0 0 0\n"


def write_csv(path, rows, cols=COLS):
    path.write_text("\n".join(
        ",".join(r.get(c, "") for c in cols) for r in [
            {c: c for c in cols}, *rows]) + "\n")


def subject_members(id, site="ABIDEII-NYU_1", ses=1, dwi=True):
    b = f"{site}/{id}/session_{ses}/"
    m = {b + "anat_1/anat.nii.gz": nii_bytes(),
         b + "rest_1/rest.nii.gz": nii_bytes((2, 2, 2, 3))}
    if dwi:
        m[b + "dti_1/dti.nii.gz"] = nii_bytes((2, 2, 2, 3))
        m[b + "dti_1/dti.bval"] = BVAL
        m[b + "dti_1/dti.bvec"] = BVEC
    return m


def make_source(root):
    """Write the harvested NITRC files of ABIDE-II in `<root>/sourcedata/`."""
    src = root / "sourcedata"
    src.mkdir(parents=True, exist_ok=True)
    make_tar(src / "ABIDEII-NYU_1.tar.gz",
             {**subject_members(29001), **subject_members(29002)})
    make_tar(src / "ABIDEII-KUL_3.tar.gz",
             subject_members(29100, "ABIDEII-KUL_3", dwi=False))
    write_csv(src / "ABIDEII-NYU_1.csv", [
        {"SUB_ID": "29001", "SITE_ID": "ABIDEII-NYU_1", "SEX": "1",
         "DX_GROUP": "2", "AGE_AT_SCAN": "10.5", "FIQ": "-9999",
         "HANDEDNESS_CATEGORY": "Ambi", "FIQ_TEST_TYPE": "WISC-IV",
         "CURRENT_MED_STATUS": "1", "SRS_VERSION": "1",
         "SRS_TOTAL_RAW": "42"},
        {"SUB_ID": "29002", "SITE_ID": "ABIDEII-NYU_1", "SEX": "2",
         "DX_GROUP": "1", "AGE_AT_SCAN": "12"},
    ])
    write_csv(src / "ABIDEII-KUL_3.csv", [
        {"SUB_ID": "29100", "SITE_ID": "ABIDEII-KUL_3", "SEX": "1"},
    ])
    return root


@pytest.fixture(params=LAYOUTS)
def root(tmp_path, request):
    root = tmp_path / "ABIDE-2"
    init_layout(root, request.param)
    return make_source(root)


def scans(id, ses="01", dwi=True):
    b = f"sub-{id}/ses-{ses}"
    out = {
        f"{b}/anat/sub-{id}_ses-{ses}_T1w.nii.gz",
        f"{b}/anat/sub-{id}_ses-{ses}_T1w.json",
        f"{b}/func/sub-{id}_ses-{ses}_task-rest_bold.nii.gz",
        f"{b}/func/sub-{id}_ses-{ses}_task-rest_bold.json",
    }
    if dwi:
        out |= {
            f"{b}/dwi/sub-{id}_ses-{ses}_dwi.nii.gz",
            f"{b}/dwi/sub-{id}_ses-{ses}_dwi.json",
            f"{b}/dwi/sub-{id}_ses-{ses}_dwi.bval",
            f"{b}/dwi/sub-{id}_ses-{ses}_dwi.bvec",
        }
    return out


RAW = scans(29001) | scans(29002) | scans(29100, dwi=False)
META = {"README", "dataset_description.json",
        "participants.json", "participants.tsv"}
PHENO = {f"phenotype/{p}.{e}" for p in PHENOS for e in ("tsv", "json")}
ALL = RAW | META | PHENO


def original(root, name="participants.tsv"):
    """Pristine copy of a table (all subjects, before trimming)."""
    return layout_of(root).original(raw_file(root, name))


def tpl(path):
    return json.loads((TEMPLATES / path).read_text(encoding="utf-8"))


def sidecar(root, path):
    return json.loads(raw_file(root, path).read_text(encoding="utf-8"))


def ids(path):
    return [r[0] for r in read_tsv(path)[1:]]


# ----------------------------------------------------------------------
#   Raw data
# ----------------------------------------------------------------------

def test_roast_raw(root):
    report = roast(root, keys=["T1w", "rest", "dti"], display=False)
    assert not report.errors, report.errors
    assert raw_tree(root) == RAW
    assert raw_file(
        root, "sub-29001/ses-01/dwi/sub-29001_ses-01_dwi.bval"
    ).read_bytes() == BVAL
    img = nib.load(raw_file(
        root, "sub-29002/ses-01/func/sub-29002_ses-01_task-rest_bold.nii.gz"))
    assert img.shape == (2, 2, 2, 3)
    assert sidecar(root, "sub-29001/ses-01/anat/sub-29001_ses-01_T1w.json") \
        == tpl("NYU/1/T1w.json")
    assert sidecar(root, "sub-29100/ses-01/func/"
                         "sub-29100_ses-01_task-rest_bold.json") \
        == {**tpl("KUL/3/bold.json"), "TaskName": "rest"}


def test_roast_all(root):
    report = roast(root, display=False)
    assert not report.errors, report.errors
    assert raw_tree(root) == ALL


def test_sidecars_only_and_no(root, monkeypatch):
    """`--sidecars only` never extracts an image (gradient tables are
    metadata: always written)"""
    placed, spooled = [], []
    real_place = roaster.place

    def place(spool, path):
        placed.append(path)
        return real_place(spool, path)

    real_add = roaster.Session.add

    def add(self, *args, **kwargs):
        real_add(self, *args, **kwargs)
        spooled.extend(self.spooled)

    monkeypatch.setattr(roaster, "place", place)
    monkeypatch.setattr(roaster.Session, "add", add)
    report = roast(root, keys="raw", json="only", display=False)
    assert not report.errors, report.errors
    assert raw_tree(root) == {p for p in RAW if not p.endswith(".nii.gz")}
    assert not placed and not spooled
    for rel in raw_tree(root):          # start from an empty raw dataset
        raw_file(root, rel).unlink()
    roast(root, keys="raw", json="no", display=False)
    assert raw_tree(root) == {p for p in RAW if not p.endswith(".json")}


def test_dir_entries(root):
    """Directory members are skipped (legacy: `return`, which ended the
    archive)"""
    make_tar(root / "sourcedata" / "ABIDEII-NYU_1.tar.gz",
             subject_members(29001),
             dirs=["ABIDEII-NYU_1", "ABIDEII-NYU_1/29001"])
    report = roast(root, keys=["T1w", "rest", "dti"], subs=[29001],
                   display=False)
    assert not report.errors
    assert raw_tree(root) == scans(29001)


def test_subject_filter(root):
    """A filtered-out subject does not end the archive (legacy: `return`)"""
    roast(root, keys=["T1w", "rest", "dti"], subs=["29002"], display=False)
    assert raw_tree(root) == scans(29002)
    roast(root, keys="T1w", exclude_subs=["sub-29002", 29001],
          display=False)
    assert {p for p in raw_tree(root) if "29100" in p} == {
        "sub-29100/ses-01/anat/sub-29100_ses-01_T1w.nii.gz",
        "sub-29100/ses-01/anat/sub-29100_ses-01_T1w.json"}


def test_key_filter(root):
    """Keys select by output (legacy: every raw key converted)"""
    report = roast(root, keys="T1w", display=False)
    assert not report.errors
    assert raw_tree(root) == {p for p in RAW if "/anat/" in p}
    roast(root, keys=["dti"], subs=[29001], display=False)
    assert {p for p in raw_tree(root) if "/dwi/" in p} == {
        p for p in scans(29001) if "/dwi/" in p}
    assert not any("/func/" in p for p in raw_tree(root))


def test_exclude_keys(root):
    report = roast(root, exclude_keys=["dti", "rest", "meta"],
                   display=False)
    assert not report.errors
    assert raw_tree(root) == {p for p in RAW if "/anat/" in p}


def test_subjects_pruned_before_extraction(root, monkeypatch):
    extracted = []
    real = roaster.TarArchive.extractfile

    def spy(self, member):
        extracted.append(member.name)
        return real(self, member)

    monkeypatch.setattr(roaster.TarArchive, "extractfile", spy)
    roast(root, keys="T1w", subs=[29002], display=False)
    assert extracted == ["ABIDEII-NYU_1/29002/session_1/anat_1/anat.nii.gz"]


def test_spool_removed(root):
    """Spooled images are moved into place, the spool folder removed"""
    roast(root, keys="raw", display=False)
    tmp = layout_of(root).state / "tmp"
    assert not tmp.exists() or not any(tmp.iterdir())


def test_runs_and_sessions(root):
    """Several scans of a kind are `run-<n>`, every one of them (owner
    decision; legacy: all written to one name); a single scan has no run
    entity; a second session is `ses-02`"""
    b = "ABIDEII-ONRC_2/29200/session_1"
    make_tar(root / "sourcedata" / "ABIDEII-ONRC_2_part1.tar.gz", {
        f"{b}/anat_1/anat.nii.gz": nii_bytes(),
        f"{b}/anat_2/anat.nii.gz": nii_bytes(value=2),
        f"{b}/rest_1/rest.nii.gz": nii_bytes((2, 2, 2, 3)),
        f"{b}/rest_2/rest.nii.gz": nii_bytes((2, 2, 2, 4)),
        "ABIDEII-ONRC_2/29200/session_2/anat_1/anat.nii.gz": nii_bytes(),
        "ABIDEII-ONRC_2/29201/session_1/rest_2/rest.nii.gz":
            nii_bytes((2, 2, 2, 3)),
        "ABIDEII-ONRC_2/29201/session_1/dti_1/dti.nii.gz":
            nii_bytes((2, 2, 2, 3)),
        "ABIDEII-ONRC_2/29201/session_1/dti_1/dti.bval": BVAL,
        "ABIDEII-ONRC_2/29201/session_1/dti_2/dti.nii.gz":
            nii_bytes((2, 2, 2, 3)),
        "ABIDEII-ONRC_2/29201/session_1/dti_2/dti.bval": BVAL,
    })
    report = roast(root, keys=["T1w", "rest", "dti"],
                   subs=["29200", "29201"], json="no", display=False)
    assert not report.errors
    assert raw_tree(root) == {
        "sub-29200/ses-01/anat/sub-29200_ses-01_run-1_T1w.nii.gz",
        "sub-29200/ses-01/anat/sub-29200_ses-01_run-2_T1w.nii.gz",
        "sub-29200/ses-01/func/sub-29200_ses-01_task-rest_run-1_bold.nii.gz",
        "sub-29200/ses-01/func/sub-29200_ses-01_task-rest_run-2_bold.nii.gz",
        "sub-29200/ses-02/anat/sub-29200_ses-02_T1w.nii.gz",
        "sub-29201/ses-01/func/sub-29201_ses-01_task-rest_bold.nii.gz",
        "sub-29201/ses-01/dwi/sub-29201_ses-01_run-1_dwi.nii.gz",
        "sub-29201/ses-01/dwi/sub-29201_ses-01_run-1_dwi.bval",
        "sub-29201/ses-01/dwi/sub-29201_ses-01_run-2_dwi.nii.gz",
        "sub-29201/ses-01/dwi/sub-29201_ses-01_run-2_dwi.bval",
    }
    img = nib.load(raw_file(
        root, "sub-29200/ses-01/anat/sub-29200_ses-01_run-2_T1w.nii.gz"))
    assert img.get_fdata().max() == 2
    # sidecars, once the names are known
    roast(root, keys=["T1w"], subs=["29200"], display=False)
    assert "sub-29200/ses-01/anat/sub-29200_ses-01_run-1_T1w.json" \
        in raw_tree(root)


def test_runs_with_sidecars_only(root):
    """`--sidecars only` names runs as the images would be"""
    b = "ABIDEII-IP_1/29210/session_1"
    make_tar(root / "sourcedata" / "ABIDEII-IP_1.tar.gz", {
        f"{b}/rest_1/rest.nii.gz": nii_bytes((2, 2, 2, 3)),
        f"{b}/rest_2/rest.nii.gz": nii_bytes((2, 2, 2, 3)),
    })
    report = roast(root, keys="rest", subs=[29210], json="only",
                   display=False)
    assert not report.errors
    assert raw_tree(root) == {
        f"sub-29210/ses-01/func/sub-29210_ses-01_task-rest_run-{n}_bold.json"
        for n in (1, 2)}


def test_rerun_does_not_spool(root, monkeypatch):
    """`--if-exists skip` with every output there: no image is spooled
    (both names it may get exist), the outputs stay"""
    roast(root, keys="raw", display=False)
    spooled = []
    real = roaster.spool

    def spy(src, path):
        spooled.append(path)
        return real(src, path)

    monkeypatch.setattr(roaster, "spool", spy)
    (root / "sourcedata" / "NYU" / "1").mkdir(parents=True)
    (root / "sourcedata" / "NYU" / "1" / "T1w.json").write_text("{}")
    report = roast(root, keys="raw", display=False)    # (shard re-runs)
    assert not report.errors and report.status == "complete"
    assert not spooled
    assert raw_tree(root) == RAW
    report = roast(root, keys="raw", ifexists="overwrite", display=False)
    assert not report.errors and spooled


def test_rerun_with_other_run_names(root):
    """A session that gained a scan since the last roast (`run-<n>` now):
    under `--if-exists skip` the new names fail, naming `--if-exists
    overwrite`, which writes them"""
    b = "ABIDEII-IP_1/29230/session_1"
    tarball = root / "sourcedata" / "ABIDEII-IP_1.tar.gz"
    make_tar(tarball, {f"{b}/rest_1/rest.nii.gz": nii_bytes((2, 2, 2, 3))})
    roast(root, keys="rest", subs=[29230], json="no", display=False)
    make_tar(tarball, {f"{b}/rest_1/rest.nii.gz": nii_bytes((2, 2, 2, 3)),
                       f"{b}/rest_2/rest.nii.gz": nii_bytes((2, 2, 2, 3))})
    report = roast(root, keys="rest", subs=[29230], json="no",
                   display=False)
    assert len(report.errors) == 1
    assert "--if-exists overwrite" in str(report.errors[0])
    report = roast(root, keys="rest", subs=[29230], json="no",
                   ifexists="overwrite", display=False)
    assert not report.errors
    assert {f"sub-29230/ses-01/func/sub-29230_ses-01_task-rest_run-{n}"
            f"_bold.nii.gz" for n in (1, 2)} <= raw_tree(root)


def test_spool_error_fails_its_output(root, monkeypatch):
    """An image that cannot be spooled is a `Fail` of that output, not
    "tarball cannot be read" """
    monkeypatch.setattr(roaster, "spool",
                        lambda src, path: "OSError: no space left")
    report = roast(root, keys="T1w", subs=[29001], display=False)
    assert len(report.errors) == 1
    assert "cannot be spooled" in str(report.errors[0])
    assert "tar.gz: cannot be read" not in str(report.errors[0])
    assert raw_tree(root) == {
        "sub-29001/ses-01/anat/sub-29001_ses-01_T1w.json"}


def test_spool_write_error(tmp_path):
    src = io.BytesIO(b"x" * 10)
    assert roaster.spool(src, tmp_path / "no" / "such" / "file") \
        .startswith("FileNotFoundError")
    assert roaster.spool(io.BytesIO(b"abc"), tmp_path / "ok") is None
    assert (tmp_path / "ok").read_bytes() == b"abc"


def test_stale_spools_swept(root):
    """Spool folders of dead processes of this host are removed at the
    start of a roast; those of live processes and other hosts stay"""
    tmp = layout_of(root).state / "tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run([sys.executable, "-c", "pass"])
    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    host = roaster._host()
    stale = tmp / f"abide2-{host}-{dead.pid}-abc"
    alive = tmp / f"abide2-{host}-{os.getppid()}-abc"
    other = tmp / f"abide2-elsewhere-{dead.pid}-abc"
    for folder in (stale, alive, other):
        folder.mkdir()
        (folder / "0.nii.gz").write_bytes(b"x")
    del proc
    report = roast(root, keys="T1w", subs=[29001], display=False)
    assert not report.errors
    assert not stale.exists() and alive.exists() and other.exists()


def test_sessions_not_together(root):
    """A session whose folders come apart in the tarball: the later ones
    are skipped, a `Fail` of the session (their names could contradict)"""
    b = "ABIDEII-IP_1/29220/session_1"
    make_tar(root / "sourcedata" / "ABIDEII-IP_1.tar.gz", {
        f"{b}/rest_1/rest.nii.gz": nii_bytes((2, 2, 2, 3)),
        "ABIDEII-IP_1/29221/session_1/rest_1/rest.nii.gz":
            nii_bytes((2, 2, 2, 3)),
        f"{b}/rest_2/rest.nii.gz": nii_bytes((2, 2, 2, 3)),
    })
    report = roast(root, keys="rest", subs=["29220", "29221"],
                   json="no", display=False)
    assert raw_tree(root) == {
        f"sub-{i}/ses-01/func/sub-{i}_ses-01_task-rest_bold.nii.gz"
        for i in (29220, 29221)}
    assert len(report.errors) == 1 and report.status == "partial"
    assert "sub-29220 ses-01 are not together" in str(report.errors[0])


def test_sessions_not_together_whatever_the_selection(root):
    """Sessions end on the members of every subject: a narrower
    selection writes no more, and the same names (`roast.scope`)"""
    b = "ABIDEII-IP_1/29220/session_1"
    make_tar(root / "sourcedata" / "ABIDEII-IP_1.tar.gz", {
        f"{b}/rest_1/rest.nii.gz": nii_bytes((2, 2, 2, 3)),
        "ABIDEII-IP_1/29221/session_1/anat_1/anat.nii.gz": nii_bytes(),
        f"{b}/rest_2/rest.nii.gz": nii_bytes((2, 2, 2, 3)),
    })
    report = roast(root, keys="rest", subs=["29220"], json="no",
                   display=False)
    assert raw_tree(root) == {
        "sub-29220/ses-01/func/sub-29220_ses-01_task-rest_bold.nii.gz"}
    assert len(report.errors) == 1
    assert "sub-29220 ses-01 are not together" in str(report.errors[0])


@pytest.mark.parametrize("scenario", SCOPE_SCENARIOS)
def test_scope_of_tarballs(root, monkeypatch, scenario):
    """`--subs` narrowing and `meta` skip the tarballs, a new subject
    re-reads the tarball of its site only (`roast.scope`)"""
    opened = open_spy(monkeypatch, roaster)
    check_scope(
        scenario, root, lambda **kw: roast(root, display=False, **kw),
        opened, a=29001, b=29100, label_a="29001", label_b="29100",
        archives={
            "ABIDEII-NYU_1.tar.gz": ({"29001", "29002"},
                                     "roast/raw/ABIDEII-NYU_1.tar.gz"),
            "ABIDEII-KUL_3.tar.gz": ({"29100"},
                                     "roast/raw/ABIDEII-KUL_3.tar.gz")},
        keys={"meta", "pheno", "T1w", "rest", "dti"}, key="dti",
        absent=29999)


def test_keys_of_tarballs(root):
    """Adding a key that a tarball shard reads re-runs it"""
    check_keys_rerun(root, lambda **kw: roast(root, display=False, **kw),
                     {"meta", "pheno", "T1w", "rest", "dti"},
                     {"roast/raw/": roaster.RAW_KEYS})


def test_hires_not_a_run(root, caplog):
    """UCLA `anat_2` may be the high-resolution scan (no template): not
    written as a run of the T1w"""
    b = "ABIDEII-UCLA_Long/29300/session_1"
    make_tar(root / "sourcedata" / "ABIDEII-UCLA_Long.tar.gz", {
        f"{b}/anat_1/anat.nii.gz": nii_bytes(),
        f"{b}/anat_2/anat.nii.gz": nii_bytes(),
    })
    with caplog.at_level(logging.WARNING):
        report = roast(root, keys="T1w", subs=[29300], display=False)
    assert not report.errors
    assert raw_tree(root) == {
        "sub-29300/ses-01/anat/sub-29300_ses-01_T1w.nii.gz",
        "sub-29300/ses-01/anat/sub-29300_ses-01_T1w.json"}
    assert "high-resolution" in caplog.text


def test_unknown_scans_skipped(root, caplog):
    """Scans without a BIDS name are skipped, with one warning per
    tarball; a second image of a scan is skipped with a warning"""
    b = "ABIDEII-BNI_1/29010/session_1"
    make_tar(root / "sourcedata" / "ABIDEII-BNI_1.tar.gz", {
        f"{b}/anat_1/anat_copy.nii.gz": nii_bytes(value=2),
        f"{b}/anat_1/anat.nii.gz": nii_bytes(),
        f"{b}/flair_1/flair.nii.gz": nii_bytes(),
        f"{b}/flair_2/flair.nii.gz": nii_bytes(),
        f"{b}/dti_1/dti.txt": b"x",
        f"{b}/anat_1/notes.txt": b"x",
        "ABIDEII-BNI_1/README.txt": b"x",
    })
    with caplog.at_level(logging.WARNING):
        report = roast(root, subs=[29010], json="no", display=False)
    assert not report.errors, report.errors
    assert {p for p in raw_tree(root) if p.startswith("sub-")} == {
        "sub-29010/ses-01/anat/sub-29010_ses-01_T1w.nii.gz"}
    assert caplog.text.count("no BIDS name") == 1
    assert "flair_<n> (2 files)" in caplog.text
    assert "2 files for .nii.gz" in caplog.text
    img = nib.load(raw_file(
        root, "sub-29010/ses-01/anat/sub-29010_ses-01_T1w.nii.gz"))
    assert img.get_fdata().max() == 1           # `anat.nii.gz` preferred


@pytest.mark.parametrize("names,used", [
    # AppleDouble files (`._*`) are skipped
    (["._anat.nii.gz", "anat.nii.gz"], 1),
    # the file named as its folder kind is preferred, in any case
    (["mprage.nii.gz", "ANAT.NII.GZ"], 1),
    # else the only image
    (["mprage.nii"], 0),
])
def test_image_choice(root, names, used):
    b = "ABIDEII-GU_1/28810/session_1/anat_1"
    data = {
        f"{b}/{name}": (nii_bytes(gz=not name.endswith(".nii"),
                                  value=i) if "._" not in name
                        else b"AppleDouble")
        for i, name in enumerate(names)}
    make_tar(root / "sourcedata" / "ABIDEII-GU_1.tar.gz", data)
    report = roast(root, keys="T1w", subs=[28810], json="no",
                   display=False)
    assert not report.errors
    ext = ".nii" if names[used].lower().endswith(".nii") else ".nii.gz"
    img = nib.load(raw_file(
        root, f"sub-28810/ses-01/anat/sub-28810_ses-01_T1w{ext}"))
    assert img.get_fdata().max() == used


def test_case_insensitive_names(root):
    b = "ABIDEII-GU_1/28820/Session_1"
    make_tar(root / "sourcedata" / "ABIDEII-GU_1.tar.gz", {
        f"{b}/DTI_1/DTI.NII.GZ": nii_bytes((2, 2, 2, 3)),
        f"{b}/DTI_1/DTI.BVALS": BVAL,
        f"{b}/DTI_1/dti.Bvecs": BVEC,
    })
    report = roast(root, keys="dti", subs=[28820], json="no",
                   display=False)
    assert not report.errors
    assert raw_tree(root) == {
        f"sub-28820/ses-01/dwi/sub-28820_ses-01_dwi{e}"
        for e in (".nii.gz", ".bval", ".bvec")}


def test_member_names_normalised(root, caplog):
    """`./`, leading folders and a leading `/` are ignored; `..` is
    skipped"""
    make_tar(root / "sourcedata" / "ABIDEII-GU_1.tar.gz", {
        "./ABIDEII-GU_1/28800/session_1/anat_1/anat.nii.gz": nii_bytes(),
        "release/ABIDEII-GU_1/28801/session_1/anat_1/anat.nii.gz":
            nii_bytes(),
        "/ABIDEII-GU_1/28802/session_1/anat_1/anat.nii.gz": nii_bytes(),
        "../ABIDEII-GU_1/28803/session_1/anat_1/anat.nii.gz": nii_bytes(),
    })
    with caplog.at_level(logging.WARNING):
        report = roast(root, keys="T1w", subs=["28800:28804"], json="no",
                       display=False)
    assert not report.errors
    assert raw_tree(root) == {
        f"sub-{i}/ses-01/anat/sub-{i}_ses-01_T1w.nii.gz"
        for i in (28800, 28801, 28802)}
    assert "outside the archive" in caplog.text


def test_site_of_members(root, caplog):
    """The site of a member is that of its folder, else of the tarball
    (U_MIA has no template: written without sidecars, one warning)"""
    make_tar(root / "sourcedata" / "ABIDEII-U_MIA_1.tar.gz", {
        "ABIDEII-U_MIA_1/30230/session_1/anat_1/anat.nii.gz": nii_bytes(),
        "U_MIA/30231/session_1/anat_1/anat.nii.gz": nii_bytes(),
    })
    make_tar(root / "sourcedata" / "ABIDEII-USM_1.tar.gz", {
        "data/30400/session_1/anat_1/anat.nii.gz": nii_bytes(),
    })
    with caplog.at_level(logging.WARNING):
        report = roast(root, keys="T1w", subs=["30230", "30231", "30400"],
                       display=False)
    assert raw_tree(root) == {
        "sub-30230/ses-01/anat/sub-30230_ses-01_T1w.nii.gz",
        "sub-30231/ses-01/anat/sub-30231_ses-01_T1w.nii.gz",
        "sub-30400/ses-01/anat/sub-30400_ses-01_T1w.nii.gz",
        "sub-30400/ses-01/anat/sub-30400_ses-01_T1w.json",
    }
    assert not report.errors and report.status == "complete"
    assert caplog.text.count("U_MIA has no sidecar templates") == 1


def test_sites_without_templates(root, monkeypatch):
    """SU, U_MIA: no templates (no printout) is not an error: the shard
    completes, and is skipped on a re-run; a missing template of another
    site is"""
    make_tar(root / "sourcedata" / "ABIDEII-SU_2.tar.gz",
             subject_members(29400, "ABIDEII-SU_2"))
    opened = []
    real = roaster.TarArchive

    def opener(path):
        opened.append(path.name)
        return real(path)

    monkeypatch.setattr(roaster, "TarArchive", opener)
    report = roast(root, keys="raw", display=False)
    assert not report.errors and report.status == "complete"
    assert scans(29400) - {p for p in scans(29400)
                           if p.endswith(".json")} <= raw_tree(root)
    assert not any("29400" in p and p.endswith(".json")
                   for p in raw_tree(root))
    opened.clear()
    roast(root, keys="raw", display=False)
    assert opened == []
    monkeypatch.setattr(roaster, "NO_TEMPLATES", ())
    report = roast(root, keys="T1w", subs=[29400], ifexists="overwrite",
                   display=False)
    assert len(report.errors) == 1


def test_kki_common_templates(root):
    """KKI: the tarballs do not tell the head coil; the sidecars hold
    what the 8- and 32-channel templates share"""
    make_tar(root / "sourcedata" / "ABIDEII-KKI_1_29273_29322.tar.gz",
             subject_members(29273, "ABIDEII-KKI_1", dwi=False))
    report = roast(root, keys=["T1w", "rest"], subs=[29273], display=False)
    assert not report.errors
    t1w = sidecar(root, "sub-29273/ses-01/anat/sub-29273_ses-01_T1w.json")
    assert t1w == tpl("KKI/1/T1w.json")
    rc8, rc32 = (tpl(f"KKI/1/acq-rc{n}chan_T1w.json") for n in (8, 32))
    assert t1w == {k: v for k, v in rc8.items() if rc32.get(k) == v}


def test_bold_template_without_tr(root):
    """UCLA/2 bold has no RepetitionTime: read from the NIfTI header,
    also under `--sidecars only` (from the member)"""
    assert "RepetitionTime" not in tpl("UCLA/2/bold.json")
    img = nib.Nifti1Image(np.zeros((2, 2, 2, 3), dtype=np.int16), np.eye(4))
    img.header.set_xyzt_units("mm", "sec")
    img.header["pixdim"][4] = 3.0
    make_tar(root / "sourcedata" / "ABIDEII-UCLA_2.tar.gz", {
        "ABIDEII-UCLA_2/29400/session_1/rest_1/rest.nii.gz":
            gzip.compress(img.to_bytes())})
    for json_policy in ("only", "yes"):
        report = roast(root, keys="rest", subs=[29400], json=json_policy,
                       ifexists="overwrite", display=False)
        assert not report.errors, report.errors
        meta = sidecar(root, "sub-29400/ses-01/func/"
                             "sub-29400_ses-01_task-rest_bold.json")
        assert meta == {**tpl("UCLA/2/bold.json"), "TaskName": "rest",
                        "RepetitionTime": 3.0}


def test_header_sidecar_under_skip_and_update(root, caplog):
    """`HeaderSidecar` (a bold template without RepetitionTime) is a
    tracked sidecar: left alone under `skip`, rewritten from a new
    template under `update`, an edit kept"""
    img = nib.Nifti1Image(np.zeros((2, 2, 2, 3), dtype=np.int16), np.eye(4))
    img.header.set_xyzt_units("mm", "sec")
    img.header["pixdim"][4] = 3.0
    make_tar(root / "sourcedata" / "ABIDEII-UCLA_2.tar.gz", {
        f"ABIDEII-UCLA_2/{i}/session_1/rest_1/rest.nii.gz":
            gzip.compress(img.to_bytes()) for i in (29400, 29401)})
    roast(root, keys="rest", subs=[29400, 29401], display=False)
    paths = [raw_file(root, f"sub-{i}/ses-01/func/"
                            f"sub-{i}_ses-01_task-rest_bold.json")
             for i in (29400, 29401)]
    old = [p.read_bytes() for p in paths]
    local = root / "sourcedata" / "UCLA" / "2" / "bold.json"
    local.parent.mkdir(parents=True)
    local.write_text(json.dumps({**tpl("UCLA/2/bold.json"), "Local": 1}))
    caplog.clear()
    roast(root, keys="rest", subs=[29400, 29401], display=False)
    assert [p.read_bytes() for p in paths] == old
    assert "WARNING" not in caplog.text
    paths[1].write_text('{"Mine": true}')
    roast(root, keys="rest", subs=[29400, 29401], ifexists="update",
          display=False)
    assert json.loads(paths[0].read_text()) == {
        **tpl("UCLA/2/bold.json"), "Local": 1, "TaskName": "rest",
        "RepetitionTime": 3.0}
    assert json.loads(paths[1].read_text()) == {"Mine": True}
    assert "was edited" in caplog.text


def test_missing_sidecar_template(root, monkeypatch):
    """A missing sidecar template is an error only if sidecars are
    written"""
    monkeypatch.setattr(roaster.RoastContext, "resource",
                        lambda self, name: None)
    report = roast(root, keys="T1w", json="no", display=False)
    assert not report.errors
    report = roast(root, keys="T1w", ifexists="overwrite", display=False)
    assert len(report.errors) == 3


def test_missing_tarballs(tmp_path):
    root = tmp_path / "ABIDE-2"
    (root / "sourcedata").mkdir(parents=True)
    report = roast(root, keys="T1w", display=False)
    assert raw_tree(root) == set()
    assert len(report.errors) == 1
    assert "tarball" in report.errors[0][2]


def test_not_asked_not_an_error(tmp_path):
    """Missing inputs are errors only when they are needed"""
    root = tmp_path / "ABIDE-2"
    src = root / "sourcedata"
    src.mkdir(parents=True)
    write_csv(src / "ABIDEII-NYU_1.csv", [{"SUB_ID": "29001"}])
    report = roast(root, keys="meta", display=False)
    assert not report.errors            # no tarball: not asked for
    make_tar(src / "ABIDEII-NYU_1.tar.gz", subject_members(29001))
    (src / "ABIDEII-NYU_1.csv").unlink()
    report = roast(root, keys="raw", display=False)
    assert not report.errors            # no phenotypes: not asked for


def test_corrupt_archive_fails_its_shard_only(root):
    src = root / "sourcedata"
    (src / "ABIDEII-GU_1.tar.gz").write_bytes(b"not a tarball")
    data = (src / "ABIDEII-NYU_1.tar.gz").read_bytes()
    (src / "ABIDEII-NYU_1.tar.gz").write_bytes(data[:len(data) // 2])
    report = roast(root, keys=["T1w", "rest"], display=False)
    assert scans(29100, dwi=False) <= raw_tree(root)
    # one per tarball (and the member that the truncation cut, if any)
    assert 2 <= len(report.errors) <= 3, report.errors
    assert any("ABIDEII-GU_1.tar.gz: cannot be read" in str(e)
               for e in report.errors)
    assert any("ABIDEII-NYU_1.tar.gz: cannot be read" in str(e)
               for e in report.errors)
    assert not any("29100" in str(e) for e in report.errors)


def test_archive_closed(root, monkeypatch):
    """The tarball is closed on every path (also when it fails)"""
    closed = []
    real = roaster.TarArchive

    class Spy(real):
        def __exit__(self, *args):
            closed.append(self.path.name)
            return super().__exit__(*args)

    monkeypatch.setattr(roaster, "TarArchive", Spy)
    src = root / "sourcedata"
    data = (src / "ABIDEII-NYU_1.tar.gz").read_bytes()
    (src / "ABIDEII-NYU_1.tar.gz").write_bytes(data[:len(data) // 2])
    roast(root, keys="raw", display=False)
    assert sorted(closed) == ["ABIDEII-KUL_3.tar.gz", "ABIDEII-NYU_1.tar.gz"]


def test_tarball_read_once(root, monkeypatch):
    """Members are read lazily (no listing first: one decompression)"""
    def listing(self):
        raise AssertionError("the tarball is listed")

    monkeypatch.setattr(roaster.TarArchive, "getmembers", listing)
    monkeypatch.setattr(roaster.TarArchive, "getnames", listing)
    report = roast(root, display=False)
    assert not report.errors
    assert raw_tree(root) == ALL


def test_one_shard_per_tarball(root, monkeypatch):
    opened = []
    real = roaster.TarArchive

    def opener(path):
        opened.append(path.name)
        return real(path)

    monkeypatch.setattr(roaster, "TarArchive", opener)
    roast(root, keys=["T1w", "rest"], display=False)
    assert sorted(opened) == ["ABIDEII-KUL_3.tar.gz", "ABIDEII-NYU_1.tar.gz"]
    # unchanged inputs and options: skipped by signature
    roast(root, keys=["T1w", "rest"], display=False)
    assert len(opened) == 2
    # a new template of a site re-runs the shards of that site only
    (root / "sourcedata" / "NYU" / "1").mkdir(parents=True)
    (root / "sourcedata" / "NYU" / "1" / "T1w.json").write_text(
        '{"EchoTime": 1}')
    roast(root, keys=["T1w", "rest"], display=False)
    assert sorted(opened) == ["ABIDEII-KUL_3.tar.gz", "ABIDEII-NYU_1.tar.gz",
                              "ABIDEII-NYU_1.tar.gz"]
    roast(root, keys=["T1w", "rest"], ifexists="overwrite", display=False)
    assert sidecar(root, "sub-29001/ses-01/anat/sub-29001_ses-01_T1w.json") \
        == {"EchoTime": 1}


def test_shard_config_has_templates(root):
    ctx = make_context(layout_of(root), roaster.KEYS, templates=TEMPLATES)
    config = roaster.template_config(
        ctx, root / "sourcedata" / "ABIDEII-NYU_1.tar.gz")
    assert config["site"] == "NYU/1"
    assert set(config["templates"]) == {"T1w.json", "bold.json", "dwi.json"}
    path, sha = config["templates"]["dwi.json"]
    assert path.endswith("NYU/1/dwi.json") and len(sha) == 64
    # no site in the name: every site's templates
    config = roaster.template_config(
        ctx, root / "sourcedata" / "other.tar.gz")
    assert config["site"] is None
    assert config["templates"]["NYU/1"]["dwi.json"][1] == sha
    assert "KKI/1" in config["templates"]


def test_no_site_tarball_reruns_on_template_change(root, monkeypatch):
    src = root / "sourcedata"
    (src / "ABIDEII-NYU_1.tar.gz").rename(src / "mydata.tar.gz")
    opened = []
    real = roaster.TarArchive

    def opener(path):
        opened.append(path.name)
        return real(path)

    monkeypatch.setattr(roaster, "TarArchive", opener)
    roast(root, keys="T1w", subs=[29001], display=False)
    roast(root, keys="T1w", subs=[29001], display=False)
    assert opened.count("mydata.tar.gz") == 1
    (src / "NYU" / "1").mkdir(parents=True)
    (src / "NYU" / "1" / "T1w.json").write_text('{"EchoTime": 1}')
    roast(root, keys="T1w", subs=[29001], ifexists="overwrite",
          display=False)
    assert opened.count("mydata.tar.gz") == 2
    assert sidecar(root, "sub-29001/ses-01/anat/sub-29001_ses-01_T1w.json") \
        == {"EchoTime": 1}


@pytest.mark.parametrize("content", [b"{not json", b"[1, 2]", None])
def test_unreadable_template(root, content):
    """A template that cannot be read fails its sidecar, not the shard"""
    tpl_path = root / "sourcedata" / "NYU" / "1" / "T1w.json"
    tpl_path.parent.mkdir(parents=True)
    if content is None:
        tpl_path.mkdir()                # (an OSError when read)
    else:
        tpl_path.write_bytes(content)
    report = roast(root, keys="T1w", subs=[29001], display=False)
    assert len(report.errors) == 1, report.errors
    assert ".tar.gz: cannot be read" not in str(report.errors[0])
    assert "template" in str(report.errors[0])
    assert "sub-29001/ses-01/anat/sub-29001_ses-01_T1w.nii.gz" \
        in raw_tree(root)


def test_jobs(root):
    report = roast(root, jobs=2, display=False)
    assert not report.errors
    assert raw_tree(root) == ALL


def test_bucket_copy_not_roasted(root, caplog):
    """`sourcedata/RawData/` (`--source s3`) is BIDS-like: not roasted,
    with a warning; the tarballs (of the sites the bucket lacks) are"""
    rawdata = root / "sourcedata" / "RawData" / "ABIDEII-GU_1" / \
        "sub-28800" / "ses-1" / "anat"
    rawdata.mkdir(parents=True)
    (rawdata / "sub-28800_ses-1_run-1_T1w.nii.gz").write_bytes(nii_bytes())
    with caplog.at_level(logging.WARNING):
        report = roast(root, keys="T1w", display=False)
    assert not report.errors
    assert raw_tree(root) == {p for p in RAW if "/anat/" in p}
    assert "RawData/" in caplog.text


# ----------------------------------------------------------------------
#   Tables
# ----------------------------------------------------------------------

def test_participants(root):
    report = roast(root, keys="meta", display=False)
    assert not report.errors
    assert raw_tree(root) == META | PHENO
    rows = read_tsv(original(root))
    assert rows[0] == list(PARTICIPANTS.columns)
    row = dict(zip(rows[0], rows[2]))
    assert {k: row[k] for k in ("site", "sample", "sex", "age", "handedness",
                                "group", "fiq", "fiq_type", "med_status",
                                "bmi")} == {
        "site": "NYU", "sample": "NYU1", "sex": "M", "age": "10.5",
        "handedness": "B", "group": "C", "fiq": "n/a", "fiq_type": "WISC-4",
        "med_status": "Y", "bmi": "n/a"}
    assert dict(zip(rows[0], rows[1]))["sample"] == "KUL"
    # trimmed to the subjects on disk (none yet)
    assert read_tsv(raw_file(root, "participants.tsv")) == [rows[0]]


def test_participants_json_samples():
    """Every value of `sample` (the site, or the site and a sample) is a
    level of `participants.json`; age in years (BIDS unit "year")"""
    meta = json.loads((TEMPLATES / "participants.json").read_text())
    values = {sites.sample_label(f"ABIDEII-{site}") for site in sites.SITES}
    values |= {sites.sample_label(f"ABIDEII-{site}_{sample}")
               for site, samples in sites.SAMPLES.items()
               for sample in samples}
    values |= {sites.sample_label(s) for s in ("ABIDE-I_UCLA_1",
                                               "ABIDE-I_PITT")}
    assert values == set(meta["sample"]["Levels"])
    assert {"KUL", "UMia", "NYU1", "UPSMLong"} <= values
    assert meta["age"]["Units"] == "year"


def test_phenotype_names_are_bids():
    """Phenotype tables are named without `.` in their stem (`cbcl_1.5-5`
    is `cbcl_1p5-5`), each with its data dictionary"""
    assert "cbcl_1p5-5" in tables.PHENOTYPES
    for name in tables.PHENOTYPES:
        assert "." not in name
        assert (TEMPLATES / "phenotype" / f"{name}.json").is_file()


def make_old_cbcl(root):
    """An earlier roast that wrote `cbcl_1.5-5` (the new files renamed,
    the manifest entry of the dictionary too)"""
    roast(root, keys="meta", display=False)
    lay = layout_of(root)
    new, old = lay.phenotype / "cbcl_1p5-5", lay.phenotype / "cbcl_1.5-5"
    for ext in (".tsv", ".json"):
        Path(f"{new}{ext}").rename(f"{old}{ext}")
    lay.original(Path(f"{new}.tsv")).rename(lay.original(Path(f"{old}.tsv")))
    manifest = lay.state / "manifest.json"
    data = json.loads(manifest.read_text())
    key = lambda p: Path(p).relative_to(lay.root).as_posix()  # noqa: E731
    data["files"][key(f"{old}.json")] = data["files"].pop(key(f"{new}.json"))
    manifest.write_text(json.dumps(data))
    return lay, old


def test_renamed_phenotype_removed(root, caplog):
    """`cbcl_1.5-5` of an earlier roast: removed (pristine copy, table,
    dictionary), under pure skip too; it does not come back"""
    lay, old = make_old_cbcl(root)
    with caplog.at_level(logging.INFO):
        report = roast(root, keys="T1w", display=False)
    assert not report.errors
    for ext in (".tsv", ".json"):
        assert not Path(f"{old}{ext}").exists()
    assert not lay.original(Path(f"{old}.tsv")).exists()
    assert "cbcl_1.5-5.tsv: removed" in caplog.text
    assert "WARNING" not in caplog.text
    roast(root, keys="meta", display=False)
    assert not Path(f"{old}.tsv").exists()
    assert (lay.phenotype / "cbcl_1p5-5.tsv").exists()


def test_renamed_phenotype_edited_kept(root, caplog):
    """An edited `cbcl_1.5-5` table or dictionary is kept (warned); the
    pristine copy is removed, so that it is not derived again"""
    lay, old = make_old_cbcl(root)
    Path(f"{old}.tsv").write_text("participant_id\tmine\n")
    Path(f"{old}.json").write_text('{"mine": {}}')
    roast(root, keys="T1w", display=False)
    assert Path(f"{old}.tsv").read_text() == "participant_id\tmine\n"
    assert Path(f"{old}.json").read_text() == '{"mine": {}}'
    assert not lay.original(Path(f"{old}.tsv")).exists()
    assert caplog.text.count("edited since brainspresso wrote it") == 2


def test_participants_sub_prefix(root):
    roast(root, keys="meta", display=False)
    assert ids(original(root)) == ["sub-29100", "sub-29001", "sub-29002"]


def test_participants_med_status_column(root):
    """`CURRENT_MED_STATUS` is read (legacy: a stray backtick)"""
    assert "`" not in "".join(n for c in PARTICIPANTS.columns
                              for n in PARTICIPANTS.names(c))
    write_csv(root / "sourcedata" / "ABIDEII-NYU_1.csv", [
        {"SUB_ID": "29001", "CURRENT_MED_STATUS": "0"}],
        cols=["SUB_ID", "CURRENT_MED_STATUS"])
    roast(root, keys="meta", display=False)
    rows = read_tsv(original(root))
    assert dict(zip(rows[0], rows[2]))["med_status"] == "N"


def test_participants_from_abide2_files(root):
    """The ABIDE-II files are read, not ABIDE-I's `Phenotypic_V1_0b.csv`
    (legacy); the composite file when there is no site file"""
    src = root / "sourcedata"
    write_csv(src / "Phenotypic_V1_0b.csv", [{"SUB_ID": "50001"}])
    roast(root, keys="meta", display=False)
    assert ids(original(root)) == ["sub-29100", "sub-29001", "sub-29002"]
    for path in src.glob("ABIDEII-*.csv"):
        path.unlink()
    write_csv(src / "ABIDEII_Composite_Phenotypic.csv", [
        {"SUB_ID": "29001", "SITE_ID": "ABIDEII-NYU_1"},
        {"SUB_ID": "29511", "SITE_ID": "ABIDE-I_PITT"}])
    report = roast(root, keys="meta", display=False)
    assert not report.errors
    rows = read_tsv(original(root))
    assert [r[:3] for r in rows[1:]] == [["sub-29001", "NYU", "NYU1"],
                                         ["sub-29511", "UPSM", "UPSMLong"]]


def test_participants_sites_and_samples(tmp_path, caplog):
    root = tmp_path / "ABIDE-2"
    src = root / "sourcedata"
    src.mkdir(parents=True)
    values = {
        "29001": "ABIDEII-U_MIA_1", "29002": "ABIDE-II_UCLA_Long",
        "29003": "ABIDE-I_UCLA_2", "29004": "ABIDEII-ETHZ_1",
        "29005": "ABIDEII-ONRC_2", "29006": "OTHER",
        "29007": "ABIDEII-UCLA_1", "29008": "ABIDE-I_UCLA_1",
        "29009": "ABIDEII-OILH_2",
    }
    write_csv(src / "ABIDEII-X_1.csv",
              [{"SUB_ID": k, "SITE_ID": v} for k, v in values.items()],
              cols=["SITE_ID", "SUB_ID"])
    with caplog.at_level(logging.WARNING):
        roast(root, keys="meta", display=False)
    got = {r[0]: (r[1], r[2]) for r in read_tsv(original(root))[1:]}
    assert got == {
        "sub-29001": ("UMia", "UMia"), "sub-29002": ("UCLA", "UCLALong"),
        "sub-29003": ("UCLA", "UCLALong"), "sub-29004": ("ETH", "ETH"),
        "sub-29005": ("ONRC", "ONRC2"), "sub-29006": ("OTHER", "OTHER"),
        "sub-29007": ("UCLA", "UCLA1"), "sub-29008": ("UCLA", "UCLALong"),
        "sub-29009": ("ONRC", "ONRC2"),
    }
    # an unknown SITE_ID is warned about once (site and sample)
    assert caplog.text.count("unknown SITE_ID 'OTHER'") == 1
    levels = tpl("participants.json")
    for sub, (site, sample) in got.items():
        if sub == "sub-29006":
            continue
        assert site in levels["site"]["Levels"]
        assert sample == site or sample in levels["sample"]["Levels"]


def test_participants_bad_rows(root, caplog):
    """Blank rows are skipped, rows without a subject (or listed twice)
    are skipped with a warning; files may order columns differently; a
    missing column is n/a"""
    csv = root / "sourcedata" / "ABIDEII-NYU_1.csv"
    csv.write_text(csv.read_text() + "\n" + "," * (len(COLS) - 1) + "\n"
                   + ",".join("ABIDEII-NYU_1" if c == "SITE_ID" else ""
                              for c in COLS) + "\n"
                   + ",".join("29001" if c == "SUB_ID" else ""
                              for c in COLS) + "\n")
    write_csv(root / "sourcedata" / "ABIDEII-SDSU_1.csv",
              [{"SUB_ID": "28853", "SEX": "2"}],
              cols=["SEX", "SUB_ID", "SITE_ID"])
    with caplog.at_level(logging.WARNING):
        report = roast(root, keys="meta", display=False)
    assert not report.errors
    rows = read_tsv(original(root))
    assert [r[0] for r in rows[1:]] == ["sub-29100", "sub-29001",
                                        "sub-29002", "sub-28853"]
    last = dict(zip(rows[0], rows[4]))
    assert (last["sex"], last["age"], last["bmi"]) == ("F", "n/a", "n/a")
    assert "" not in {cell for row in rows for cell in row}
    assert caplog.text.count("row skipped") == 2


def test_csv_encoding_and_cells(tmp_path):
    """Windows-1252 files are read; line breaks in a cell are spaces"""
    root = tmp_path / "ABIDE-2"
    src = root / "sourcedata"
    src.mkdir(parents=True)
    (src / "ABIDEII-IP_1.csv").write_bytes(
        'SUB_ID,CURRENT_MEDICATION_NAME\n29001,"Méthyl\nphenidate"\n'
        .encode("cp1252"))
    report = roast(root, keys="meta", display=False)
    assert not report.errors
    rows = [line.split("\t") for line in
            original(root).read_text(encoding="utf-8").splitlines()]
    assert dict(zip(rows[0], rows[1]))["med_name"] == "Méthyl phenidate"
    assert read_csv(src / "ABIDEII-IP_1.csv")[1][0] == "29001"


def test_phenotypes(root):
    """Phenotype tables are written complete to `.brainspresso/orig/`
    and trimmed to the subjects on disk, as participants.tsv"""
    report = roast(root, keys="pheno", display=False)
    assert not report.errors
    assert raw_tree(root) == PHENO
    srs = read_tsv(original(root, "phenotype/srs.tsv"))
    assert srs[0] == list(PHENOS["srs"].columns)
    row = dict(zip(srs[0], srs[2]))
    assert (row["participant_id"], row["version"], row["informant"],
            row["total_raw"]) == ("sub-29001", "C", "n/a", "42")
    assert read_tsv(raw_file(root, "phenotype/srs.tsv")) == [srs[0]]
    assert sidecar(root, "phenotype/srs.json") == tpl("phenotype/srs.json")
    roast(root, keys=["pheno", "T1w"], subs=[29001], display=False)
    assert ids(raw_file(root, "phenotype/srs.tsv")) == ["sub-29001"]
    roast(root, keys=["meta", "T1w"], subs=[29001], display=False)
    assert ids(raw_file(root, "participants.tsv")) == ["sub-29001"]


def test_phenotype_templates():
    """Every phenotype table has a data dictionary (legacy: 20 empty
    files, `rbsr5`/`rbsr6` named unlike their templates), and README is
    ABIDE-II's (legacy: CoRR's)"""
    assert "rbsr_5" in PHENOS and "rbsr_6" in PHENOS
    for name, table in PHENOS.items():
        meta = tpl(f"phenotype/{name}.json")
        assert "MeasurementToolMetadata" in meta, name
        assert set(meta) - {"MeasurementToolMetadata"} \
            <= set(table.columns), name
    assert (TEMPLATES / "README").read_text(encoding="utf-8").startswith(
        "# Autism Brain Imaging Data Exchange II")


def test_column_names_without_case_and_spaces(tmp_path):
    """Columns are found by name, without case or whitespace (the legend
    spells `RBSR_5SUBSCALE_RITUAL   ISTIC`, `ADI_R_A3_Total`); known
    typos are accepted both ways"""
    root = tmp_path / "ABIDE-2"
    src = root / "sourcedata"
    src.mkdir(parents=True)
    cols = ["sub_id", "RBSR_5SUBSCALE_RITUALISTIC", "ADI_R_A3_TOTAL",
            "CBCL_6-18_SOMATIC_COMPLAINT_T", "VINELAND_DAILYLVNG_STANDARD"]
    write_csv(src / "ABIDEII-TCD_1.csv",
              [dict(zip(cols, ["29001", "3", "4", "50", "80"]))], cols=cols)
    report = roast(root, keys="pheno", display=False)
    assert not report.errors

    def cell(table, column):
        rows = read_tsv(original(root, f"phenotype/{table}.tsv"))
        return dict(zip(rows[0], rows[1]))[column]

    assert cell("rbsr_5", "ritualistic") == "3"
    assert cell("adir_a3", "total") == "4"
    assert cell("cbcl_6-18", "somatic_complaint") == "50"
    assert cell("vineland", "dailylvng") == "80"


def test_quotes_in_cells(root, caplog):
    """A `"` in a cell becomes `'` (warned once per table), so that no
    cell needs quoting; the table survives the trim"""
    write_csv(root / "sourcedata" / "ABIDEII-NYU_1.csv", [
        {"SUB_ID": "29001", "NONASD_PSYDX_LABEL": '"ADHD, combined'},
        {"SUB_ID": "29002", "CURRENT_MEDICATION_NAME": 'Ritalin 5" tabs'}],
        cols=["SUB_ID", "NONASD_PSYDX_LABEL", "CURRENT_MEDICATION_NAME"])
    path = root / "sourcedata" / "ABIDEII-NYU_1.csv"
    path.write_text(path.read_text().replace(
        '"ADHD, combined', '"""ADHD, combined"').replace(
        'Ritalin 5" tabs', '"Ritalin 5"" tabs"'))
    with caplog.at_level(logging.WARNING):
        report = roast(root, keys=["meta", "T1w"], display=False)
    assert not report.errors, report.errors
    for path in (original(root), raw_file(root, "participants.tsv")):
        rows = [line.split("\t") for line in
                path.read_text(encoding="utf-8").splitlines()]
        got = {r[0]: dict(zip(rows[0], r)) for r in rows[1:]}
        assert got["sub-29001"]["nonasd_psydx_label"] == "'ADHD, combined"
        assert got["sub-29002"]["med_name"] == "Ritalin 5' tabs"
    assert caplog.text.count("participants.tsv: '\"' replaced") == 1


def test_absent_column_warned_once_per_table(root, caplog):
    with caplog.at_level(logging.WARNING):
        roast(root, keys="meta", display=False)
    assert caplog.text.count("participants.tsv: no CSV file has a column "
                             "for") == 0           # (all columns there)
    write_csv(root / "sourcedata" / "ABIDEII-NYU_1.csv",
              [{"SUB_ID": "29001"}], cols=["SUB_ID", "SITE_ID"])
    write_csv(root / "sourcedata" / "ABIDEII-KUL_3.csv",
              [{"SUB_ID": "29100"}], cols=["SUB_ID", "SITE_ID"])
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        roast(root, keys="meta", ifexists="overwrite", display=False)
    assert caplog.text.count("participants.tsv: no CSV file has a column "
                             "for") == 1
    assert "sex, age" in caplog.text


def test_handedness(tmp_path, caplog):
    """Categories R, L, B; numeric codes (not in the legend of the legacy
    roaster) are kept, with one warning, until the codes are checked"""
    root = tmp_path / "ABIDE-2"
    src = root / "sourcedata"
    src.mkdir(parents=True)
    values = ["R", "l", "Ambi", "Mixed", "1", "2", "3"]
    write_csv(src / "ABIDEII-GU_1.csv",
              [{"SUB_ID": str(29001 + i), "HANDEDNESS_CATEGORY": v}
               for i, v in enumerate(values)],
              cols=["SUB_ID", "HANDEDNESS_CATEGORY"])
    with caplog.at_level(logging.WARNING):
        roast(root, keys="meta", display=False)
    rows = read_tsv(original(root))
    got = [dict(zip(rows[0], r))["handedness"] for r in rows[1:]]
    assert got == ["R", "L", "B", "B", "1", "2", "3"]
    assert caplog.text.count("HANDEDNESS_CATEGORY: numeric") == 1


def test_longitudinal_baseline(tmp_path, caplog):
    """`*_Long.csv`: a subject's baseline row (the lowest AGE_AT_SCAN)
    gives its participants row; one warning per file"""
    root = tmp_path / "ABIDE-2"
    src = root / "sourcedata"
    src.mkdir(parents=True)
    write_csv(src / "ABIDEII-UCLA_Long.csv", [
        {"SUB_ID": "29001", "SITE_ID": "ABIDE-II_UCLA_Long",
         "AGE_AT_SCAN": "14.1", "BMI": "22"},
        {"SUB_ID": "29001", "SITE_ID": "ABIDE-I_UCLA_1",
         "AGE_AT_SCAN": "11.9", "BMI": "18"},
        {"SUB_ID": "29002", "SITE_ID": "ABIDE-I_UCLA_2",
         "AGE_AT_SCAN": "10", "BMI": "17"},
        {"SUB_ID": "29002", "SITE_ID": "ABIDE-II_UCLA_Long",
         "AGE_AT_SCAN": "12", "BMI": "19"},
    ])
    with caplog.at_level(logging.WARNING):
        report = roast(root, keys="meta", display=False)
    assert not report.errors
    rows = read_tsv(original(root))
    got = {r[0]: dict(zip(rows[0], r)) for r in rows[1:]}
    assert [got[s]["age"] for s in ("sub-29001", "sub-29002")] == \
        ["11.9", "10"]
    assert {r["sample"] for r in got.values()} == {"UCLALong"}
    srs = read_tsv(original(root, "phenotype/srs.tsv"))
    assert len(srs) == 3
    assert caplog.text.count("ABIDEII-UCLA_Long.csv: 2 rows skipped") == 1


@pytest.mark.parametrize("age", ["-9999", "nan", "n/a", "", "inf"])
def test_baseline_ignores_missing_ages(tmp_path, age):
    """A missing age (`-9999`, `nan`...) never makes a row the baseline"""
    path = tmp_path / "ABIDEII-UCLA_Long.csv"
    path.write_text("SUB_ID,SITE_ID,AGE_AT_SCAN,BMI\n"
                    "29001,ABIDE-I_UCLA_1,10,base\n"
                    f"29001,ABIDEII-UCLA_Long,{age},follow\n")
    rows = list(PARTICIPANTS.rows([path]))
    assert dict(zip(rows[0], rows[1]))["bmi"] == "base"


def test_missing_phenotypes(root):
    for path in (root / "sourcedata").glob("ABIDEII-*.csv"):
        path.unlink()
    report = roast(root, keys="meta", display=False)
    assert raw_tree(root) == (META - {"participants.tsv"}) | {
        p for p in PHENO if p.endswith(".json")}
    assert len(report.errors) == 2           # participants, phenotypes


def test_trim_phenotype_sessions_table(tmp_path):
    """A harvested `phenotype/*_sessions.tsv` is trimmed by
    `participant_id` (it was trimmed by `session_id` against
    `phenotype/ses-*`, which dropped every row)"""
    root = tmp_path / "DS"
    (root / "sub-01" / "ses-1").mkdir(parents=True)
    orig = tmp_path / "cog_sessions.tsv"
    orig.write_text("participant_id\tsession_id\tx\nsub-01\tses-1\t1\n"
                    "sub-01\tses-2\t2\nsub-02\tses-1\t3\n")
    assert trim_tables(root, {"phenotype/cog_sessions.tsv": orig}) == \
        ["phenotype/cog_sessions.tsv"]
    assert [r.split("\t")[:2] for r in (
        root / "phenotype" / "cog_sessions.tsv").read_text().splitlines()
            ] == [["participant_id", "session_id"], ["sub-01", "ses-1"],
                  ["sub-01", "ses-2"]]


def test_trim_keeps_quoted_cells(tmp_path):
    """Regression (round 2 of #37): tables written with quotes (INDI
    `format_tsv`, BIDS quotes a value with a tab) survive the trim byte
    for byte"""
    root = tmp_path / "DS"
    for sub in ("sub-01", "sub-02", "sub-03"):
        (root / sub).mkdir(parents=True)
    text = format_tsv([
        {"participant_id": "sub-01", "notes": "line1\nline2", "age": "10"},
        {"participant_id": "sub-02", "notes": "a\tb", "age": "11"},
        {"participant_id": "sub-03", "notes": 'said "x"', "age": "12"},
        {"participant_id": "sub-04", "notes": "gone", "age": "13"},
    ], ["participant_id"])
    orig = tmp_path / "orig.tsv"
    orig.write_bytes(text.encode("utf-8"))
    trim_tables(root, {"participants.tsv": orig})
    assert (root / "participants.tsv").read_bytes().decode("utf-8") == \
        text.replace("sub-04\tgone\t13\n", "")


def test_trim_phenotype_tables(tmp_path):
    """`trim_tables` trims phenotype tables by `participant_id`"""
    assert is_phenotype("rawbids/phenotype/srs.tsv")
    assert is_phenotype("phenotype/srs.tsv")
    assert not is_phenotype("participants.tsv")
    assert not is_phenotype("sub-1/phenotype.tsv")
    root = tmp_path / "DS"
    (root / "sub-1").mkdir(parents=True)
    orig = tmp_path / "srs.tsv"
    orig.write_text("participant_id\tx\nsub-1\t1\nsub-2\t2\n")
    assert trim_tables(root, {"phenotype/srs.tsv": orig}) == \
        ["phenotype/srs.tsv"]
    assert ids(root / "phenotype" / "srs.tsv") == ["sub-1"]


def test_compost_retrims_phenotypes(root):
    """Compost trims the phenotype tables that have a pristine copy"""
    roast(root, display=False)
    assert ids(raw_file(root, "phenotype/srs.tsv")) == [
        "sub-29100", "sub-29001", "sub-29002"]
    layout = layout_of(root)
    for path in sorted((layout.raw / "sub-29002").rglob("*"),
                       reverse=True):
        path.unlink() if path.is_file() else path.rmdir()
    (layout.raw / "sub-29002").rmdir()
    # a phenotype table without a pristine copy is left as it is
    (layout.raw / "phenotype" / "extra.tsv").write_text(
        "participant_id\nsub-29002\n")
    retrim_tables(layout)
    assert ids(raw_file(root, "phenotype/srs.tsv")) == [
        "sub-29100", "sub-29001"]
    assert ids(raw_file(root, "participants.tsv")) == [
        "sub-29100", "sub-29001"]
    assert ids(raw_file(root, "phenotype/extra.tsv")) == ["sub-29002"]


# ----------------------------------------------------------------------
#   Sites
# ----------------------------------------------------------------------

@pytest.mark.parametrize("name,expected", [
    ("ABIDEII-KKI_1_29273_29322", ("KKI", "1")),
    ("ABIDEII-ONRC_2_part1", ("ONRC", "2")),
    ("ABIDEII-U_MIA_1", ("U_MIA", "1")),
    ("ABIDEII-UCLA_Long", ("UCLA", "Long")),
    ("ABIDEII-ETHZ_1", ("ETH", "1")),
    ("ABIDE-II_NYU_2", ("NYU", "2")),
    ("ABIDE-I_PITT", ("UPSM", "1")),
    ("ABIDE-I_UCLA_2", ("UCLA", "2")),
    ("abideii-su_2", ("SU", "2")),
    ("IU", ("IU", None)),
])
def test_parse_site(name, expected):
    assert sites.parse_site(name) == expected


def test_parse_site_unknown():
    with pytest.raises(TemplateError):
        sites.parse_site("ABIDEII-XYZ_1")


def test_site_template():
    assert sites.site_template("NYU", "2", "dwi.json") == \
        TEMPLATES / "NYU/2/dwi.json"
    with pytest.raises(TemplateError, match="KUL sample 3"):
        sites.site_template("KUL", "3", "dwi.json")


# ----------------------------------------------------------------------
#   Command line
# ----------------------------------------------------------------------

@pytest.mark.parametrize("layout", LAYOUTS)
def test_cli_layout(tmp_path, layout):
    root = make_source(tmp_path / "ABIDE-2")
    bx("abide2", "roast", "--out", root, "--layout", layout)
    assert Layout.detect(root).kind == layout
    assert raw_tree(root) == ALL


def test_cli_keys_subjects_sidecars(tmp_path):
    root = make_source(tmp_path / "ABIDE-2")
    bx("abide2", "roast", "--out", root, "--keys", "T1w", "--subs", "29001",
       "0029100", "--sidecars", "no")
    assert raw_tree(root) == {
        "sub-29001/ses-01/anat/sub-29001_ses-01_T1w.nii.gz",
        "sub-29100/ses-01/anat/sub-29100_ses-01_T1w.nii.gz"}
    bx("abide2", "roast", "--out", root, "--exclude-keys", "rest", "meta",
       "dti", "--exclude-subs", "sub-29001", "--sidecars", "only")
    assert "sub-29002/ses-01/anat/sub-29002_ses-01_T1w.json" \
        in raw_tree(root)
    assert "sub-29002/ses-01/anat/sub-29002_ses-01_T1w.nii.gz" \
        not in raw_tree(root)
    assert "participants.tsv" not in raw_tree(root)


def test_cli_subject_ranges(tmp_path):
    root = make_source(tmp_path / "ABIDE-2")
    subs = tmp_path / "subs.txt"
    subs.write_text("29002\n")
    bx("abide2", "roast", "--out", root, "--keys", "T1w", "--subs",
       "29000:29002", str(subs), "--sidecars", "no")
    assert raw_tree(root) == {
        f"sub-{i}/ses-01/anat/sub-{i}_ses-01_T1w.nii.gz"
        for i in (29001, 29002)}


def test_dataset_declares_the_roaster():
    abide = ABIDE2()
    sources = abide.sources()
    assert sources["nitrc"].roaster is roaster.roast_nitrc
    assert sources["s3"].roaster is roaster.roast_nitrc
    # one roaster: no `roast --source`
    assert "source" not in abide.groups("roast")
    assert abide.labels(["29001", "sub-0029002", "29003:29005"]) == {
        "29001", "29002", "29003", "29004"}
