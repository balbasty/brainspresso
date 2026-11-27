"""Characterization tests for the OASIS-I roast step."""
import gzip
import json
import os

import nibabel as nib
import numpy as np
import pytest
from cli_utils import bx
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

from brainspresso.actions.state import read_task_states
from brainspresso.datasets.oasis.i import roaster
from brainspresso.datasets.oasis.i.roaster import roast

openpyxl = pytest.importorskip("openpyxl")

DRV = "oasis-processed"
FS = "oasis-freesurfer"
META = {"README", "dataset_description.json", "participants.json",
        "participants.tsv"}
TAL = "sub-0001/anat/sub-0001_space-Talairach_res-1mm"


def drv(root, name=DRV):
    """Root of a derivative dataset"""
    return layout_of(root).derivative(name).root


def drv_tree(root, name=DRV):
    """Files of a derivative dataset (without its sourcedata)"""
    folder = drv(root, name)
    if not folder.is_dir():
        return set()
    return {p.relative_to(folder).as_posix() for p in folder.rglob("*")
            if p.is_file() and p.relative_to(folder).parts[0] != "sourcedata"}


def _analyze(members, base, data=None):
    hdr, img = analyze_pair(data=data)
    members[base + ".hdr"] = hdr
    members[base + ".img"] = img


def image_disc(src, disc=1, ids=(1, 2), avg=True):
    """An image disc (`avg=False`: without the average scans)"""
    m = {}
    for id in ids:
        s = f"disc{disc}/OAS1_{id:04d}_MR1"
        for run in (1, 2):
            _analyze(m, f"{s}/RAW/OAS1_{id:04d}_MR1_mpr-{run}_anon")
        if avg:
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
    if 2 in ids:
        _analyze(m, f"disc{disc}/OAS1_0002_MR2/RAW/OAS1_0002_MR2_mpr-1_anon")
    return make_tar(src / f"oasis_cross-sectional_disc{disc}.tar.gz", m)


def mgz_bytes(value=1) -> bytes:
    img = nib.MGHImage(np.full((2, 2, 2), value, np.float32), np.eye(4))
    return gzip.compress(img.to_bytes())


def freesurfer_disc(src, disc=1):
    s = f"disc{disc}/OAS1_0001_MR1"
    return make_tar(src / f"oasis_cs_freesurfer_disc{disc}.tar.gz", {
        f"{s}/mri/aseg.mgz": mgz_bytes(),
        f"{s}/stats/aseg.stats": b"# not bidsifiable",
        f"disc{disc}/OAS1_0002_MR2/mri/aseg.mgz": mgz_bytes(),
    }, dirs=[f"disc{disc}", s])


@pytest.fixture(params=LAYOUTS)
def root(tmp_path, request):
    root = tmp_path / "OASIS-1"
    init_layout(root, request.param)
    src = root / "sourcedata"
    src.mkdir(parents=True)
    image_disc(src)

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


def tal_files(id=1):
    return {f"sub-{id:04d}/anat/sub-{id:04d}_space-Talairach_res-1mm"
            f"{desc}_T1w.{ext}" for ext in ("nii.gz", "json")
            for desc in ("", "_desc-skullstripped")}


def test_roast_raw(root):
    roast(root, keys={"raw"}, discs=[1])
    assert raw_tree(root) == raw_runs(1) | raw_runs(2)
    img = nib.load(raw_file(root, "sub-0001/anat/sub-0001_run-1_T1w.nii.gz"))
    assert img.shape == (2, 2, 2)
    assert np.array_equal(np.asarray(img.dataobj).ravel(), np.arange(8))
    tpl = json.loads((roaster.TEMPLATES / "T1w.json").read_text())
    assert json.loads(raw_file(
        root, "sub-0001/anat/sub-0001_run-1_T1w.json").read_text()) == tpl


def test_roast_raw_filters(root):
    roast(root, keys={"raw"}, discs=[1], exclude_subs=[1], json="only")
    assert raw_tree(root) == {p for p in raw_runs(2) if p.endswith("json")}


def test_missing_disc(root):
    report = roast(root, keys={"raw"}, discs=[2], display=False)
    assert raw_tree(root) == set()
    assert report.counts["raw"] == {"error": 1}


def test_meta(root):
    # (the participants table lists the subjects on disk)
    roast(root, keys={"meta", "raw"})
    assert raw_tree(root) == META | raw_runs(1) | raw_runs(2)
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert [r[0] for r in rows] == ["participant_id", "sub-0001", "sub-0002"]
    assert rows[1][:4] == ["sub-0001", "F", "R", "74"]
    assert rows[2][4] == "n/a"


def test_meta_trimmed(root):
    """The participants table lists the subjects on disk; its pristine
    copy (`.brainspresso/orig/`) all of them"""
    roast(root, keys={"meta"})
    assert raw_tree(root) == META
    table = raw_file(root, "participants.tsv")
    assert len(read_tsv(table)) == 1
    original = layout_of(root).original(table)
    assert [r[0] for r in read_tsv(original)] == [
        "participant_id", "sub-0001", "sub-0002"]
    roast(root, keys={"raw"}, subs=[2])
    assert [r[0] for r in read_tsv(table)] == ["participant_id", "sub-0002"]


def test_meta_empty_cell(root):
    roast(root, keys={"meta", "raw"})
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert rows[2][5] == "n/a"


def test_meta_header(root):
    roast(root, keys={"meta", "raw"})
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert rows[0] == ["participant_id", "sex", "handedness", "age", "educ",
                       "ses", "mmse", "cdr", "etiv", "nwbv", "asf"]
    assert len(rows[0]) == len(rows[1])


def test_missing_spreadsheet(root):
    (root / "sourcedata" / "oasis_cross-sectional.xlsx").unlink()
    report = roast(root, keys={"meta"}, display=False)
    assert report.counts["meta"] == {"done": 3, "error": 1}
    assert raw_tree(root) == META - {"participants.tsv"}


def test_avg(root):
    roast(root, keys={"avg"}, discs=[1], subs=[1])
    assert raw_tree(root) == set()
    folder = drv(root)
    assert drv_tree(root) == {"sub-0001/anat/sub-0001_res-1mm_T1w.nii.gz",
                              "sub-0001/anat/sub-0001_res-1mm_T1w.json"}
    meta = json.loads(
        (folder / "sub-0001/anat/sub-0001_res-1mm_T1w.json").read_text())
    assert meta["BiasCorrected"] == "N4"
    assert len(meta["Sources"]) == 2
    assert nib.load(
        folder / "sub-0001/anat/sub-0001_res-1mm_T1w.nii.gz"
    ).shape == (2, 2, 2)


def test_avg_sources(root):
    roast(root, keys={"avg"}, discs=[1], subs=[1], json="only")
    meta = json.loads((drv(root) / "sub-0001/anat/"
                       "sub-0001_res-1mm_T1w.json").read_text())
    assert meta["Sources"] == [
        f"bids:raw:sub-0001/anat/sub-0001_run-{run}_T1w.nii.gz"
        for run in (1, 2)]


def test_tal(root):
    roast(root, keys={"tal"}, discs=[1], subs=[1])
    anat = drv(root) / "sub-0001/anat"
    assert (anat / "sub-0001_space-Talairach_res-1mm_T1w.nii.gz").exists()
    assert (anat / "sub-0001_space-Talairach_res-1mm_desc-skullstripped"
                   "_T1w.nii.gz").exists()
    assert drv_tree(root) == tal_files()
    meta = json.loads((drv(root) / f"{TAL}_T1w.json").read_text())
    masked = json.loads((drv(root) /
                         f"{TAL}_desc-skullstripped_T1w.json").read_text())
    assert (meta["SkullStripped"], masked["SkullStripped"]) == (False, True)
    assert masked["Sources"][1] == \
        "bids:raw:sub-0001/anat/sub-0001_run-2_T1w.nii.gz"


def test_fsl_relabel(root):
    roast(root, keys={"fsl"}, discs=[1], subs=[1])
    f = (drv(root) / "sub-0001/anat"
         / "sub-0001_space-Talairach_res-1mm_dseg.nii.gz")
    dat = np.asarray(nib.load(f).dataobj).ravel()
    # `relabel(vol, {1: 2, 2: 3, 3: 1})` maps {new: old}: 1, 2, 3 (FSL:
    # CSF, GM, WM) become 3, 1, 2 (GM, WM, CSF). The xfail of step 1
    # expected [0, 2, 3, 1] ({old: new}); the lookup is kept as it was
    # (to be confirmed by the owner, see the PR)
    assert list(dat[:4]) == [0, 3, 1, 2]


def test_missing_processed_image(root):
    """A subject without its average scan: an error, other files kept"""
    image_disc(root / "sourcedata", ids=(1,), avg=False)
    report = roast(root, keys={"avg", "raw"}, display=False)
    assert report.counts["avg"] == {"error": 1}
    assert drv_tree(root) == set()
    assert raw_tree(root) == raw_runs(1)


# ----------------------------------------------------------------------
#   FreeSurfer
# ----------------------------------------------------------------------

SEG = "sub-0001/anat/sub-0001_atlas-Aseg_dseg"
LOOKUP = {"atlas-Aseg_dseg.tsv", "atlas-AsegDesikanKillian_dseg.tsv",
          "atlas-Desikan-Killian_dseg.tsv", "atlas-Destrieux_dseg.tsv"}


def test_freesurfer(root):
    freesurfer_disc(root / "sourcedata")
    report = roast(root, keys={"fs"}, discs=[1], display=False)
    assert not report.errors
    fsdrv = layout_of(root).derivative(FS)
    unpacked = fsdrv.source / "sub-0001"
    assert (unpacked / "mri" / "aseg.mgz").exists()
    assert not (unpacked / "stats").exists()        # only with fs-all
    assert not (fsdrv.source / "sub-0002").exists()     # repeat session
    assert drv_tree(root, FS) == LOOKUP | {f"{SEG}.nii.gz", f"{SEG}.json"}
    assert raw_tree(root) == set()                  # no raw key
    assert drv_tree(root) == set()                  # no avg/tal/fsl key

    roast(root, keys={"fs-all"}, json="only", discs=[1])
    # unpacked files are inputs: --sidecars only does not drop them
    assert (unpacked / "stats" / "aseg.stats").exists()


def test_freesurfer_sidecars_only(root):
    """`--sidecars only --keys fs`: sidecars from the member names"""
    freesurfer_disc(root / "sourcedata")
    report = roast(root, keys={"fs"}, json="only", discs=[1],
                   display=False)
    assert not report.errors
    assert not layout_of(root).derivative(FS).source.exists()
    assert drv_tree(root, FS) == LOOKUP | {f"{SEG}.json"}


def test_missing_freesurfer_disc(root):
    freesurfer_disc(root / "sourcedata")
    # disc 2 is missing; there is no FreeSurfer disc 12
    report = roast(root, keys={"fs"}, discs=[1, 2, 12], display=False)
    assert report.counts["fs"]["error"] == 1
    assert f"{SEG}.nii.gz" in drv_tree(root, FS)


@pytest.mark.parametrize("keys", [{"meta"}, {"fs"}])
def test_category_keys_open_no_image_disc(root, monkeypatch, keys):
    opened = []
    real = roaster.TarArchive

    def opener(path, *a, **k):
        opened.append(path.name)
        return real(path, *a, **k)

    monkeypatch.setattr(roaster, "TarArchive", opener)
    freesurfer_disc(root / "sourcedata")
    report = roast(root, keys=keys, discs=[1], display=False)
    assert not report.errors
    assert "oasis_cross-sectional_disc1.tar.gz" not in opened


# ----------------------------------------------------------------------
#   Command line
# ----------------------------------------------------------------------

def cli(root, *args):
    bx("oasis1", "roast", "--out", root, *args)


def test_cli_keys_and_subjects(root):
    cli(root, "--keys", "raw", "tal", "--subs", "1")
    assert raw_tree(root) == raw_runs(1)
    assert drv_tree(root) == tal_files()
    # --exclude-keys alone: all but these
    cli(root, "--exclude-keys", "fs", "fs-all", "tal", "fsl", "avg",
        "--exclude-subs", "sub-0001")
    assert raw_tree(root) == META | raw_runs(1) | raw_runs(2)
    assert drv_tree(root) == tal_files()
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert [r[0] for r in rows] == ["participant_id", "sub-0001", "sub-0002"]


def test_cli_sidecars(root):
    cli(root, "--keys", "raw", "avg", "--sidecars", "no")
    assert raw_tree(root) == {p for p in raw_runs(1) | raw_runs(2)
                              if p.endswith(".nii.gz")}
    assert {p.split("_", 1)[1] for p in drv_tree(root)} == {
        "res-1mm_T1w.nii.gz"}
    cli(root, "--keys", "raw", "avg", "--sidecars", "only")
    assert raw_tree(root) == raw_runs(1) | raw_runs(2)
    assert len(drv_tree(root)) == 4


def test_cli_discs(root):
    cli(root, "--keys", "raw", "--discs", "2")
    assert raw_tree(root) == set()
    image_disc(root / "sourcedata", disc=2, ids=(3,))
    cli(root, "--keys", "raw", "--discs", "2")
    assert raw_tree(root) == raw_runs(3)


@pytest.mark.parametrize("layout", LAYOUTS)
def test_cli_layout(tmp_path, layout):
    root = tmp_path / "OASIS-1"
    image_disc(root / "sourcedata", ids=(1,))
    cli(root, "--keys", "raw", "--layout", layout)
    assert layout_of(root).kind == layout
    assert raw_tree(root) == raw_runs(1)
    assert (root / "rawbids").is_dir() == (layout == "study")


def test_rerun_skips_subjects(root, monkeypatch):
    """Under `skip`, unchanged subjects are not roasted again; another
    `--if-exists` policy or a changed disc runs them again"""
    report = roast(root, keys={"raw"}, discs=[1], display=False)
    assert report.counts["raw"] == {"done": 8}
    states = read_task_states(layout_of(root).state)
    assert {"roast/subjects/sub-0001", "roast/subjects/sub-0002"} <= \
        set(states)

    calls = []
    real = roaster.roast_images

    def spy(ctx, sub, tarball):
        calls.append(sub)
        return real(ctx, sub, tarball)

    monkeypatch.setattr(roaster, "roast_images", spy)
    report = roast(root, keys={"raw"}, discs=[1], display=False)
    # (a skipped shard counts what it did last time as skipped)
    assert calls == [] and report.counts == {"raw": {"skipped": 8}}

    # a file edited by hand is replaced by "overwrite"
    sidecar = raw_file(root, "sub-0001/anat/sub-0001_run-1_T1w.json")
    sidecar.write_text("{}")
    roast(root, keys={"raw"}, discs=[1], ifexists="overwrite",
          display=False)
    assert sorted(calls) == ["0001", "0002"]
    assert json.loads(sidecar.read_text()) != {}

    calls.clear()
    os.utime(root / "sourcedata" / "oasis_cross-sectional_disc1.tar.gz",
             (1, 1))
    report = roast(root, keys={"raw"}, discs=[1], display=False)
    assert sorted(calls) == ["0001", "0002"]
    assert report.counts["raw"] == {"skipped": 8}


def test_roast_shards_in_processes(root):
    freesurfer_disc(root / "sourcedata")
    report = roast(root, discs=[1], jobs=2, display=False)
    assert not report.errors
    assert raw_tree(root) == META | raw_runs(1) | raw_runs(2)
    assert f"{SEG}.nii.gz" in drv_tree(root, FS)
    assert len(drv_tree(root)) == 2 * 8
