"""Characterization tests for the OASIS-I roast step."""
import gzip
import json
import os
import shutil

import nibabel as nib
import numpy as np
import pytest
from cli_utils import bx
from roast_utils import (
    LAYOUTS,
    SCOPE_SCENARIOS,
    analyze_pair,
    check_keys_rerun,
    check_scope,
    init_layout,
    layout_of,
    make_tar,
    open_spy,
    raw_file,
    raw_tree,
    read_tsv,
    readme_counts,
    scoped_records,
)

from brainspresso.actions.state import read_task_states
from brainspresso.datasets.oasis.i import roaster
from brainspresso.datasets.oasis.i.dataset import OASIS1, DiscOptions
from brainspresso.datasets.oasis.i.roaster import roast
from brainspresso.datasets.options import KeyOptions
from brainspresso.errors import UsageError

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
    """Files of a derivative dataset (without its sourcedata and its
    description: `test_derivative_descriptions`)"""
    folder = drv(root, name)
    if not folder.is_dir():
        return set()
    return {p.relative_to(folder).as_posix() for p in folder.rglob("*")
            if p.is_file() and p.relative_to(folder).parts[0] != "sourcedata"
            and p.relative_to(folder).as_posix() != "dataset_description.json"}


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
        f"{s}/mri/orig/001.mgz": mgz_bytes(),       # (its raw run 1)
        f"{s}/stats/aseg.stats": b"# not bidsifiable",
        f"disc{disc}/OAS1_0002_MR2/mri/aseg.mgz": mgz_bytes(),
    }, dirs=[f"disc{disc}", s])


def make_source(root):
    """Write the harvested files of OASIS-I (one image disc)."""
    src = root / "sourcedata"
    src.mkdir(parents=True, exist_ok=True)
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


@pytest.fixture(params=LAYOUTS)
def root(tmp_path, request):
    root = tmp_path / "OASIS-1"
    init_layout(root, request.param)
    return make_source(root)


def raw_runs(id):
    return {
        f"sub-{id:04d}/anat/sub-{id:04d}_run-{r}_T1w.{e}"
        for r in (1, 2) for e in ("nii.gz", "json")
    }


def tal_files(id=1):
    return {f"sub-{id:04d}/anat/sub-{id:04d}_space-Talairach_res-1mm"
            f"{desc}_T1w.{ext}" for ext in ("nii.gz", "json")
            for desc in ("", "_desc-skullstripped")}


def test_age_units():
    """`age` in years: BIDS unit "year" (not "years")"""
    for name in ('participants.json',):
        meta = json.loads((roaster.TEMPLATES / name).read_text())
        assert meta["age"]["Units"] == "year"


def test_roast_raw(root):
    roast(root, keys={"raw"}, discs=[1])
    assert raw_tree(root) == raw_runs(1) | raw_runs(2)
    img = nib.load(raw_file(root, "sub-0001/anat/sub-0001_run-1_T1w.nii.gz"))
    assert img.shape == (2, 2, 2)
    assert np.array_equal(np.asarray(img.dataobj).ravel(), np.arange(8))
    tpl = json.loads((roaster.TEMPLATES / "T1w.json").read_text())
    assert json.loads(raw_file(
        root, "sub-0001/anat/sub-0001_run-1_T1w.json").read_text()) == tpl


@pytest.mark.parametrize("subs", [["sub-0002"], ["2"], [2], ["2:3"]])
def test_subject_values(root, subs, tmp_path):
    """`roast(sub=)` takes the values of `--sub`"""
    roast(root, keys={"raw"}, discs=[1], sub=subs)
    assert raw_tree(root) == raw_runs(2)


def test_subject_file(root, tmp_path):
    (tmp_path / "subs.txt").write_text("sub-0001\n")
    roast(root, keys={"raw"}, discs=[1],
          exclude_sub=[str(tmp_path / "subs.txt")])
    assert raw_tree(root) == raw_runs(2)


def test_roast_raw_filters(root):
    roast(root, keys={"raw"}, discs=[1], exclude_sub=[1], json="only")
    assert raw_tree(root) == {p for p in raw_runs(2) if p.endswith("json")}


def test_missing_disc(root, caplog):
    """A missing disc named with `--discs` is an error, other missing
    discs are skipped with a warning"""
    report = roast(root, keys={"raw"}, discs=[2], display=False)
    assert raw_tree(root) == set()
    assert report.counts == {"discs": {"error": 1},
                             **readme_counts(root, "done")}
    report = roast(root, keys={"raw"}, display=False)
    assert not report.errors
    assert raw_tree(root) == raw_runs(1) | raw_runs(2)
    assert "oasis_cross-sectional_disc12.tar.gz not found (skipped)" \
        in caplog.text


def test_unreadable_disc(root):
    """A truncated disc is an error; the other discs are roasted"""
    src = root / "sourcedata"
    image_disc(src, disc=2, ids=(3,))
    data = (src / "oasis_cross-sectional_disc1.tar.gz").read_bytes()
    (src / "oasis_cross-sectional_disc1.tar.gz").write_bytes(
        data[:len(data) // 2])
    report = roast(root, keys={"raw"}, display=False)
    assert report.counts["discs"]["error"] == 1
    assert raw_tree(root) >= raw_runs(3)


def test_duplicate_subject_warns(root, caplog):
    image_disc(root / "sourcedata", disc=2, ids=(1,))
    roast(root, keys={"raw"}, display=False)
    assert "sub-0001 is on several raw discs" in caplog.text
    assert "sub-0002 is on several" not in caplog.text


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
    roast(root, keys={"raw"}, sub=[2])
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


def test_participants_by_name(root, caplog):
    """Columns are found by name (any order, case and spaces ignored); an
    ID that cannot be parsed is skipped with a warning"""
    wb = openpyxl.Workbook()
    wb.active.append(["m/f", "id", "HAND", "Age", "Educ", "SES", "MMSE",
                      "CDR", "e TIV", "nWBV", "ASF", "Delay"])
    wb.active.append(["F", "OAS1_0001_MR1", "R", 74.0, 2, 3, 29, 0.0, 1344,
                      0.743, 1.306, "N/A"])
    wb.active.append(["M", "OAS1_x", "R", 55, 2, 3, 29, 0, 1344, 0.7, 1.3,
                      "N/A"])
    wb.save(root / "sourcedata" / "oasis_cross-sectional.xlsx")
    report = roast(root, keys={"meta"}, display=False)
    assert not report.errors
    rows = read_tsv(layout_of(root).original(raw_file(root,
                                                      "participants.tsv")))
    assert rows[1:] == [["sub-0001", "F", "R", "74", "2", "3", "29", "0",
                         "1344", "0.743", "1.306"]]
    assert "oasis_cross-sectional.xlsx, row 3: subject ID 'OAS1_x' " \
        "(skipped)" in caplog.text


def test_unreadable_spreadsheet(root):
    """An error of `participants.tsv` only"""
    (root / "sourcedata" / "oasis_cross-sectional.xlsx").write_bytes(b"x")
    report = roast(root, keys={"meta"}, display=False)
    assert [name for _, name, _ in report.errors] == ["participants.tsv"]
    assert "oasis_cross-sectional.xlsx: cannot be read" in \
        report.errors[0][2]
    assert raw_tree(root) == META - {"participants.tsv"}


def test_missing_spreadsheet(root):
    (root / "sourcedata" / "oasis_cross-sectional.xlsx").unlink()
    report = roast(root, keys={"meta"}, display=False)
    assert report.counts["meta"] == {"done": 3, "error": 1}
    assert raw_tree(root) == META - {"participants.tsv"}


def test_avg(root):
    roast(root, keys={"avg"}, discs=[1], sub=[1])
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
    roast(root, keys={"avg"}, discs=[1], sub=[1], json="only")
    meta = json.loads((drv(root) / "sub-0001/anat/"
                       "sub-0001_res-1mm_T1w.json").read_text())
    assert meta["Sources"] == [
        f"bids:raw:sub-0001/anat/sub-0001_run-{run}_T1w.nii.gz"
        for run in (1, 2)]


def test_tal(root):
    roast(root, keys={"tal"}, discs=[1], sub=[1])
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
    roast(root, keys={"fsl"}, discs=[1], sub=[1])
    f = (drv(root) / "sub-0001/anat"
         / "sub-0001_space-Talairach_res-1mm_dseg.nii.gz")
    dat = np.asarray(nib.load(f).dataobj).ravel()
    # `relabel(vol, {1: 2, 2: 3, 3: 1})` maps {new: old}: FAST's 1, 2, 3
    # (CSF, GM, WM) become 3, 1, 2, i.e. GM = 1, WM = 2, CSF = 3 (SPM
    # order, confirmed on review of #32; the xfail of step 1 expected
    # {old: new}), as the dataset-level dseg.tsv says
    assert list(dat[:4]) == [0, 3, 1, 2]
    rows = read_tsv(drv(root) / roaster.DSEG_NAME)
    assert rows == [["index", "name"], ["1", "GM"], ["2", "WM"],
                    ["3", "CSF"]]


def test_missing_masked_talairach(root):
    """A missing member fails its own conversion only"""
    src = root / "sourcedata"
    m = {}
    s = "disc1/OAS1_0001_MR1"
    for run in (1, 2):
        _analyze(m, f"{s}/RAW/OAS1_0001_MR1_mpr-{run}_anon")
    _analyze(m, f"{s}/PROCESSED/MPRAGE/T88_111/"
                "OAS1_0001_MR1_mpr_n4_anon_111_t88_gfc")
    make_tar(src / "oasis_cross-sectional_disc1.tar.gz", m)
    report = roast(root, keys={"raw", "tal"}, display=False)
    assert report.counts["tal"] == {"done": 3, "error": 1}
    assert raw_tree(root) == raw_runs(1)
    assert drv_tree(root) == tal_files() - {
        f"{TAL}_desc-skullstripped_T1w.nii.gz"}


def test_missing_member_sidecars_only(root):
    """`--sidecars only`: a missing image is not an error (no data)"""
    image_disc(root / "sourcedata", ids=(1,), avg=False)
    report = roast(root, keys={"avg", "tal"}, json="only", display=False)
    assert not report.errors and report.status == "complete"
    assert drv_tree(root) == {p for p in tal_files() if p.endswith("json")}


def test_unreadable_disc_is_closed(root, monkeypatch):
    closed = []
    real = roaster.TarArchive

    class Spy(real):
        def getmembers(self):
            raise EOFError("truncated")

        def __exit__(self, *args):
            closed.append(self.path.name)
            return super().__exit__(*args)

    monkeypatch.setattr(roaster, "TarArchive", Spy)
    report = roast(root, keys={"raw"}, discs=[1], display=False)
    assert report.counts == {"discs": {"error": 1},
                             **readme_counts(root, "done")}
    assert closed == ["oasis_cross-sectional_disc1.tar.gz"]


def test_members_read_in_archive_order(root, monkeypatch):
    """One forward pass: members are extracted in archive order"""
    read = []
    real = roaster.TarArchive

    class Spy(real):
        def extractfile(self, member):
            read.append(self.getmember(member).offset)
            return super().extractfile(member)

    monkeypatch.setattr(roaster, "TarArchive", Spy)
    report = roast(root, keys={"raw", "avg", "tal", "fsl"}, discs=[1],
                   display=False)
    assert not report.errors
    assert len(read) == 2 * 2 * 6 and read == sorted(read)


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
LOOKUP = {"atlas-Aseg_dseg.tsv", "atlas-AsegDesikanKilliany_dseg.tsv",
          "atlas-DesikanKilliany_dseg.tsv", "atlas-Destrieux_dseg.tsv"}


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


@pytest.mark.parametrize("keys", [{"fs"}, {"avg"}, {"tal", "fsl"}])
def test_derivative_descriptions(root, keys):
    """Each derivative that a roast writes is a BIDS derivative dataset
    (`GeneratedBy`, `SourceDatasets`: its top-level dseg.tsv are
    inherited); only those of the keys"""
    freesurfer_disc(root / "sourcedata")
    report = roast(root, keys=keys, discs=[1], display=False)
    assert not report.errors
    for name, wanted, generator in ((FS, {"fs"}, "FreeSurfer"),
                                    (DRV, {"avg", "tal", "fsl"}, "OASIS")):
        drv = layout_of(root).derivative(name)
        if not keys & wanted:
            assert not drv.description.exists()
            continue
        desc = json.loads(drv.description.read_text())
        assert desc["DatasetType"] == "derivative"
        assert desc["BIDSVersion"]
        assert [g["Name"] for g in desc["GeneratedBy"]] == [
            generator, "brainspresso"]
        assert desc["SourceDatasets"] == [{"URL": drv.source_url}]
    if "fs" in keys:
        desc = json.loads(layout_of(root).derivative(FS).description
                          .read_text())
        assert desc["GeneratedBy"][0]["Version"] == "4.0"


def test_processed_derivative_only_with_images(root):
    """No avg/tal/fsl image converted (no subject of the disc kept, no
    disc): no description nor lookup table of `oasis-processed`"""
    proc = layout_of(root).derivative(DRV)
    roast(root, keys={"avg", "tal", "fsl"}, discs=[1], sub=[99],
          display=False)
    roast(root, keys={"avg", "tal", "fsl"}, discs=[2], display=False)
    assert not proc.description.exists()
    assert not (proc.root / roaster.DSEG_NAME).exists()
    roast(root, keys={"fsl"}, discs=[1], sub=[1], display=False)
    assert proc.description.exists()
    assert (proc.root / roaster.DSEG_NAME).exists()


def test_freesurfer_sidecars_only(root):
    """`--sidecars only --keys fs`: sidecars from the member names"""
    freesurfer_disc(root / "sourcedata")
    report = roast(root, keys={"fs"}, json="only", discs=[1],
                   display=False)
    assert not report.errors
    assert not layout_of(root).derivative(FS).source.exists()
    assert drv_tree(root, FS) == LOOKUP | {f"{SEG}.json"}


def test_freesurfer_sources_from_orig_volumes(root):
    """`Sources` of a tree: the runs of its `mri/orig/<NNN>.mgz`"""
    s = "disc1/OAS1_0001_MR1/mri"
    make_tar(root / "sourcedata" / "oasis_cs_freesurfer_disc1.tar.gz", {
        f"{s}/rawavg.mgz": mgz_bytes(),
        **{f"{s}/orig/{run:03d}.mgz": mgz_bytes() for run in (1, 2, 3)},
    })
    report = roast(root, keys={"fs"}, json="only", discs=[1],
                   display=False)
    assert not report.errors
    sidecar = drv(root, FS) / "sub-0001/anat/sub-0001_desc-orig_T1w.json"
    assert json.loads(sidecar.read_text())["Sources"] == [
        f"bids:raw:sub-0001/anat/sub-0001_run-{run}_T1w.nii.gz"
        for run in (1, 2, 3)]


def test_freesurfer_sources_from_image_disc(root):
    """A tree without `mri/orig/` volumes: the runs of the image disc"""
    s = "disc1/OAS1_0001_MR1/mri"
    make_tar(root / "sourcedata" / "oasis_cs_freesurfer_disc1.tar.gz", {
        f"{s}/rawavg.mgz": mgz_bytes()})
    roast(root, keys={"fs"}, discs=[1], display=False)
    sidecar = drv(root, FS) / "sub-0001/anat/sub-0001_desc-orig_T1w.json"
    assert json.loads(sidecar.read_text())["Sources"] == [
        f"bids:raw:sub-0001/anat/sub-0001_run-{run}_T1w.nii.gz"
        for run in (1, 2)]


def test_missing_freesurfer_disc(root):
    freesurfer_disc(root / "sourcedata")
    # disc 2 is missing; there is no FreeSurfer disc 12
    report = roast(root, keys={"fs"}, discs=[1, 2, 12], display=False)
    assert report.counts["discs"]["error"] == 1
    assert f"{SEG}.nii.gz" in drv_tree(root, FS)
    # not named: skipped (as by the legacy roaster)
    report = roast(root, keys={"fs"}, display=False)
    assert not report.errors


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
    cli(root, "--keys", "raw", "tal", "--sub", "1")
    assert raw_tree(root) == raw_runs(1)
    assert drv_tree(root) == tal_files()
    # --exclude-keys alone: all but these
    cli(root, "--exclude-keys", "fs", "fs-all", "tal", "fsl", "avg",
        "--exclude-sub", "sub-0001")
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


def test_discs_named_all(root):
    """`--discs` naming every disc: the missing ones are errors"""
    ds = OASIS1()
    groups = {"keys": KeyOptions(keys=("raw",)),
              "discs": DiscOptions(discs=tuple(range(1, 13)))}
    report = ds.execute("roast", out=root, **groups)
    assert report.counts["discs"] == {"error": 11}
    report = ds.execute("roast", out=root, keys=KeyOptions(keys=("raw",)))
    assert not report.errors


@pytest.mark.parametrize("discs", [[0], [13], [1, 14]])
def test_discs_out_of_range(root, discs):
    with pytest.raises(UsageError, match="discs are 1 to 12"):
        roast(root, keys={"raw"}, discs=discs)
    with pytest.raises(UsageError, match="discs are 1 to 12"):
        OASIS1().execute("roast", out=root,
                         discs=DiscOptions(discs=tuple(discs)))


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


@pytest.mark.parametrize("scenario", SCOPE_SCENARIOS)
def test_scope_of_image_discs(root, monkeypatch, scenario):
    """`--sub` narrowing and `meta` skip the discs, a new subject
    re-reads the disc that holds it only (`roast.scope`)"""
    image_disc(root / "sourcedata", disc=2, ids=(3,))
    opened = open_spy(monkeypatch, roaster)
    check_scope(
        scenario, root, lambda **kw: roast(root, display=False, **kw),
        opened, a=1, b=3, label_a="0001", label_b="0003",
        archives={
            "oasis_cross-sectional_disc1.tar.gz":
                ({"0001", "0002"}, "roast/discs/raw-disc01"),
            "oasis_cross-sectional_disc2.tar.gz":
                ({"0003"}, "roast/discs/raw-disc02")},
        keys={"meta", "raw", "avg", "tal", "fsl"}, key="tal", absent=9)


def test_keys_of_discs(root):
    """Adding a key that a disc shard reads re-runs it (`avg`, `tal`,
    `fsl` share a folder: the audit cannot tell them apart)"""
    freesurfer_disc(root / "sourcedata")
    check_keys_rerun(
        root, lambda **kw: roast(root, display=False, **kw),
        {"meta", "raw", "avg", "tal", "fsl", "fs", "fs-all"},
        {"roast/discs/raw-": roaster.DISC_KEYS["raw"],
         "roast/discs/fs-": roaster.DISC_KEYS["fs"]})


def test_scope_of_freesurfer_discs(root, monkeypatch):
    """Every subject of a FreeSurfer disc is seen, also one without
    files that BIDS describes (it has some with `fs-all`)"""
    s = "disc1/OAS1_0001_MR1"
    make_tar(root / "sourcedata" / "oasis_cs_freesurfer_disc1.tar.gz", {
        f"{s}/mri/aseg.mgz": mgz_bytes(),
        "disc1/OAS1_0003_MR1/stats/aseg.stats": b"# not bidsifiable",
        "disc1/OAS1_0004_MR2/mri/aseg.mgz": mgz_bytes(),
    })
    opened = open_spy(monkeypatch, roaster)
    roast(root, keys={"fs"}, sub=[1], discs=[1], display=False)
    record = scoped_records(root)["roast/discs/fs-disc01"]
    assert record["seen"] == {"sub": ["0001", "0003"]}
    del opened[:]
    roast(root, keys={"fs", "meta"}, sub=[1, 4], discs=[1], display=False)
    assert opened == []
    roast(root, keys={"fs-all"}, sub=[1, 3], discs=[1], display=False)
    assert opened
    fsdrv = layout_of(root).derivative(FS)
    assert (fsdrv.source / "sub-0003" / "stats" / "aseg.stats").exists()


def test_rerun_skips_discs(root, monkeypatch):
    """Under `skip`, unchanged discs are not read again; another
    `--if-exists` policy, a changed disc or template runs them again"""
    report = roast(root, keys={"raw"}, discs=[1], display=False)
    assert report.counts["raw"] == {"done": 8}
    states = read_task_states(layout_of(root).state)
    assert states["roast/discs/raw-disc01"]["status"] == "done"

    calls = []
    real = roaster.roast_image_disc

    def spy(ctx, path):
        calls.append(path.name)
        return real(ctx, path)

    monkeypatch.setattr(roaster, "roast_image_disc", spy)
    report = roast(root, keys={"raw"}, discs=[1], display=False)
    # (a skipped shard counts what it did last time as skipped)
    assert calls == [] and report.counts == {
        "raw": {"skipped": 8}, **readme_counts(root, "skipped")}

    # a file edited by hand is replaced by "overwrite"
    sidecar = raw_file(root, "sub-0001/anat/sub-0001_run-1_T1w.json")
    sidecar.write_text("{}")
    roast(root, keys={"raw"}, discs=[1], ifexists="overwrite",
          display=False)
    assert calls == ["oasis_cross-sectional_disc1.tar.gz"]
    assert json.loads(sidecar.read_text()) != {}

    calls.clear()
    # (a recent mtime: Windows cannot convert times near the epoch)
    disc = root / "sourcedata" / "oasis_cross-sectional_disc1.tar.gz"
    mtime = disc.stat().st_mtime - 3600
    os.utime(disc, (mtime, mtime))
    report = roast(root, keys={"raw"}, discs=[1], display=False)
    assert len(calls) == 1 and report.counts["raw"] == {"skipped": 8}

    # a template (`ctx.resource`: `sourcedata/` first) is an input
    calls.clear()
    (root / "sourcedata" / "T1w.json").write_text('{"EchoTime": 1}')
    roast(root, keys={"raw"}, discs=[1], display=False)
    assert len(calls) == 1
    roast(root, keys={"raw"}, discs=[1], ifexists="overwrite",
          display=False)
    assert json.loads(sidecar.read_text()) == {"EchoTime": 1}


def test_spreadsheet_edit_updates_participants(root):
    roast(root, keys={"meta", "raw"}, discs=[1], display=False)
    xlsx = root / "sourcedata" / "oasis_cross-sectional.xlsx"
    book = openpyxl.load_workbook(xlsx)
    book.active["B2"] = "M"
    book.active.append([None] * 12)             # a blank trailing row
    book.active["A6"] = "  "
    book.save(xlsx)
    report = roast(root, keys={"meta", "raw"}, discs=[1], display=False)
    assert not report.errors
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert rows[1][:2] == ["sub-0001", "M"] and len(rows) == 3


def test_roast_shards_in_processes(root):
    freesurfer_disc(root / "sourcedata")
    report = roast(root, discs=[1], jobs=2, display=False)
    assert not report.errors
    assert raw_tree(root) == META | raw_runs(1) | raw_runs(2)
    assert f"{SEG}.nii.gz" in drv_tree(root, FS)
    assert len(drv_tree(root)) == 2 * 8 + 1        # + dseg.tsv


@pytest.mark.parametrize("template", [None, "{"])
def test_missing_or_unreadable_sidecar_template(root, tmp_path, monkeypatch,
                                                template):
    """No (or an unreadable) `T1w.json`: each sidecar that it gives is a
    `Fail` of its own; the images are written (was a `TypeError` of the
    disc)"""
    templates = tmp_path / "templates"
    shutil.copytree(roaster.TEMPLATES, templates)
    (templates / "T1w.json").unlink()
    if template is not None:
        (root / "sourcedata" / "T1w.json").write_text(template)
    monkeypatch.setattr(roaster, "TEMPLATES", templates)
    report = roast(root, keys={"raw", "avg"}, discs=[1], display=False)
    names = sorted(name for _, name, _ in report.errors)
    assert names == sorted(
        [f"sub-{i:04d}_run-{r}_T1w.json" for i in (1, 2) for r in (1, 2)] +
        [f"sub-{i:04d}_res-1mm_T1w.json" for i in (1, 2)])
    message = report.errors[0][2]
    assert message.startswith("T1w.json not found" if template is None
                              else "template ")
    assert raw_tree(root) == {p for p in raw_runs(1) | raw_runs(2)
                              if p.endswith(".nii.gz")}
