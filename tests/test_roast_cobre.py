"""Characterization tests for the COBRE roast step (on the roast engine)."""
import json
import os

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

from brainspresso.actions.locks import FileLock, find_dataset
from brainspresso.bids.layout import Layout
from brainspresso.datasets.indi.cobre import roaster
from brainspresso.datasets.indi.cobre.dataset import COBRE
from brainspresso.datasets.indi.cobre.roaster import roast

CSV = (
    ",Current Age,Gender,Handedness,Subject Type,Diagnosis\n"
    "0040000,20,Male,Right,Patient,295.3 Schizophrenia\n"
    "0040001,30,Female,Left,Control,None\n"
    "0040002,Disenrolled,Disenrolled,Disenrolled,Disenrolled,Disenrolled\n"
)


def make_source(root):
    """Write the harvested files of COBRE in `<root>/sourcedata/`."""
    src = root / "sourcedata"
    src.mkdir(parents=True)
    (src / "COBRE_phenotypic_data.csv").write_text(CSV)
    make_tar(src / "COBRE_scan_data.tgz", {
        "COBRE/0040000/session_1/anat_1/mprage.nii.gz": nii_bytes(),
        "COBRE/0040000/session_1/rest_1/rest.nii.gz": nii_bytes((2, 2, 2, 3)),
        "COBRE/0040001/session_1/anat_1/mprage.nii.gz": nii_bytes(),
    }, dirs=["COBRE", "COBRE/0040000"])
    return root


@pytest.fixture(params=LAYOUTS)
def root(tmp_path, request):
    root = tmp_path / "COBRE"
    init_layout(root, request.param)
    return make_source(root)


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
HEADER = ["participant_id", "age", "sex", "handedness", "patient", "icd9",
          "icd9_subtype"]


def original(root, name="participants.tsv"):
    """Pristine copy of a table (all subjects, before trimming)."""
    return layout_of(root).original(raw_file(root, name))


def test_roast_all(root):
    report = roast(root, keys={"meta", "T1w", "func"}, display=False)
    assert not report.errors
    assert raw_tree(root) == RAW | META
    img = nib.load(raw_file(root, "sub-0040000/func/"
                                  "sub-0040000_task-rest_bold.nii.gz"))
    assert img.shape == (2, 2, 2, 3)
    assert np.allclose(img.affine, np.diag([2., 2., 2., 1.]))
    assert json.loads(
        raw_file(root, "sub-0040000/anat/sub-0040000_T1w.json").read_text()
    ) == json.loads((roaster.TEMPLATES / "T1w.json").read_text())


def test_participants(root):
    roast(root, display=False)
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert rows[0] == HEADER
    assert rows[1] == ["sub-0040000", "20", "M", "R", "Y", "295.3",
                       "Schizophrenia"]
    assert rows[2][:5] == ["sub-0040001", "30", "F", "L", "N"]
    # trimmed to the subjects on disk; the pristine copy has them all
    assert [r[0] for r in rows[1:]] == ["sub-0040000", "sub-0040001"]
    rows = read_tsv(original(root))
    assert [r[0] for r in rows[1:]] == ["sub-0040000", "sub-0040001",
                                        "sub-0040002"]
    assert raw_file(root, "participants.tsv").read_bytes() == \
        b"".join(line for line in original(root).read_bytes()
                 .splitlines(keepends=True) if b"0040002" not in line)


def test_participants_meta_only(root):
    """`--keys meta`: the table lists the subjects on disk (none)"""
    roast(root, keys={"meta"}, display=False)
    assert raw_tree(root) == META
    assert read_tsv(raw_file(root, "participants.tsv")) == [HEADER]
    assert len(read_tsv(original(root))) == 4


def test_participants_sub_prefix(root):
    roast(root, keys={"meta"}, display=False)
    rows = read_tsv(original(root))
    assert rows[1][0] == "sub-0040000"


def test_participants_disenrolled(root):
    roast(root, keys={"meta"}, display=False)
    rows = read_tsv(original(root))
    assert rows[3] == ["sub-0040002"] + ["n/a"] * 6


def test_participants_missing_values(root):
    """No diagnosis or subtype: n/a (BIDS), not an empty cell"""
    roast(root, keys={"meta"}, display=False)
    rows = read_tsv(original(root))
    assert rows[2] == ["sub-0040001", "30", "F", "L", "N", "n/a", "n/a"]
    assert "" not in {cell for row in rows for cell in row}


def test_participants_unpadded_ids(root):
    """CSV ids that are not zero-padded match the tarball labels"""
    csv = root / "sourcedata" / "COBRE_phenotypic_data.csv"
    csv.write_text(CSV.replace("\n0040", "\n40"))
    assert "\n40000," in csv.read_text()
    roast(root, display=False)
    assert [r[0] for r in read_tsv(raw_file(root, "participants.tsv"))] == [
        "participant_id", "sub-0040000", "sub-0040001"]


def test_participants_bad_rows(root, caplog):
    """Blank and short rows are skipped (short ones with a warning), empty
    cells are n/a"""
    csv = root / "sourcedata" / "COBRE_phenotypic_data.csv"
    csv.write_text(CSV + "\n0040003,40\n,,,,,\n,50,Male,Right,Patient,None\n"
                   "0040004,,,,,\n")
    report = roast(root, keys={"meta"}, display=False)
    assert not report.errors
    rows = read_tsv(original(root))
    assert [r[0] for r in rows[1:]] == ["sub-0040000", "sub-0040001",
                                        "sub-0040002", "sub-0040004"]
    assert rows[4] == ["sub-0040004"] + ["n/a"] * 6
    assert caplog.text.count("row skipped") == 2


def test_keys_and_subject_filters(root):
    roast(root, keys={"T1w"}, exclude_subs=[40001], display=False)
    assert raw_tree(root) == {
        "sub-0040000/anat/sub-0040000_T1w.nii.gz",
        "sub-0040000/anat/sub-0040000_T1w.json",
    }


def test_json_only_and_no(root):
    roast(root, keys={"T1w", "func"}, json="only", subs=[40000],
          display=False)
    assert raw_tree(root) == {p for p in RAW
                              if p.endswith(".json") and "0040000" in p}
    roast(root, keys={"T1w"}, json="no", subs=[40001], display=False)
    assert "sub-0040001/anat/sub-0040001_T1w.nii.gz" in raw_tree(root)
    assert "sub-0040001/anat/sub-0040001_T1w.json" not in raw_tree(root)


def test_participants_refreshed_under_different(root):
    """The table is regenerated and compared (not skipped)."""
    csv = root / "sourcedata" / "COBRE_phenotypic_data.csv"
    roast(root, keys={"meta"}, ifexists="different", display=False)
    tsv = original(root)
    first = tsv.stat()
    # unchanged input: the table is left untouched
    roast(root, keys={"meta"}, ifexists="different", display=False)
    assert (tsv.stat().st_ino, tsv.stat().st_mtime_ns) == \
        (first.st_ino, first.st_mtime_ns)
    # new input (e.g. after a harvest): the table is rewritten
    csv.write_text(CSV.replace("0040001,30", "0040001,31"))
    os.utime(csv, (first.st_atime + 10, first.st_mtime + 10))
    roast(root, keys={"meta"}, ifexists="different", display=False)
    rows = read_tsv(tsv)
    assert rows[2][:2] == ["sub-0040001", "31"]
    assert tsv.stat().st_mtime_ns != first.st_mtime_ns


def test_state_folder_exists_before_locks(tmp_path, monkeypatch):
    """The roast creates `.brainspresso/` before any lock."""
    root = tmp_path / "COBRE"
    (root / "sourcedata").mkdir(parents=True)
    (root / "sourcedata" / "COBRE_phenotypic_data.csv").write_text(CSV)
    seen = []
    acquire = FileLock.acquire

    def spy(self):
        if self.path.is_relative_to(root):      # (not package templates)
            seen.append(find_dataset(self.path))
        return acquire(self)

    monkeypatch.setattr(FileLock, "acquire", spy)
    roast(root, keys={"meta"}, display=False)
    assert len(seen) >= 4 and set(seen) == {root}


def test_missing_archive(tmp_path):
    """A missing tarball is an error (it was a warning only)."""
    root = tmp_path / "COBRE"
    (root / "sourcedata").mkdir(parents=True)
    report = roast(root, keys={"T1w", "func"}, display=False)
    assert raw_tree(root) == set()
    assert len(report.errors) == 1
    assert roaster.TARBALL in str(report.errors[0])


def test_missing_phenotypes(tmp_path):
    root = make_source(tmp_path / "COBRE")
    (root / "sourcedata" / "COBRE_phenotypic_data.csv").unlink()
    report = roast(root, keys={"meta"}, display=False)
    assert raw_tree(root) == META - {"participants.tsv"}
    assert len(report.errors) == 1


def test_dataset_files_compared(root):
    """`dataset_description.json` and `participants.json` are compared
    (tables), so a new template replaces them under `skip`"""
    roast(root, keys={"meta"}, display=False)
    files = [raw_file(root, n) for n in ("dataset_description.json",
                                         "participants.json")]
    first = [f.stat().st_mtime_ns for f in files]
    roast(root, keys={"meta"}, display=False)
    assert [f.stat().st_mtime_ns for f in files] == first
    (root / "sourcedata" / "dataset_description.json").write_text(
        json.dumps({"Name": "COBRE (local)", "BIDSVersion": "1.8.0"}))
    roast(root, keys={"meta"}, display=False)
    assert json.loads(files[0].read_text())["Name"] == "COBRE (local)"


def test_subject_labels_in_python_api(root):
    roast(root, keys={"T1w"}, subs=["sub-0040001"], display=False)
    assert raw_tree(root) == {p for p in RAW if "0040001" in p}
    roast(root, keys={"T1w"}, subs=["0040000:0040001"], json="no",
          display=False)
    assert "sub-0040000/anat/sub-0040000_T1w.nii.gz" in raw_tree(root)


def test_missing_sidecar_template(root, monkeypatch):
    """A missing sidecar resource is an error only if sidecars are wanted"""
    monkeypatch.setattr(roaster.RoastContext, "resource",
                        lambda self, name: None)
    report = roast(root, keys={"T1w"}, json="no", display=False)
    assert not report.errors
    assert raw_tree(root) == {p for p in RAW if p.endswith("T1w.nii.gz")}
    report = roast(root, keys={"T1w"}, ifexists="overwrite", display=False)
    assert len(report.errors) == 2


def test_tarball_read_once(root, monkeypatch):
    """Members are read lazily (no listing first: one decompression)"""
    def listing(self):
        raise AssertionError("the tarball is listed")

    monkeypatch.setattr(roaster.TarArchive, "getmembers", listing)
    monkeypatch.setattr(roaster.TarArchive, "getnames", listing)
    report = roast(root, display=False)
    assert not report.errors
    assert raw_tree(root) == RAW | META


def test_template_change_reruns_the_shard(root, monkeypatch):
    opened = []
    real = roaster.TarArchive

    def opener(path):
        opened.append(path)
        return real(path)

    monkeypatch.setattr(roaster, "TarArchive", opener)
    roast(root, keys={"T1w"}, display=False)
    roast(root, keys={"T1w"}, display=False)
    assert len(opened) == 1
    # (a new sidecar template: the shard signature changes, under `skip`)
    (root / "sourcedata" / "T1w.json").write_text('{"EchoTime": 1}')
    roast(root, keys={"T1w"}, display=False)
    assert len(opened) == 2
    roast(root, keys={"T1w"}, display=False)
    assert len(opened) == 2


def test_exclude_keys(root):
    roast(root, exclude_keys={"func", "meta"}, display=False)
    assert raw_tree(root) == {p for p in RAW if "/func/" not in p}


def test_subjects_pruned_before_extraction(root, monkeypatch):
    extracted = []
    real = roaster.ExtractMember

    def spy(archive, member, dst, **kwargs):
        extracted.append(member.name)
        return real(archive, member, dst, **kwargs)

    monkeypatch.setattr(roaster, "ExtractMember", spy)
    roast(root, keys={"T1w", "func"}, subs=[40001], display=False)
    assert extracted == ["COBRE/0040001/session_1/anat_1/mprage.nii.gz"]


def test_rerun_skips_by_signature(root, monkeypatch):
    """A re-run with the same inputs and options does not open the tarball;
    new options (subjects) do"""
    opened = []
    real = roaster.TarArchive

    def opener(path):
        opened.append(path)
        return real(path)

    monkeypatch.setattr(roaster, "TarArchive", opener)
    roast(root, display=False)
    assert len(opened) == 1
    report = roast(root, display=False)
    assert not report.errors
    assert len(opened) == 1
    assert raw_tree(root) == RAW | META
    roast(root, subs=[40000], display=False)
    assert len(opened) == 2


def test_jobs(root):
    report = roast(root, jobs=2, display=False)
    assert not report.errors
    assert raw_tree(root) == RAW | META


# ----------------------------------------------------------------------
#   Command line
# ----------------------------------------------------------------------

@pytest.mark.parametrize("layout", LAYOUTS)
def test_cli_layout(tmp_path, layout):
    root = make_source(tmp_path / "COBRE")
    bx("cobre", "roast", "--out", root, "--layout", layout)
    assert Layout.detect(root).kind == layout
    assert raw_tree(root) == RAW | META
    assert (root / "rawbids").is_dir() == (layout == "study")


def test_cli_keys_subjects_sidecars(tmp_path):
    root = make_source(tmp_path / "COBRE")
    bx("cobre", "roast", "--out", root, "--keys", "T1w", "func",
       "--subs", "40000", "--sidecars", "no")
    assert raw_tree(root) == {p for p in RAW
                              if p.endswith(".nii.gz") and "0040000" in p}
    bx("cobre", "roast", "--out", root, "--exclude-keys", "func", "meta",
       "--exclude-subs", "sub-0040000", "--sidecars", "only")
    assert "sub-0040001/anat/sub-0040001_T1w.json" in raw_tree(root)
    assert "sub-0040001/anat/sub-0040001_T1w.nii.gz" not in raw_tree(root)
    assert "participants.tsv" not in raw_tree(root)


def test_cli_if_exists(tmp_path):
    root = make_source(tmp_path / "COBRE")
    bx("cobre", "roast", "--out", root, "--keys", "T1w")
    sidecar = raw_file(root, "sub-0040000/anat/sub-0040000_T1w.json")
    sidecar.write_text("{}")
    bx("cobre", "roast", "--out", root, "--keys", "T1w")
    assert sidecar.read_text() == "{}"                  # skip (default)
    bx("cobre", "roast", "--out", root, "--keys", "T1w",
       "--if-exists", "overwrite")
    assert json.loads(sidecar.read_text()) == json.loads(
        (roaster.TEMPLATES / "T1w.json").read_text())


def test_dataset_declares_the_roaster():
    cobre = COBRE()
    assert cobre.sources()["nitrc"].roaster is roaster.roast_cobre
    assert cobre.labels(["40000", "sub-0040001"]) == {"0040000", "0040001"}
