"""Tests of the Bullitt roast step."""
import json
import logging
import re
import zlib
from pathlib import Path

import nibabel as nib
import numpy as np
import pytest
from cli_utils import bx
from roast_utils import (
    LAYOUTS,
    init_layout,
    layout_of,
    raw_file,
    raw_tree,
    read_tsv,
    scoped_records,
)

from brainspresso.actions.state import read_task_states
from brainspresso.datasets.bullitt import roaster
from brainspresso.datasets.bullitt.dataset import Bullitt
from brainspresso.datasets.bullitt.roaster import roast as roast_api
from brainspresso.roast import make_context

TEMPLATES = (Path(__file__).parents[1] / "brainspresso" / "datasets"
             / "bullitt" / "templates")
TRE = b"ObjectType = Scene\nNDims = 3\n"


def mha_bytes(value, shape=(4, 3, 2)) -> bytes:
    """A small compressed MetaImage (`.mha`) filled with `value`."""
    data = np.full(shape, value, np.int16)
    raw = zlib.compress(data.T.tobytes())
    head = (
        "ObjectType = Image\nNDims = 3\nBinaryData = True\n"
        "BinaryDataByteOrderMSB = False\nCompressedData = True\n"
        f"CompressedDataSize = {len(raw)}\n"
        "TransformMatrix = 1 0 0 0 1 0 0 0 1\nOffset = 0 0 0\n"
        "ElementSpacing = 1 1 1\nDimSize = 4 3 2\n"
        "ElementType = MET_SHORT\nElementDataFile = LOCAL\n"
    )
    return head.encode() + raw


# As harvested from data.kitware.com (`harvest.py`): subject 001 has
# everything, 002 the "T!-Flash" typo of the server and no MPRAGE nor
# skull-stripped image, 003 only an MPRAGE in a folder named "MPRage"
SOURCE = {
    "Normal-001/T1-Flash/Normal001-T1-Flash.mha": mha_bytes(1),
    "Normal-001/T1-MPRage/Normal001-MPRage.mha": mha_bytes(6),
    "Normal-001/T2/Normal001-T2.mha": mha_bytes(2),
    "Normal-001/MRA/Normal001-MRA.mha": mha_bytes(3),
    "Normal-001/DTI/Normal001-DTI.mha": mha_bytes(4),
    "Normal-001/AuxillaryData/SkullStripped-T1-Flash.mha": mha_bytes(5),
    "Normal-001/AuxillaryData/VascularNetwork.tre": TRE,
    "Normal-002/T!-Flash/Normal002-T1-Flash.mha": mha_bytes(1),
    "Normal-002/T2/Normal002-T2.mha": mha_bytes(2),
    "Normal-002/MRA/Normal002-MRA.mha": mha_bytes(3),
    "Normal-002/AuxillaryData/VascularNetwork.tre": TRE,
    "Normal-003/MPRage/Normal003-MPRage.mha": mha_bytes(6),
    "Administration/Adjunct information/NormalDatabase2.pdf": b"%PDF",
    "Phantoms/phantom.mha": mha_bytes(7),
}

META = {"README", "dataset_description.json", "participants.json",
        "participants.tsv"}
DERIV_META = {"README", "dataset_description.json", ".bidsignore"}


def anat(sub, suffix, ext=("nii.gz", "json")):
    """Files of a raw scan."""
    return {f"sub-{sub}/anat/sub-{sub}_{suffix}.{e}" for e in ext}


RAW = (anat("001", "acq-flash_T1w") | anat("001", "acq-mprage_T1w")
       | anat("001", "T2w") | anat("001", "angio")
       | anat("002", "acq-flash_T1w") | anat("002", "T2w")
       | anat("002", "angio")
       | anat("003", "acq-mprage_T1w"))
BRAIN = "sub-001/anat/sub-001_acq-flash_desc-brain_T1w"
DERIV = {f"{BRAIN}.nii.gz", f"{BRAIN}.json",
         "sub-001/anat/sub-001_desc-vessels_tubes.tre",
         "sub-002/anat/sub-002_desc-vessels_tubes.tre"}


@pytest.fixture(params=LAYOUTS)
def root(tmp_path, request):
    """A harvested Bullitt dataset (`<tmp>/Bullitt`)."""
    root = tmp_path / "Bullitt"
    init_layout(root, request.param)
    for name, data in SOURCE.items():
        path = root / "sourcedata" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    return root


def corrupt(path):
    """Replace the (compressed) data of a MetaImage file."""
    head = path.read_bytes().split(b"ElementDataFile = LOCAL\n")[0]
    path.write_bytes(head + b"ElementDataFile = LOCAL\nnot zlib data")


def roast(root, *flags):
    """`bx bullitt roast <tmp> <flags>`."""
    bx("bullitt", "roast", root.parent, *flags)


def deriv_tree(root) -> set[str]:
    """Files of `derivatives/bullitt/`."""
    drv = root / "derivatives" / "bullitt"
    return {p.relative_to(drv).as_posix() for p in drv.rglob("*")
            if p.is_file()}


def test_roast_all(root):
    roast(root)
    layout = layout_of(root)
    assert raw_tree(root) == META | RAW
    assert deriv_tree(root) == DERIV_META | DERIV
    # nothing else at the top of the dataset
    top = {p.name for p in root.iterdir()}
    expected = {".brainspresso", "dataset_description.json", "derivatives",
                "sourcedata"}
    if layout.kind == "study":
        expected |= {"rawbids"}
        study = json.loads((root / "dataset_description.json").read_text())
        assert study["DatasetType"] == "study"
    else:
        expected |= META | {"sub-001", "sub-002", "sub-003"}
    assert top == expected


def test_images(root):
    roast(root)
    for suffix, value in [("acq-flash_T1w", 1), ("acq-mprage_T1w", 6),
                          ("T2w", 2), ("angio", 3)]:
        img = nib.load(raw_file(root, f"sub-001/anat/sub-001_{suffix}"
                                      ".nii.gz"))
        assert img.shape == (4, 3, 2)
        assert np.all(np.asarray(img.dataobj) == value)
        assert nib.aff2axcodes(img.affine) == ("L", "P", "S")
    drv = root / "derivatives" / "bullitt"
    img = nib.load(drv / f"{BRAIN}.nii.gz")
    assert np.all(np.asarray(img.dataobj) == 5)
    assert (drv / "sub-002/anat/sub-002_desc-vessels_tubes.tre"
            ).read_bytes() == TRE


def test_sidecars_and_templates(root):
    roast(root)
    for suffix in ("acq-flash_T1w", "acq-mprage_T1w", "T2w", "angio"):
        assert json.loads(raw_file(
            root, f"sub-001/anat/sub-001_{suffix}.json").read_text()
        ) == json.loads((TEMPLATES / f"{suffix}.json").read_text())
    assert raw_file(root, "README").read_bytes() == \
        (TEMPLATES / "README").read_bytes()
    # trimmed to the subjects on disk (legacy: the 109 rows of the
    # template)
    template = (TEMPLATES / "participants.tsv").read_text().splitlines()
    assert raw_file(root, "participants.tsv").read_text().splitlines() \
        == template[:4]
    for name in ("participants.json",):
        assert json.loads(raw_file(root, name).read_text()) == \
            json.loads((TEMPLATES / name).read_text())
    description = json.loads(raw_file(
        root, "dataset_description.json").read_text())
    assert description["DatasetType"] == "raw"
    assert description["Name"] == json.loads(
        (TEMPLATES / "dataset_description.json").read_text())["Name"]

    drv = root / "derivatives" / "bullitt"
    assert json.loads((drv / f"{BRAIN}.json").read_text()) == {
        "SkullStripped": True}
    assert (drv / "README").read_bytes() == \
        (TEMPLATES / "derivatives_README").read_bytes()
    assert (drv / ".bidsignore").read_bytes() == \
        (TEMPLATES / "derivatives_bidsignore").read_bytes()
    description = json.loads((drv / "dataset_description.json").read_text())
    assert description["DatasetType"] == "derivative"
    url = {"study": "../../rawbids/", "flat": "../../"}[
        layout_of(root).kind]
    assert description["SourceDatasets"][0] == {"URL": url}
    assert "kitware" in description["SourceDatasets"][1]["URL"]


def test_keys(root):
    roast(root, "--keys", "T2w", "--sidecars", "no")
    assert raw_tree(root) == {"sub-001/anat/sub-001_T2w.nii.gz",
                              "sub-002/anat/sub-002_T2w.nii.gz"}
    assert not (root / "derivatives").exists()


def test_exclude_keys(root):
    roast(root, "--exclude-keys", "meta", "angio", "T2w", "flash",
          "skullstripped")
    assert raw_tree(root) == anat("001", "acq-mprage_T1w") | \
        anat("003", "acq-mprage_T1w")
    # (legacy: no derivative dataset files without the meta key)
    assert deriv_tree(root) == DERIV_META | {
        "sub-001/anat/sub-001_desc-vessels_tubes.tre",
        "sub-002/anat/sub-002_desc-vessels_tubes.tre"}


@pytest.mark.parametrize("subs", [["2"], ["002"], ["sub-002"]])
def test_subjects(root, subs):
    roast(root, "--keys", "flash", "angio", "--subs", *subs)
    assert raw_tree(root) == anat("002", "acq-flash_T1w") | \
        anat("002", "angio")


def test_exclude_subjects(root):
    roast(root, "--keys", "mprage", "--exclude-subs", "1")
    assert raw_tree(root) == anat("003", "acq-mprage_T1w")


def test_sidecars_only_raw(root):
    roast(root, "--keys", "meta", "flash", "mprage", "T2w", "angio",
          "--sidecars", "only")
    assert raw_tree(root) == META | {p for p in RAW if p.endswith(".json")}


def test_corrupt_image(root):
    """A corrupt image is an error of its own output; the rest is
    written."""
    corrupt(root / "sourcedata" / "Normal-001/T2/Normal001-T2.mha")
    roast(root)
    assert raw_tree(root) == META | (RAW - {"sub-001/anat/sub-001_T2w.nii.gz"})
    assert deriv_tree(root) == DERIV_META | DERIV


def test_rerun_skips(root):
    roast(root)
    nii = raw_file(root, "sub-001/anat/sub-001_T2w.nii.gz")
    mtime = nii.stat().st_mtime_ns
    roast(root)
    assert nii.stat().st_mtime_ns == mtime
    assert raw_tree(root) == META | RAW


def test_scope_of_subject_shards(root, monkeypatch):
    """Toggling `meta`, fewer subjects: no subject is roasted again;
    another sidecar policy, or other keys (its inputs follow the wanted
    keys) roast it again"""
    calls = []
    real = roaster.roast_subject

    def spy(ctx, sub):
        calls.append(sub)
        return real(ctx, sub)

    monkeypatch.setattr(roaster, "roast_subject", spy)
    assert not roast_api(root, display=False).errors
    assert calls
    scoped_records(root)
    del calls[:]
    for options in ({"exclude_keys": "meta"}, {"subs": [1]},
                    {"exclude_keys": "meta", "subs": [2]}):
        assert not roast_api(root, display=False, **options).errors
    assert calls == []
    roast_api(root, json="no", subs=[1], display=False)
    roast_api(root, exclude_keys="vessels", subs=[2], display=False)
    assert calls == ["001", "002"]


# ----------------------------------------------------------------------
#   Bugs of the legacy roaster
# ----------------------------------------------------------------------

def test_sidecars_only_derivatives(root):
    """Legacy: `--sidecars only` wrote the derivative images (and `.tre`)"""
    roast(root, "--keys", "skullstripped", "vessels", "--sidecars", "only")
    assert {p for p in deriv_tree(root) if p.startswith("sub-")} == {
        f"{BRAIN}.json"}


def test_sidecars_only_never_writes_images(root):
    roast(root, "--sidecars", "only")
    assert raw_tree(root) == META | {p for p in RAW if p.endswith(".json")}
    assert deriv_tree(root) == DERIV_META | {f"{BRAIN}.json"}


def test_sidecars_no_derivatives(root):
    """Legacy: `--sidecars no` wrote the sidecar of the skull-stripped
    image"""
    roast(root, "--keys", "skullstripped", "--sidecars", "no")
    assert {p for p in deriv_tree(root) if p.startswith("sub-")} == {
        f"{BRAIN}.nii.gz"}


def test_derivatives_without_meta(root):
    """Legacy: the derivative dataset files were only written with the
    meta key"""
    roast(root, "--keys", "skullstripped", "vessels")
    assert deriv_tree(root) == DERIV_META | DERIV
    assert raw_tree(root) == set()


# ----------------------------------------------------------------------
#   Engine port
# ----------------------------------------------------------------------

def test_api_keys_and_subjects(root):
    """`roast(keys=)` takes a bare string; subjects as `--subs`"""
    report = roast_api(root, keys="T2w", subs=[2], json="no", display=False)
    assert not report.errors
    assert raw_tree(root) == {"sub-002/anat/sub-002_T2w.nii.gz"}


@pytest.mark.parametrize("subs", [["sub-001"], ["001"], ["1"], [1],
                                  ["1:2"]])
def test_api_subject_values(root, subs):
    roast_api(root, keys=["angio"], subs=subs, display=False)
    assert raw_tree(root) == anat("001", "angio")


def test_api_subject_file(root, tmp_path):
    (tmp_path / "subs.txt").write_text("sub-001\nsub-002\n")
    roast_api(root, keys="mprage", exclude_subs=[str(tmp_path / "subs.txt")],
              display=False)
    assert raw_tree(root) == anat("003", "acq-mprage_T1w")


@pytest.mark.parametrize("keys, expected", [
    ("raw", RAW),
    ("T1w", {p for p in RAW if "_T1w" in p}),
    ("anat", RAW),
    ("derivatives", set()),
])
def test_key_groups(root, keys, expected):
    """Groups select their keys (and name them: missing scans are
    errors)"""
    report = roast_api(root, keys=keys, display=False)
    assert report.errors
    assert raw_tree(root) == expected
    if keys == "derivatives":
        assert deriv_tree(root) == DERIV_META | DERIV


def test_default_roast_has_no_errors(root):
    """Missing scans are not errors by default (the source distributes
    them incompletely)"""
    report = roast_api(root, display=False)
    assert not report.errors
    assert report.status == "complete"


def test_missing_scans_named(root):
    """A missing scan is an error when `--keys` names its key"""
    report = roast_api(root, keys=["mprage", "skullstripped"],
                       display=False)
    assert sorted(name for _, name, _ in report.errors) == [
        "sub-002_acq-flash_desc-brain_T1w.nii.gz",
        "sub-002_acq-mprage_T1w.nii.gz",
        "sub-003_acq-flash_desc-brain_T1w.nii.gz",
    ]
    assert raw_tree(root) == anat("001", "acq-mprage_T1w") | \
        anat("003", "acq-mprage_T1w")
    # (through a group, in any case)
    report = roast_api(root, keys="T1W", display=False)
    assert sorted(name for _, name, _ in report.errors) == [
        "sub-002_acq-mprage_T1w.nii.gz", "sub-003_acq-flash_T1w.nii.gz"]
    # (no image is written: no error)
    report = roast_api(root, keys="T1w", json="only", display=False)
    assert not report.errors


def test_missing_scans_named_cli(root):
    spec = Bullitt().keys_for("roast")
    assert roaster.roast_options(spec.default) == {"named": []}
    assert roaster.roast_options(()) == {"named": []}
    assert roaster.roast_options(["raw"]) == {
        "named": ["T2w", "angio", "flash", "mprage"]}
    # (keys that cover every choice still name them: review of #36)
    assert roaster.roast_options(["raw", "derivatives", "meta"]) == {
        "named": ["T2w", "angio", "flash", "mprage", "skullstripped",
                  "vessels"]}


MISSING_ALL = [     # missing scans and auxiliary files of the fixture
    "sub-002_acq-flash_desc-brain_T1w.nii.gz",
    "sub-002_acq-mprage_T1w.nii.gz",
    "sub-003_T2w.nii.gz",
    "sub-003_acq-flash_T1w.nii.gz",
    "sub-003_acq-flash_desc-brain_T1w.nii.gz",
    "sub-003_angio.nii.gz",
    "sub-003_desc-vessels_tubes.tre",
]


def errors(caplog):
    """Names of the errors logged by the engine since the last call (not
    read from the display: its columns wrap with the width of the
    terminal)."""
    names = {record.getMessage().split(": ")[0]
             for record in caplog.records
             if record.name == "brainspresso.actions.engine"
             and record.levelno >= logging.ERROR}
    caplog.clear()
    return sorted(n for n in names if re.fullmatch(r"sub-\d{3}_\S+", n))


@pytest.mark.parametrize("keys, missing", [
    (["raw", "derivatives", "meta"], MISSING_ALL),
    (["raw", "derivatives"], MISSING_ALL),
    (list(roaster.ROAST_KEYS), []),     # (the CLI default)
    ([], []),
])
def test_missing_scans_named_by_cli_keys(root, caplog, keys, missing):
    """`--keys` that cover every key name them (only the CLI default,
    or no `--keys`, names nothing)"""
    caplog.clear()
    roast(root, *(["--keys", *keys] if keys else []))
    assert errors(caplog) == missing


def test_named_keys_rerun_shards(root, caplog):
    """A run that names more keys does not skip the shards of an earlier
    run (`named` is part of the shard signature)"""
    roast(root, "--exclude-keys", "meta")
    assert errors(caplog) == []
    roast(root, "--keys", "raw", "derivatives")
    assert "sub-002_acq-mprage_T1w.nii.gz" in errors(caplog)


def test_shard_inputs(root):
    """Only the files that the actions read, for the wanted keys"""
    roast_api(root, keys="T2w", json="no", display=False)
    state = read_task_states(layout_of(root).state)[
        "roast/subjects/sub-001"]
    # touching a file that the run does not read: skipped
    dti = root / "sourcedata" / "Normal-001/DTI/Normal001-DTI.mha"
    dti.write_bytes(dti.read_bytes() + b"\n")
    roast_api(root, keys="T2w", json="no", display=False)
    assert read_task_states(layout_of(root).state)[
        "roast/subjects/sub-001"] == state
    ctx = make_context(layout_of(root), roaster.KEYS, select_keys=["T2w"],
                       json="no", templates=TEMPLATES)
    assert roaster.shard_inputs(
        ctx,
        root / "sourcedata" / "Normal-001") == [
        root / "sourcedata" / "Normal-001/T2/Normal001-T2.mha"]


def test_missing_source(root):
    """No subject folder: an error (legacy: a warning)"""
    for path in (root / "sourcedata").iterdir():
        if path.name.startswith("Normal-"):
            for f in sorted(path.rglob("*"), reverse=True):
                f.unlink() if f.is_file() else f.rmdir()
            path.rmdir()
    report = roast_api(root, display=False)
    assert [name for _, name, _ in report.errors] == ["sourcedata"]
    assert raw_tree(root) == META
    # (only when images are wanted)
    report = roast_api(root, keys="meta", display=False)
    assert not report.errors


def test_missing_sidecar_template(root, monkeypatch, tmp_path):
    """A missing sidecar template is an error only when sidecars are
    written"""
    templates = tmp_path / "templates"
    templates.mkdir()
    for path in TEMPLATES.iterdir():
        if path.name != "T2w.json":
            (templates / path.name).write_bytes(path.read_bytes())
    monkeypatch.setattr(roaster, "TEMPLATES", templates)
    report = roast_api(root, keys="T2w", subs="1:3", display=False)
    assert sorted(name for _, name, _ in report.errors) == [
        "sub-001_T2w.json", "sub-002_T2w.json"]
    assert raw_tree(root) == {"sub-001/anat/sub-001_T2w.nii.gz",
                              "sub-002/anat/sub-002_T2w.nii.gz"}
    report = roast_api(root, keys="T2w", subs="1:3", json="no",
                       display=False)
    assert not report.errors


def test_corrupt_image_is_an_error(root):
    corrupt(root / "sourcedata" / "Normal-001/T2/Normal001-T2.mha")
    report = roast_api(root, display=False)
    assert [name for _, name, _ in report.errors] == ["sub-001_T2w.nii.gz"]
    assert "gzip" in report.errors[0][2]   # (neither zlib nor gzip data)
    assert report.status == "partial"


def test_participants_rows(root, caplog):
    """Bad rows are skipped with a warning, empty cells are n/a, IDs
    have `sub-`"""
    (root / "sourcedata" / "participants.tsv").write_text(
        "participant_id\tage\tsex\n"
        "001\t54\tF\n"
        "\n"
        "sub-002\t\tM\n"
        "sub-x\t1\tM\n"
        "sub-003\t20\n")
    roast_api(root, keys=["meta", "T2w", "mprage"], display=False)
    assert read_tsv(raw_file(root, "participants.tsv")) == [
        ["participant_id", "age", "sex"],
        ["sub-001", "54", "F"],
        ["sub-002", "n/a", "M"],
    ]
    assert "'sub-x' with 3 cells (skipped)" in caplog.text
    assert "'sub-003' with 2 cells (skipped)" in caplog.text


@pytest.mark.parametrize("content", [b"", b"\n", b"age\tsex\n",
                                     "participant_id\té\n".encode("latin1")])
def test_participants_unreadable(root, content):
    """An empty, headless or non-UTF-8 participants table is an error"""
    (root / "sourcedata" / "participants.tsv").write_bytes(content)
    report = roast_api(root, keys=["meta", "T2w"], subs=[1],
                       display=False)
    assert [name for _, name, _ in report.errors] == ["participants.tsv"]
    assert raw_tree(root) == META - {"participants.tsv"} | \
        {"sub-001/anat/sub-001_T2w.nii.gz", "sub-001/anat/sub-001_T2w.json"}


def test_participants_bom(root):
    (root / "sourcedata" / "participants.tsv").write_text(
        "\ufeffparticipant_id \tage\n001\t54\n", encoding="utf-8")
    roast_api(root, keys=["meta", "T2w"], display=False)
    assert read_tsv(raw_file(root, "participants.tsv")) == [
        ["participant_id", "age"], ["sub-001", "54"]]


def test_template_participants():
    rows = roaster.participants(TEMPLATES / "participants.tsv")
    assert len(rows) == 110
    assert all(len(r) == len(rows[0]) for r in rows)
    assert [r[0] for r in rows[1:]] == [f"sub-{i:03d}" for i in
                                       range(1, 110)]


def test_shards(root):
    roast_api(root, display=False)
    states = read_task_states(layout_of(root).state)
    assert {f"roast/subjects/sub-{s}" for s in ("001", "002", "003")} \
        <= set(states)


def test_subject_folder_names(root, caplog):
    (root / "sourcedata" / "Normal-4" / "T2").mkdir(parents=True)
    report = roast_api(root, keys="angio", display=False)
    assert [name for _, name, _ in report.errors] == ["sub-003_angio.nii.gz"]
    assert "Normal-4: not a subject folder" in caplog.text
