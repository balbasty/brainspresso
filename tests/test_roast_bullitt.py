"""Tests of the Bullitt roast step."""
import json
import zlib
from pathlib import Path

import nibabel as nib
import numpy as np
import pytest
from cli_utils import bx
from roast_utils import LAYOUTS, init_layout, layout_of, raw_file, raw_tree

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
    for name in ("README", "participants.tsv"):
        assert raw_file(root, name).read_bytes() == \
            (TEMPLATES / name).read_bytes()
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
    assert deriv_tree(root) == {
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
    path = root / "sourcedata" / "Normal-001/T2/Normal001-T2.mha"
    path.write_bytes(path.read_bytes()[:-20])
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


# ----------------------------------------------------------------------
#   Known bugs of the legacy roaster
# ----------------------------------------------------------------------

@pytest.mark.xfail(strict=True, reason="legacy: --sidecars only writes the "
                   "derivative images (desc-brain_T1w.nii.gz, .tre)")
def test_sidecars_only_derivatives(root):
    roast(root, "--keys", "skullstripped", "vessels", "--sidecars", "only")
    assert {p for p in deriv_tree(root) if p.startswith("sub-")} == {
        f"{BRAIN}.json"}


@pytest.mark.xfail(strict=True, reason="legacy: --sidecars no writes the "
                   "sidecar of the skull-stripped image")
def test_sidecars_no_derivatives(root):
    roast(root, "--keys", "skullstripped", "--sidecars", "no")
    assert {p for p in deriv_tree(root) if p.startswith("sub-")} == {
        f"{BRAIN}.nii.gz"}


@pytest.mark.xfail(strict=True, reason="legacy: the derivative dataset "
                   "files are only written with the meta key")
def test_derivatives_without_meta(root):
    roast(root, "--keys", "skullstripped", "vessels")
    assert deriv_tree(root) == DERIV_META | DERIV
