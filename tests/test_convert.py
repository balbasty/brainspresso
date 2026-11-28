"""Converters (`brainspresso.convert`): round trips and the error
contract (a corrupt input is a `ConvertError` naming its file)"""
import ast
import io
import zlib
from pathlib import Path

import nibabel as nib
import numpy as np
import pytest

import brainspresso
from brainspresso.convert import (
    CONVERTERS,
    Convert,
    ConvertError,
    convert,
    converter_of,
)
from brainspresso.convert import nrrd as convert_nrrd
from brainspresso.convert.analyze import analyze_action, analyze_to_nifti
from brainspresso.convert.metaimage import metaimage_to_nifti, read
from brainspresso.convert.nibabel import nibabel_convert, output_format
from brainspresso.convert.nifti import header_tr, image_ext
from brainspresso.convert.nrrd import nrrd_to_nifti
from brainspresso.convert.volume import make_affine, relabel

try:
    import nrrd
except ImportError:         # (optional: brainspresso[nrrd])
    nrrd = None

needs_nrrd = pytest.mark.skipif(nrrd is None, reason="needs pynrrd")


def run(action) -> list[dict]:
    """Run an action, return its statuses."""
    return list(action)


# ----------------------------------------------------------------------
#   MetaImage
# ----------------------------------------------------------------------

def write_mha(path: Path, data: np.ndarray, *, compressed=True,
              directions=None, offset=(0, 0, 0), spacing=(1, 1, 1),
              channels=1, external=False, element="MET_SHORT") -> Path:
    """Write a MetaImage the way ITK does (x fastest, LPS geometry)"""
    shape = data.shape[:3]
    if channels > 1:   # x, y, z, c -> c fastest
        raw = np.ascontiguousarray(np.moveaxis(data.T, 0, -1)).tobytes()
    else:
        raw = data.T.tobytes()
    if compressed:
        raw = zlib.compress(raw)
    directions = np.eye(3) if directions is None else np.asarray(directions)
    lines = [
        "ObjectType = Image",
        "NDims = 3",
        "BinaryData = True",
        "BinaryDataByteOrderMSB = False",
        f"CompressedData = {compressed}",
        f"CompressedDataSize = {len(raw)}" if compressed else "",
        "TransformMatrix = " + " ".join(map(str, directions.ravel())),
        "Offset = " + " ".join(map(str, offset)),
        "AnatomicalOrientation = RAI",
        "ElementSpacing = " + " ".join(map(str, spacing)),
        "DimSize = " + " ".join(map(str, shape)),
        f"ElementNumberOfChannels = {channels}" if channels > 1 else "",
        f"ElementType = {element}",
    ]
    if external:
        (path.parent / (path.stem + ".raw")).write_bytes(raw)
        lines.append(f"ElementDataFile = {path.stem}.raw")
        tail = b""
    else:
        lines.append("ElementDataFile = LOCAL")
        tail = raw
    head = "\n".join(x for x in lines if x) + "\n"
    path.write_bytes(head.encode() + tail)
    return path


@pytest.fixture
def volume():
    return np.arange(4 * 3 * 2, dtype=np.int16).reshape(4, 3, 2)


@pytest.mark.parametrize("compressed", [True, False])
def test_read_data_and_identity_geometry(tmp_path, volume, compressed):
    p = write_mha(tmp_path / "x.mha", volume, compressed=compressed,
                  spacing=(0.5, 1, 2), offset=(10, 20, 30))
    data, affine, header = read(p)
    assert data.shape == (4, 3, 2)
    np.testing.assert_array_equal(data, volume)
    # LPS -> RAS: x and y flip sign, z does not
    np.testing.assert_allclose(affine, [
        [-0.5, 0, 0, -10],
        [0, -1, 0, -20],
        [0, 0, 2, 30],
        [0, 0, 0, 1],
    ])
    assert header["ElementType"] == "MET_SHORT"


def test_rows_of_transform_matrix_are_axes(tmp_path, volume):
    # first voxel axis points along LPS +y, second along LPS +x
    directions = [[0, 1, 0], [1, 0, 0], [0, 0, 1]]
    p = write_mha(tmp_path / "x.mha", volume, directions=directions,
                  spacing=(2, 3, 4))
    _, affine, _ = read(p)
    np.testing.assert_allclose(affine[:3, :3], [
        [0, -3, 0],
        [-2, 0, 0],
        [0, 0, 4],
    ])


def test_external_data_file(tmp_path, volume):
    p = write_mha(tmp_path / "x.mhd", volume, compressed=False,
                  external=True)
    data, _, _ = read(p)
    np.testing.assert_array_equal(data, volume)


def test_multichannel(tmp_path):
    vol = np.arange(4 * 3 * 2 * 2, dtype=np.float32).reshape(4, 3, 2, 2)
    p = write_mha(tmp_path / "x.mha", vol, channels=2,
                  element="MET_FLOAT")
    data, _, _ = read(p)
    assert data.shape == (4, 3, 2, 2)
    np.testing.assert_array_equal(data, vol)


def test_truncated(tmp_path, volume):
    p = write_mha(tmp_path / "x.mha", volume, compressed=False)
    p.write_bytes(p.read_bytes()[:-4])
    with pytest.raises(ValueError, match="truncated"):
        read(p)


def test_to_nifti(tmp_path, volume):
    p = write_mha(tmp_path / "x.mha", volume, spacing=(1, 1, 2))
    out = tmp_path / "x.nii.gz"
    metaimage_to_nifti(p, out)
    img = nib.load(out)
    np.testing.assert_array_equal(np.asarray(img.dataobj), volume)
    assert nib.aff2axcodes(img.affine) == ("L", "P", "S")
    assert img.header.get_zooms() == (1, 1, 2)
    assert img.header["sform_code"] == 1


# ----------------------------------------------------------------------
#   NRRD
# ----------------------------------------------------------------------

def write_nrrd(path, data, directions, origin, space="left-posterior-superior",
               kinds=None, extra=None):
    header = {
        "space": space,
        "space directions": directions,
        "space origin": np.asarray(origin, float),
        "encoding": "gzip",
    }
    if kinds:
        header["kinds"] = kinds
    header.update(extra or {})
    nrrd.write(str(path), data, header)


@needs_nrrd
def test_lps_affine(tmp_path):
    data = np.arange(4 * 5 * 6, dtype=np.int16).reshape(4, 5, 6)
    dirs = np.diag([0.5, 0.5, 0.8])
    write_nrrd(tmp_path / "t1.nrrd", data, dirs, (-10, -20, -30))
    img = nrrd_to_nifti(tmp_path / "t1.nrrd", tmp_path / "t1.nii.gz")
    out = nib.load(tmp_path / "t1.nii.gz")
    # LPS -> RAS: x and y flipped
    np.testing.assert_allclose(out.affine, [
        [-0.5, 0, 0, 10],
        [0, -0.5, 0, 20],
        [0, 0, 0.8, -30],
        [0, 0, 0, 1],
    ])
    assert nib.aff2axcodes(out.affine) == ("L", "P", "S")
    np.testing.assert_array_equal(np.asarray(out.dataobj), data)
    np.testing.assert_allclose(out.header.get_zooms(), (0.5, 0.5, 0.8))
    assert img.get_data_dtype() == np.int16


def dwi_header(grads, bmax=1000, frame=None):
    extra = {"modality": "DWMRI", "DWMRI_b-value": str(bmax)}
    for i, g in enumerate(grads):
        extra[f"DWMRI_gradient_{i:04d}"] = " ".join(map(str, g))
    if frame is not None:
        extra["measurement frame"] = np.asarray(frame, float)
    return extra


def convert_dwi(tmp_path, grads, dirs, list_axis_first=False, **kw):
    n = len(grads)
    shape = (n, 3, 3, 2) if list_axis_first else (3, 3, 2, n)
    data = np.zeros(shape, dtype=np.int16)
    space_dirs = [list(d) for d in dirs]
    kinds = ["space"] * 3
    if list_axis_first:
        space_dirs = [[np.nan] * 3] + space_dirs
        kinds = ["list"] + kinds
    else:
        space_dirs = space_dirs + [[np.nan] * 3]
        kinds = kinds + ["list"]
    write_nrrd(
        tmp_path / "dwi.nrrd", data, np.asarray(space_dirs, float),
        (0, 0, 0), kinds=kinds, extra=dwi_header(grads, **kw),
    )
    img = nrrd_to_nifti(
        tmp_path / "dwi.nrrd", tmp_path / "dwi.nii.gz",
        bval=tmp_path / "dwi.bval", bvec=tmp_path / "dwi.bvec",
    )
    bvals = np.loadtxt(tmp_path / "dwi.bval", ndmin=1)
    bvecs = np.loadtxt(tmp_path / "dwi.bvec", ndmin=2)
    return img, bvals, bvecs


@needs_nrrd
def test_dwi_bvals_bvecs_axis_aligned(tmp_path):
    grads = [(0, 0, 0), (1, 0, 0), (0, 1, 0), (0, 0, 1),
             (0, 0, np.sqrt(0.5))]          # norm**2 scales the b-value
    img, bvals, bvecs = convert_dwi(tmp_path, grads, np.eye(3))
    assert img.shape == (3, 3, 2, 5)
    np.testing.assert_allclose(bvals, [0, 1000, 1000, 1000, 500])
    # LPS, positive directions -> RAS affine diag(-1,-1,1): det > 0, so
    # the x component is flipped (FSL convention)
    np.testing.assert_allclose(bvecs.T, [
        [0, 0, 0], [-1, 0, 0], [0, 1, 0], [0, 0, 1], [0, 0, 1],
    ], atol=1e-6)


@needs_nrrd
def test_dwi_list_axis_first(tmp_path):
    img, bvals, bvecs = convert_dwi(
        tmp_path, [(0, 0, 0), (0, 1, 0)], np.eye(3), list_axis_first=True
    )
    assert img.shape == (3, 3, 2, 2)
    np.testing.assert_allclose(bvals, [0, 1000])


@needs_nrrd
def test_dwi_rotated_grid_and_measurement_frame(tmp_path):
    # Voxel axes are a permutation of world axes: voxel i -> world z,
    # j -> world x, k -> world y. A gradient along world z must therefore
    # be along voxel axis i, whatever the measurement frame convention,
    # once expressed in world coordinates.
    dirs = np.array([[0, 0, 2.0], [1.0, 0, 0], [0, 1.5, 0]])  # rows
    # measurement frame = identity: gradient given in world space
    _, _, bvecs = convert_dwi(tmp_path, [(0, 0, 1)], dirs)
    assert np.allclose(np.abs(bvecs[:, 0]), [1, 0, 0], atol=1e-6)

    # measurement frame that swaps x and z: a measured (1,0,0) is world z
    frame = [[0, 0, 1], [0, 1, 0], [1, 0, 0]]
    _, _, bvecs = convert_dwi(tmp_path, [(1, 0, 0)], dirs, frame=frame)
    assert np.allclose(np.abs(bvecs[:, 0]), [1, 0, 0], atol=1e-6)


@needs_nrrd
def test_dwi_requires_outputs(tmp_path):
    grads = [(0, 0, 0)]
    data = np.zeros((2, 2, 2, 1), dtype=np.int16)
    write_nrrd(
        tmp_path / "dwi.nrrd", data,
        np.asarray([[1, 0, 0], [0, 1, 0], [0, 0, 1], [np.nan] * 3]),
        (0, 0, 0), kinds=["space"] * 3 + ["list"],
        extra=dwi_header(grads),
    )
    with pytest.raises(ValueError, match="bval"):
        nrrd_to_nifti(tmp_path / "dwi.nrrd", tmp_path / "dwi.nii.gz")


@needs_nrrd
def test_truncated_file(tmp_path):
    data = np.zeros((8, 8, 8), dtype=np.int16)
    header = {"space": "left-posterior-superior",
              "space directions": np.eye(3), "encoding": "raw"}
    nrrd.write(str(tmp_path / "t.nrrd"), data, header)
    raw = (tmp_path / "t.nrrd").read_bytes()
    (tmp_path / "t.nrrd").write_bytes(raw[:-200])
    with pytest.raises(ValueError, match="truncated"):
        nrrd_to_nifti(tmp_path / "t.nrrd", tmp_path / "t.nii.gz")


# ----------------------------------------------------------------------
#   MetaImage: corrupt inputs (`ConvertError`, the file named)
# ----------------------------------------------------------------------

def header_lines(path: Path) -> bytes:
    """The header of a MetaImage written by `write_mha` (without data)."""
    return path.read_bytes().split(b"ElementDataFile")[0]


def cut_header(path, volume):
    """The header cut in the middle of a field (`DimSize`)."""
    head = header_lines(write_mha(path, volume))
    path.write_bytes(head.split(b"DimSize")[0] + b"DimSi")


def empty_datafile(path, volume):
    """The header ending at `ElementDataFile = ` (the regression: it
    named the folder of the file, "Is a directory")."""
    path.write_bytes(header_lines(write_mha(path, volume)) +
                     b"ElementDataFile = ")


def folder_datafile(path, volume):
    write_mha(path, volume, compressed=False, external=True)
    (path.parent / "x.raw").unlink()
    (path.parent / "x.raw").mkdir()


def missing_datafile(path, volume):
    write_mha(path, volume, compressed=False, external=True)
    (path.parent / "x.raw").unlink()


def unknown_element(path, volume):
    write_mha(path, volume, element="MET_COMPLEX")


def short_data(path, volume):
    write_mha(path, volume, compressed=False)
    path.write_bytes(path.read_bytes()[:-4])


def bad_zlib(path, volume):
    head = header_lines(write_mha(path, volume))
    path.write_bytes(head + b"ElementDataFile = LOCAL\nnot zlib data")


def two_dims(path, volume):
    write_mha(path, volume)
    path.write_bytes(path.read_bytes().replace(b"NDims = 3", b"NDims = 2"))


def bad_dims(path, volume):
    write_mha(path, volume)
    path.write_bytes(path.read_bytes().replace(b"DimSize = 4 3 2",
                                               b"DimSize = 4 x 2"))


def bad_geometry(path, volume):
    write_mha(path, volume)
    path.write_bytes(path.read_bytes().replace(b"Offset = 0 0 0",
                                               b"Offset = 0 zero 0"))


@pytest.mark.parametrize("make, match", [
    (lambda p, v: p.write_bytes(b""), "not a MetaImage header"),
    (cut_header, "no DimSize, ElementType, ElementDataFile"),
    (empty_datafile, "empty ElementDataFile"),
    (folder_datafile, "'x.raw' is not a file"),
    (missing_datafile, "'x.raw' is not a file"),
    (unknown_element, "unknown ElementType 'MET_COMPLEX'"),
    (short_data, "truncated data"),
    (bad_zlib, "neither zlib nor gzip"),
    (two_dims, "only 3D"),
    (bad_dims, "invalid DimSize"),
    (bad_geometry, "invalid geometry"),
], ids=["empty", "cut-header", "empty-datafile", "folder-datafile",
        "missing-datafile", "unknown-element", "short-data", "bad-zlib",
        "2d", "bad-dims", "bad-geometry"])
def test_metaimage_corrupt(tmp_path, volume, make, match):
    path = tmp_path / "x.mha"
    make(path, volume)
    out = tmp_path / "x.nii.gz"
    with pytest.raises(ConvertError, match=match) as info:
        metaimage_to_nifti(path, out)
    assert str(info.value).startswith("x.mha: ")
    assert "directory" not in str(info.value)
    assert not out.exists()


def test_metaimage_truncated_header_is_not_a_directory(tmp_path, volume):
    """The bug of `utils.metaimage`: a header ending at
    `ElementDataFile = ` read the folder of the file as its data"""
    path = tmp_path / "x.mha"
    empty_datafile(path, volume)
    with pytest.raises(ConvertError) as info:
        read(path)
    assert not isinstance(info.value.__cause__, IsADirectoryError)
    assert "truncated header" in str(info.value)


def test_output_errors_pass_through(tmp_path, volume):
    """An error writing the output is not the input's"""
    path = write_mha(tmp_path / "x.mha", volume)
    with pytest.raises(OSError) as info:
        metaimage_to_nifti(path, tmp_path / "no" / "such" / "x.nii.gz")
    assert not isinstance(info.value, ConvertError)


# ----------------------------------------------------------------------
#   NRRD: corrupt inputs
# ----------------------------------------------------------------------

@needs_nrrd
def test_nrrd_corrupt(tmp_path):
    path = tmp_path / "t.nrrd"
    path.write_bytes(b"NRRD0004\ntype: what\n\n")
    with pytest.raises(ConvertError, match=r"^t\.nrrd: "):
        nrrd_to_nifti(path, tmp_path / "t.nii.gz")
    path.write_bytes(b"not a nrrd file")
    with pytest.raises(ConvertError, match=r"^t\.nrrd: "):
        nrrd_to_nifti(path, tmp_path / "t.nii.gz")
    assert not (tmp_path / "t.nii.gz").exists()


@needs_nrrd
def test_nrrd_unsupported_space(tmp_path):
    data = np.zeros((2, 2, 2), dtype=np.int16)
    write_nrrd(tmp_path / "t.nrrd", data, np.eye(3), (0, 0, 0),
               space="right-posterior-inferior")
    with pytest.raises(ConvertError, match="Unsupported NRRD space"):
        nrrd_to_nifti(tmp_path / "t.nrrd", tmp_path / "t.nii.gz")


@needs_nrrd
def test_nrrd_gradients_do_not_match(tmp_path):
    data = np.zeros((2, 2, 2, 3), dtype=np.int16)
    write_nrrd(
        tmp_path / "dwi.nrrd", data,
        np.asarray([[1, 0, 0], [0, 1, 0], [0, 0, 1], [np.nan] * 3]),
        (0, 0, 0), kinds=["space"] * 3 + ["list"],
        extra=dwi_header([(0, 0, 0), (1, 0, 0)]),
    )
    with pytest.raises(ConvertError, match="2 gradients for 3 volumes"):
        nrrd_to_nifti(tmp_path / "dwi.nrrd", tmp_path / "dwi.nii.gz",
                      bval=tmp_path / "dwi.bval", bvec=tmp_path / "dwi.bvec")
    assert not list(tmp_path.glob("dwi.b*"))


# ----------------------------------------------------------------------
#   Analyze
# ----------------------------------------------------------------------

def write_analyze(folder: Path, data, name="x") -> tuple[Path, Path]:
    """An Analyze pair `<name>.hdr` + `<name>.img`."""
    img = nib.AnalyzeImage(data, np.eye(4))
    nib.save(img, folder / f"{name}.img")
    return folder / f"{name}.hdr", folder / f"{name}.img"


@pytest.fixture
def labels():
    return np.array([0, 1, 2, 3] * 6, dtype=np.uint8).reshape(4, 3, 2)


def test_analyze_from_paths(tmp_path, labels):
    hdr, img = write_analyze(tmp_path, labels)
    affine = make_affine([4, 3, 2], [1, 2, 3], orient="ASL", center="x/2")
    analyze_to_nifti(hdr, img, tmp_path / "x.nii.gz", affine=affine)
    out = nib.load(tmp_path / "x.nii.gz")
    np.testing.assert_array_equal(np.asarray(out.dataobj), labels)
    np.testing.assert_allclose(out.affine, affine)
    assert nib.aff2axcodes(out.affine) == ("A", "S", "L")


def test_analyze_from_files_with_sources(tmp_path, labels):
    """Members of an archive are file objects; `sources` relabels"""
    hdr, img = write_analyze(tmp_path, labels)
    with open(hdr, "rb") as fh, open(img, "rb") as fi:
        analyze_to_nifti(io.BytesIO(fh.read()), fi, tmp_path / "x.nii.gz",
                         sources={1: 2, 2: 3, 3: 1})
    data = np.asarray(nib.load(tmp_path / "x.nii.gz").dataobj)
    np.testing.assert_array_equal(data, relabel(labels, {1: 2, 2: 3, 3: 1}))
    assert set(np.unique(data[labels == 1])) == {3}


def test_analyze_corrupt(tmp_path, labels):
    hdr, img = write_analyze(tmp_path, labels)
    img.write_bytes(img.read_bytes()[:-5])
    with pytest.raises(ConvertError, match=r"^x\.img: cannot be read"):
        analyze_to_nifti(hdr, img, tmp_path / "x.nii.gz")
    hdr.write_bytes(b"short")
    with pytest.raises(ConvertError, match=r"^x\.img: "):
        analyze_to_nifti(hdr, img, tmp_path / "x.nii.gz")
    assert not (tmp_path / "x.nii.gz").exists()


class Archive:
    """`extractfile` of an open archive (files of a folder), with the
    names read in order."""

    def __init__(self, folder):
        self.folder, self.read = folder, []

    def extractfile(self, name):
        self.read.append(name)
        return open(self.folder / name, "rb")


def test_analyze_action_reads_in_the_given_order(tmp_path, labels):
    write_analyze(tmp_path, labels)
    archive = Archive(tmp_path)
    out = tmp_path / "out" / "x.nii.gz"
    action = analyze_action(archive, tmp_path / "x.img", ["x.img", "x.hdr"],
                            out, sources={1: 1})
    assert run(action)[-1]["status"] == "done"
    assert archive.read == ["x.img", "x.hdr"]
    np.testing.assert_array_equal(np.asarray(nib.load(out).dataobj),
                                  np.where(labels == 1, 1, 0))


def test_analyze_action_corrupt(tmp_path, labels):
    _, img = write_analyze(tmp_path, labels)
    img.write_bytes(b"")
    out = tmp_path / "out" / "x.nii.gz"
    status = run(analyze_action(Archive(tmp_path), tmp_path / "x.img",
                                ["x.hdr", "x.img"], out))[-1]
    assert status["status"] == "error"
    assert status["message"].startswith("x.img: cannot be read")
    assert not out.exists()


# ----------------------------------------------------------------------
#   nibabel formats, NIfTI headers, volumes
# ----------------------------------------------------------------------

def test_nibabel_convert_mgz(tmp_path, volume):
    nib.save(nib.MGHImage(volume.astype(np.float32), np.eye(4)),
             tmp_path / "x.mgz")
    nibabel_convert(tmp_path / "x.mgz", tmp_path / "x.nii.gz")
    out = nib.load(tmp_path / "x.nii.gz")
    np.testing.assert_array_equal(np.asarray(out.dataobj), volume)
    assert isinstance(out, nib.Nifti1Image)


def test_nibabel_convert_corrupt(tmp_path, volume):
    nib.save(nib.MGHImage(volume.astype(np.float32), np.eye(4)),
             tmp_path / "x.mgz")
    (tmp_path / "x.mgz").write_bytes((tmp_path / "x.mgz").read_bytes()[:40])
    with pytest.raises(ConvertError, match=r"^x\.mgz: cannot be read"):
        nibabel_convert(tmp_path / "x.mgz", tmp_path / "x.nii.gz")
    assert not (tmp_path / "x.nii.gz").exists()


def test_output_format():
    assert output_format("a/x.NII.GZ") is nib.Nifti1Image
    assert output_format("x.mgz") is nib.MGHImage
    with pytest.raises(ValueError, match="x.vtk"):
        output_format("x.vtk")


def test_image_ext():
    assert image_ext("a.NII.GZ") == ".nii.gz"
    assert image_ext("a.nii") == ".nii"
    assert image_ext("a.mgz") is None


def test_header_tr_unreadable():
    with pytest.raises(ConvertError, match=r"^rest\.nii\.gz: unreadable"):
        header_tr(io.BytesIO(b"not gzip"), "rest.nii.gz")
    with pytest.raises(ConvertError, match="unreadable NIfTI header"):
        header_tr(io.BytesIO(b"\0" * 10), "rest.nii")


def test_relabel_direction():
    """`{new: old}`: FAST's CSF, GM, WM (1, 2, 3) in the SPM order"""
    fast = np.array([0, 1, 2, 3, 4])
    np.testing.assert_array_equal(relabel(fast, {1: 2, 2: 3, 3: 1}),
                                  [0, 3, 1, 2, 0])
    np.testing.assert_array_equal(relabel(fast, {1: [1, 2]}),
                                  [0, 1, 1, 0, 0])


# ----------------------------------------------------------------------
#   Registry and the `Convert` action
# ----------------------------------------------------------------------

def test_registry(tmp_path, volume):
    assert converter_of("A.MHA") is metaimage_to_nifti
    assert converter_of("x.nhdr") is nrrd_to_nifti
    assert set(CONVERTERS) >= {".mha", ".mhd", ".nrrd", ".hdr", ".img",
                               ".mgz"}
    with pytest.raises(ConvertError, match="x.dcm: no converter reads"):
        converter_of("x.dcm")
    path = write_mha(tmp_path / "x.mha", volume)
    convert(path, tmp_path / "x.nii.gz")
    assert nib.load(tmp_path / "x.nii.gz").shape == volume.shape
    # (an Analyze pair, by its .img or its .hdr)
    write_analyze(tmp_path, volume, name="y")
    convert(tmp_path / "y.img", tmp_path / "y.nii.gz")
    assert nib.load(tmp_path / "y.nii.gz").shape == volume.shape


def test_convert_action(tmp_path, volume):
    path = write_mha(tmp_path / "x.mha", volume)
    out = tmp_path / "out" / "x.nii.gz"
    assert run(Convert(path, out, tag="anat"))[-1]["status"] == "done"
    assert nib.load(out).shape == volume.shape


def test_convert_action_corrupt(tmp_path, volume):
    """A corrupt input is an error of its output: nothing is written"""
    path = tmp_path / "x.mha"
    empty_datafile(path, volume)
    out = tmp_path / "out" / "x.nii.gz"
    status = run(Convert(path, out))[-1]
    assert status["status"] == "error"
    assert status["message"].startswith("x.mha: incomplete MetaImage")
    assert not out.exists()


@needs_nrrd
def test_convert_action_outputs(tmp_path, monkeypatch):
    """The three outputs of a DWI: all written, or none (an error
    writing the image, after the gradients, passes through)"""
    data = np.zeros((2, 2, 2, 2), dtype=np.int16)
    write_nrrd(
        tmp_path / "dwi.nrrd", data,
        np.asarray([[1, 0, 0], [0, 1, 0], [0, 0, 1], [np.nan] * 3]),
        (0, 0, 0), kinds=["space"] * 3 + ["list"],
        extra=dwi_header([(0, 0, 0), (1, 0, 0)]),
    )
    out = tmp_path / "out"
    outputs = {"bval": out / "dwi.bval", "bvec": out / "dwi.bvec"}
    action = Convert(tmp_path / "dwi.nrrd", out / "dwi.nii.gz",
                     outputs=outputs, kind="data")
    assert action.dst == [out / "dwi.nii.gz", *outputs.values()]

    def full(*args, **kwargs):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(convert_nrrd.nib, "save", full)
    status = run(action)[-1]
    assert status["status"] == "error"
    assert "No space left" in status["message"]
    assert not out.exists() or not list(out.iterdir())
    monkeypatch.undo()
    assert run(Convert(tmp_path / "dwi.nrrd", out / "dwi.nii.gz",
                       outputs=outputs, kind="data"))[-1]["status"] == "done"
    assert sorted(p.name for p in out.iterdir()) == [
        "dwi.bval", "dwi.bvec", "dwi.nii.gz"]


# ----------------------------------------------------------------------
#   Layering
# ----------------------------------------------------------------------

def imported(folder: Path) -> set[str]:
    """The `brainspresso` packages that the modules of a folder import."""
    out = set()
    for path in folder.glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            names = [a.name for a in node.names] \
                if isinstance(node, ast.Import) else \
                [node.module or ""] if isinstance(node, ast.ImportFrom) \
                else []
            out |= {".".join(n.split(".")[:2]) for n in names
                    if n.startswith("brainspresso.")}
    return out


def test_layering():
    """`actions/` imports no format package; `convert/` builds on
    `actions/` and `utils/` only"""
    package = Path(brainspresso.__file__).parent
    assert imported(package / "actions") <= {
        "brainspresso.actions", "brainspresso.utils", "brainspresso.errors"}
    assert imported(package / "convert") <= {
        "brainspresso.actions", "brainspresso.utils", "brainspresso.convert"}
