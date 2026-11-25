"""NRRD -> NIfTI conversion (orientation, DWI gradients)"""
import nibabel as nib
import numpy as np
import pytest

nrrd = pytest.importorskip("nrrd")

from brainspresso.utils.nrrd import nrrd_to_nifti  # noqa: E402


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


def test_dwi_list_axis_first(tmp_path):
    img, bvals, bvecs = convert_dwi(
        tmp_path, [(0, 0, 0), (0, 1, 0)], np.eye(3), list_axis_first=True
    )
    assert img.shape == (3, 3, 2, 2)
    np.testing.assert_allclose(bvals, [0, 1000])


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


def test_truncated_file(tmp_path):
    data = np.zeros((8, 8, 8), dtype=np.int16)
    header = {"space": "left-posterior-superior",
              "space directions": np.eye(3), "encoding": "raw"}
    nrrd.write(str(tmp_path / "t.nrrd"), data, header)
    raw = (tmp_path / "t.nrrd").read_bytes()
    (tmp_path / "t.nrrd").write_bytes(raw[:-200])
    with pytest.raises(ValueError, match="truncated"):
        nrrd_to_nifti(tmp_path / "t.nrrd", tmp_path / "t.nii.gz")
