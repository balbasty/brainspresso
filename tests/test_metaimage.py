"""MetaImage (.mha/.mhd) reader and NIfTI conversion"""
import zlib
from pathlib import Path

import nibabel as nib
import numpy as np
import pytest

from brainspresso.utils.metaimage import metaimage_to_nifti, read


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
