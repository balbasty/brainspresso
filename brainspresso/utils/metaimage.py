"""
Minimal reader for MetaImage files (`.mha`, `.mhd` + raw), the ITK/VTK
format, and conversion to NIfTI.

Geometry follows ITK: `TransformMatrix` holds the direction cosines of
the voxel axes (one axis per row, as written by ITK) and `Offset` the
position of the first voxel, both in LPS world coordinates. They are
converted to a RAS NIfTI affine.
"""
from __future__ import annotations

import gzip
import zlib
from pathlib import Path

import nibabel as nib
import numpy as np

ELEMENT_TYPES = {
    "MET_CHAR": "i1", "MET_UCHAR": "u1",
    "MET_SHORT": "i2", "MET_USHORT": "u2",
    "MET_INT": "i4", "MET_UINT": "u4",
    "MET_LONG": "i8", "MET_ULONG": "u8",
    "MET_LONG_LONG": "i8", "MET_ULONG_LONG": "u8",
    "MET_FLOAT": "f4", "MET_DOUBLE": "f8",
}
LPS_TO_RAS = np.diag([-1.0, -1.0, 1.0])


def read_header(path: str | Path) -> tuple[dict, int]:
    """Return `(header, data_offset)`; the offset is for LOCAL data"""
    header, offset = {}, 0
    with open(path, "rb") as f:
        for line in f:
            offset += len(line)
            text = line.decode("latin1").strip()
            if not text or "=" not in text:
                continue
            key, value = (x.strip() for x in text.split("=", 1))
            header[key] = value
            if key == "ElementDataFile":
                break
    return header, offset


def read(path: str | Path) -> tuple[np.ndarray, np.ndarray, dict]:
    """
    Read a MetaImage file.

    Returns
    -------
    data : array, shape `DimSize` (x first)
    affine : (4, 4) array, voxel -> RAS (mm)
    header : dict
    """
    path = Path(path)
    header, offset = read_header(path)
    ndim = int(header["NDims"])
    shape = [int(x) for x in header["DimSize"].split()]
    channels = int(header.get("ElementNumberOfChannels", 1))
    dtype = np.dtype(ELEMENT_TYPES[header["ElementType"]])
    msb = header.get(
        "BinaryDataByteOrderMSB", header.get("ElementByteOrderMSB", "False")
    )
    dtype = dtype.newbyteorder(">" if msb.lower() == "true" else "<")

    datafile = header.get("ElementDataFile", "LOCAL")
    if datafile == "LOCAL":
        raw = path.read_bytes()[offset:]
    else:
        raw = (path.parent / datafile).read_bytes()
    if header.get("CompressedData", "False").lower() == "true":
        try:
            raw = zlib.decompress(raw)
        except zlib.error:
            raw = gzip.decompress(raw)

    count = int(np.prod(shape)) * channels
    if len(raw) < count * dtype.itemsize:
        raise ValueError(
            f"{path.name} is truncated: {len(raw)} bytes for "
            f"{count * dtype.itemsize} expected"
        )
    data = np.frombuffer(raw, dtype=dtype, count=count)
    # x varies fastest (Fortran order); channels, if any, fastest of all
    if channels > 1:
        data = data.reshape([*shape[::-1], channels])
        data = np.moveaxis(data, -1, 0).T     # x, y, z, channels
    else:
        data = data.reshape(shape[::-1]).T
    data = data.astype(dtype.newbyteorder("="))

    spacing = np.asarray(
        header.get("ElementSpacing", "1 " * ndim).split(), float
    )[:3]
    directions = np.asarray(
        header.get("TransformMatrix", " ".join(
            "1" if i == j else "0" for i in range(ndim) for j in range(ndim)
        )).split(), float
    ).reshape(ndim, ndim)[:3, :3]
    origin = np.asarray(
        header.get("Offset", header.get("Origin", "0 " * ndim)).split(),
        float,
    )[:3]
    if ndim < 3:
        raise ValueError("Only 3D (or more) MetaImages are supported")

    affine = np.eye(4)
    # rows of TransformMatrix are axis directions -> columns of the affine
    affine[:3, :3] = LPS_TO_RAS @ (directions.T * spacing[None, :])
    affine[:3, 3] = LPS_TO_RAS @ origin
    return data, affine, header


def metaimage_to_nifti(
    inp: str | Path, out: str | Path
) -> nib.Nifti1Image:
    """Convert a MetaImage (`.mha`/`.mhd`) file to NIfTI"""
    data, affine, header = read(inp)
    img = nib.Nifti1Image(data, affine)
    img.set_qform(affine, code=1)
    img.set_sform(affine, code=1)
    img.header.set_xyzt_units("mm")
    nib.save(img, str(out))
    return img
