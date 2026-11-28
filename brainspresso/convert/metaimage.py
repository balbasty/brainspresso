"""
MetaImage files (`.mha`, `.mhd` + raw) to NIfTI.

A minimal reader of the ITK/VTK format, and conversion to NIfTI.

Geometry follows ITK: `TransformMatrix` holds the direction cosines of
the voxel axes (one axis per row, as written by ITK) and `Offset` the
position of the first voxel, both in LPS world coordinates. They are
converted to a RAS NIfTI affine.

A corrupt, truncated or unsupported file is a `ConvertError` (missing or
empty header field, unknown element type, data file missing or not a
file, data shorter than declared, compressed data that neither zlib nor
gzip reads).
"""
from __future__ import annotations

import gzip
import zlib
from pathlib import Path

import nibabel as nib
import numpy as np

from brainspresso.convert.base import ConvertError, check_size, input_errors

__all__ = ["ELEMENT_TYPES", "metaimage_to_nifti", "read", "read_header",
           "sources"]

ELEMENT_TYPES = {
    "MET_CHAR": "i1", "MET_UCHAR": "u1",
    "MET_SHORT": "i2", "MET_USHORT": "u2",
    "MET_INT": "i4", "MET_UINT": "u4",
    "MET_LONG": "i8", "MET_ULONG": "u8",
    "MET_LONG_LONG": "i8", "MET_ULONG_LONG": "u8",
    "MET_FLOAT": "f4", "MET_DOUBLE": "f8",
}
#: Fields that a header must have (`ElementDataFile` is the last one)
REQUIRED = ("NDims", "DimSize", "ElementType", "ElementDataFile")
LPS_TO_RAS = np.diag([-1.0, -1.0, 1.0])


def read_header(path: str | Path) -> tuple[dict, int]:
    """
    Read the header of a MetaImage file.

    Parameters
    ----------
    path : path
        `.mha` or `.mhd` file

    Returns
    -------
    header : dict
        Its fields, up to `ElementDataFile` (the last one)
    offset : int
        Offset of the data that follows it (`ElementDataFile = LOCAL`)

    Raises
    ------
    ConvertError
        If a required field (`REQUIRED`) is missing or empty: a header
        cut before or at `ElementDataFile = `, or not a MetaImage file
    """
    path = Path(path)
    header, offset = {}, 0
    with input_errors(path.name), open(path, "rb") as f:
        for line in f:
            offset += len(line)
            text = line.decode("latin1").strip()
            if not text or "=" not in text:
                continue
            key, value = (x.strip() for x in text.split("=", 1))
            header[key] = value
            if key == "ElementDataFile":
                break
    if not header:
        raise ConvertError(f"{path.name}: not a MetaImage header (empty "
                           f"file, or no `key = value` line)")
    missing = [key for key in REQUIRED if key not in header]
    empty = [key for key in REQUIRED if key in header and not header[key]]
    if missing or empty:
        problems = []
        if missing:
            problems.append(f"no {', '.join(missing)}")
        if empty:
            problems.append(f"empty {', '.join(empty)}")
        cut = " (truncated header?)" if "ElementDataFile" in missing or \
            "ElementDataFile" in empty else ""
        raise ConvertError(f"{path.name}: incomplete MetaImage header: "
                           f"{'; '.join(problems)}{cut}")
    return header, offset


def sources(path: str | Path) -> list[Path]:
    """
    List the files of a MetaImage (the sources of its conversion).

    Parameters
    ----------
    path : path
        `.mha` or `.mhd` file

    Returns
    -------
    list[Path]
        `path`, then the data file that its `ElementDataFile` names, if
        it is one file that exists (never raises: a header that cannot
        be read is the conversion's error)
    """
    path = Path(path)
    try:
        header, _ = read_header(path)
    except ConvertError:
        return [path]
    datafile = header["ElementDataFile"]
    if datafile == "LOCAL" or datafile.split()[0] == "LIST" or \
            "%" in datafile:
        return [path]
    data = path.parent / datafile
    return [path, data] if data.is_file() else [path]


def _ints(header: dict, key: str, name: str) -> list[int]:
    """Parse the integers of a header field (`ConvertError` if not)."""
    try:
        return [int(x) for x in header[key].split()]
    except ValueError as e:
        raise ConvertError(f"{name}: invalid {key} {header[key]!r}") from e


def _int(header: dict, key: str, name: str) -> int:
    """Parse the positive integer of a field (`ConvertError` if not)."""
    values = _ints(header, key, name)
    if len(values) != 1 or values[0] < 1:
        raise ConvertError(f"{name}: invalid {key} {header[key]!r}")
    return values[0]


def _data(path: Path, header: dict, offset: int) -> bytes:
    """Read the (decompressed) data bytes of a MetaImage file."""
    name = path.name
    datafile = header["ElementDataFile"]
    if datafile == "LOCAL":
        with input_errors(name):
            raw = path.read_bytes()[offset:]
    elif datafile.split()[0] == "LIST" or "%" in datafile:
        raise ConvertError(f"{name}: ElementDataFile {datafile!r}: data "
                           f"in several files is not supported")
    else:
        source = path.parent / datafile
        if not source.is_file():
            raise ConvertError(f"{name}: ElementDataFile {datafile!r} is "
                               f"not a file (missing, or a folder)")
        with input_errors(name, f"ElementDataFile {datafile!r} cannot be "
                                f"read"):
            raw = source.read_bytes()
    if header.get("CompressedData", "False").lower() == "true":
        try:
            raw = zlib.decompress(raw)
        except zlib.error:
            try:
                raw = gzip.decompress(raw)
            except (OSError, EOFError, zlib.error) as e:
                raise ConvertError(
                    f"{name}: compressed data that neither zlib nor gzip "
                    f"can read ({type(e).__name__}: {e})") from e
    return raw


def read(path: str | Path) -> tuple[np.ndarray, np.ndarray, dict]:
    """
    Read a MetaImage file.

    Parameters
    ----------
    path : path
        `.mha` file, or `.mhd` file (its data file next to it)

    Returns
    -------
    data : array
        Of shape `DimSize` (x first; channels last)
    affine : (4, 4) array
        voxel -> RAS (mm)
    header : dict
        Its header

    Raises
    ------
    ConvertError
        If the file is corrupt, truncated or not supported
    """
    path = Path(path)
    name = path.name
    header, offset = read_header(path)
    ndim = _int(header, "NDims", name)
    if ndim < 3:
        raise ConvertError(f"{name}: only 3D (or more) MetaImages are "
                           f"supported (NDims = {ndim})")
    shape = _ints(header, "DimSize", name)
    if len(shape) != ndim or min(shape) < 1:
        raise ConvertError(f"{name}: invalid DimSize {header['DimSize']!r} "
                           f"(NDims = {ndim} positive sizes expected)")
    channels = _int(header, "ElementNumberOfChannels", name) \
        if "ElementNumberOfChannels" in header else 1
    if header["ElementType"] not in ELEMENT_TYPES:
        raise ConvertError(f"{name}: unknown ElementType "
                           f"{header['ElementType']!r}")
    dtype = np.dtype(ELEMENT_TYPES[header["ElementType"]])
    msb = header.get(
        "BinaryDataByteOrderMSB", header.get("ElementByteOrderMSB", "False")
    )
    dtype = dtype.newbyteorder(">" if msb.lower() == "true" else "<")

    # (Python integers: `np.prod` overflows for an absurd `DimSize`)
    check_size(name, [*shape, channels], dtype.itemsize, field="DimSize")
    raw = _data(path, header, offset)
    nbytes = check_size(name, [*shape, channels], dtype.itemsize, len(raw),
                        field="DimSize")
    data = np.frombuffer(raw, dtype=dtype, count=nbytes // dtype.itemsize)
    # x varies fastest (Fortran order); channels, if any, fastest of all
    if channels > 1:
        data = data.reshape([*shape[::-1], channels])
        data = np.moveaxis(data, -1, 0).T     # x, y, z, channels
    else:
        data = data.reshape(shape[::-1]).T
    data = data.astype(dtype.newbyteorder("="))

    with input_errors(name, "invalid geometry"):
        spacing = np.asarray(
            header.get("ElementSpacing", "1 " * ndim).split(), float
        )[:3]
        directions = np.asarray(
            header.get("TransformMatrix", " ".join(
                "1" if i == j else "0"
                for i in range(ndim) for j in range(ndim)
            )).split(), float
        ).reshape(ndim, ndim)[:3, :3]
        origin = np.asarray(
            header.get("Offset", header.get("Origin", "0 " * ndim)).split(),
            float,
        )[:3]
        if len(spacing) < 3 or len(origin) < 3:
            raise ValueError("ElementSpacing and Offset need 3 values")

    affine = np.eye(4)
    # rows of TransformMatrix are axis directions -> columns of the affine
    affine[:3, :3] = LPS_TO_RAS @ (directions.T * spacing[None, :])
    affine[:3, 3] = LPS_TO_RAS @ origin
    return data, affine, header


def metaimage_to_nifti(
    inp: str | Path, out: str | Path
) -> nib.Nifti1Image:
    """
    Convert a MetaImage (`.mha`/`.mhd`) file to NIfTI.

    Parameters
    ----------
    inp : path
        MetaImage file
    out : path
        Output `.nii` or `.nii.gz` file

    Returns
    -------
    nib.Nifti1Image
        The image written

    Raises
    ------
    ConvertError
        If the input is corrupt, truncated or not supported (nothing is
        written); an error writing `out` passes through
    """
    data, affine, header = read(inp)
    img = nib.Nifti1Image(data, affine)
    img.set_qform(affine, code=1)
    img.set_sform(affine, code=1)
    img.header.set_xyzt_units("mm")
    nib.save(img, str(out))
    return img
