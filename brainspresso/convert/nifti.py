"""
NIfTI names and headers.

`image_ext` (the extension of an image name) and `header_tr` (the
RepetitionTime of a NIfTI-1 header).
"""
from __future__ import annotations

import gzip
from typing import IO

import nibabel as nib

from brainspresso.convert.base import input_errors, name_of

__all__ = ["NIFTI", "TR_RANGE", "header_tr", "image_ext"]

#: Extensions of NIfTI images (longest first)
NIFTI = (".nii.gz", ".nii")

#: plausible RepetitionTime (s) read from a NIfTI header
TR_RANGE: tuple[float, float] = (0.1, 30.)


def image_ext(name: str) -> str | None:
    """
    Give the NIfTI extension of an image name.

    Parameters
    ----------
    name : str
        File name

    Returns
    -------
    str | None
        `.nii.gz` or `.nii`, in any case in `name` (None: not one)
    """
    lower = name.lower()
    return next((e for e in NIFTI if lower.endswith(e)), None)


def header_tr(fileobj: IO[bytes], name: str = "") -> float | None:
    """
    Read the RepetitionTime of a NIfTI-1 header.

    Parameters
    ----------
    fileobj : file
        Opened image (gzip-compressed if `name` ends with `.nii.gz`, in
        any case)
    name : str
        Its name

    Returns
    -------
    float | None
        `pixdim[4]` in seconds (time unit of `xyzt_units`); None if
        unset, of unknown unit or not plausible (`TR_RANGE`)

    Raises
    ------
    ConvertError
        If the header cannot be read (truncated, not NIfTI-1, not gzip)
    """
    with input_errors(name or name_of(fileobj), "unreadable NIfTI header"):
        if image_ext(name) == ".nii.gz":
            fileobj = gzip.GzipFile(fileobj=fileobj)
        hdr = nib.Nifti1Header.from_fileobj(fileobj)
    if hdr["dim"][0] < 4:
        return None
    scale = {"sec": 1., "msec": 1e-3, "usec": 1e-6}.get(
        hdr.get_xyzt_units()[1])
    if scale is None:
        return None
    tr = float(hdr["pixdim"][4]) * scale
    lo, hi = TR_RANGE
    return round(tr, 6) if lo <= tr <= hi else None
