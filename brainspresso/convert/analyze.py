"""
Conversion of Analyze 7.5 images (a `.hdr` and `.img` pair) to NIfTI.

The pair is read from paths or from binary file objects, in memory: no
temporary folder. `analyze_action` converts a pair of members of an
open archive (read in the order given: archive order).
"""
from __future__ import annotations

import io
import os
from pathlib import Path, PurePosixPath
from typing import IO, Iterable, Mapping, Sequence

import nibabel as nib
import numpy as np
from nibabel.fileholders import FileHolder

from brainspresso.actions.action import Action
from brainspresso.convert.base import ConvertError, input_errors, name_of
from brainspresso.convert.volume import relabel
from brainspresso.utils.log import LoggingOutputSuppressor

__all__ = ["analyze_action", "analyze_to_nifti"]


def _bytes(src: str | os.PathLike | IO[bytes]) -> bytes:
    """Read a path or a binary file object, whole."""
    if isinstance(src, (str, os.PathLike)):
        return Path(src).read_bytes()
    return src.read()


def analyze_to_nifti(
    hdr: str | os.PathLike | IO[bytes],
    img: str | os.PathLike | IO[bytes],
    dst: str | os.PathLike,
    *,
    affine: np.ndarray | None = None,
    sources: Mapping[int, int | Iterable[int]] | None = None,
    name: str | None = None,
) -> nib.Nifti1Image:
    """
    Convert an Analyze image to NIfTI.

    Parameters
    ----------
    hdr, img : path | file
        Its header (`.hdr`) and data (`.img`): paths, or binary file
        objects (read whole, `hdr` first)
    dst : path
        Output `.nii` or `.nii.gz` file
    affine : (4, 4) array, optional
        Orientation matrix (default: that of the Analyze header, which
        is often wrong: OASIS gives its own, `convert.volume.make_affine`)
    sources : dict[int, int | list[int]], optional
        Relabel a label volume (`convert.volume.relabel`, `{new: old}`)
    name : str, optional
        Name of the image in messages (default: that of `img`)

    Returns
    -------
    nib.Nifti1Image
        The image written

    Raises
    ------
    ConvertError
        If the pair cannot be read (nothing is written); an error
        writing `dst` passes through
    """
    name = name or name_of(img)
    with input_errors(name):
        file_map = {
            "header": FileHolder(fileobj=io.BytesIO(_bytes(hdr))),
            "image": FileHolder(fileobj=io.BytesIO(_bytes(img))),
        }
        image = nib.AnalyzeImage.from_file_map(file_map)
        data = np.asarray(image.dataobj)
    if sources is not None:
        data = relabel(data, sources)
    out = nib.Nifti1Image(data, image.affine if affine is None else affine,
                          image.header)
    with LoggingOutputSuppressor('nibabel.global'):
        nib.save(out, str(dst))
    return out


def analyze_action(
    archive, src: str | os.PathLike, members: Sequence[str],
    dst: str | os.PathLike, *, affine: np.ndarray | None = None,
    sources: Mapping[int, int | Iterable[int]] | None = None,
    tag: str | None = None,
) -> Action:
    """
    Convert an Analyze pair of an open archive to NIfTI (an `Action`).

    Parameters
    ----------
    archive : utils.archive.TarArchive
        The open archive (`extractfile(name)`; it stays open until the
        action has run)
    src : path
        Its path (source of the action)
    members : list[str]
        Names of its `.hdr` and `.img` members, in the order they are
        read (archive order)
    dst : path
        Output file (`.nii.gz`)
    affine, sources
        See `analyze_to_nifti`
    tag : str
        Progress group

    Returns
    -------
    Action
        The conversion (an error of its output if the pair is corrupt)
    """
    def convert(path: Path) -> None:
        """Read the members (in the order given), then convert them."""
        content = {}
        for member in members:
            with archive.extractfile(member) as f:
                content[PurePosixPath(member).suffix.lower()] = f.read()
        image = next((m for m in members if m.lower().endswith(".img")),
                     members[-1])
        if set(content) != {".hdr", ".img"}:
            raise ConvertError(f"{PurePosixPath(image).name}: an Analyze "
                               f"image is a .hdr and an .img")
        analyze_to_nifti(io.BytesIO(content[".hdr"]),
                         io.BytesIO(content[".img"]), path, affine=affine,
                         sources=sources, name=PurePosixPath(image).name)

    return Action(src, Path(dst), convert, input="path", tag=tag)
