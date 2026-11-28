"""
Conversions between the volume formats of nibabel.

NIfTI, MGH/MGZ, Analyze: `nibabel_convert`, and its action
`BabelConvert`.
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import nibabel
import numpy as np

from brainspresso.actions.action import Action, IfExistsChoice, Kind
from brainspresso.convert.base import input_errors
from brainspresso.utils.log import LoggingOutputSuppressor

__all__ = ["FORMATS", "BabelConvert", "nibabel_convert"]

lg = logging.getLogger(__name__)

#: Output format of an extension
FORMATS = {
    ".nii": nibabel.Nifti1Image, ".nii.gz": nibabel.Nifti1Image,
    ".mgh": nibabel.MGHImage, ".mgz": nibabel.MGHImage,
    ".img": nibabel.AnalyzeImage, ".hdr": nibabel.AnalyzeImage,
}


def output_format(path: str | Path):
    """
    Give the nibabel image class that writes a file name.

    Parameters
    ----------
    path : path
        Output file

    Returns
    -------
    type
        Its `nibabel` image class (`FORMATS`)

    Raises
    ------
    ValueError
        If its extension is not one of `FORMATS`
    """
    name = Path(path).name.lower()
    for ext in sorted(FORMATS, key=len, reverse=True):
        if name.endswith(ext):
            return FORMATS[ext]
    raise ValueError(f"{Path(path).name}: no nibabel format writes this "
                     f"extension ({', '.join(FORMATS)})")


def nibabel_convert(
        src,
        dst,
        *,
        inp_format=None,
        out_format=None,
        affine=None,
        makedirs=True,
):
    """
    Convert a volume between formats.

    Parameters
    ----------
    src : str or Path
        Path to source volume
    dst : src or Path
        Path to destination volume
    inp_format : nibabel.Image subclass
        Input format (default: guess)
    out_format : nibabel.Image subclass
        Output format (default: from the extension of `dst`,
        `output_format`)
    affine : np.ndarray
        Orientation matrix (default: from input)
    makedirs : bool
        Create the folder of `dst`

    Raises
    ------
    ConvertError
        If the source cannot be read (nothing is written); an error
        writing `dst` passes through
    """
    src = Path(src)
    dst = Path(dst)

    lg.info(f'write {dst.name}')
    if out_format is None:
        out_format = output_format(dst)
    with input_errors(src.name):
        if inp_format is None:
            f = nibabel.load(src)
        else:
            f = inp_format.load(src)
        data = np.asarray(f.dataobj)
    if affine is None:
        affine = f.affine
    if makedirs:
        dst.parent.mkdir(parents=True, exist_ok=True)
    with LoggingOutputSuppressor('nibabel.global'):
        nibabel.save(out_format(data, affine, f.header), dst)


class BabelConvert(Action):
    """Convert a neuroimaging file with nibabel."""

    def __init__(
        self,
        src: str,
        dst: str,
        *,
        inp_format=None,
        out_format=None,
        affine=None,
        ifexists: IfExistsChoice | None = None,
        kind: Kind | None = None,
        tag: str | None = None,
        staging: bool = False,
        size: int = None,
        mtime: datetime = None,
        digests: dict = None,
        compare: bool | None = None,
    ):
        """
        Build the action.

        Parameters
        ----------
        src : str | Path
            Path to input file
        dst : str | Path
            Path to output file

        Other Parameters
        ----------------
        inp_format : nibabel.Image subclass
            Input format (default: guess)
        out_format : nibabel.Image subclass
            Output format  (default: guess)
        affine : np.ndarray
            Orientation matrix (default: from input)
        ifexists : {'error', 'skip', 'overwrite', 'different', 'refresh',
                    'update'}
            Behaviour if destination file already exists
        kind : str
            Kind of output (default: set by the layer that runs it)
        tag : str
            Progress group (`actions.engine`)
        staging : bool
            Intermediate file, not subject to the JSON policy
        size : int
            Expected output size, in bytes
        mtime : datetime
            Expected last-modified time
        digests : dict | None
            Expected digest(s) of the file.
            Keys are algorithm names (e.g. "sha256") and values are the
            digests.
        compare : bool | None
            Regenerate and compare the output under "different"
            (`Action`; default: set by the layer that runs it)
        """
        super().__init__(
            src=src,
            dst=dst,
            action=self.action,
            mode="wb",
            input="path",
            ifexists=ifexists,
            kind=kind,
            tag=tag,
            staging=staging,
            size=size,
            mtime=mtime,
            digests=digests,
            compare=compare,
        )
        self.inp_format = inp_format
        self.out_format = out_format
        self.affine = affine

    def action(self, path: Path):
        """Write the output (the actor of the action)."""
        return nibabel_convert(
            self.src, path,
            inp_format=self.inp_format,
            out_format=self.out_format,
            affine=self.affine,
        )
