"""
Converters of source image formats to NIfTI, with one error contract.

Also between the formats of nibabel.

* `metaimage`: MetaImage (`.mha`, `.mhd` + raw), `metaimage_to_nifti`;
* `nrrd`: NRRD (`.nrrd`, `.nhdr`), with DWI gradients, `nrrd_to_nifti`;
* `analyze`: Analyze pairs (`.hdr` + `.img`, paths or members of an
  open archive), `analyze_to_nifti`;
* `nibabel`: what nibabel reads (`.mgz`/`.mgh`...), `nibabel_convert`
  and its action `BabelConvert`;
* `nifti`: NIfTI names and headers (`image_ext`, `header_tr`);
* `volume`: `make_affine`, `relabel`.

```python
yield Convert(src, "sub-001_T2w.nii.gz", tag="subjects")   # by extension
yield Convert(src, nii, converter=nrrd_to_nifti,
              outputs={"bval": bval, "bvec": bvec}, kind="data")
```

Error contract (`ConvertError`, a `ValueError`): a corrupt, truncated or
unsupported input raises it, with the file name in its message
(`<file name>: <reason>`) and the original error chained; an error
writing an output passes through unchanged; a missing optional package
stays an `ImportError`. A conversion runs inside its action: a corrupt
input is an error of its own outputs (none is written, the shard goes
on). A converter called while producing actions (a header read to
choose a name) goes through `roast.helpers.attempt`.

Layering: `convert/` builds on `actions/` (and `utils/`), never on
`bids/`, `roast/` or the datasets.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Callable, Iterable, Mapping

from brainspresso.actions.action import Action, IfExistsChoice, Kind
from brainspresso.convert import analyze, metaimage, nrrd
from brainspresso.convert.analyze import analyze_to_nifti
from brainspresso.convert.base import ConvertError
from brainspresso.convert.metaimage import metaimage_to_nifti
from brainspresso.convert.nibabel import BabelConvert, nibabel_convert
from brainspresso.convert.nrrd import nrrd_to_nifti

__all__ = ["CONVERTERS", "SOURCES", "BabelConvert", "Convert",
           "ConvertError", "convert", "converter_of", "sources_of"]


def _analyze(src: str | os.PathLike, dst: str | os.PathLike,
             **options) -> object:
    """Convert an Analyze pair named by its `.hdr` or its `.img`."""
    hdr, img = analyze.pair(src)        # (the other file in any case)
    return analyze_to_nifti(hdr, img, dst, name=Path(src).name, **options)


#: Converter of a source extension (lower case), `fn(src, dst, **options)`
CONVERTERS: dict[str, Callable] = {
    ".mha": metaimage_to_nifti,
    ".mhd": metaimage_to_nifti,
    ".nrrd": nrrd_to_nifti,
    ".nhdr": nrrd_to_nifti,
    ".hdr": _analyze,
    ".img": _analyze,
    ".mgz": nibabel_convert,
    ".mgh": nibabel_convert,
}


#: Files that a source of an extension consists of, `fn(src) -> [paths]`
#: (`src` first; a detached header names its data file): the sources
#: of its `Convert` action. Other extensions: `[src]`.
SOURCES: dict[str, Callable[[Path], list[Path]]] = {
    ".mha": metaimage.sources,
    ".mhd": metaimage.sources,
    ".nrrd": nrrd.sources,
    ".nhdr": nrrd.sources,
    ".hdr": analyze.sources,
    ".img": analyze.sources,
}


def sources_of(src: str | os.PathLike) -> list[Path]:
    """
    List the files that a source consists of.

    Parameters
    ----------
    src : path
        Source file

    Returns
    -------
    list[Path]
        `src`, then its companion data files that exist (`SOURCES`;
        never raises)
    """
    src = Path(src)
    hook = SOURCES.get(src.suffix.lower())
    return hook(src) if hook is not None else [src]


def converter_of(src: str | os.PathLike) -> Callable:
    """
    Find the converter of a source file, by its extension.

    Parameters
    ----------
    src : path
        Source file

    Returns
    -------
    callable(src, dst, **options)
        Its converter (`CONVERTERS`)

    Raises
    ------
    ConvertError
        If no converter reads its extension
    """
    name = Path(src).name
    ext = Path(name.lower()).suffix
    if ext not in CONVERTERS:
        what = ext or "files without an extension"
        raise ConvertError(f"{name}: no converter reads {what} (only "
                           f"{', '.join(CONVERTERS)})")
    return CONVERTERS[ext]


def convert(src: str | os.PathLike, dst: str | os.PathLike,
            **options) -> object:
    """
    Convert a file with the converter of its extension.

    Parameters
    ----------
    src : path
        Source file (`CONVERTERS`)
    dst : path
        Output file
    **options
        Options of the converter

    Returns
    -------
    object
        What the converter returns (the image written)

    Raises
    ------
    ConvertError
        If `src` is corrupt, truncated or not supported
    """
    return converter_of(src)(src, dst, **options)


class Convert(Action):
    """
    Convert a source file (an `Action`).

    The conversion runs when the action runs: a corrupt input is an
    error of its outputs only (the engine reports the `ConvertError`
    under the name of the output, none of them is written). Its sources
    are every file of the input (`sources_of`: a detached header and
    its data), so that they are locked while it runs and their times
    count for `--if-exists refresh`/`update`.

    Parameters
    ----------
    src : path
        Source file
    dst : path
        Output file
    converter : callable(src, dst, **options), optional
        Converter (default: that of the extension of `src`,
        `converter_of`, found when the action runs)
    outputs : dict[str, path], optional
        More outputs written by the same conversion, passed to the
        converter by keyword (`{"bval": ..., "bvec": ...}`); the action
        then writes `dst` and them together
    tag : str, optional
        Progress group
    kind : str, optional
        Kind of the outputs (default: set by the layer that runs it;
        give `"data"` for several outputs that are written together)
    ifexists : str, optional
        Behaviour if the outputs exist (`Action`)
    **options
        Options of the converter
    """

    def __init__(
        self,
        src: str | os.PathLike,
        dst: str | os.PathLike,
        *,
        converter: Callable | None = None,
        outputs: Mapping[str, str | os.PathLike] | None = None,
        tag: str | None = None,
        kind: Kind | None = None,
        ifexists: IfExistsChoice | None = None,
        **options,
    ):
        self.source = Path(src)
        self.converter = converter
        self.names = list(outputs or ())
        self.options = options
        dsts: Iterable = [Path(dst), *map(Path, (outputs or {}).values())]
        super().__init__(
            sources_of(src), Path(dst) if not self.names else list(dsts),
            self.convert,
            input="path", tag=tag, kind=kind, ifexists=ifexists,
        )

    def convert(self, path, *paths) -> None:
        """Run the conversion to the (temporary) output paths."""
        converter = self.converter or converter_of(self.source)
        converter(self.source, path, **dict(zip(self.names, paths)),
                  **self.options)
