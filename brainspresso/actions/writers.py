"""
Simple writers wrapped in an `Action`.

```python
Tabular = Iterable[Iterable[str]]

WriteJSON(json: dict, dst: Path): ...       # Write a JSON dictionary
WriteTSV(tsv: Tabular, dst: Path): ...      # Write a TSV table
WriteBytes(io: IO|bytes, dst: Path): ...    # Write from a binary buffer
WriteText(io: IO|str, dst: Path): ...       # Write from a text buffer
CopyJSON(src: Path, dst: Path): ...         # Copy a JSON file
CopyBytes(io: IO, dst: Path): ...           # Copy a binary file
Unlink(dst: Path): ...                      # Remove a file
BabelConvert(src: Path, dst: Path): ...     # Convert a neuroimaging file
Freesurfer2Gifti(src: Path, dst: Path): ... # Convert a surface file
```
"""
from pathlib import Path
from datetime import datetime
from typing import Iterable, BinaryIO, TextIO, Mapping

from brainspresso.utils.io import write_json
from brainspresso.utils.io import copy_json
from brainspresso.utils.io import write_tsv
from brainspresso.utils.io import write_from_buffer
from brainspresso.utils.io import copy_from_buffer
from brainspresso.utils.io import write_text
from brainspresso.utils.io import nibabel_convert
from brainspresso.freesurfer.io import nibabel_fs2gii
from brainspresso.actions.action import Action
from brainspresso.actions.action import IfExistsChoice
from brainspresso.actions.action import Kind


def _check_source(action: Action) -> None:
    """Raise if a copy has no source (e.g. no `ctx.resource(name)`)."""
    if not action.src:
        raise FileNotFoundError(
            f"No source to copy to {Path(action.dst).name} "
            f"(missing input or resource)"
        )


class WriteJSON(Action):
    """Write a JSON dictionary."""

    def __init__(
        self,
        json: dict,
        dst: str | Path,
        *,
        src: str | Path | Iterable[str | Path] = tuple(),
        ifexists: IfExistsChoice | None = None,
        kind: Kind | None = None,
        tag: str | None = None,
        staging: bool = False,
        size: int | None = None,
        mtime: datetime | None = None,
        digests: Mapping[str, str] | None = None,
        **json_opt,
    ):
        """
        Build the action.

        Parameters
        ----------
        json : dict
            JSON dictionary
        dst : str | Path
            Path to output JSON file

        Other Parameters
        ----------------
        src : str | Path | sequence[str | Path]
            Dependencies (only used to compute mtime)
        ifexists : {'error', 'skip', 'overwrite', 'different', 'refresh'}
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
        **json_opt : dict
            JSON options
        """
        self.json = json
        self.json_opt = json_opt

        super().__init__(
            src=src,
            dst=dst,
            action=self.action,
            mode="wt",
            input="file",
            ifexists=ifexists,
            kind=kind,
            tag=tag,
            staging=staging,
            size=size,
            mtime=mtime,
            digests=digests,
        )

    def action(self, file: TextIO):
        """Write the output (the actor of the action)."""
        return write_json(self.json, file, **self.json_opt)


class CopyJSON(Action):
    """Copy a JSON file."""

    def __init__(
        self,
        src: str,
        dst: str,
        *,
        ifexists: IfExistsChoice | None = None,
        kind: Kind | None = None,
        tag: str | None = None,
        staging: bool = False,
        size: int = None,
        mtime: datetime = None,
        digests: dict = None,
        **json_opt,
    ):
        """
        Build the action.

        Parameters
        ----------
        src : str | Path
            Path to input JSON file
        dst : str | Path
            Path to output JSON file

        Other Parameters
        ----------------
        ifexists : {'error', 'skip', 'overwrite', 'different', 'refresh'}
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
        **json_opt : dict
            JSON options
        """
        self.json_opt = json_opt

        super().__init__(
            src=src,
            dst=dst,
            action=self.action,
            mode="wt",
            input="file",
            ifexists=ifexists,
            kind=kind,
            tag=tag,
            staging=staging,
            size=size,
            mtime=mtime,
            digests=digests,
        )

    def action(self, file: TextIO):
        """Write the output (the actor of the action)."""
        _check_source(self)
        return copy_json(self.src, file, **self.json_opt)


class WriteTSV(Action):
    """Write a TSV table."""

    def __init__(
        self,
        tsv: Iterable[Iterable[str]],
        dst: str,
        *,
        src: str = tuple(),
        ifexists: IfExistsChoice | None = None,
        kind: Kind | None = None,
        tag: str | None = None,
        staging: bool = False,
        size: int = None,
        mtime: datetime = None,
        digests: dict = None,
        **tsv_opt,
    ):
        """
        Build the action.

        Parameters
        ----------
        tsv : Iterable[Iterable[str]]
            Iterable or TSV rows
        dst : str | Path
            Path to output TSV file

        Other Parameters
        ----------------
        src : str | Path | sequence[str | Path]
            Dependencies (only used to compute mtime)
        ifexists : {'error', 'skip', 'overwrite', 'different', 'refresh'}
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
        **tsv_opt : dict
            TSV options
        """
        self.tsv = tsv
        self.tsv_opt = tsv_opt

        super().__init__(
            src=src,
            dst=dst,
            action=self.action,
            mode="wt",
            input="path",
            ifexists=ifexists,
            kind=kind,
            tag=tag,
            staging=staging,
            size=size,
            mtime=mtime,
            digests=digests,
        )

    def action(self, path: Path):
        """Write the output (the actor of the action)."""
        return write_tsv(self.tsv, path, **self.tsv_opt)


class WriteBytes(Action):
    """Write from a buffer."""

    def __init__(
        self,
        bytes: BinaryIO,
        dst: str,
        *,
        src: str = tuple(),
        ifexists: IfExistsChoice | None = None,
        kind: Kind | None = None,
        tag: str | None = None,
        staging: bool = False,
        size: int = None,
        mtime: datetime = None,
        digests: dict = None,
    ):
        """
        Build the action.

        Parameters
        ----------
        bytes : dict
            An opened file object, or bytes
        dst : str | Path
            Path to output TSV file

        Other Parameters
        ----------------
        src : str | Path | sequence[str | Path]
            Dependencies (only used to compute mtime)
        ifexists : {'error', 'skip', 'overwrite', 'different', 'refresh'}
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
        """
        self.bytes = bytes

        super().__init__(
            src=src,
            dst=dst,
            action=self.action,
            mode="wb",
            input="file",
            ifexists=ifexists,
            kind=kind,
            tag=tag,
            staging=staging,
            size=size,
            mtime=mtime,
            digests=digests,
        )

    def action(self, file: BinaryIO):
        """Write the output (the actor of the action)."""
        return write_from_buffer(self.bytes, file)


class CopyBytes(Action):
    """Copy from a buffer."""

    def __init__(
        self,
        src: str,
        dst: str,
        *,
        ifexists: IfExistsChoice | None = None,
        kind: Kind | None = None,
        tag: str | None = None,
        staging: bool = False,
        size: int = None,
        mtime: datetime = None,
        digests: dict = None,
    ):
        """
        Build the action.

        Parameters
        ----------
        src : str | Path
            Path to input JSON file
        dst : str | Path
            Path to output JSON file

        Other Parameters
        ----------------
        ifexists : {'error', 'skip', 'overwrite', 'different', 'refresh'}
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
        """
        super().__init__(
            src=src,
            dst=dst,
            action=self.action,
            mode="wb",
            input="file",
            ifexists=ifexists,
            kind=kind,
            tag=tag,
            staging=staging,
            size=size,
            mtime=mtime,
            digests=digests,
        )

    def action(self, file: BinaryIO):
        """Write the output (the actor of the action)."""
        _check_source(self)
        return copy_from_buffer(self.src, file)


class WriteText(Action):
    """Write a string of text."""

    def __init__(
        self,
        text: str,
        dst: str,
        *,
        src: str = tuple(),
        ifexists: IfExistsChoice | None = None,
        kind: Kind | None = None,
        tag: str | None = None,
        staging: bool = False,
        size: int = None,
        mtime: datetime = None,
        digests: dict = None,
    ):
        """
        Build the action.

        Parameters
        ----------
        text : str
            Some text
        dst : str | Path
            Path to output text file

        Other Parameters
        ----------------
        src : str | Path | sequence[str | Path]
            Dependencies (only used to compute mtime)
        ifexists : {'error', 'skip', 'overwrite', 'different', 'refresh'}
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
        """
        self.text = text

        super().__init__(
            src=src,
            dst=dst,
            action=self.action,
            mode="wt",
            input="file",
            ifexists=ifexists,
            kind=kind,
            tag=tag,
            staging=staging,
            size=size,
            mtime=mtime,
            digests=digests,
        )

    def action(self, file: TextIO):
        """Write the output (the actor of the action)."""
        return write_text(self.text, file)


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
        ifexists : {'error', 'skip', 'overwrite', 'different', 'refresh'}
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


class Freesurfer2Gifti(Action):
    """Convert a FreeSurfer surface file to GIfTI."""

    def __init__(
        self,
        src: str,
        dst: str,
        *,
        ifexists: IfExistsChoice | None = None,
        kind: Kind | None = None,
        tag: str | None = None,
        staging: bool = False,
        size: int = None,
        mtime: datetime = None,
        digests: dict = None,
    ):
        """
        Build the action.

        Parameters
        ----------
        src : str | Path
            Path to input freesurfer file
        dst : str | Path
            Path to output gifti file

        Other Parameters
        ----------------
        ifexists : {'error', 'skip', 'overwrite', 'different', 'refresh'}
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
        )

    def action(self, path: Path):
        """Write the output (the actor of the action)."""
        return nibabel_fs2gii(self.src, path)


class Unlink(Action):
    """Remove a file."""

    def __init__(
        self,
        dst: str,
        *,
        missing_ok=True,
    ):
        """
        Build the action.

        Parameters
        ----------
        dst : str | Path
            Path to file to remove/unlink
        missing_ok : bool
            Whether to raise an error is the file is missing
        """
        super().__init__(
            src=[],
            dst=dst,
            action=self.action,
            mode="w",
            input="path",
            ifexists="overwrite",
        )
        self.missing_ok = missing_ok

    def action(self, path: Path):
        """Write the output (the actor of the action)."""
        path.unlink(missing_ok=self.missing_ok)
