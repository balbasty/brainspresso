"""
Actions that convert FreeSurfer files.

`Freesurfer2Gifti`: surfaces, curvatures and annotations to GIfTI
(`freesurfer.io.nibabel_fs2gii`).
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from brainspresso.actions.action import Action, IfExistsChoice, Kind
from brainspresso.freesurfer.io import nibabel_fs2gii

__all__ = ["Freesurfer2Gifti"]


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
        compare: bool | None = None,
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

    def action(self, path: Path):
        """Write the output (the actor of the action)."""
        return nibabel_fs2gii(self.src, path)
