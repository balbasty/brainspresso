"""
Actions that extract members of archives (`utils.archive`).

```python
ExtractMember(archive, member, dst)       # one member of an open archive
ExtractMembers(path, claim, rename)       # members only known once opened
```
"""
from __future__ import annotations

import os
from logging import getLogger
from pathlib import Path
from typing import Callable, Iterator

from brainspresso.actions.action import Action, Claim, Kind
from brainspresso.utils.archive import TarArchive, is_unsafe
from brainspresso.utils.io import copy_from_buffer

lg = getLogger(__name__)


class ExtractMember(Action):
    """
    Copy one member of an *open* archive to `dst`.

    The archive (`TarArchive`, `FolderArchive`, `tarfile.TarFile`) must
    stay open until the action has run (the engine, `actions.engine`,
    runs each action before asking its producer for the next one).

    Parameters
    ----------
    archive : TarArchive | FolderArchive | tarfile.TarFile
        The open archive
    member : str | TarInfo
        The member
    dst : str | PathLike
        Output file
    kind, tag, **kwargs
        See `Action`
    """

    def __init__(
        self,
        archive,
        member,
        dst: str | os.PathLike,
        *,
        kind: Kind | None = None,
        tag: str | None = None,
        **kwargs,
    ):
        name = getattr(member, "name", member)
        sources = getattr(archive, "sources", None)
        src = sources(name) if sources else [archive.name]
        self.archive = archive
        self.member = member
        super().__init__(
            src, dst, self.extract, mode="wb", input="file",
            kind=kind, tag=tag, **kwargs,
        )

    def extract(self, file) -> None:
        """Copy the member into an opened output file."""
        with self.archive.extractfile(self.member) as f:
            copy_from_buffer(f, file)


class ExtractMembers(Action):
    """
    Extract the members of an archive (outputs known once it is opened).

    A claim on one folder or a list of folders. Each member is extracted
    by an `ExtractMember` sub-action, which the engine accepts (and to
    which it applies `IfExists`) like any other action. Members whose
    name is unsafe (a `..` part, or absolute: `utils.archive.is_unsafe`)
    are dropped before `rename` sees them, with one warning per archive;
    `rename` gets the other names as they are stored.

    Parameters
    ----------
    archive : str | PathLike
        Path of the archive
    claim : Claim | str | PathLike
        Claimed outputs (a folder, a list of folders, or a `Claim`)
    rename : callable(member_name) -> Path | None
        Output path of a member (None: ignore it); never called for an
        unsafe name
    opener : callable(path) -> archive
        Opens the archive (default: `TarArchive`)
    tag : str
        Progress group
    staging : bool
        Intermediate files (see `Action`)
    **kwargs
        Options of the `ExtractMember` sub-actions
    """

    def __init__(
        self,
        archive: str | os.PathLike,
        claim: Claim | str | os.PathLike,
        rename: Callable[[str], Path | None],
        *,
        opener: Callable = TarArchive,
        tag: str | None = None,
        staging: bool = False,
        **kwargs,
    ):
        if not isinstance(claim, Claim):
            claim = Claim(claim)
        self.archive = Path(archive)
        self.rename = rename
        self.opener = opener
        self.kwargs = kwargs
        super().__init__(
            self.archive, None, self.extract, claim=claim, tag=tag,
            staging=staging,
        )

    def extract(self) -> Iterator[Action]:
        """Open the archive and yield one `ExtractMember` per member."""
        unsafe = []
        try:
            with self.opener(self.archive) as archive:
                for member in archive:
                    if getattr(member, "isfile", lambda: True)() is False:
                        continue
                    if is_unsafe(member.name):
                        unsafe.append(member.name)
                        continue
                    dst = self.rename(member.name)
                    if dst is not None:
                        yield ExtractMember(
                            archive, member, dst, tag=self.tag,
                            staging=self.staging, **self.kwargs
                        )
        finally:
            if unsafe:
                lg.warning(f"{self.archive.name}: {len(unsafe)} member(s) "
                           f"outside the archive (`..` or an absolute "
                           f"path) skipped, e.g. {unsafe[0]}")
