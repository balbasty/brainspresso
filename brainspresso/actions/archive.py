"""
Actions that extract members of archives (`utils.archive`).

```python
ExtractMember(archive, member, dst)       # one member of an open archive
ExtractMembers(path, claim, rename)       # members only known once opened
```
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Callable, Iterator

from brainspresso.actions.action import Action, Claim, Kind
from brainspresso.utils.archive import TarArchive
from brainspresso.utils.io import copy_from_buffer


class ExtractMember(Action):
    """
    Copy one member of an *open* archive (`TarArchive`, `FolderArchive`,
    `tarfile.TarFile`) to `dst`. The archive must stay open until the
    action has run (the engine, `actions.engine`, runs each action
    before asking its producer for the next one).
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
        with self.archive.extractfile(self.member) as f:
            copy_from_buffer(f, file)


class ExtractMembers(Action):
    """
    Extract the members of an archive whose outputs are only known once
    it is opened (a claim on one folder or a list of folders).
    `rename(member_name)` returns the output path of a member, or None to
    ignore it. Each member is extracted by an `ExtractMember` sub-action,
    which the engine accepts (and to which it applies `IfExists`) like
    any other action. `staging=True`: intermediate files (see `Action`).
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
        with self.opener(self.archive) as archive:
            for member in archive:
                if getattr(member, "isfile", lambda: True)() is False:
                    continue
                dst = self.rename(member.name)
                if dst is not None:
                    yield ExtractMember(
                        archive, member, dst, tag=self.tag,
                        staging=self.staging, **self.kwargs
                    )
