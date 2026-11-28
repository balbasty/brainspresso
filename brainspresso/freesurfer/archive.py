"""
FreeSurfer subjects shipped in source archives.

OASIS-I, OASIS-III and ABIDE-I distribute FreeSurfer trees as tarballs:
each roaster unpacks the files that BIDS describes (every file with the
key `fs-all`) to `derivatives/<fs>/sourcedata/sub-<label>/`, then
converts them (`freesurfer.bidsify`); with `--sidecars only`, nothing is
unpacked, the sidecars are written from the names of the members. What
differs between datasets is where a subject's tree starts in a member
name, the *relative path* of a member in its tree:

* `FreeSurferTree.relpath` (ABIDE-I): under any number of wrapper
  folders, the tree starts before its deepest FreeSurfer folder;
* `fixed_root(depth)` (OASIS-I: 2, OASIS-III: 6): after a fixed number
  of folders.

```python
yield from tree_actions(ctx, archive, src, dst, [t1],
                        relpath=FreeSurferTree(archive.name, sub).relpath,
                        everything=ctx.wants("fs-all"))
```
"""
from __future__ import annotations

import re
from functools import partial
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Callable, Iterator

from brainspresso.actions.action import Action, Claim
from brainspresso.actions.archive import ExtractMembers
from brainspresso.freesurfer import bidsify as fs
from brainspresso.roast import archives
from brainspresso.utils.archive import TarArchive

lg = getLogger(__name__)

__all__ = ["FS_DIRS", "FS_TEMPLATES", "FreeSurferTree", "fixed_root",
           "tree_actions", "tree_exists", "tree_rename"]

#: Path of a member in its FreeSurfer tree (None: not in it)
RelPath = Callable[[str], PurePosixPath | None]

#: Top-level folders of a FreeSurfer subject (they mark its root)
FS_DIRS = frozenset({"mri", "surf", "label", "stats", "scripts", "touch"})

#: FreeSurfer template subjects (never the subject's tree)
FS_TEMPLATES = re.compile(r"fsaverage\d*|[lr]h\.EC_average|bert",
                          re.IGNORECASE)


def _unsafe(path: PurePosixPath) -> bool:
    """Whether a member path is absolute or has `..`."""
    return path.is_absolute() or ".." in path.parts


class FreeSurferTree:
    """
    Locate the FreeSurfer subject in the members of an archive.

    Members may sit under any number of wrapper folders
    (`<exp>/assessors/<label>/<resource>/files/<subject>/mri/...`). The
    root of a member is the folder just before its deepest FreeSurfer
    folder (`FS_DIRS`), so a wrapper named like one does not re-root the
    tree. Members are read in archive order (one pass): the first root
    is the subject's (a warning if its name lacks the subject's id), and
    every later member under it belongs to the tree (also outside the
    FreeSurfer folders: `tmp/`, `bem/`, files at the root; `fs-all`
    unpacks them); members of another root (a second tree, e.g.
    `fsaverage/`) are skipped with a warning, as are template subjects
    (`FS_TEMPLATES`), absolute paths and paths with `..`. Members under
    the root that come before its first FreeSurfer folder cannot be
    placed yet and are skipped (one pass).

    Parameters
    ----------
    archive : str
        Name of the archive (for the warnings)
    sub : str
        Subject label (`51456`): the root should contain its id
    """

    def __init__(self, archive: str = "", sub: str = ""):
        self.archive = archive
        self.sub = sub
        self.root: tuple | None = None
        self.others: set = set()

    @staticmethod
    def split(member: str) -> tuple[tuple, PurePosixPath] | None:
        """
        Split a member into its root and its path in the tree.

        Parameters
        ----------
        member : str
            Name of the member

        Returns
        -------
        (root, relpath) | None
            None if the member is in no FreeSurfer folder, or its path is
            absolute or has `..`
        """
        path = PurePosixPath(member)
        parts = path.parts
        if _unsafe(path):
            return None
        anchors = [i for i, part in enumerate(parts[:-1]) if part in FS_DIRS]
        if not anchors:
            return None
        i = anchors[-1]
        return parts[:i], PurePosixPath(*parts[i:])

    def relpath(self, member: str) -> PurePosixPath | None:
        """
        Path of a member in the subject's tree.

        Parameters
        ----------
        member : str
            Name of the member

        Returns
        -------
        PurePosixPath | None
            None for members outside it (warned once per other tree)
        """
        path = PurePosixPath(member)
        if _unsafe(path):
            return None
        if self.root is not None and len(path.parts) > len(self.root) \
                and path.parts[:len(self.root)] == self.root:
            return PurePosixPath(*path.parts[len(self.root):])
        split = self.split(member)
        if split is None:
            return None
        root, rel = split
        template = bool(root) and FS_TEMPLATES.fullmatch(root[-1])
        if self.root is None and not template:
            self.root = root
            self.check_root()
            return rel
        if root not in self.others:
            self.others.add(root)
            kept = (f"the subject's is {'/'.join(self.root) or '.'}/"
                    if self.root is not None else "template subject")
            lg.warning(f"{self.archive}: FreeSurfer tree "
                       f"{'/'.join(root) or '.'}/ skipped ({kept})")
        return None

    def check_root(self) -> None:
        """Warn if the chosen root is not named after the subject."""
        number = self.sub.lstrip("0") if self.sub.isdigit() else self.sub
        if self.root and number and number not in self.root[-1]:
            lg.warning(f"{self.archive}: FreeSurfer tree "
                       f"{'/'.join(self.root)}/ does not name subject "
                       f"{self.sub}: converted as sub-{self.sub}, check "
                       f"that it is not another subject's tree")


def fixed_root(depth: int) -> RelPath:
    """
    Return the relative path of trees that start at a fixed depth.

    ```python
    fixed_root(2)("disc1/OAS1_0001_MR1/mri/aseg.mgz")   # mri/aseg.mgz
    ```

    Parameters
    ----------
    depth : int
        Number of folders before the tree (OASIS-I: `<disc>/<subject>/`,
        2; OASIS-III: 6)

    Returns
    -------
    callable(member) -> PurePosixPath | None
        The path after these folders; None for a member with no part
        left, an absolute path or a path with `..` (picklable: it can be
        an argument of a shard)
    """
    return partial(_after, depth)


def _after(depth: int, member: str) -> PurePosixPath | None:
    """Return the path of a member after `depth` folders (`fixed_root`)."""
    path = PurePosixPath(member)
    if _unsafe(path) or len(path.parts) <= depth:
        return None
    return PurePosixPath(*path.parts[depth:])


def tree_rename(relpath: RelPath, src: Path,
                everything: bool) -> Callable[[str], Path | None]:
    """
    Return the `rename` of `ExtractMembers` for a FreeSurfer tree.

    Parameters
    ----------
    relpath : callable(member) -> PurePosixPath | None
        Path of a member in the tree (`FreeSurferTree.relpath`,
        `fixed_root`)
    src : Path
        Where the tree is unpacked (`derivatives/<fs>/sourcedata/...`)
    everything : bool
        Every file of the tree (`fs-all`), else only those that BIDS
        describes (`freesurfer.bidsify.bidsifiable_outputs`)

    Returns
    -------
    callable(member) -> Path | None
        Its output path (None: not unpacked)
    """
    def rename(member: str) -> Path | None:
        rel = relpath(member)
        if rel is None:
            return None
        if not everything and not rel.as_posix().endswith(
                fs.bidsifiable_outputs):
            return None
        return src.joinpath(*rel.parts)

    return rename


def tree_exists(archive: Path, src: Path, relpath: RelPath, *,
                opener: Callable | None = None,
                label: str | None = None) -> Callable[[Path], bool]:
    """
    List a FreeSurfer tree as if it were unpacked.

    For `freesurfer.bidsify(exists=)` with `--sidecars only`: the names
    that `ExtractMembers` would give to `rename` (`archives.names`,
    `extractable`), placed under `src`.

    Parameters
    ----------
    archive : Path
        The archive
    src : Path
        Where the tree would be unpacked
    relpath : callable(member) -> PurePosixPath | None
        Path of a member in the tree
    opener : callable(path) -> archive
        Opens it (default: `TarArchive`)
    label : str | None
        How messages name it (default: its file name)

    Returns
    -------
    callable(Path) -> bool
        Whether a file of the tree exists

    Raises
    ------
    roast.archives.ArchiveError
        If the archive cannot be opened or listed
    """
    names = archives.names(archive, opener=opener or TarArchive, label=label,
                           extractable=True)
    members = {src.joinpath(*rel.parts)
               for rel in map(relpath, names) if rel is not None}
    return members.__contains__


def tree_actions(
    ctx,
    archive: Path,
    src: Path,
    dst: Path,
    sources: list[str],
    *,
    relpath: RelPath,
    everything: bool,
    list_only: bool | None = None,
    tag: str = "fs",
    opener: Callable | None = None,
    label: str | None = None,
) -> Iterator[Action]:
    """
    Unpack a FreeSurfer subject from an archive, then convert it.

    The unpacked files are inputs of the conversion (`staging`: the JSON
    policy does not apply to them); they are converted once unpacked
    (the engine runs each action before it asks for the next one). With
    `list_only`, nothing is unpacked: the sidecars are written from the
    names of the members (`tree_exists`), and an archive that cannot be
    listed is a `Fail`.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast (`json`, `layout.root`)
    archive : Path
        The archive
    src : Path
        Where the tree is unpacked (`derivatives/<fs>/sourcedata/...`)
    dst : Path
        The BIDS derivative subject (or session)
    sources : list[str]
        The raw images the tree was made from (`Sources`)
    relpath : callable(member) -> PurePosixPath | None
        Path of a member in the tree (`FreeSurferTree.relpath`,
        `fixed_root`)
    everything : bool
        Every file of the tree (`fs-all`)
    list_only : bool | None
        List instead of unpacking (default: `--sidecars only`; OASIS
        unpacks the tree with `fs-all` all the same)
    tag : str
        Progress group
    opener : callable(path) -> archive
        Opens the archive (default: `TarArchive`; that of
        `ExtractMembers`, looked up when it is made)
    label : str | None
        How messages name it (default: its file name)

    Yields
    ------
    Action
        The unpacking, then the converted files; or the sidecars only
    """
    if list_only is None:
        list_only = ctx.json == "only"
    if list_only:
        try:
            exists = tree_exists(archive, src, relpath, opener=opener,
                                 label=label)
        except archives.ArchiveError as e:
            yield e.fail(tag=tag)
            return
        yield from fs.bidsify(src, dst, sources, json=ctx.json,
                              exists=exists)
        return
    yield ExtractMembers(archive, Claim(src),
                         tree_rename(relpath, src, everything), tag=tag,
                         root=ctx.layout.root, staging=True,
                         **({"opener": opener} if opener else {}))
    yield from fs.bidsify(src, dst, sources, json=ctx.json)
