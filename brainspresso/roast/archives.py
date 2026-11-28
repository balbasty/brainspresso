"""
Reading source archives in a roaster.

```python
seen = set()
try:
    with archives.reading(path, opener=TarArchive) as tar:
        for member in tar:                  # lazily, in archive order
            parsed = parse_member(member.parts)
            if parsed is None:
                continue
            kept = ctx.keeps_subject(parsed.sub)    # before any pruning
            if kept and ctx.wants(key):
                yield ExtractMember(tar.archive, member.info, dst, tag=key)
except archives.ArchiveError as e:
    yield e.fail(tag="raw")
```

A helper library, not a framework: the roaster keeps its loop, its
`ctx.keeps_subject` calls and its `Fail`s. The opener is the roaster's
own (its module-level `TarArchive`, read when it calls `reading`), so
that tests can patch it there.

Errors. Only what the archive does is converted to `ArchiveError`:
opening it, moving to its next member (`Reader.__iter__`), listing it
(`Reader.listing`), opening or reading a member (`Member.open`). Code
that the roaster runs between these (templates, `ctx.resource`, its
output folder) raises what it raises: an `OSError` there is not "the
archive cannot be read". `ArchiveError.fail` is the one `Fail` of an
unreadable archive: `"<name>: cannot be read (<Type>: <message>)"`.

Names. `Member.parts` and `Member.path` are normalised (`member_parts`:
no `.`, empty parts or leading `/`); members with a `..` part are
skipped, with one warning per archive.

Forward only. A gz tarball is decompressed from the start again by a
backward seek: a roaster extracts each member as it is reached (the
engine runs an action before asking for the next one), or lists the
archive once (`Reader.listing`) and sorts its actions by the offset of
their first member (`in_order`). `strict=True` (or the environment
variable `BRAINSPRESSO_STRICT_ARCHIVES=1`) raises `BackwardRead` when a
member is opened before the last one opened.

Spools. `spool_folder`, `spool_prefix`, `sweep_spools`, `spool` and
`place`: the images that a roaster can only name once later members
are read (ABIDE-II) are copied to a spool folder of the dataset
(`.brainspresso/tmp/<name>-<host>-<pid>-*`) as they are reached, then
moved into place.
"""
from __future__ import annotations

import io
import os
import re
import shutil
import socket
import tarfile
import zlib
from logging import getLogger
from pathlib import Path
from typing import IO, Callable, Iterable, Iterator

from brainspresso.actions.action import Action, Fail
from brainspresso.actions.runlock import pid_alive
from brainspresso.roast.context import RoastContext
from brainspresso.utils.archive import member_parts
from brainspresso.utils.path import sorted_paths

lg = getLogger(__name__)

__all__ = [
    "ARCHIVE_ERRORS",
    "STRICT_ENV",
    "ArchiveError",
    "BackwardRead",
    "Member",
    "Reader",
    "in_order",
    "member_parts",
    "names",
    "place",
    "reading",
    "spool",
    "spool_folder",
    "spool_host",
    "spool_prefix",
    "sweep_spools",
]

#: What reading a corrupt or truncated (gz) tarball raises
ARCHIVE_ERRORS = (tarfile.TarError, EOFError, zlib.error, OSError)

#: Environment variable that makes every `reading` strict (tests)
STRICT_ENV = "BRAINSPRESSO_STRICT_ARCHIVES"


class ArchiveError(Exception):
    """
    An archive that cannot be read (corrupt, truncated, missing).

    Parameters
    ----------
    path : path
        The archive
    cause : Exception
        What the archive raised
    label : str
        How messages name it (default: its file name)
    """

    def __init__(self, path, cause: BaseException, label: str | None = None):
        self.path = Path(path)
        self.cause = cause
        self.label = label or self.path.name
        super().__init__(self.path, cause, self.label)

    def __str__(self) -> str:
        """`<label>: cannot be read (<Type>: <message>)`."""
        return (f"{self.label}: cannot be read ({type(self.cause).__name__}:"
                f" {self.cause})")

    def fail(self, *, tag: str, name: str | None = None,
             note: str = "") -> Fail:
        """
        Report the archive: the one `Fail` of an unreadable archive.

        Parameters
        ----------
        tag : str
            Progress group
        name : str
            Name shown in the display (default: the archive's file name)
        note : str
            Appended to the message (`"; sub-X ses-Y not written"`)

        Returns
        -------
        Fail
        """
        return Fail(f"{self}{note}", name=name or self.path.name, tag=tag)


class BackwardRead(RuntimeError):
    """A member opened before the last one opened (`strict` reading)."""


def _strict(strict: bool) -> bool:
    """Whether reading is strict (the argument, or `STRICT_ENV`)."""
    value = os.environ.get(STRICT_ENV, "").strip().lower()
    return strict or value in ("1", "true", "yes", "on")


class _MemberFile(io.BufferedIOBase):
    """A member opened for reading: read errors are `ArchiveError`s."""

    def __init__(self, file: IO[bytes], reader: "Reader"):
        super().__init__()
        self.file = file
        self.reader = reader

    def _call(self, method: str, *args):
        try:
            return getattr(self.file, method)(*args)
        except ARCHIVE_ERRORS as e:
            raise self.reader.error(e) from e

    def read(self, n: int | None = -1) -> bytes:
        """Read up to `n` bytes (all: -1)."""
        return self._call("read", -1 if n is None else n)

    def read1(self, n: int = -1) -> bytes:
        """Read up to `n` bytes, with at most one read of the archive."""
        if hasattr(self.file, "read1"):
            return self._call("read1", n)
        return self._call("read", n)

    def readinto(self, buffer) -> int:
        """Read into a buffer."""
        return self._call("readinto", buffer)

    def readable(self) -> bool:
        """Return True."""
        return True

    def close(self) -> None:
        """Close the member."""
        if not self.closed:
            self.file.close()
        super().close()


class Member:
    """
    A regular file of an archive, as `Reader` yields it.

    Parameters
    ----------
    reader : Reader
        The archive being read
    info : TarInfo | FolderArchive.Member
        The member, as the archive gives it
    parts : tuple[str]
        Its normalised parts (`member_parts`)

    Attributes
    ----------
    name : str
        Its name, as stored
    parts : tuple[str]
        Its normalised parts
    path : str
        `"/".join(parts)`
    info : TarInfo | FolderArchive.Member
        The member (`ExtractMember(reader.archive, member.info, dst)`)
    offset : int
        Its position in the archive (0 for a folder)
    """

    def __init__(self, reader: "Reader", info, parts: tuple[str, ...]):
        self.reader = reader
        self.info = info
        self.name = info.name
        self.parts = parts
        self.path = "/".join(parts)
        self.offset = getattr(info, "offset", 0)

    def __repr__(self) -> str:
        """`Member('<name>')`."""
        return f"Member({self.name!r})"

    def open(self) -> _MemberFile:
        """
        Open the member.

        Returns
        -------
        file
            Its content; errors reading it are `ArchiveError`s

        Raises
        ------
        ArchiveError
            If it cannot be opened
        """
        try:
            file = self.reader.archive.extractfile(self.info)
        except ARCHIVE_ERRORS as e:
            raise self.reader.error(e) from e
        return _MemberFile(file, self.reader)

    def read(self, n: int = -1) -> bytes:
        """
        Read the member.

        Parameters
        ----------
        n : int
            Number of bytes (-1: all)

        Returns
        -------
        bytes
        """
        with self.open() as f:
            return f.read(n)


class Reader:
    """
    An open archive, read through `reading`.

    Parameters
    ----------
    archive : TarArchive | FolderArchive
        The open archive
    path : Path
        Its path
    label : str
        How messages name it
    strict : bool
        Raise `BackwardRead` on a member opened before the last one

    Attributes
    ----------
    archive : TarArchive | FolderArchive
        The open archive (`ExtractMember(reader.archive, ...)`)
    outside : list[str]
        Members skipped because they have a `..` part
    """

    def __init__(self, archive, path: Path, label: str, strict: bool):
        self.archive = archive
        self.path = path
        self.label = label
        self.outside: list[str] = []
        self.last = -1
        if strict:
            self._check_order()

    def error(self, cause: BaseException) -> ArchiveError:
        """Return the `ArchiveError` of this archive, caused by `cause`."""
        return ArchiveError(self.path, cause, self.label)

    def _check_order(self) -> None:
        """Wrap `archive.extractfile`: raise on a backward read."""
        archive = self.archive
        extractfile = archive.extractfile

        def checked(member):
            info = archive.getmember(member) if isinstance(member, str) \
                else member
            offset = getattr(info, "offset", None)
            if offset is not None:
                if offset < self.last:
                    raise BackwardRead(
                        f"{self.label}: {info.name} (offset {offset}) "
                        f"opened after a member at offset {self.last}")
                self.last = offset
            return extractfile(member)

        archive.extractfile = checked

    def _member(self, info) -> Member | None:
        """Return a `Member` (None: not a regular file, or outside)."""
        if getattr(info, "isfile", lambda: True)() is False:
            return None
        parts = member_parts(info.name)
        if parts is None:
            self.outside.append(info.name)
            return None
        return Member(self, info, parts)

    def __iter__(self) -> Iterator[Member]:
        """
        Iterate over the regular files, lazily, in archive order.

        The archive is never listed first (`getmembers`): extracting
        each member as it is reached reads a gz tarball once.

        Yields
        ------
        Member

        Raises
        ------
        ArchiveError
            If the next member cannot be read
        """
        members = iter(self.archive)
        while True:
            try:
                info = next(members)
            except StopIteration:
                return
            except ARCHIVE_ERRORS as e:
                raise self.error(e) from e
            member = self._member(info)
            if member is not None:
                yield member

    def listing(self) -> list[Member]:
        """
        List the regular files (one full pass of a gz tarball).

        Returns
        -------
        list[Member]
            In archive order

        Raises
        ------
        ArchiveError
            If the archive cannot be listed
        """
        try:
            infos = self.archive.getmembers()
        except ARCHIVE_ERRORS as e:
            raise self.error(e) from e
        return [m for m in map(self._member, infos) if m is not None]

    def index(self) -> dict[str, Member]:
        """
        List the regular files by normalised name (`Member.path`).

        Returns
        -------
        dict[str, Member]
            In archive order; of two members of the same name, the last
            (as `tarfile`)

        Raises
        ------
        ArchiveError
            If the archive cannot be listed
        """
        out = {}
        for member in self.listing():
            out[member.path] = member
        return out


class reading:  # (lower case: used as a function, `with reading(...)`)
    """
    Open an archive for reading; close it on every path.

    It is closed (through the archive's `__exit__`) when the `with`
    block ends: at the end of the members, on an error, or when the
    generator of the roaster is closed.

    Parameters
    ----------
    path : path
        The archive
    opener : callable(path) -> archive
        Opens it: the roaster's own `TarArchive` (or `FolderArchive`)
    label : str
        How messages name it (default: its file name; ABIDE-I XNAT:
        `<folder>/<name>`)
    strict : bool
        Raise `BackwardRead` on a member opened before the last one
        (also set by `STRICT_ENV`)

    Returns
    -------
    ContextManager[Reader]

    Raises
    ------
    ArchiveError
        If it cannot be opened (on entering the `with` block)
    """

    def __init__(self, path, *, opener: Callable, label: str | None = None,
                 strict: bool = False):
        self.path = Path(path)
        self.opener = opener
        self.label = label or self.path.name
        self.strict = _strict(strict)
        self.reader: Reader | None = None
        self.archive = None

    def __enter__(self) -> Reader:
        """Open the archive (`ArchiveError` if it cannot be opened)."""
        try:
            self.archive = self.opener(self.path)
            archive = self.archive.__enter__()
        except ARCHIVE_ERRORS as e:
            raise ArchiveError(self.path, e, self.label) from e
        self.reader = Reader(archive, self.path, self.label, self.strict)
        return self.reader

    def __exit__(self, *args) -> None:
        """Close the archive; warn about the members outside it."""
        try:
            self.archive.__exit__(*args)
        finally:
            outside = self.reader.outside
            if outside:
                lg.warning(f"{self.label}: {len(outside)} member(s) "
                           f"outside the archive (`..`) skipped, e.g. "
                           f"{outside[0]}")


def names(path, *, opener: Callable, label: str | None = None) -> list[str]:
    """
    List the normalised names of the regular files of an archive.

    Parameters
    ----------
    path : path
        The archive
    opener : callable(path) -> archive
        Opens it (see `reading`)
    label : str
        How messages name it

    Returns
    -------
    list[str]
        `Member.path` of its regular files, in archive order; the
        archive is closed

    Raises
    ------
    ArchiveError
        If it cannot be opened or listed
    """
    with reading(path, opener=opener, label=label) as reader:
        return [member.path for member in reader.listing()]


def in_order(items: Iterable[tuple[Iterable[Member], Action]]
             ) -> Iterator[Action]:
    """
    Sort actions by the offset of the first member that each one reads.

    Parameters
    ----------
    items : iterable of (members, action)
        The members that an action reads (none: first), and the action

    Yields
    ------
    Action
        In archive order (a stable sort: actions that read nothing keep
        their order, first)
    """
    def position(item) -> int:
        return min((m.offset for m in item[0]), default=-1)

    for _, action in sorted(items, key=position):
        yield action


# ----------------------------------------------------------------------
#   Spools
# ----------------------------------------------------------------------

def spool_folder(ctx: RoastContext) -> Path | None:
    """Folder of the spooled files (`.brainspresso/tmp/`, else None)."""
    folder = Path(ctx.layout.state) / "tmp"
    try:
        folder.mkdir(parents=True, exist_ok=True)
    except OSError:
        return None                     # (the system's temporary folder)
    return folder


def spool_host() -> str:
    """Return the name of this host, as a part of a folder name."""
    return re.sub(r"[^A-Za-z0-9.]", "_", socket.gethostname()) or "host"


def spool_prefix(name: str) -> str:
    """
    Prefix of the spool folder of a shard.

    Parameters
    ----------
    name : str
        Name of the roaster's spools (`abide2`)

    Returns
    -------
    str
        `<name>-<host>-<pid>-`
    """
    return f"{name}-{spool_host()}-{os.getpid()}-"


def sweep_spools(ctx: RoastContext, name: str) -> list[Path]:
    """
    Remove the spool folders of dead roasts (left by a hard kill).

    A spool folder is named after the host and process of its shard
    (`<name>-<host>-<pid>-<random>`): those of this host whose process
    is gone are removed. Roasts hold the run lock of the dataset (shared:
    other roasts may run, whose processes are alive), so compost does not
    run meanwhile. Folders of other hosts are left (their processes
    cannot be checked).

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    name : str
        Name of the roaster's spools (`abide2`)

    Returns
    -------
    list[Path]
        The folders removed
    """
    folder = Path(ctx.layout.state) / "tmp"
    if not folder.is_dir():
        return []
    host = spool_host()
    removed = []
    for spool_ in sorted_paths(folder.glob(f"{name}-*")):
        match = re.fullmatch(rf"{re.escape(name)}-(?P<host>.+)-"
                             rf"(?P<pid>\d+)-[^-]+", spool_.name)
        if not (spool_.is_dir() and match and match["host"] == host):
            continue
        if int(match["pid"]) == os.getpid() or pid_alive(int(match["pid"])):
            continue
        shutil.rmtree(spool_, ignore_errors=True)
        removed.append(spool_)
    if removed:
        lg.info(f"Removed {len(removed)} spool folder(s) of interrupted "
                f"roasts in {folder}")
    return removed


def spool(src, path: Path) -> str | None:
    """
    Copy an archive member to a spool file.

    Errors reading the member (a corrupt archive) are raised; errors
    writing the spool file are returned (a `Fail` of that output).

    Parameters
    ----------
    src : file
        The member, opened
    path : Path
        Spool file

    Returns
    -------
    str | None
        The error writing the spool file, if any
    """
    try:
        dst = open(path, "wb")
    except OSError as e:
        error = e
    else:
        error = None
        with dst:
            while chunk := src.read(1 << 20):
                if error is not None:
                    continue            # (read on: forward only)
                try:
                    dst.write(chunk)
                except OSError as e:
                    error = e
    return None if error is None else f"{type(error).__name__}: {error}"


def place(spool: Path | None, path: str) -> None:
    """
    Move a spooled file into place (copy it across filesystems).

    Parameters
    ----------
    spool : Path | None
        The spooled file (None: it was not spooled, an error)
    path : str
        Its destination

    Raises
    ------
    FileNotFoundError
        If it was not spooled
    """
    if spool is None:
        raise FileNotFoundError(
            "the image was not spooled (an earlier roast wrote it under "
            "the other name: with or without `run-<n>`): roast again with "
            "--if-exists overwrite")
    try:
        os.replace(spool, path)
    except OSError:
        shutil.copyfile(spool, path)
