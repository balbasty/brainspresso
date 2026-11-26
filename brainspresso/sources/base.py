"""
Sources: where the files of a dataset come from (design doc, section 4).

A **source** is a generic backend (static URLs, an S3 prefix, Girder,
XNAT, OpenNeuro, Dataverse, files the user put in `sourcedata/`), that a
dataset instantiates, or subclasses, for its own host. It declares

* `provides`: the dataset keys it can harvest (`{"raw", "meta"}`);
* `roaster`: the roaster that understands what it writes into
  `sourcedata/` (optional: `None` for hosted-BIDS sources, or datasets
  whose roast is not ported yet);
* `list(selection, keys) -> Iterable[RemoteEntry]`: the files to fetch,
  pruned by the selection (subjects, through the dataset's label
  mapping) and by the wanted keys;
* `fetch(entries, layout, options)`: download them. The default is the
  shared `mirror` (one `Downloader` per entry, run by a
  `DownloadManager`) into `layout.source` (or a sub-folder of it);
  sources with their own transport override it;
* `auth()`: an optional downloader authentication hook (credentials are
  resolved when the harvest starts, never when the source is declared).

A dataset declares its sources and, per key, an ordered list of default
sources (`{"raw": ["s3", "nitrc"], "meta": ["nitrc"]}`); `plan_sources`
picks the sources of a harvest (`--source` overrides the defaults) and
`harvest_sources` runs them, falling back to the next source of a key
when one is unavailable (`SourceUnavailable`).

```python
sources = {"s3": FCP("ABIDE"), "nitrc": NITRCTarballs(...)}
defaults = {"raw": ["s3", "nitrc"], "meta": ["s3", "nitrc"]}
harvest_sources(sources, defaults, layout, selection=Selection(subs=...),
                keys=Wants({"raw"}, tree), options=DownloadOptions())
```
"""
from __future__ import annotations

import copy
import datetime
import inspect
import os
import secrets
from dataclasses import dataclass, field, fields
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    Mapping,
    Protocol,
    Sequence,
    runtime_checkable,
)
from urllib.parse import urlparse

from humanize import naturalsize

from brainspresso.actions.locks import FileLock, LockError
from brainspresso.bids.select import Selection
from brainspresso.download import CHUNK_SIZE, Downloader, DownloadManager
from brainspresso.errors import UsageError
from brainspresso.utils.keys import compat_keys, lower_keys

lg = getLogger(__name__)

__all__ = [
    "RemoteEntry", "DownloadOptions", "Wants", "LabelMap", "Source",
    "SourceUnavailable", "StaticURLs", "CompositeSource", "LocalSource",
    "mirror", "write_content",
    "run_downloads", "plan_sources", "harvest_sources", "configure_sources",
    "select_roaster",
    "numeric_labels", "prefix_labels", "subject_labels", "subject_selection",
]


# ----------------------------------------------------------------------
#   Entries
# ----------------------------------------------------------------------

@dataclass
class RemoteEntry:
    """
    A file of a remote host.

    Parameters
    ----------
    path : str
        POSIX path relative to the root of the listing (bucket prefix,
        Girder folder, dataset root...). Sources map it to a local path.
    url : str, optional
        Download URL (None if the source has its own transport).
    size : int, optional
        Size in bytes, if the listing gives it.
    mtime : datetime, optional
        Last-modified time, if the listing gives it.
    digests : dict[str, str]
        Known checksums `{algorithm: hex digest}` (e.g. `{"md5": ...}`).
    meta : dict
        Host-specific fields (Girder item metadata, XNAT scan, S3 key...).
    """

    path: str
    url: str | None = None
    size: int | None = None
    mtime: datetime.datetime | None = None
    digests: dict[str, str] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict, compare=False)

    def __post_init__(self):
        """Normalise `path`, `digests` and `meta`."""
        if isinstance(self.path, PurePosixPath):
            self.path = self.path.as_posix()
        self.digests = dict(self.digests or {})
        self.meta = dict(self.meta or {})

    @property
    def relpath(self) -> PurePosixPath:
        """`path` as a `PurePosixPath`."""
        return PurePosixPath(self.path)

    @property
    def name(self) -> str:
        """File name of the entry."""
        return self.relpath.name


# ----------------------------------------------------------------------
#   Options
# ----------------------------------------------------------------------

@dataclass
class DownloadOptions:
    """
    Options of a harvest, shared by all sources.

    They are the `DownloadOptions` group of the command line.

    Parameters
    ----------
    ifexists : {"skip", "overwrite", "different", "refresh", "error"}
        Behaviour if a local file already exists
    jobs : int, optional
        Number of concurrent downloads (default: that of the source)
    chunk_size : int
        Download chunk size, in bytes
    insecure : bool, optional
        Do not verify TLS certificates (default: that of the source;
        some hosts, e.g. NITRC, have broken certificate chains)
    dry_run : bool
        Print what would be downloaded instead of downloading it
    path : {"short", "name", "full", "abs"}
        Paths shown in the progress table
    on_error : {"yield", "raise"}
        A failed download is reported in the table ("yield") or raises
    """

    ifexists: str = "skip"
    jobs: int | None = None
    chunk_size: int = CHUNK_SIZE
    insecure: bool | None = None
    dry_run: bool = False
    path: str = "short"
    on_error: str = "yield"

    @classmethod
    def from_any(cls, options: "DownloadOptions | Mapping | None"):
        """Build options from options, a mapping or None.

        Parameters
        ----------
        options : DownloadOptions | Mapping | None
            Options (returned as is), keyword arguments, or None
            (defaults).

        Returns
        -------
        DownloadOptions
        """
        if options is None:
            return cls()
        if isinstance(options, cls):
            return options
        names = {f.name for f in fields(cls)}
        unknown = set(options) - names
        if unknown:
            raise TypeError(f"Unknown download options: {sorted(unknown)}")
        return cls(**dict(options))

    def replace(self, **kwargs) -> "DownloadOptions":
        """Return a copy with some options changed.

        Parameters
        ----------
        **kwargs
            Options to change (they must exist).

        Returns
        -------
        DownloadOptions
        """
        out = copy.copy(self)
        for key, value in kwargs.items():
            if not hasattr(out, key):
                raise TypeError(f"Unknown download option: {key}")
            setattr(out, key, value)
        return out


# ----------------------------------------------------------------------
#   Keys
# ----------------------------------------------------------------------

def _related_in_tree(tree) -> Callable[[str, str], bool]:
    """Build `related(a, b)` for a dataset key tree.

    `related(a, b)` tells whether two keys are equal, or one is below the
    other, in a dataset key tree (nested dict/set, or `KeyAliases`).
    """
    if tree is None:
        return lambda a, b: a == b or "all" in (a, b)
    expand = getattr(tree, "expand", None)
    if expand is not None:          # bids.keys.KeyAliases
        def related(a, b):
            if a == b:
                return True
            try:
                return bool(expand(a) & expand(b))
            except KeyError:
                return False
        return related
    cache: dict[str, set[str]] = {}

    def related(a, b):
        if a not in cache:
            cache[a] = compat_keys(a, tree) | {a}
        return b in cache[a]
    return related


class Wants:
    """
    The keys a harvest wants: `wants(key) -> bool`.

    A key is wanted if it is related (equal, above or below, in the key
    tree of the dataset) to one of the selected keys, and, for the part
    of a harvest given to one source, to one of the keys of its group.

    Parameters
    ----------
    keys : iterable[str] | None
        Selected keys (`--keys`). None or empty: all keys.
    tree : dict | KeyAliases, optional
        Key hierarchy of the dataset (`utils.keys` nested dict, or a
        `bids.keys.KeyAliases`)
    exclude : iterable[str]
        Keys not wanted (`--exclude-keys`): a key below one of them is not
        wanted.
    """

    def __init__(self, keys: Iterable[str] | str | None = None, tree=None,
                 exclude: Iterable[str] | str = (), within=None):
        if isinstance(keys, str):
            keys = [keys]
        if isinstance(exclude, str):
            exclude = [exclude]
        self.keys = {str(getattr(k, "value", k)) for k in keys or ()}
        self.exclude = {str(getattr(k, "value", k)) for k in exclude}
        self.tree = tree
        self.within = None if within is None else set(within)
        self._related = _related_in_tree(tree)

    def related(self, a: str, b: str) -> bool:
        """Whether two keys are equal, or one is below the other."""
        return self._related(a, b)

    def __call__(self, key: str) -> bool:
        """Whether `key` is wanted."""
        if self.keys and not any(self._related(key, k) for k in self.keys):
            return False
        if self.within is not None and not any(
            self._related(key, k) for k in self.within
        ):
            return False
        # an excluded key and every key below it
        if any(self._below(key, k) for k in self.exclude):
            return False
        return True

    def _below(self, key: str, upper: str) -> bool:
        if key == upper:
            return True
        if self.tree is None:
            return False
        if hasattr(self.tree, "expand"):
            # KeyAliases: a group excludes its leaves
            try:
                return key in self.tree.expand(upper)
            except (KeyError, ValueError):     # unknown key
                return False
        return key in lower_keys(upper, self.tree)

    def restrict(self, group: Iterable[str]) -> "Wants":
        """Wanted keys related to one of the keys of `group`."""
        out = copy.copy(self)
        group = set(group)
        out.within = group if self.within is None else {
            g for g in group if any(self._related(g, k) for k in self.within)
        }
        return out

    def __repr__(self):
        """Represent the wanted keys."""
        return (f"Wants({sorted(self.keys) or 'all'}"
                + (f", within={sorted(self.within)}" if self.within
                   is not None else "")
                + (f", exclude={sorted(self.exclude)}" if self.exclude
                   else "") + ")")


def as_wants(keys) -> Callable[[str], bool]:
    """None (everything), a collection of key names, or a predicate."""
    if keys is None:
        return Wants()
    if callable(keys):
        return keys
    wants = getattr(keys, "wants", None)
    if callable(wants):             # KeySelection
        return wants
    return Wants(keys)


# ----------------------------------------------------------------------
#   Labels
# ----------------------------------------------------------------------

def _identity(x):
    return x


@dataclass(frozen=True)
class LabelMap:
    """
    Mapping between the subject identifiers of a host and BIDS labels.

    It is supplied by the dataset (`OAS30001` <-> `0001`, `0051456` <->
    `51456`, INDI site codes...). Sources use it to prune their listings
    with `selection.keeps_subject`.

    Parameters
    ----------
    to_bids : callable(str) -> str | None
        BIDS label (without `sub-`) of a host identifier; None if the
        identifier is not a subject.
    to_host : callable(str) -> str | None, optional
        Host identifier of a BIDS label (lets a source list only the
        selected subjects).
    """

    to_bids: Callable[[str], str | None] = _identity
    to_host: Callable[[str], str | None] | None = None

    def keeps(self, selection, host_id) -> bool:
        """Whether files of a host subject may be selected."""
        label = self.to_bids(str(host_id))
        if label is None:
            return False
        if selection is None:
            return True
        return selection.keeps_subject(label)

    def hosts(self, selection) -> list[str] | None:
        """Host identifiers of the subjects explicitly selected.

        None if all subjects are selected, or labels cannot be mapped back.
        """
        subs = getattr(selection, "subs", None)
        if subs is None:
            subs = getattr(getattr(selection, "base", None), "subs", None)
        if not subs or self.to_host is None:
            return None
        out = []
        for label in sorted(subs):
            host = self.to_host(label)
            if host is not None and selection.keeps_subject(label):
                out.append(host)
        return out


def numeric_labels(width: int = 0, prefix: str = "") -> LabelMap:
    """
    Host identifiers that are numbers (with an optional prefix).

    `numeric_labels()` maps `0051456` to `51456`, `numeric_labels(3)`
    maps `12` to `012`, `numeric_labels(4, "OAS3")` maps `OAS30001`
    to `0001` (and back).
    """
    def to_bids(host: str) -> str | None:
        if prefix:
            if not host.startswith(prefix):
                return None
            host = host[len(prefix):]
        if not host.isdigit():
            return None
        return f"{int(host):0{width}d}"

    def to_host(label: str) -> str | None:
        label = label[4:] if label.startswith("sub-") else label
        if not label.isdigit():
            return None
        return f"{prefix}{int(label):0{width}d}"

    return LabelMap(to_bids, to_host)


def prefix_labels(prefix: str) -> LabelMap:
    """Host identifiers `<prefix><label>` (e.g. INDI site codes)."""
    def to_bids(host: str) -> str:
        return prefix + host

    def to_host(label: str) -> str | None:
        return label[len(prefix):] if label.startswith(prefix) else None

    return LabelMap(to_bids, to_host)


# ----------------------------------------------------------------------
#   Mirror
# ----------------------------------------------------------------------

def mirror(
    entries: Iterable[RemoteEntry],
    dst: str | os.PathLike,
    keep: Callable[[RemoteEntry], bool] | None = None,
    rename: Callable[[RemoteEntry], str | os.PathLike | None] | None = None,
    **download_opts,
) -> Iterator[Downloader]:
    """
    Yield one `Downloader` per entry.

    Each entry is saved under `dst` with its path (or `rename(entry)`).
    Sizes, mtimes and digests from the listing
    are passed on, so that downloads need no extra request and are
    verified.

    Parameters
    ----------
    entries : iterable[RemoteEntry]
        Files to download (entries without a path, or whose path ends
        with a slash, are skipped).
    dst : path
        Local folder
    keep : callable(RemoteEntry) -> bool
        Filter
    rename : callable(RemoteEntry) -> path | None
        Local path, relative to `dst` (None: skip the entry)
    **download_opts
        `Downloader` options (`ifexists`, `chunk_size`, `auth`,
        `insecure`, `get_opt`...)

    Yields
    ------
    Downloader
        One downloader per kept entry.

    Raises
    ------
    ValueError
        If an entry that is kept (by `keep` and `rename`) has no URL.
    """
    dst = Path(dst)
    for entry in entries:
        if not entry.path or entry.path.endswith("/"):
            continue
        if keep is not None and not keep(entry):
            continue
        local = rename(entry) if rename else entry.path
        if local is None:
            continue
        # only entries that will be downloaded need a URL
        if entry.url is None:
            raise ValueError(f"{entry.path}: no URL to download it from")
        yield Downloader(
            entry.url, dst / local,
            size=entry.size,
            mtime=entry.mtime,
            digests=entry.digests or None,
            **download_opts,
        )


def run_downloads(
    downloaders: Iterable[Downloader],
    options: DownloadOptions | Mapping | None = None,
) -> list[Downloader]:
    """
    Run downloaders with the options of a harvest.

    With `options.dry_run`, the downloaders are printed (with their
    size) instead of being run.

    Parameters
    ----------
    downloaders : iterable[Downloader]
        Downloaders to run (possibly a lazy generator).
    options : DownloadOptions | Mapping | None
        Options of the harvest (`ifexists`, `jobs`, `path`, `on_error`,
        `dry_run`).

    Returns
    -------
    list[Downloader]
        The downloaders that were run (or printed).
    """
    options = DownloadOptions.from_any(options)
    if options.dry_run:
        downloaders = list(downloaders)
        total = 0
        for dl in downloaders:
            size = dl._size
            total += size or 0
            size = naturalsize(size) if size is not None else "?"
            print(f"{size:>10}  {dl.dst}")
        print(f"{len(downloaders)} files, {naturalsize(total)}")
        return downloaders
    done = []

    def track():
        for dl in downloaders:
            done.append(dl)
            yield dl

    DownloadManager(
        track(), ifexists=options.ifexists, jobs=options.jobs,
        path=options.path, on_error=options.on_error,
    ).run()
    return done


def write_content(dst: str | os.PathLike, content: bytes | str,
                  ifexists: str = "different", *,
                  timeout: float | None = None) -> bool:
    """
    Write a generated file.

    The file is not written if it exists with the same content, or if it
    exists and `ifexists` is "skip" or "error". It is written as the
    other writers of the tree write theirs (`actions.File`, downloads):
    under the exclusive lock of `dst` (`actions.locks`), so never while
    a roast reads it, and with the mode of a new file (0o666 less the
    umask). Its temporary file is unique, next to `dst`: not the
    `<name>.tmp/` folder of `actions.File`, which a writer removes, and
    which may be part of the mirrored tree.

    Parameters
    ----------
    dst : path
        Output file.
    content : bytes | str
        Content of the file (str is UTF-8 encoded).
    ifexists : str
        Behaviour if the file exists with a different content.
    timeout : float | None
        Seconds to wait for the lock of the file (default:
        `locks.lock_timeout()`, as downloads that replace a file).

    Returns
    -------
    bool
        Whether the file was written.

    Raises
    ------
    FileExistsError
        If the file exists with another content and `ifexists` is
        "error".
    LockTimeout
        If others hold the lock of the file for longer than `timeout`.
    LockError
        If the lock cannot be taken at all (`actions.locks`).
    OSError
        If the file cannot be written.
    """
    dst = Path(dst)
    if isinstance(content, str):
        content = content.encode()
    dst.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(dst, exclusive=True, timeout=timeout):
        if dst.exists():
            if dst.read_bytes() == content:
                return False
            if ifexists == "error":
                raise FileExistsError(dst)
            if ifexists == "skip":
                return False
        # short (a valid name plus a suffix may be too long), with the
        # start of the name, so that a leftover tells its file
        tmp = dst.with_name(f".{dst.name[:40]}.{secrets.token_hex(8)}.part")
        # created as `open` creates files (0o666 less the umask), unlike
        # `tempfile.mkstemp` (0o600, kept by the rename)
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                     | getattr(os, "O_BINARY", 0), 0o666)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(content)
            os.replace(tmp, dst)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
    return True


class _UnwrittenFile:
    """
    A generated file that could not be written, as a failed download.

    `DownloadManager` iterates it like a `Downloader`: it raises the
    error, which the manager reports (or raises, `on_error`).

    Parameters
    ----------
    dst : path
        The file
    error : Exception
        Why it was not written
    """

    def __init__(self, dst: str | os.PathLike, error: Exception):
        self.dst = Path(dst)
        self.error = error
        self.session = None
        self._size = None

    def __aiter__(self):
        """Raise the error (an async iterator of no status)."""
        return self._fail()

    async def _fail(self):
        raise self.error
        yield                   # (an async generator)


# ----------------------------------------------------------------------
#   Source
# ----------------------------------------------------------------------

class SourceUnavailable(RuntimeError):
    """A source cannot be used (host offline, no access...).

    A harvest falls back to the next source of the key.
    """


@runtime_checkable
class SourceProtocol(Protocol):
    provides: frozenset
    roaster: Callable | None

    def list(self, selection=None, keys=None) -> Iterable[RemoteEntry]:
        ...

    def fetch(self, entries, layout, options=None):
        ...


class Source:
    """
    Base class of sources (see module docstring).

    Parameters (also class attributes, for subclasses)
    ----------
    provides : set[str]
        Dataset keys this source can harvest
    roaster : callable, optional
        Roaster that reads what this source writes
    labels : LabelMap, optional
        Host identifiers <-> BIDS labels (pruning)
    folder : str
        Sub-folder of `layout.source` that this source writes to
    insecure : bool
        Default of `DownloadOptions.insecure` for this source
    jobs : int, optional
        Default number of concurrent downloads for this source
    name : str
        Name in messages and in the manifest
    **options
        Other attributes of the source (credentials, sites...); see
        also `configure` and `with_options` (per-run copies)
    """

    name: str = ""
    provides: frozenset = frozenset()
    roaster: Callable | None = None
    labels: LabelMap = LabelMap()
    folder: str = ""
    insecure: bool = False
    jobs: int | None = None

    def __init__(self, *, provides: Iterable[str] | None = None,
                 roaster: Callable | None = None,
                 labels: LabelMap | None = None, folder: str | None = None,
                 insecure: bool | None = None, jobs: int | None = None,
                 name: str | None = None, **options):
        if provides is not None:
            self.provides = frozenset(provides)
        else:
            self.provides = frozenset(type(self).provides)
        if roaster is None:
            # a function declared as a class attribute is not a method
            roaster = inspect.getattr_static(type(self), "roaster", None)
            roaster = getattr(roaster, "__func__", roaster)
        self.roaster = roaster
        if labels is not None:
            self.labels = labels
        if folder is not None:
            self.folder = folder
        if insecure is not None:
            self.insecure = insecure
        if jobs is not None:
            self.jobs = jobs
        if name is not None:
            self.name = name
        if not self.name:
            self.name = type(self).__name__
        for key, value in options.items():
            if not self._is_option(key):
                raise TypeError(f"{self.name}: unknown option {key!r}")
            setattr(self, key, value)

    # ------------------------------------------------------------------
    def configure(self, **options) -> "Source":
        """
        Return a copy of the source with the per-run options it knows.

        Other options, and None values, are ignored: options of a
        command are offered to every source of the dataset.

        Parameters
        ----------
        **options
            Per-run options (credentials, sites, token...).

        Returns
        -------
        Source
            A configured copy.
        """
        known = {k: v for k, v in options.items()
                 if v is not None and self._is_option(k)}
        return self.with_options(**known)

    def _is_option(self, key: str) -> bool:
        return not key.startswith("_") and hasattr(self, key) and \
            not callable(getattr(type(self), key, None))

    def with_options(self, **options) -> "Source":
        """
        Return a copy of the source with per-run options set.

        Parameters
        ----------
        **options
            Options (credentials, token...); they must be existing
            attributes.

        Returns
        -------
        Source
            A copy with the options set.
        """
        out = copy.copy(self)
        for key, value in options.items():
            if not self._is_option(key):
                raise TypeError(f"{self.name}: unknown option {key!r}")
            setattr(out, key, value)
        return out

    def __repr__(self):
        """Represent the source by its name and keys."""
        return (f"{type(self).__name__}(name={self.name!r}, "
                f"provides={sorted(self.provides)})")

    # ------------------------------------------------------------------
    #   Hooks
    # ------------------------------------------------------------------

    def auth(self) -> Callable | None:
        """
        Return the authentication hook of the downloads.

        The hook is called on 401/403 with the aiohttp session. This
        method is called once per fetch.

        Returns
        -------
        callable | None
            The hook, or None (no authentication).
        """
        return None

    def keeps_subject(self, selection, host_id) -> bool:
        """
        Whether a host subject may be selected (label mapping).

        Parameters
        ----------
        selection : Selection | None
            Subject selection.
        host_id : str
            Identifier of the subject on the host.

        Returns
        -------
        bool
        """
        return self.labels.keeps(selection, host_id)

    def destination(self, layout) -> Path:
        """
        Local folder of the files of this source.

        Parameters
        ----------
        layout : Layout | path
            Layout of the dataset (or the source folder itself).

        Returns
        -------
        Path
            `layout.source`, or its `folder` sub-folder.
        """
        root = Path(getattr(layout, "source", layout))
        return root / self.folder if self.folder else root

    def local_path(self, entry: RemoteEntry) -> str | None:
        """
        Path of an entry relative to `destination`.

        Parameters
        ----------
        entry : RemoteEntry
            Remote file.

        Returns
        -------
        str | None
            Relative local path (None: skip the entry).
        """
        return entry.path

    def resolve(self, layout, entry: RemoteEntry) -> Path | None:
        """
        Local path of an entry.

        The default is `destination(layout) / local_path(entry)`; sources
        that place files in the BIDS dataset use `layout.resolve`.

        Parameters
        ----------
        layout : Layout | path
            Layout of the dataset.
        entry : RemoteEntry
            Remote file.

        Returns
        -------
        Path | None
            Local path (None: skip the entry).
        """
        local = self.local_path(entry)
        return None if local is None else self.destination(layout) / local

    def download_options(self, options: DownloadOptions) -> dict:
        """
        Return the `Downloader` options of this source.

        Parameters
        ----------
        options : DownloadOptions
            Options of the harvest.

        Returns
        -------
        dict
            Keyword arguments of `Downloader` (`ifexists`, `chunk_size`,
            `insecure`, `auth`...).
        """
        insecure = self.insecure if options.insecure is None \
            else options.insecure
        opts = dict(ifexists=options.ifexists,
                    chunk_size=options.chunk_size)
        if insecure:
            opts["insecure"] = True
        # no credentials (nor prompt) in a dry run
        auth = None if options.dry_run else self.auth()
        if auth is not None:
            opts["auth"] = auth
        return opts

    # ------------------------------------------------------------------
    #   Protocol
    # ------------------------------------------------------------------

    def list(self, selection=None, keys=None) -> Iterable[RemoteEntry]:
        """
        List the files to fetch for a selection and the wanted keys.

        Parameters
        ----------
        selection : Selection | None
            Subject selection (None: everything).
        keys : Wants | list[str] | None
            Wanted keys (None: all).

        Returns
        -------
        iterable[RemoteEntry]
            Files to fetch.
        """
        raise NotImplementedError

    def fetch(self, entries: Iterable[RemoteEntry], layout,
              options: DownloadOptions | Mapping | None = None):
        """
        Download entries to `resolve(layout, entry)` (shared `mirror`).

        Entries with a `meta["content"]` (bytes or str) are files
        generated from the listing: they are written, not downloaded.

        Parameters
        ----------
        entries : iterable[RemoteEntry]
            Files to fetch (from `list`).
        layout : Layout | path
            Layout of the dataset.
        options : DownloadOptions | Mapping | None
            Options of the harvest.

        Returns
        -------
        list[Downloader]
            The downloaders that were run.
        """
        options = DownloadOptions.from_any(options)
        if options.jobs is None and self.jobs is not None:
            options = options.replace(jobs=self.jobs)
        return run_downloads(self.downloaders(entries, layout, options),
                             options)

    def downloaders(self, entries: Iterable[RemoteEntry], layout,
                    options: DownloadOptions | Mapping | None = None
                    ) -> Iterator[Downloader]:
        """
        Return the downloaders of `fetch`.

        Generated files are written as the iterator is consumed. One
        that cannot be written (its lock is held for too long by a
        roast that reads it, a full disk...) is a failed download: the
        iterator yields an object that reports the error to the
        `DownloadManager` as a downloader would (`on_error`), and goes
        on with the next entries.

        Parameters
        ----------
        entries : iterable[RemoteEntry]
            Files to fetch.
        layout : Layout | path
            Layout of the dataset.
        options : DownloadOptions | Mapping | None
            Options of the harvest.

        Returns
        -------
        iterator[Downloader]
            Downloaders (and failed generated files).

        Raises
        ------
        ValueError
            If an entry to download has no URL (`mirror`).
        """
        options = DownloadOptions.from_any(options)
        opts = self.download_options(options)

        def rename(entry):
            return self.resolve(layout, entry)

        def jobs():
            for entry in entries:
                if entry.meta.get("content") is None:
                    yield from mirror([entry], "", rename=rename, **opts)
                    continue
                dst = rename(entry)
                if dst is None or options.dry_run:
                    continue
                try:
                    # generated from the listing: always up to date
                    write_content(dst, entry.meta["content"], "overwrite")
                except (LockError, OSError) as e:
                    # a failed download of this file, not of the listing
                    yield _UnwrittenFile(dst, e)

        return jobs()

    def harvest(self, layout, selection=None, keys=None,
                options: DownloadOptions | Mapping | None = None):
        """
        List and fetch: `fetch(list(selection, keys))`.

        Parameters
        ----------
        layout : Layout | path
            Layout of the dataset.
        selection : Selection | None
            Subject selection.
        keys : Wants | list[str] | None
            Wanted keys.
        options : DownloadOptions | Mapping | None
            Options of the harvest.

        Returns
        -------
        list
            The result of `fetch`.
        """
        return self.fetch(self.list(selection, keys), layout, options)


class StaticURLs(Source):
    """
    A fixed list of URLs per key.

    Parameters
    ----------
    urls : dict[str, list[str | RemoteEntry]]
        URLs (or entries) per key. A URL is saved under its file name;
        an entry under its path.
    keep : callable(RemoteEntry, selection, wants) -> bool
        Filter (also a method that subclasses override)
    """

    urls: Mapping[str, Sequence[str | RemoteEntry]] = {}

    def __init__(self, urls: Mapping | None = None, *, keep=None,
                 **kwargs):
        if urls is not None:
            self.urls = urls
        if kwargs.get("provides") is None:
            kwargs["provides"] = set(type(self).provides or self.urls)
        super().__init__(**kwargs)
        if keep is not None:
            self.keep = keep

    @staticmethod
    def entry(url: str | RemoteEntry, path: str | None = None,
              **meta) -> RemoteEntry:
        """Build an entry for a URL, saved under `path` (default: its name)."""
        if isinstance(url, RemoteEntry):
            return url
        return RemoteEntry(path or PurePosixPath(urlparse(url).path).name,
                           url, meta=meta)

    def keep(self, entry: RemoteEntry, selection, wants) -> bool:
        """Whether to fetch an entry (filter that subclasses override)."""
        return True

    def list(self, selection=None, keys=None) -> list[RemoteEntry]:
        """List the entries of `urls` for the wanted keys."""
        wants = as_wants(keys)
        out, seen = [], set()
        for key, urls in self.urls.items():
            if not wants(key):
                continue
            for url in urls:
                entry = self.entry(url)
                if entry.path in seen:
                    continue
                if not self.keep(entry, selection, wants):
                    continue
                seen.add(entry.path)
                out.append(entry)
        return out


class CompositeSource(Source):
    """
    Several sources seen as one.

    For example images from XNAT and phenotypes from static URLs: each
    part lists and fetches its own entries.
    """

    def __init__(self, *parts: Source, **kwargs):
        self.parts = list(parts)
        if kwargs.get("provides") is None:
            kwargs["provides"] = set().union(*(p.provides for p in parts))
        super().__init__(**kwargs)

    def with_options(self, **options) -> "CompositeSource":
        """Options are passed to the parts that have them."""
        unknown = {k for k in options
                   if not any(p._is_option(k) for p in self.parts)}
        if unknown:
            raise TypeError(f"{self.name}: unknown options {unknown}")
        return self.configure(**options)

    def configure(self, **options) -> "CompositeSource":
        """Configure each part (see `Source.configure`)."""
        out = copy.copy(self)
        out.parts = [part.configure(**options) for part in self.parts]
        return out

    def list(self, selection=None, keys=None) -> list[RemoteEntry]:
        """List the entries of the parts that provide wanted keys."""
        wants = as_wants(keys)
        out = []
        for i, part in enumerate(self.parts):
            if not any(wants(k) for k in part.provides):
                continue
            for entry in part.list(selection, wants):
                entry.meta["part"] = i
                out.append(entry)
        return out

    def fetch(self, entries, layout, options=None) -> list:
        """Fetch the entries of each part with that part."""
        entries = list(entries)
        out = []
        for i, part in enumerate(self.parts):
            mine = [e for e in entries if e.meta.get("part") == i]
            if mine:
                out += part.fetch(mine, layout, options) or []
        return out


class LocalSource(Source):
    """
    Data that is already local.

    Data the user put in `sourcedata/` themselves (data under a data use
    agreement, ADNI...), or data shipped with brainspresso. It lists and
    fetches nothing; roast reads the folder.
    """

    name = "local"

    def list(self, selection=None, keys=None) -> list[RemoteEntry]:
        """List nothing (the files are already local)."""
        return []

    def fetch(self, entries, layout, options=None) -> list:
        """Fetch nothing (the files are already local)."""
        return []

    def harvest(self, layout, selection=None, keys=None, options=None):
        """Harvest nothing; log where the files are read from."""
        lg.info(f"{self.name}: nothing to download, the files are read "
                f"from {self.destination(layout)}")
        return []

    def present(self, layout) -> bool:
        """Whether the folder holds anything."""
        folder = self.destination(layout)
        return folder.is_dir() and any(folder.iterdir())


# ----------------------------------------------------------------------
#   Per-key source choice
# ----------------------------------------------------------------------

def plan_sources(
    sources: Mapping[str, Source],
    defaults: Mapping[str, Sequence[str]],
    keys: Callable[[str], bool] | Iterable[str] | None = None,
    override: str | Sequence[str] | None = None,
) -> list[tuple[tuple[str, ...], list[str]]]:
    """
    Choose the sources of a harvest, per key.

    Parameters
    ----------
    sources : dict[str, Source]
        Sources of the dataset
    defaults : dict[str, list[str]]
        Ordered default sources per key (the first available one is
        used, the next ones are fallbacks). A key without defaults is
        harvested from the sources that provide it, in declaration
        order.
    keys : Wants | list[str] | None
        Wanted keys (the keys of `defaults` that are wanted are
        planned)
    override : str | list[str], optional
        `--source`: the sources to use instead of the defaults (in this
        order), for the keys they provide

    Returns
    -------
    plan : list[(keys, candidates)]
        Keys grouped by their (ordered) candidate sources
    """
    wants = as_wants(keys)
    if isinstance(override, str):
        override = [override]
    override = [str(getattr(s, "value", s)) for s in override or ()]
    unknown = [s for s in override if s not in sources]
    if unknown:
        raise UsageError(f"Unknown sources {unknown}. "
                         f"Available: {', '.join(sources)}")
    all_keys = list(defaults)
    for src in sources.values():
        all_keys += [k for k in sorted(src.provides) if k not in all_keys]
    groups: dict[tuple[str, ...], list[str]] = {}
    for key in all_keys:
        if not wants(key):
            continue
        if override:
            candidates = [s for s in override
                          if key in sources[s].provides]
            if not candidates:
                lg.info(f"{key}: not provided by {', '.join(override)}")
                continue
        elif key in defaults:
            candidates = list(defaults[key])
        else:
            providers = [n for n, s in sources.items() if key in s.provides]
            lg.info(f"{key}: not harvested by default (use --source "
                    f"{' or '.join(providers)})")
            continue
        missing = [s for s in candidates if s not in sources]
        if missing:
            raise ValueError(f"{key}: unknown default sources {missing}")
        groups.setdefault(tuple(candidates), []).append(key)
    return [(tuple(keys), list(candidates))
            for candidates, keys in groups.items() if candidates]


def harvest_sources(
    sources: Mapping[str, Source],
    defaults: Mapping[str, Sequence[str]],
    layout,
    selection=None,
    keys: Callable[[str], bool] | Iterable[str] | None = None,
    options: DownloadOptions | Mapping | None = None,
    override: str | Sequence[str] | None = None,
) -> dict[str, str]:
    """
    Harvest a dataset from its sources.

    For each group of keys of `plan_sources`, the first candidate that
    is not unavailable is harvested.

    Parameters
    ----------
    sources : dict[str, Source]
        Sources of the dataset.
    defaults : dict[str, list[str]]
        Ordered default sources per key (see `plan_sources`).
    layout : Layout | path
        Layout of the dataset.
    selection : Selection | None
        Subject selection.
    keys : Wants | list[str] | None
        Wanted keys.
    options : DownloadOptions | Mapping | None
        Options of the harvest.
    override : str | list[str], optional
        `--source`: the sources to use instead of the defaults.

    Returns
    -------
    used : dict[str, str]
        Source used for each key

    Raises
    ------
    SourceUnavailable
        If no candidate of a group of keys is available.
    """
    wants = keys if isinstance(keys, Wants) else (
        Wants(keys) if keys is None or not callable(keys) else keys
    )
    used = {}
    for group, candidates in plan_sources(sources, defaults, wants,
                                          override):
        group_wants = wants.restrict(group) if isinstance(wants, Wants) \
            else wants
        errors = []
        for name in candidates:
            source = sources[name]
            try:
                source.harvest(layout, selection, group_wants, options)
            except SourceUnavailable as e:
                lg.warning(f"{name} is unavailable ({e})"
                           + (", trying the next source" if
                              name != candidates[-1] else ""))
                errors.append(f"{name}: {e}")
                continue
            used.update({key: name for key in group})
            break
        else:
            raise SourceUnavailable(
                f"No available source for {', '.join(group)}: "
                + "; ".join(errors)
            )
    return used


def configure_sources(sources: Mapping[str, Source], **options
                      ) -> dict[str, Source]:
    """
    Offer per-run options (credentials, sites...) to every source.

    Parameters
    ----------
    sources : dict[str, Source]
        Sources of the dataset.
    **options
        Per-run options (see `Source.configure`).

    Returns
    -------
    dict[str, Source]
        Configured copies of the sources.
    """
    return {name: src.configure(**options) for name, src in sources.items()}


def select_roaster(sources: Iterable[Source], ctx) -> Source | None:
    """
    Select the source whose roaster should read `sourcedata/`.

    It is the first source whose `roaster.detect(ctx)` is true, else the
    first one with a roaster (roast without a harvest manifest, mixed
    `sourcedata/`).

    Parameters
    ----------
    sources : iterable[Source]
        Sources of the dataset.
    ctx : object
        Roast context, passed to `roaster.detect`.

    Returns
    -------
    Source | None
        The selected source (None if no source has a roaster).
    """
    sources = [s for s in sources if s.roaster is not None]
    for source in sources:
        detect = getattr(source.roaster, "detect", None)
        if detect is not None and detect(ctx):
            return source
    return sources[0] if sources else None


# ----------------------------------------------------------------------
#   Command-line helpers
# ----------------------------------------------------------------------

def subject_labels(values, width: int | None = 0, prefix: str = "sub-"
                   ) -> set[str]:
    """
    Convert command-line subject values to BIDS labels.

    Values are numbers (formatted with `width` digits), labels (with or
    without `sub-`), ranges `start:stop[:step]` and files that list one
    subject per line. With `width=None`, labels are kept verbatim
    (`0005` stays `0005`, no ranges): only `sub-` is stripped, and files
    are still read.
    """
    if values is None:
        return set()
    if isinstance(values, (str, int)):
        values = [values]
    out = set()

    def add(value):
        value = str(getattr(value, "value", value)).strip()
        if not value:
            return
        if value.startswith(prefix):
            value = value[len(prefix):]
        if width is not None and value.isdigit():
            value = f"{int(value):0{width}d}"
        out.add(value)

    for value in values:
        value = getattr(value, "value", value)
        if width is not None and isinstance(value, str) and ":" in value \
                and not Path(value).is_file():     # (`C:\\subs.txt`)
            start, stop, *step = value.split(":")
            try:
                step = int(step[0]) if step and step[0] else 1
                start = int(start or 0)
                stop = int(stop) if stop else None
            except ValueError:
                raise UsageError(f"Subject range: not numbers: {value!r} "
                                 f"(start:stop[:step])") from None
            if step < 1:
                raise UsageError("Subject range: step must be positive")
            if stop is None:
                raise UsageError("Subject range: stop must be provided")
            for i in range(start, stop, step):
                add(i)
        elif isinstance(value, str) and not value.isdigit() and \
                Path(value).is_file():
            for line in Path(value).read_text().splitlines():
                add(line)
        else:
            add(value)
    return out


def subject_selection(subs=(), exclude_subs=(), width: int | None = 0):
    """
    Build a `Selection` of subjects from command-line values.

    Parameters
    ----------
    subs, exclude_subs : list
        Subjects to include and exclude (see `subject_labels`).
    width : int | None
        Number of digits of numeric labels (None: labels verbatim).

    Returns
    -------
    Selection
    """
    return Selection(subs=subject_labels(subs, width),
                     exclude_subs=subject_labels(exclude_subs, width))
