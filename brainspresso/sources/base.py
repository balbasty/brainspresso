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
from dataclasses import dataclass, field, fields
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import (
    Any, Callable, Iterable, Iterator, Mapping, Protocol, Sequence,
    runtime_checkable,
)
from urllib.parse import urlparse

from humanize import naturalsize

from brainspresso.download import CHUNK_SIZE, Downloader, DownloadManager

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
        if isinstance(self.path, PurePosixPath):
            self.path = self.path.as_posix()
        self.digests = dict(self.digests or {})
        self.meta = dict(self.meta or {})

    @property
    def relpath(self) -> PurePosixPath:
        """`path` as a `PurePosixPath`"""
        return PurePosixPath(self.path)

    @property
    def name(self) -> str:
        return self.relpath.name


# ----------------------------------------------------------------------
#   Options
# ----------------------------------------------------------------------

@dataclass
class DownloadOptions:
    """
    Options of a harvest, shared by all sources (the `DownloadOptions`
    group of the command line).

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
    """`related(a, b)`: whether two keys are equal, or one is below the
    other, in a dataset key tree (nested dict/set, or `KeyAliases`)"""
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
    from brainspresso.utils.keys import compat_keys
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
        return self._related(a, b)

    def __call__(self, key: str) -> bool:
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
        if self.tree is None or hasattr(self.tree, "expand"):
            return key == upper
        from brainspresso.utils.keys import lower_keys
        return key in lower_keys(upper, self.tree)

    def restrict(self, group: Iterable[str]) -> "Wants":
        """Wanted keys related to one of the keys of `group`"""
        out = copy.copy(self)
        group = set(group)
        out.within = group if self.within is None else {
            g for g in group if any(self._related(g, k) for k in self.within)
        }
        return out

    def __repr__(self):
        return (f"Wants({sorted(self.keys) or 'all'}"
                + (f", within={sorted(self.within)}" if self.within
                   is not None else "")
                + (f", exclude={sorted(self.exclude)}" if self.exclude
                   else "") + ")")


def as_wants(keys) -> Callable[[str], bool]:
    """None (everything), a collection of key names, or a predicate"""
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
    Mapping between the subject identifiers of a host and BIDS labels,
    supplied by the dataset (`OAS30001` <-> `0001`, `0051456` <->
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
        """Whether files of a host subject may be selected"""
        label = self.to_bids(str(host_id))
        if label is None:
            return False
        if selection is None:
            return True
        return selection.keeps_subject(label)

    def hosts(self, selection) -> list[str] | None:
        """Host identifiers of the subjects explicitly selected (None if
        all subjects are selected, or labels cannot be mapped back)"""
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
    Host identifiers that are numbers (with an optional prefix):
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
    """Host identifiers `<prefix><label>` (e.g. INDI site codes)"""
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
    Yield one `Downloader` per entry, saved under `dst` with the entry's
    path (or `rename(entry)`). Sizes, mtimes and digests from the listing
    are passed on, so that downloads need no extra request and are
    verified.

    Parameters
    ----------
    entries : iterable[RemoteEntry]
    dst : path
        Local folder
    keep : callable(RemoteEntry) -> bool
        Filter
    rename : callable(RemoteEntry) -> path | None
        Local path, relative to `dst` (None: skip the entry)
    **download_opts
        `Downloader` options (`ifexists`, `chunk_size`, `auth`,
        `insecure`, `get_opt`...)
    """
    dst = Path(dst)
    for entry in entries:
        if not entry.path or entry.path.endswith("/"):
            continue
        if entry.url is None:
            raise ValueError(f"{entry.path}: no URL to download it from")
        if keep is not None and not keep(entry):
            continue
        local = rename(entry) if rename else entry.path
        if local is None:
            continue
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
    """Run downloaders with the options of a harvest (or print them,
    with `dry_run`). Returns the downloaders."""
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
                  ifexists: str = "different") -> bool:
    """Write a generated file (unless it exists with the same content, or
    `ifexists` is "skip" or "error"). Returns whether it was written."""
    dst = Path(dst)
    if isinstance(content, str):
        content = content.encode()
    if dst.exists():
        if dst.read_bytes() == content:
            return False
        if ifexists == "error":
            raise FileExistsError(dst)
        if ifexists == "skip":
            return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".tmp")
    tmp.write_bytes(content)
    os.replace(tmp, dst)
    return True


# ----------------------------------------------------------------------
#   Source
# ----------------------------------------------------------------------

class SourceUnavailable(RuntimeError):
    """A source cannot be used (host offline, no access...): a harvest
    falls back to the next source of the key"""


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
        """A copy of the source with the per-run options it knows
        (others, and None values, are ignored): options of a command are
        offered to every source of the dataset"""
        known = {k: v for k, v in options.items()
                 if v is not None and self._is_option(k)}
        return self.with_options(**known)

    def _is_option(self, key: str) -> bool:
        return not key.startswith("_") and hasattr(self, key) and \
            not callable(getattr(type(self), key, None))

    def with_options(self, **options) -> "Source":
        """A copy of the source with per-run options set (credentials,
        token...); options must be existing attributes"""
        out = copy.copy(self)
        for key, value in options.items():
            if not self._is_option(key):
                raise TypeError(f"{self.name}: unknown option {key!r}")
            setattr(out, key, value)
        return out

    def __repr__(self):
        return (f"{type(self).__name__}(name={self.name!r}, "
                f"provides={sorted(self.provides)})")

    # ------------------------------------------------------------------
    #   Hooks
    # ------------------------------------------------------------------

    def auth(self) -> Callable | None:
        """Authentication hook of the downloads (called on 401/403 with
        the aiohttp session), or None. Called once per fetch."""
        return None

    def keeps_subject(self, selection, host_id) -> bool:
        """Whether a host subject may be selected (label mapping)"""
        return self.labels.keeps(selection, host_id)

    def destination(self, layout) -> Path:
        """Local folder of the files of this source: `layout.source`
        (or its `folder`). `layout` may also be the source folder."""
        root = Path(getattr(layout, "source", layout))
        return root / self.folder if self.folder else root

    def local_path(self, entry: RemoteEntry) -> str | None:
        """Path of an entry relative to `destination` (None: skip)"""
        return entry.path

    def resolve(self, layout, entry: RemoteEntry) -> Path | None:
        """Local path of an entry (None: skip). Default:
        `destination(layout) / local_path(entry)`; sources that place
        files in the BIDS dataset use `layout.resolve`."""
        local = self.local_path(entry)
        return None if local is None else self.destination(layout) / local

    def download_options(self, options: DownloadOptions) -> dict:
        """`Downloader` options of this source"""
        insecure = self.insecure if options.insecure is None \
            else options.insecure
        opts = dict(ifexists=options.ifexists,
                    chunk_size=options.chunk_size)
        if insecure:
            opts["insecure"] = True
        auth = self.auth()
        if auth is not None:
            opts["auth"] = auth
        return opts

    # ------------------------------------------------------------------
    #   Protocol
    # ------------------------------------------------------------------

    def list(self, selection=None, keys=None) -> Iterable[RemoteEntry]:
        """Files to fetch for a selection and the wanted keys"""
        raise NotImplementedError

    def fetch(self, entries: Iterable[RemoteEntry], layout,
              options: DownloadOptions | Mapping | None = None):
        """
        Download entries to `resolve(layout, entry)` (shared `mirror`).
        Entries with a `meta["content"]` (bytes or str) are files
        generated from the listing: they are written, not downloaded.
        """
        options = DownloadOptions.from_any(options)
        if options.jobs is None and self.jobs is not None:
            options = options.replace(jobs=self.jobs)
        return run_downloads(self.downloaders(entries, layout, options),
                             options)

    def downloaders(self, entries: Iterable[RemoteEntry], layout,
                    options: DownloadOptions | Mapping | None = None
                    ) -> Iterator[Downloader]:
        """The downloaders of `fetch` (generated files are written as
        the iterator is consumed)"""
        options = DownloadOptions.from_any(options)

        def remote():
            for entry in entries:
                if entry.meta.get("content") is None:
                    yield entry
                    continue
                dst = self.resolve(layout, entry)
                if dst is not None and not options.dry_run:
                    # generated from the listing: always up to date
                    write_content(dst, entry.meta["content"], "overwrite")

        return mirror(remote(), "", rename=lambda e: self.resolve(layout, e),
                      **self.download_options(options))

    def harvest(self, layout, selection=None, keys=None,
                options: DownloadOptions | Mapping | None = None):
        """`fetch(list(selection, keys))`"""
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
        """An entry for a URL, saved under `path` (default: its name)"""
        if isinstance(url, RemoteEntry):
            return url
        return RemoteEntry(path or PurePosixPath(urlparse(url).path).name,
                           url, meta=meta)

    def keep(self, entry: RemoteEntry, selection, wants) -> bool:
        return True

    def list(self, selection=None, keys=None) -> list[RemoteEntry]:
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
    Several sources seen as one (e.g. images from XNAT and phenotypes
    from static URLs): each part lists and fetches its own entries.
    """

    def __init__(self, *parts: Source, **kwargs):
        self.parts = list(parts)
        if kwargs.get("provides") is None:
            kwargs["provides"] = set().union(*(p.provides for p in parts))
        super().__init__(**kwargs)

    def with_options(self, **options) -> "CompositeSource":
        """Options are passed to the parts that have them"""
        unknown = {k for k in options
                   if not any(p._is_option(k) for p in self.parts)}
        if unknown:
            raise TypeError(f"{self.name}: unknown options {unknown}")
        return self.configure(**options)

    def configure(self, **options) -> "CompositeSource":
        out = copy.copy(self)
        out.parts = [part.configure(**options) for part in self.parts]
        return out

    def list(self, selection=None, keys=None) -> list[RemoteEntry]:
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
        entries = list(entries)
        out = []
        for i, part in enumerate(self.parts):
            mine = [e for e in entries if e.meta.get("part") == i]
            if mine:
                out += part.fetch(mine, layout, options) or []
        return out


class LocalSource(Source):
    """
    Data the user put in `sourcedata/` themselves (data under a data use
    agreement, ADNI...), or data shipped with brainspresso. It lists and
    fetches nothing; roast reads the folder.
    """

    name = "local"

    def list(self, selection=None, keys=None) -> list[RemoteEntry]:
        return []

    def fetch(self, entries, layout, options=None) -> list:
        return []

    def harvest(self, layout, selection=None, keys=None, options=None):
        lg.info(f"{self.name}: nothing to download, the files are read "
                f"from {self.destination(layout)}")
        return []

    def present(self, layout) -> bool:
        """Whether the folder holds anything"""
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
        raise ValueError(f"Unknown sources {unknown}. "
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
    Harvest a dataset from its sources: for each group of keys of
    `plan_sources`, the first candidate that is not unavailable.

    Returns
    -------
    used : dict[str, str]
        Source used for each key
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
    """Per-run options (credentials, sites...) offered to every source"""
    return {name: src.configure(**options) for name, src in sources.items()}


def select_roaster(sources: Iterable[Source], ctx) -> Source | None:
    """
    The source whose roaster should read `sourcedata/`: the first one
    whose `roaster.detect(ctx)` is true, else the first one with a
    roaster (roast without a harvest manifest, mixed `sourcedata/`).
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

def subject_labels(values, width: int = 0, prefix: str = "sub-") -> set[str]:
    """
    BIDS labels from command-line subject values: numbers (formatted
    with `width` digits), labels (with or without `sub-`), ranges
    `start:stop[:step]` and files that list one subject per line.
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
        if value.isdigit():
            value = f"{int(value):0{width}d}"
        out.add(value)

    for value in values:
        value = getattr(value, "value", value)
        if isinstance(value, str) and ":" in value:
            start, stop, *step = value.split(":")
            step = int(step[0]) if step and step[0] else 1
            if step < 1:
                raise ValueError("Subject range: step must be positive")
            if not stop:
                raise ValueError("Subject range: stop must be provided")
            for i in range(int(start or 0), int(stop), step):
                add(i)
        elif isinstance(value, str) and not value.isdigit() and \
                Path(value).is_file():
            for line in Path(value).read_text().splitlines():
                add(line)
        else:
            add(value)
    return out


def subject_selection(subs=(), exclude_subs=(), width: int = 0):
    """`Selection` of subjects from command-line values"""
    from brainspresso.bids.select import Selection
    return Selection(subs=subject_labels(subs, width),
                     exclude_subs=subject_labels(exclude_subs, width))
