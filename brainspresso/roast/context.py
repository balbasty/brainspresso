"""
Context of a roast: what a roaster needs to know about it.

```python
ctx = make_context(root, KEYS, select_keys=["T1w"], json="no")
ctx.wants("meta")               # is a category wanted?
ctx.resource("README")          # ctx.source, then the dataset templates
ctx.keeps(path)                 # is an output selected?
ctx.keeps_subject("01")         # is a subject kept? (and report it)
```
"""
from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Literal

from brainspresso.actions.action import IfExists, Kind
from brainspresso.actions.engine import Context
from brainspresso.bids.harvest import manifest_files
from brainspresso.bids.keys import KeyAliases, KeySelection
from brainspresso.bids.kinds import accepts
from brainspresso.bids.layout import (
    MANIFEST,
    ORIG_DIR,
    Layout,
    resolve_layout,
)

__all__ = ["JsonPolicy", "RoastContext", "as_list", "make_context"]

#: Sidecar policies (`--sidecars yes|no|only`)
JsonPolicy = Literal["yes", "no", "only"]


@dataclass
class RoastContext(Context):
    """
    What a roaster needs to know about the roast.

    It is the `actions.engine.Context` of the roast: `root` and `state`
    are those of the layout, and `stage` is "roast".

    Attributes
    ----------
    layout : Layout
        Paths of the dataset
    selection : KeySelection
        Selection on the raw dataset (keys, subjects, sessions), and
        the categories wanted (`wants`)
    aliases : KeyAliases | None
        Keys of the dataset (predicates, categories and their folders)
    json : {"yes", "no", "only"}
        Write sidecars only (`only`), not at all (`no`), or with data
    ifexists : {"skip", "update", "different", "overwrite", "refresh", "error"}
        Default `IfExists` of actions that do not set their own
    options : dict
        Dataset-specific options
    templates : Path | None
        Folder of templates shipped with the dataset (`resource`)
    tag : str
        Progress group of the roaster that is running (default of
        `progress` and of the actions it yields)
    source : Path | None
        Folder of the source data the running roaster reads (default:
        `layout.source`; a roaster or `Shard` with a `source` attribute
        sets it while it runs)
    """

    layout: Layout
    selection: KeySelection = field(default_factory=KeySelection)
    aliases: KeyAliases | None = None
    json: JsonPolicy = "yes"
    ifexists: IfExists.Choice = "skip"
    options: dict = field(default_factory=dict)
    templates: Path | None = None
    tag: str = "roast"
    source: Path | None = None

    stage = "roast"

    def __post_init__(self):
        """Normalize `json` and `source`."""
        self._reporter = None
        self._folders: list[tuple[Path, frozenset]] | None = None
        self._recorded: dict[str, str] | None = None
        if isinstance(self.json, bool):
            self.json = "yes" if self.json else "no"
        self.json = str(self.json).lower()
        if self.json not in ("yes", "no", "only"):
            raise ValueError(f"json must be yes, no or only: {self.json}")
        self.source = Path(self.source) if self.source is not None \
            else Path(self.layout.source)

    def __getstate__(self) -> dict:
        """Pickle without the reporter and the manifest read."""
        state = super().__getstate__()
        state["_recorded"] = None
        return state

    def forget(self) -> None:
        """Drop the manifest read by `recorded` (a new run starts)."""
        self._recorded = None

    def recorded(self, path: str | os.PathLike) -> str | None:
        """
        SHA-256 recorded in the manifest for an output.

        The hashes are read once per process and version of the manifest
        (`_recorded_hashes`: keyed by its path, inode, size and mtime),
        so that the shards a worker runs share one read; the manifest
        changes at the end of a roast only.

        Parameters
        ----------
        path : str | PathLike
            Absolute path of an output

        Returns
        -------
        str | None
            Its `sha256` (None: no entry, or no hash)
        """
        if self._recorded is None:
            self._recorded = _recorded_hashes(self.layout)
        try:
            key = Path(path).relative_to(self.layout.root).as_posix()
        except ValueError:
            return None
        return self._recorded.get(key)

    @property
    def root(self) -> Path:
        """Dataset folder (`layout.root`): outputs are relative to it."""
        return Path(self.layout.root)

    @property
    def state(self) -> Path:
        """State folder of the dataset (`layout.state`)."""
        return Path(self.layout.state)

    # ------------------------------------------------------------------
    def wants(self, key: str) -> bool:
        """
        Whether a key is wanted.

        Parameters
        ----------
        key : str
            A category (metadata, a pipeline...) or a BIDS key

        Returns
        -------
        bool
            `selection.wants(key)`
        """
        return self.selection.wants(key)

    def wants_any(self, keys: Iterable[str]) -> bool:
        """
        Whether any of some keys is wanted.

        Parameters
        ----------
        keys : iterable of str
            Categories or BIDS keys (`wants`)

        Returns
        -------
        bool
            True if `wants(key)` for one of them
        """
        return any(map(self.wants, keys))

    def keeps_subject(self, label: str) -> bool:
        """
        Whether a subject is kept, and report it as seen.

        A shard that declares `scope(subs=SEEN)` asks this for every
        subject its inputs hold, before any other pruning: the subjects
        seen by a complete run tell a later run whether it adds one that
        this shard holds (`RoastPolicy.covers`).

        Parameters
        ----------
        label : str
            Subject label (with or without `sub-`)

        Returns
        -------
        bool
            `selection.keeps_subject(label)`
        """
        label = str(label)
        label = label[4:] if label.startswith("sub-") else label
        self.seen("sub", label)
        return self.selection.keeps_subject(label)

    def resource(self, name: str | os.PathLike) -> Path | None:
        """
        Find a metadata file.

        Parameters
        ----------
        name : str | PathLike
            Its name (relative path)

        Returns
        -------
        Path | None
            From `ctx.source` if there, else from the templates shipped
            with the dataset, else None (copying None is an error;
            `roast.copy_templates` reports it)
        """
        for folder in (self.source, self.templates):
            if folder is not None and (Path(folder) / name).exists():
                return Path(folder) / name
        return None

    def accepts(self, kind: Kind) -> bool:
        """
        Apply the JSON policy to a kind of output.

        `staging` actions are not subject to it (`roast.RoastPolicy`).

        Parameters
        ----------
        kind : {"data", "sidecar", "meta", "table"}
            Kind of output (`bids.kinds`)

        Returns
        -------
        bool
            False for sidecars under `--sidecars no` and data under
            `--sidecars only`
        """
        return accepts(self.json, kind)

    def _unorig(self, path) -> Path:
        """Map a pristine table (`<state>/orig/...`) to its table."""
        path = Path(path)
        try:
            return self.layout.root / path.relative_to(
                Path(self.layout.state) / ORIG_DIR)
        except ValueError:
            return path

    def _relative(self, path) -> tuple[Path, str] | None:
        """(BIDS dataset, relative path) of an output."""
        try:
            dataset, rel = self.layout.relative(self._unorig(path))
        except ValueError:
            return None
        return Path(dataset), "" if rel == "." else rel

    def _categories(self, path) -> list[frozenset]:
        """Categories of each category folder that holds a path."""
        if self._folders is None:
            folders = defaultdict(set)
            for category, prefixes in getattr(
                    self.aliases, "folders", {}).items():
                for prefix in prefixes:
                    folders[self.layout.resolve(prefix)].add(category)
            self._folders = [(p, frozenset(c)) for p, c in folders.items()]
        if not self._folders:
            return []
        path = self._unorig(path)
        return [categories for prefix, categories in self._folders
                if path == prefix or prefix in path.parents]

    def _blocked(self, path) -> bool:
        """In the folder of categories that are all unwanted."""
        return any(not any(map(self.selection.wants, categories))
                   for categories in self._categories(path))

    def keeps(
        self, path: str | os.PathLike, partial: bool = False
    ) -> bool | None:
        """
        Whether an output is selected.

        `selection.keeps` on its path in the raw dataset, the
        subject/session filters elsewhere (derivatives, `sourcedata/`);
        False in the folder of a category that is not wanted. Pristine
        tables (`<state>/orig/`) are judged as the table they are the
        copy of.

        Parameters
        ----------
        path : str | PathLike
            Absolute path of an output, or of a claim
        partial : bool
            `path` is a claim (a folder or the start of file names)

        Returns
        -------
        bool | None
            True (kept), False (dropped), None (claim: undecided)
        """
        if self._blocked(path):
            return False
        found = self._relative(path)
        if found is None:
            return True             # not in the dataset: not ours to filter
        dataset, rel = found
        if dataset == Path(self.layout.raw):
            return self.selection.keeps(rel, partial)
        return self.selection.base.keeps(rel, partial)


#: `_recorded_hashes`: (manifest path, version) and its hashes, per process
_HASHES: dict[str, tuple[tuple, dict[str, str]]] = {}


def _recorded_hashes(layout: Layout) -> dict[str, str]:
    """SHA-256 of the files of the manifest, cached per manifest version."""
    path = Path(layout.state) / MANIFEST
    try:
        stat = path.stat()
        version = (stat.st_ino, stat.st_size, stat.st_mtime_ns)
    except OSError:
        version = None
    cached = _HASHES.get(str(path))
    if version is not None and cached is not None and cached[0] == version:
        return cached[1]
    try:
        files = manifest_files(layout)
    except OSError:
        files = {}
    hashes = {key: entry["sha256"] for key, entry in files.items()
              if isinstance(entry.get("sha256"), str)}
    if version is not None:
        _HASHES[str(path)] = (version, hashes)
    return hashes


def make_context(
    root: str | os.PathLike | Layout,
    aliases: KeyAliases | None = None,
    *,
    layout: str | None = None,
    select_keys: Iterable[str] = (),
    exclude_keys: Iterable[str] = (),
    json: JsonPolicy | bool = "yes",
    ifexists: IfExists.Choice = "skip",
    templates: str | os.PathLike | None = None,
    options: dict | None = None,
    source: str | os.PathLike | None = None,
    **filters,
) -> RoastContext:
    """
    Build the context of a roast from command-line values.

    Parameters
    ----------
    root : path | Layout
        Dataset folder (`<root>/<DS>`)
    aliases : KeyAliases
        Keys of the dataset
    layout : {"study", "flat"}
        Layout of a new dataset (must match an existing one)
    select_keys, exclude_keys : list[str]
        `--keys`, `--exclude-keys`
    json : {"yes", "no", "only"} | bool
        Sidecar policy (`RoastOptions.sidecar_policy`)
    ifexists : {"skip", "update", "different", "overwrite", "refresh", "error"}
        Default `IfExists`
    templates : path
        Templates shipped with the dataset
    options : dict
        Dataset-specific options
    source : path
        Source folder (default: `layout.source`)
    **filters
        `Selection` filters (`subs`, `exclude_subs`, `sessions`...:
        the fields of `--sub`, `--exclude-sub`, `--ses`...)

    Returns
    -------
    RoastContext
        The context
    """
    aliases = aliases or KeyAliases(set())
    return RoastContext(
        layout=resolve_layout(root, layout),
        selection=aliases.selection(select_keys, exclude_keys, **filters),
        aliases=aliases,
        json=json,
        ifexists=ifexists,
        options=dict(options or {}),
        templates=Path(templates) if templates is not None else None,
        source=source,
    )


def as_list(value, fmt: str | None = None) -> list:
    """
    Turn a command-line value into a list.

    Parameters
    ----------
    value : None | str | int | PathLike | iterable
        One value, a list, or None (paths become `str`: a file of
        subjects)
    fmt : str | None
        Format of each value (e.g. "{:03d}" for subject numbers)

    Returns
    -------
    list
        The values
    """
    if value is None:
        return []
    if isinstance(value, (str, int, os.PathLike)):
        value = [value]
    value = [os.fsdecode(v) if isinstance(v, os.PathLike) else v
             for v in value]
    if fmt is not None:
        value = [fmt.format(int(v) if "d" in fmt else v) for v in value]
    return value
