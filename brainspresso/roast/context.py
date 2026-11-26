"""
Context of a roast: what a roaster needs to know about it.

```python
ctx = make_context(root, KEYS, select_keys=["T1w"], json="no")
ctx.wants("meta")               # is a category wanted?
ctx.resource("README")          # ctx.source, then the dataset templates
ctx.keeps(path)                 # is an output selected?
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
from brainspresso.bids.keys import KeyAliases, KeySelection
from brainspresso.bids.layout import ORIG_DIR, Layout, resolve_layout

__all__ = ["JsonPolicy", "RoastContext", "as_list", "make_context"]

#: `--json` values
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
    ifexists : {"skip", "overwrite", "different", "refresh", "error"}
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
        if isinstance(self.json, bool):
            self.json = "yes" if self.json else "no"
        self.json = str(self.json).lower()
        if self.json not in ("yes", "no", "only"):
            raise ValueError(f"json must be yes, no or only: {self.json}")
        self.source = Path(self.source) if self.source is not None \
            else Path(self.layout.source)

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
            False for sidecars under `--json no` and data under
            `--json only`
        """
        if self.json == "no":
            return kind != "sidecar"
        if self.json == "only":
            return kind != "data"
        return True

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

    def _blocked(self, path) -> bool:
        """In the folder of categories that are all unwanted."""
        if self._folders is None:
            folders = defaultdict(set)
            for category, prefixes in getattr(
                    self.aliases, "folders", {}).items():
                for prefix in prefixes:
                    folders[self.layout.resolve(prefix)].add(category)
            self._folders = [(p, frozenset(c)) for p, c in folders.items()]
        if not self._folders:
            return False
        path = self._unorig(path)
        for prefix, categories in self._folders:
            if path == prefix or prefix in path.parents:
                if not any(map(self.selection.wants, categories)):
                    return True
        return False

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
        `--json`
    ifexists : {"skip", "overwrite", "different", "refresh", "error"}
        Default `IfExists`
    templates : path
        Templates shipped with the dataset
    options : dict
        Dataset-specific options
    source : path
        Source folder (default: `layout.source`)
    **filters
        `Selection` filters (`subs`, `exclude_subs`, `sessions`...)

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
    value : None | str | int | iterable
        One value, a list, or None
    fmt : str | None
        Format of each value (e.g. "{:03d}" for subject numbers)

    Returns
    -------
    list
        The values
    """
    if value is None:
        return []
    if isinstance(value, (str, int)):
        value = [value]
    value = list(value)
    if fmt is not None:
        value = [fmt.format(int(v) if "d" in fmt else v) for v in value]
    return value
