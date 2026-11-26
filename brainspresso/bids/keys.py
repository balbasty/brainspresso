"""
Dataset keys (`--keys`, `--exclude-keys`) as selection predicates.

Each dataset names what can be converted with its own keys (IXI: `T1w`,
`dwi`, `meta`; OASIS-III: `TSE`, `PIB`, `fs`...). A `KeyAliases` maps
them to:

* **BIDS predicates**: `Selection` filters on the paths of the raw
  dataset (`{"suffixes": ["T2w"], "entities": {"acq": ["TSE"]}}`);
* **categories**: anything that is not a set of raw BIDS files
  (metadata, phenotypes, a derivative pipeline, source-only files).
  Roasters ask `ctx.wants(category)`.

Keys may be grouped (`raw` -> `anat` -> `T1w`...). A group selects all
the keys below it.

Several keys select the *union* of their predicates (`T1w` and `TSE` are
`suffix T1w` or `suffix T2w with acq-TSE`, which one `Selection`, a
conjunction of filters, cannot express). `KeySelection` is that union,
with the subject and session filters of the command line applied to
each branch; it has the `keeps`/`keeps_subject`/`select` interface of
`Selection`. When no key restricts the raw data (all keys, no exclusion),
it is the plain `Selection` of the command line, so files that no key
describes are kept too.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Collection, Iterable, Mapping

from brainspresso.bids.select import OPTIONAL_FOLDERS, Selection

# Suffix that no data file has: a selection that keeps dataset files
# and tables (`participants.tsv`, `*_sessions.tsv`), but no data
_NO_DATA = "__none__"


def _fold_any(verdicts: Iterable[bool | None]) -> bool | None:
    """True if any is True, else None if any is None, else False"""
    out = False
    for v in verdicts:
        if v is True:
            return True
        if v is None:
            out = None
    return out


@dataclass
class KeySelection:
    """
    Union of selections on the raw dataset, plus non-BIDS categories.

    Attributes
    ----------
    base : Selection
        Subject and session filters (applied to every branch, and alone
        to files outside the raw dataset: derivatives, sourcedata)
    branches : tuple[Selection]
        A raw path is kept if any branch keeps it
    categories : frozenset[str] | None
        Categories wanted (None: all)
    keys : frozenset[str] | None
        Keys (leaves) selected, for the record
    """
    base: Selection = field(default_factory=Selection)
    branches: tuple[Selection, ...] | None = None
    categories: frozenset[str] | None = None
    keys: frozenset[str] | None = None

    def __post_init__(self):
        if self.branches is None:
            self.branches = (self.base,)
        if not self.branches:
            self.branches = (_restrict(self.base, {"suffixes": [_NO_DATA]}),)
        self.branches = tuple(self.branches)

    def wants(self, category: str) -> bool:
        """Whether a non-BIDS category is wanted"""
        return self.categories is None or category in self.categories

    def keeps_subject(self, label: str) -> bool:
        return self.base.keeps_subject(label)

    def keeps(self, relpath: str, partial: bool = False) -> bool | None:
        """`Selection.keeps` on a path of the raw dataset"""
        return _fold_any(b.keeps(relpath, partial) for b in self.branches)

    def select(self, paths: Iterable[str]) -> list[str]:
        paths = list(paths)
        out = set()
        for branch in self.branches:
            out.update(branch.select(paths))
        return sorted(out)


def _restrict(base: Selection, predicate: Mapping) -> Selection:
    """`base` (subjects, sessions...) with the filters of a key"""
    kwargs = {
        "subs": base.subs, "exclude_subs": base.exclude_subs,
        "sessions": base.sessions, "tasks": base.tasks,
        "datatypes": base.datatypes, "suffixes": base.suffixes,
        "include": base.include, "exclude": base.exclude,
        "folders": base.folders, "entities": dict(base.entities),
    }
    for name, values in predicate.items():
        if name == "entities":
            kwargs["entities"] = {**kwargs["entities"], **values}
        else:
            kwargs[name] = values
    return Selection(**kwargs)


class KeyAliases:
    """
    Keys of a dataset.

    ```python
    KEYS = KeyAliases(
        {"meta": {"pheno"}, "raw": {"anat": {"T1w", "TSE"}}},
        bids={"T1w": {"suffixes": ["T1w"]},
              "TSE": {"suffixes": ["T2w"], "entities": {"acq": ["TSE"]}}},
    )
    ```

    Parameters
    ----------
    tree : dict | set | list
        Hierarchy of keys (as in `datasets/*/keys.py`): leaves are the
        members of sets and the dictionary keys that map to nothing.
        Empty group names (`""`) are transparent.
    bids : dict[str, dict]
        Leaf -> `Selection` filters (`suffixes`, `datatypes`,
        `entities`...). Leaves not listed are categories.
    """

    def __init__(
        self,
        tree: Mapping | Collection[str],
        bids: Mapping[str, Mapping] | None = None,
    ):
        self.groups: dict[str, set[str]] = {}
        self.leaves: set[str] = set()
        self._walk(tree)
        self.bids: dict[str, dict] = {k: dict(v) for k, v in
                                      (bids or {}).items()}
        unknown = set(self.bids) - self.leaves
        if unknown:
            raise ValueError(f"BIDS predicates for unknown keys {unknown}")

    def _walk(self, tree) -> set[str]:
        if isinstance(tree, str):
            tree = {tree}
        if not isinstance(tree, Mapping):
            self.leaves.update(tree)
            return set(tree)
        out = set()
        for name, sub in tree.items():
            below = self._walk(sub) if sub else {name}
            if not sub:
                self.leaves.add(name)
            elif name:
                self.groups[name] = self.groups.get(name, set()) | below
            out |= below
        return out

    @property
    def names(self) -> set[str]:
        """All keys (leaves and groups)"""
        return self.leaves | set(self.groups)

    @property
    def categories(self) -> set[str]:
        return self.leaves - set(self.bids)

    def _lookup(self, key: str) -> set[str]:
        key = str(getattr(key, "value", key))
        for exact in (True, False):
            for name in self.names:
                if name == key if exact else name.lower() == key.lower():
                    return self.groups.get(name, set()) | (
                        {name} if name in self.leaves else set()
                    )
        raise ValueError(
            f"Unknown key {key!r}. Choose from: {sorted(self.names)}"
        )

    def expand(self, keys: Iterable[str] | str) -> set[str]:
        """Leaves selected by keys (or groups of keys)"""
        if isinstance(keys, str):
            keys = [keys]
        out = set()
        for key in keys or ():
            out |= self._lookup(key)
        return out

    def selection(
        self,
        keys: Iterable[str] | str = (),
        exclude_keys: Iterable[str] | str = (),
        **filters,
    ) -> KeySelection:
        """
        Selection of a roast.

        Parameters
        ----------
        keys : list[str]
            Keys to convert (default: all)
        exclude_keys : list[str]
            Keys not to convert
        **filters
            `Selection` filters of the command line (`subs`,
            `exclude_subs`, `sessions`...)
        """
        filters.setdefault("folders", OPTIONAL_FOLDERS)
        base = Selection(**filters)
        wanted = self.expand(keys) if keys else set(self.leaves)
        wanted -= self.expand(exclude_keys)
        bids = wanted & set(self.bids)
        branches = None
        if bids != set(self.bids):
            branches = tuple(
                _restrict(base, self.bids[k]) for k in sorted(bids)
            )
        return KeySelection(
            base, branches, frozenset(wanted - set(self.bids)),
            frozenset(wanted),
        )
