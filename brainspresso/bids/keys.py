"""
Dataset keys (`--keys`, `--exclude-keys`) as selection predicates.

Each dataset names what can be converted with its own keys (IXI: `T1w`,
`dwi`, `meta`; OASIS-III: `TSE`, `PIB`, `fs`...). A `KeyAliases` maps
them to:

* **BIDS predicates**: `Selection` filters on the paths of the raw
  dataset (`{"suffixes": ["T2w"], "entities": {"acq": ["TSE"]}}`);
* **categories**: anything that is not a set of raw BIDS files
  (metadata, phenotypes, a derivative pipeline, source-only files).
  Roasters ask `ctx.wants(category)`. A category may also be mapped to
  folders of the dataset (`folders={"fs": ["derivatives/fs"]}`), which
  the roast policy then drops when the category is not wanted.

Keys may be grouped (`raw` -> `anat` -> `T1w`...). A group selects all
the keys below it.

Classification
--------------
A path of the raw dataset is **classified** (`KeyAliases.key_of`): its
key is the first BIDS key, in declared order, whose predicate keeps it
as a data file (so more specific keys come first: `TSE`, i.e. acq-TSE
T2w, before `T2w`). `KeySelection.keeps` then decides:

1. Dataset files (`README`, `participants.tsv`, `phenotype/`...) and
   subject/session tables (`*_sessions.tsv`, `*_scans.tsv`) are not
   described by keys: kept if the subject/session filters keep them.
2. Without a key filter (all BIDS keys wanted; excluding categories
   only does not count), every path is judged by the subject/session
   filters alone: files that no key describes are kept too.
3. With a key filter, a data file is kept iff its key is wanted;
   a metadata file (sidecar, companion) iff a wanted key may describe
   it: a key up to its own class (in declared order) whose predicate
   may apply to it under BIDS inheritance (`sub-01_T2w.json` applies to
   both `acq-TSE_T2w` and `T2w`; `acq-TSE_T2w.json` only to `TSE`).
4. Unclassified files with a key filter: metadata (`_events.tsv`,
   `_aslcontext.tsv`, a top-level `T1w.json`) follow rule 3, against
   all keys. Other files (`_sbref`, `_m0scan`, unknown suffixes)
   follow the key of their scans: they are kept iff a wanted key whose
   predicate, without its suffix filter, still says something (a
   datatype, an entity) keeps them (`func/*_sbref` follows `bold`,
   `perf/*_m0scan` follows the ASL keys). Otherwise they are dropped.
5. Claims (`partial=True`) are judged against the wanted keys only:
   a key set with no BIDS key (`--keys meta`) answers False for every
   claim of a subject folder, so that no archive is opened for them.
"""
from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass, field
from typing import Collection, Iterable, Mapping

from brainspresso.bids.select import (
    _DATA, _META, OPTIONAL_FOLDERS, BidsName, Selection, _applies,
    _classify,
)

# Companions that are tables of a subject or session (rule 1)
_TABLES = frozenset({"sessions", "scans"})


@dataclass(frozen=True)
class _Key:
    """Predicates of a BIDS key."""

    name: str
    bare: Selection             # the key's filters alone (classification)
    branch: Selection           # with the subject/session filters (claims)
    scans: Selection | None     # without suffix filter, if any is left
    scans_branch: Selection | None


def _restrict(base: Selection, predicate: Mapping) -> Selection:
    """Restrict `base` (subjects, sessions...) to the filters of a key."""
    predicate = dict(predicate)
    entities = {**base.entities, **predicate.pop("entities", {})}
    return dataclasses.replace(base, entities=entities, **predicate)


def _scans(predicate: Mapping) -> dict | None:
    """Predicate of the scans of a key (suffix filter removed) or None."""
    out = {k: v for k, v in predicate.items() if k != "suffixes" and v}
    return out or None


@dataclass
class KeySelection:
    """
    Selection of a roast.

    Subject/session filters, wanted keys and their predicates. See the
    module docstring for the rules.

    Attributes
    ----------
    base : Selection
        Subject and session filters (applied to every path, and alone
        to files outside the raw dataset: derivatives, sourcedata)
    predicates : tuple[_Key]
        BIDS keys, in declared order (classification)
    wanted : frozenset[str] | None
        Leaves wanted (BIDS keys and categories); None: everything
    filtered : bool
        Whether the BIDS keys are filtered (rules 3-5 apply)
    """

    base: Selection = field(default_factory=Selection)
    predicates: tuple = ()
    wanted: frozenset | None = None
    filtered: bool = False

    @property
    def keys(self) -> frozenset | None:
        """Leaves wanted, for the record (None: everything)."""
        return self.wanted

    def wants(self, key: str) -> bool:
        """Whether a key (a category, or a BIDS key) is wanted."""
        return self.wanted is None or key in self.wanted

    def keeps_subject(self, label: str) -> bool:
        """Whether a subject is kept (`base.keeps_subject`)."""
        return self.base.keeps_subject(label)

    # ------------------------------------------------------------------
    def key_of(self, relpath: str) -> str | None:
        """Key of a path of the raw dataset (first match), or None."""
        kind, name = _classify(str(relpath))
        if kind not in (_DATA, _META):
            return None
        index = self._index(name)
        return None if index is None else self.predicates[index].name

    def _index(self, name: BidsName) -> int | None:
        for i, key in enumerate(self.predicates):
            if key.bare._check(name, key.bare._filters(), missing=False):
                return i
        return None

    def _wanted_index(self, i: int) -> bool:
        return self.predicates[i].name in self.wanted

    def _describes(self, name: BidsName, kind: int) -> bool:
        """Rules 3-4 on a file that the base selection keeps."""
        index = self._index(name)
        if kind == _DATA:
            if index is not None:
                return self._wanted_index(index)
            return any(
                key.scans is not None and key.name in self.wanted
                and key.scans._check(name, key.scans._filters(), False)
                for key in self.predicates
            )
        # metadata: a wanted key, up to its own class, may describe it
        stop = len(self.predicates) if index is None else index + 1
        return any(
            key.name in self.wanted
            and key.bare._check(name, key.bare._filters(), missing=True)
            for key in self.predicates[:stop]
        )

    def keeps(self, relpath: str, partial: bool = False) -> bool | None:
        """`Selection.keeps` on a path of the raw dataset (or a claim)."""
        relpath = str(relpath)
        if partial:
            return self._keeps_claim(relpath)
        if not self.base.keeps(relpath):
            return False
        if not self.filtered:
            return True
        kind, name = _classify(relpath)
        if kind not in (_DATA, _META) or name.suffix in _TABLES:
            return True
        return self._describes(name, kind)

    def _keeps_claim(self, relpath: str) -> bool | None:
        verdict = self.base.keeps(relpath, partial=True)
        if verdict is False or not self.filtered:
            return verdict
        parts = [p for p in relpath.split("/") if p and p != "."]
        if not parts or not parts[0].startswith("sub-"):
            return verdict      # dataset files and folders
        wanted, other = [], []
        for key in self.predicates:
            (wanted if key.name in self.wanted else other).append(key)
        verdicts = [k.branch.keeps(relpath, partial=True) for k in wanted]
        # unclassified files that follow the scans of a wanted key
        verdicts += [
            None for k in wanted if k.scans is not None and
            k.scans_branch.keeps(relpath, partial=True) is not False
        ]
        if all(v is False for v in verdicts):
            return False
        if True in verdicts and all(
            k.branch.keeps(relpath, partial=True) is False for k in other
        ):
            return verdict
        return None

    def select(self, paths: Iterable[str]) -> list[str]:
        """Paths of a listing to keep (with metadata inheritance)."""
        kept = self.base.select(paths)
        if not self.filtered:
            return kept
        by_dir: dict[str, list[tuple[BidsName, bool]]] = {}
        out, meta = [], []
        for path in kept:
            kind, name = _classify(path)
            if kind == _DATA:
                keep = self._describes(name, kind)
                if keep:
                    out.append(path)
                parent = name._dir
                while True:
                    by_dir.setdefault(parent, []).append((name, keep))
                    if not parent:
                        break
                    parent = parent.rpartition("/")[0]
            elif kind == _META and name.suffix not in _TABLES:
                meta.append(name)
            else:
                out.append(path)
        for m in meta:
            applies = [keep for d, keep in by_dir.get(m._dir, ())
                       if _applies(m, d)]
            if any(applies) or (not applies and self._describes(m, _META)):
                out.append(m.path)
        return sorted(out)

    def fingerprint(self) -> str:
        """Stable description (state signatures)."""
        def norm(obj):
            if dataclasses.is_dataclass(obj):
                return norm(dataclasses.asdict(obj))
            if isinstance(obj, Mapping):
                return {str(k): norm(v) for k, v in sorted(obj.items())}
            if isinstance(obj, (set, frozenset)):
                return sorted(map(norm, obj))
            if isinstance(obj, (list, tuple)):
                return [norm(v) for v in obj]
            return obj
        return json.dumps(norm({
            "base": self.base, "wanted": self.wanted,
            "keys": [k.name for k in self.predicates],
        }), sort_keys=True, default=str)


class KeyAliases:
    """
    Keys of a dataset.

    ```python
    KEYS = KeyAliases(
        {"meta": {"pheno"}, "raw": {"anat": {"T1w", "TSE"}}},
        bids={"TSE": {"suffixes": ["T2w"], "entities": {"acq": ["TSE"]}},
              "T1w": {"suffixes": ["T1w"]}},
        folders={"pheno": ["phenotype"]},
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
        `entities`...), in classification order (first match wins).
        Leaves not listed are categories.
    folders : dict[str, list[str]]
        Category -> folders it writes, as paths of a flat BIDS view of
        the dataset (`phenotype`, `derivatives/<name>`; placed with
        `Layout.resolve`). The roast policy drops outputs in a folder whose
        categories are all unwanted.
    """

    def __init__(
        self,
        tree: Mapping | Collection[str],
        bids: Mapping[str, Mapping] | None = None,
        folders: Mapping[str, Iterable[str]] | None = None,
    ):
        self.groups: dict[str, set[str]] = {}
        self.leaves: set[str] = set()
        self._walk(tree)
        self.bids: dict[str, dict] = {k: dict(v) for k, v in
                                      (bids or {}).items()}
        unknown = set(self.bids) - self.leaves
        if unknown:
            raise ValueError(f"BIDS predicates for unknown keys {unknown}")
        self.folders: dict[str, tuple[str, ...]] = {
            k: tuple(v) for k, v in (folders or {}).items()
        }
        unknown = set(self.folders) - self.categories
        if unknown:
            raise ValueError(f"Folders of unknown categories {unknown}")

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
        """All keys (leaves and groups)."""
        return self.leaves | set(self.groups)

    @property
    def categories(self) -> set[str]:
        """Leaves that are categories (not BIDS keys)."""
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
        """Leaves selected by keys (or groups of keys)."""
        if isinstance(keys, str):
            keys = [keys]
        out = set()
        for key in keys or ():
            out |= self._lookup(key)
        return out

    def key_of(self, relpath: str) -> str | None:
        """
        Key of a path of the raw dataset.

        The first BIDS key, in declared order, whose predicate keeps it
        as a data file.
        """
        return self.selection().key_of(relpath)

    def selection(
        self,
        keys: Iterable[str] | str = (),
        exclude_keys: Iterable[str] | str = (),
        **filters,
    ) -> KeySelection:
        """
        Build the selection of a roast.

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
        predicates = []
        for name, pred in self.bids.items():
            scans = _scans(pred)
            predicates.append(_Key(
                name, Selection(**pred), _restrict(base, pred),
                Selection(**scans) if scans else None,
                _restrict(base, scans) if scans else None,
            ))
        predicates = tuple(predicates)
        return KeySelection(
            base, predicates, frozenset(wanted),
            filtered=not set(self.bids) <= wanted,
        )
