"""
Select a subset of a (remote) BIDS dataset from its file listing.

The selection works on relative paths only, so it can be applied to any
listing (OpenNeuro GraphQL, S3, local directory...) before downloading
anything.

Selection rules
---------------
1. *Data files* are files inside `sub-*/` that are not sidecars or tables
   (see `METADATA_EXTENSIONS`). They are kept if they pass all filters:
   subjects, sessions, datatypes, suffixes, tasks, include/exclude globs.
2. *Metadata files* (`.json`, `.tsv`, `.bval`, `.bvec`), wherever they are,
   are kept if they apply to at least one kept data file under the BIDS
   inheritance principle: their directory is an ancestor of (or equal to)
   the data file's directory, all their entities match the data file's
   entities, and their suffix is the same as the data file's suffix (or
   is a companion such as `events`, `scans`, `sessions`, `physio`).
3. Dataset-level files (`dataset_description.json`, `participants.tsv`,
   `README`, `CHANGES`, `.bidsignore`...), other top-level files that are
   not sidecars, and `phenotype/` are always kept. Top-level sidecars
   (`T1w.json`, `task-rest_bold.json`) follow rule 2.
4. `derivatives/`, `sourcedata/`, `code/`, `stimuli/` are skipped unless
   requested. Hidden files and folders (`.datalad/`, `.gitattributes`...)
   are always skipped.
"""
from __future__ import annotations

import fnmatch
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Iterable, Sequence

DATATYPES = frozenset({
    "anat", "func", "dwi", "fmap", "perf", "beh", "eeg", "meg", "ieeg",
    "pet", "micr", "nirs", "motion", "mrs",
})
METADATA_EXTENSIONS = frozenset({".json", ".tsv", ".bval", ".bvec"})
# Metadata whose suffix differs from the data file it describes
COMPANION_SUFFIXES = frozenset({
    "scans", "sessions", "events", "physio", "stim", "channels",
    "electrodes", "coordsystem", "aslcontext", "asllabeling", "beh",
})
OPTIONAL_FOLDERS = ("derivatives", "sourcedata", "code", "stimuli")
# Dataset-level files, always kept
TOP_LEVEL_FILES = frozenset({
    "dataset_description.json", "participants.tsv", "participants.json",
    "samples.tsv", "samples.json", "genetic_info.json",
    "README", "README.md", "README.txt", "README.rst",
    "CHANGES", "LICENSE", "CITATION.cff", ".bidsignore",
})


@dataclass(frozen=True)
class BidsName:
    """Parsed components of a BIDS path"""
    path: str
    entities: dict = field(hash=False)
    suffix: str | None
    extension: str
    datatype: str | None

    @classmethod
    def parse(cls, path: str) -> "BidsName":
        p = PurePosixPath(path)
        name = p.name
        stem, dot, ext = name.partition(".")
        extension = dot + ext
        parts = stem.split("_")
        entities = {}
        suffix = None
        for i, part in enumerate(parts):
            key, dash, value = part.partition("-")
            if dash and key and value:
                entities[key] = value
            elif i == len(parts) - 1:
                suffix = part
            else:
                # not a BIDS name
                entities, suffix = {}, None
                break
        parent = p.parent.name
        datatype = parent if parent in DATATYPES else None
        return cls(path, entities, suffix, extension, datatype)

    @property
    def is_metadata(self) -> bool:
        return self.extension in METADATA_EXTENSIONS

    @property
    def directory(self) -> PurePosixPath:
        return PurePosixPath(self.path).parent


@dataclass
class Selection:
    """
    Filters applied to a BIDS listing. Empty filters select everything.
    Subject and session labels are given without their `sub-`/`ses-`
    prefix. Globs are matched against the path relative to the dataset
    root.
    """
    subs: Sequence[str] = ()
    exclude_subs: Sequence[str] = ()
    sessions: Sequence[str] = ()
    datatypes: Sequence[str] = ()
    suffixes: Sequence[str] = ()
    tasks: Sequence[str] = ()
    include: Sequence[str] = ()
    exclude: Sequence[str] = ()
    folders: Sequence[str] = ()     # subset of OPTIONAL_FOLDERS

    def __post_init__(self):
        strip = (lambda x, p: str(x)[len(p):] if str(x).startswith(p)
                 else str(x))
        self.subs = {strip(s, "sub-") for s in self.subs}
        self.exclude_subs = {strip(s, "sub-") for s in self.exclude_subs}
        self.sessions = {strip(s, "ses-") for s in self.sessions}
        self.tasks = {strip(s, "task-") for s in self.tasks}
        self.datatypes = set(self.datatypes)
        self.suffixes = set(self.suffixes)
        self.folders = set(self.folders)
        unknown = self.folders - set(OPTIONAL_FOLDERS)
        if unknown:
            raise ValueError(f"Unknown optional folders: {unknown}")

    # ------------------------------------------------------------------
    def _glob(self, path: str, patterns) -> bool:
        return any(fnmatch.fnmatchcase(path, pat) for pat in patterns)

    def keep_data(self, name: BidsName) -> bool:
        ent = name.entities
        if self.subs and ent.get("sub") not in self.subs:
            return False
        if ent.get("sub") in self.exclude_subs:
            return False
        if self.sessions and ent.get("ses") not in self.sessions:
            return False
        if self.datatypes and name.datatype not in self.datatypes:
            return False
        if self.suffixes and name.suffix not in self.suffixes:
            return False
        if self.tasks and ent.get("task") not in self.tasks:
            return False
        if self.include and not self._glob(name.path, self.include):
            return False
        return True

    def keep_subject(self, label: str) -> bool:
        if self.subs and label not in self.subs:
            return False
        return label not in self.exclude_subs

    # ------------------------------------------------------------------
    def select(self, paths: Iterable[str]) -> list[str]:
        """Return the subset of `paths` to download (sorted)"""
        data, meta, always = [], [], []
        folder_files = []
        for path in paths:
            parts = PurePosixPath(path).parts
            if not parts or path in TOP_LEVEL_FILES:
                if parts:
                    always.append(path)
                continue
            if any(p.startswith(".") for p in parts):
                continue
            top = parts[0]
            if len(parts) > 1 and top in OPTIONAL_FOLDERS:
                if top in self.folders:
                    folder_files.append(path)
                continue
            name = BidsName.parse(path)
            if top.startswith("sub-") and len(parts) > 1:
                (meta if name.is_metadata else data).append(name)
            elif top == "phenotype":
                always.append(path)
            elif len(parts) == 1:
                if name.is_metadata and name.suffix:
                    # top-level sidecar: T1w.json, task-rest_bold.json
                    meta.append(name)
                else:
                    always.append(path)
            # other top-level folders (e.g. "models/") are skipped

        kept_data = [d for d in data if self.keep_data(d)]
        kept = set(d.path for d in kept_data)
        kept |= set(always)
        kept |= set(folder_files)

        # Inheritance closure: index kept data by directory ancestors
        by_dir: dict[PurePosixPath, list[BidsName]] = {}
        for d in kept_data:
            for parent in [d.directory, *d.directory.parents]:
                by_dir.setdefault(parent, []).append(d)
        for m in meta:
            for d in by_dir.get(m.directory, ()):
                if _applies(m, d):
                    kept.add(m.path)
                    break

        if self.exclude:
            protected = {"dataset_description.json"}
            kept = {p for p in kept
                    if p in protected or not self._glob(p, self.exclude)}
        return sorted(kept)


def _applies(meta: BidsName, data: BidsName) -> bool:
    """Whether a metadata file applies to a data file (inheritance)"""
    if meta.suffix != data.suffix and meta.suffix not in COMPANION_SUFFIXES:
        return False
    if meta.datatype and meta.datatype != data.datatype:
        return False
    return all(data.entities.get(k) == v for k, v in meta.entities.items())
