"""
Select a subset of a (remote) BIDS dataset from its file listing.

The selection works on relative paths only, so it can be applied to any
listing (OpenNeuro GraphQL, S3, local directory...) before downloading
anything. It knows no modality: what it needs from BIDS is the layout
(`sub-*/[ses-*/]<datatype>/<file>`), the inheritance principle and the
short table of companion files below (`COMPANIONS`).

Kinds of files
--------------
* *Data files* are the files of a subject tree that are not metadata.
  Unknown files are data files too, so that nothing is lost by default.
* *Directory-format recordings* (`*_meg.ds/`, `*.ome.zarr/`, `*.mefd/`,
  BTi/4D `*_meg/`...): any directory found inside a datatype directory.
  It is one data unit: its name is the BIDS name of all its files, which
  are kept or dropped together (filters and globs see the directory).
* *Sidecars*: `.json`, `.bval`, `.bvec` files. They describe the data
  files with the same suffix (`T1w.json`, `task-rest_bold.json`).
* *Companions*: metadata whose suffix differs from the data files they
  describe (`events`, `channels`, `electrodes`, `blood`, `scans`...), in
  any extension (`_blood.tsv`, `_physio.tsv.gz`, `_headshape.pos`...).
* *Dataset files* (`dataset_description.json`, `participants.tsv`,
  `README`, `.bidsignore`, other top-level files that are not metadata,
  `phenotype/`).

Selection rules
---------------
1. A data file is kept if it passes all filters: subjects, sessions,
   tasks and other entities, datatypes, suffixes, include globs.
2. A metadata file (sidecar or companion), wherever it is, is kept if it
   applies to at least one kept data file under the BIDS inheritance
   principle: its directory is the data file's directory or one of its
   ancestors, its datatype directory (if any) is the data file's, all its
   entities have the data file's values, and it has the data file's
   suffix (sidecars only). Entities that only qualify a companion
   (`space-` of `_electrodes.tsv`, `recording-` of `_blood.tsv`) are not
   compared. A metadata file that applies to no data file of the listing,
   or whose suffix is requested explicitly (`suffix=["events"]`), is
   judged as a data file instead (rule 1).
3. Dataset files are always kept.
4. `derivatives/`, `sourcedata/`, `code/`, `stimuli/`, `docs/`, `logs/`
   are skipped unless requested (`folders`). When requested, the subject
   and session filters still apply to their files. Hidden files and
   folders (`.datalad/`, `.gitattributes`...) are skipped, except inside
   directory-format recordings (`.zattrs` of an OME-Zarr).
5. Exclude globs are applied last, to every kept path but
   `dataset_description.json`. They do not change which metadata
   applies: `exclude=["*.nii.gz"]` keeps the sidecars of the images.

An empty selection therefore keeps every file of a valid BIDS dataset
outside of the optional folders.
"""
from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Collection, Iterable, Iterator, Mapping

DATATYPES = frozenset({
    "anat", "func", "dwi", "fmap", "perf", "beh", "eeg", "meg", "ieeg",
    "pet", "micr", "nirs", "motion", "mrs", "emg",
})
# Metadata that describe the data files with the same suffix
SIDECAR_EXTENSIONS = frozenset({".json", ".bval", ".bvec"})
# Companion suffix -> entities that only qualify the companion (the data
# files it describes do not have them, so they are not compared). From
# BIDS 1.11 (schema: `rules.files.raw`, `meta.associations`); checked
# against `bidsschematools` by the tests when it is installed.
COMPANIONS: dict[str, frozenset] = {
    # tables (subject and session level)
    "scans": frozenset(), "sessions": frozenset(),
    # task events and physiological recordings of any modality
    "events": frozenset(),
    "physio": frozenset({"recording"}),
    "physioevents": frozenset({"recording"}),
    "stim": frozenset({"recording"}),
    # EEG, iEEG, MEG, EMG, NIRS, motion
    "channels": frozenset(),
    "electrodes": frozenset({"space"}),
    "coordsystem": frozenset({"space"}),
    "optodes": frozenset(),
    "photo": frozenset(),
    "headshape": frozenset(),
    "markers": frozenset({"space"}),
    # ASL
    "aslcontext": frozenset(), "asllabeling": frozenset(),
    # PET: blood measurements, one per `recording-` (manual, autosampler)
    "blood": frozenset({"recording"}),
}
# `_meg` files that serve every recording of their session (MEG
# fine-calibration and crosstalk): (suffix, entity, value) -> own entities
SESSION_COMPANIONS = {
    ("meg", "acq", "crosstalk"): frozenset({"acq"}),
    ("meg", "acq", "calibration"): frozenset({"acq"}),
}
# Entities whose values are indices: `run-01` is `run-1`
INDEX_ENTITIES = frozenset({"run", "echo", "flip", "inv", "chunk", "split"})
OPTIONAL_FOLDERS = (
    "derivatives", "sourcedata", "code", "stimuli", "docs", "logs",
)
# Dataset-level files, always kept
TOP_LEVEL_FILES = frozenset({
    "dataset_description.json", "participants.tsv", "participants.json",
    "samples.tsv", "samples.json", "genetic_info.json",
    "README", "README.md", "README.txt", "README.rst",
    "LICENSE", "LICENSE.md", "LICENSE.txt", "LICENSE.rst",
    "CHANGES", "CITATION.cff", ".bidsignore",
})

_DIR_ENTITY = re.compile(r"(sub|ses)-([A-Za-z0-9]+)")

#: Former keyword names of the filters -> their names (those of the
#: BIDS entities and of the command line): a `TypeError` names the new
#: one (`check_former_names`)
FORMER_NAMES = {
    "subs": "sub", "exclude_subs": "exclude_sub", "sessions": "ses",
    "datatypes": "datatype", "suffixes": "suffix", "tasks": "task",
}


def check_former_names(function: str, kwargs) -> None:
    """
    Refuse a former name of a filter, naming the new one.

    Parameters
    ----------
    function : str
        Name of the function, in the message
    kwargs : Collection[str]
        Its keyword arguments

    Raises
    ------
    TypeError
        `<function>() got an unexpected keyword argument 'subs' (use
        'sub')`, for the first former name (in alphabetical order)
    """
    old = sorted(FORMER_NAMES.keys() & set(kwargs))
    if old:
        raise TypeError(f"{function}() got an unexpected keyword argument "
                        f"{old[0]!r} (use {FORMER_NAMES[old[0]]!r})")


@dataclass(frozen=True)
class BidsName:
    """
    Parsed components of a BIDS path.

    `entities` are those of the file name, completed with the subject and
    session of its directories. Files inside a directory-format recording
    take the components of the recording (`unit` is its path).
    """

    path: str
    entities: dict = field(hash=False)
    suffix: str | None
    extension: str
    datatype: str | None
    unit: str | None = None

    @classmethod
    def parse(cls, path: str) -> "BidsName":
        """Parse a path relative to a BIDS dataset."""
        parts = _parts(path)
        if not parts:
            return cls(path, {}, None, "", None)
        dirs = {}
        i = 0                    # index of the first non sub/ses directory
        for key in ("sub", "ses"):
            match = _DIR_ENTITY.fullmatch(parts[i])
            if i == len(parts) - 1 or not match or match[1] != key:
                break
            dirs[key] = match[2]
            i += 1
        datatype = unit = None
        name = parts[-1]
        if "sub" in dirs and i < len(parts) - 1:
            datatype = parts[i]
            if i + 1 < len(parts) - 1:
                # file of a directory-format recording
                name = parts[i + 1]
                unit = "/".join(parts[:i + 2])
        elif len(parts) > 1 and parts[-2] in DATATYPES:
            datatype = parts[-2]
        entities, suffix, extension = _parse_filename(name)
        for key, value in dirs.items():
            entities.setdefault(key, value)
        return cls(path, entities, suffix, extension, datatype, unit)

    @property
    def companion(self) -> frozenset | None:
        """Own entities if this is a companion file, else None."""
        if self.unit is not None:
            return None
        if self.suffix in COMPANIONS:
            return COMPANIONS[self.suffix]
        for (suffix, key, value), own in SESSION_COMPANIONS.items():
            if self.suffix == suffix and self.entities.get(key) == value:
                return own
        return None

    @property
    def is_metadata(self) -> bool:
        """Whether this is a metadata file (sidecar or companion)."""
        if self.unit is not None:
            return False
        return (self.extension in SIDECAR_EXTENSIONS
                or self.companion is not None)

    @property
    def directory(self) -> PurePosixPath:
        """Directory that contains the file (or its recording)."""
        return PurePosixPath(self.unit or self.path).parent

    @property
    def _dir(self) -> str:
        """`directory` as a string ("" for the dataset root)."""
        return "/".join(_parts(self.unit or self.path)[:-1])


def _parts(path: str) -> tuple[str, ...]:
    """Components of a relative POSIX path (faster than `PurePosixPath`)."""
    return tuple(p for p in path.split("/") if p and p != ".")


def _parse_filename(name: str) -> tuple[dict, str | None, str]:
    stem, dot, ext = name.partition(".")
    parts = stem.split("_")
    entities, suffix = {}, None
    for i, part in enumerate(parts):
        key, dash, value = part.partition("-")
        if dash and key and value:
            entities[key] = value
        elif i == len(parts) - 1:
            suffix = part or None
        else:
            # not a BIDS name
            entities, suffix = {}, None
            break
    return entities, suffix, dot + ext


def _norm(key: str, value) -> str:
    value = str(value)
    if key in INDEX_ENTITIES and value.isdigit():
        return str(int(value))
    return value


def _strip(value, prefix: str) -> str:
    value = str(value)
    return value[len(prefix):] if value.startswith(prefix) else value


def _fold(verdicts: Iterable[bool | None]) -> bool | None:
    """Fold verdicts: False if any is, else None if any is, else True."""
    out = True
    for v in verdicts:
        if v is False:
            return False
        if v is None:
            out = None
    return out


def _hidden(parts: tuple[str, ...]) -> bool:
    """Hidden file or folder, outside of directory-format recordings."""
    for part in parts:
        if part.startswith("."):
            return True
        if "-" not in part or "." not in part:
            continue
        entities, _, extension = _parse_filename(part)
        if entities and extension:
            # BIDS name with an extension: from here on we are inside a
            # recording (`*.ome.zarr/.zattrs`) or this is the file itself
            return False
    return False


# kinds of paths
_SKIP, _DATASET, _FOLDER, _DATA, _META = range(5)


def _classify(path: str) -> tuple[int, BidsName | None]:
    parts = _parts(path)
    if not parts:
        return _SKIP, None
    if path in TOP_LEVEL_FILES:
        return _DATASET, None
    if _hidden(parts):
        return _SKIP, None
    top = parts[0]
    if len(parts) > 1 and top in OPTIONAL_FOLDERS:
        return _FOLDER, None
    name = BidsName.parse(path)
    if top.startswith("sub-") and len(parts) > 1:
        return (_META if name.is_metadata else _DATA), name
    if top == "phenotype":
        return _DATASET, None
    if len(parts) == 1:
        if name.is_metadata and name.suffix:
            # top-level metadata: T1w.json, task-rest_bold.json
            return _META, name
        return _DATASET, None
    # other top-level folders (e.g. "models/") are skipped
    return _SKIP, None


@dataclass
class Selection:
    """
    Filters applied to a BIDS dataset. Empty filters select everything.

    Parameters
    ----------
    sub, exclude_sub, ses, task : list[str]
        Shortcuts for `entities={"sub": ..., "ses": ..., "task": ...}`
        (subjects to keep or drop, sessions and tasks to keep). Labels are
        given with or without their `sub-`/`ses-`/`task-` prefix. Named
        after the BIDS entities, as the command line (`--sub`...).
    datatype : list[str]
        Datatype directories to keep (`anat`, `eeg`, `micr`...).
    suffix : list[str]
        Suffixes of the data files to keep (`T1w`, `bold`, `eeg`...).
    include, exclude : list[str]
        Glob patterns matched against the path relative to the dataset
        root (`*` matches `/`). A data file is kept only if it matches an
        `include` pattern; any kept file that matches an `exclude`
        pattern is dropped. Files of a directory-format recording are
        matched through the path of the recording.
    folders : list[str]
        Optional top-level folders to keep (see `OPTIONAL_FOLDERS`).
    entities : dict[str, list[str]]
        Values to keep, per entity key as it appears in file names, e.g.
        `{"acq": ["mprage"], "run": ["1", "2"]}`. Index entities (`run`,
        `echo`...) compare as numbers: `"1"` matches `run-01`.
    """

    sub: Collection[str] = ()
    exclude_sub: Collection[str] = ()
    ses: Collection[str] = ()
    datatype: Collection[str] = ()
    suffix: Collection[str] = ()
    task: Collection[str] = ()
    include: Collection[str] = ()
    exclude: Collection[str] = ()
    folders: Collection[str] = ()     # subset of OPTIONAL_FOLDERS
    entities: Mapping[str, Collection[str]] = field(default_factory=dict)

    def __post_init__(self):
        """Normalize the entity filters."""
        entities = {}
        for key, values in dict(self.entities or {}).items():
            if isinstance(values, str):
                values = [values]
            values = {_strip(v, f"{key}-") for v in values}
            if values:
                entities[key] = values
        for key in ("sub", "ses", "task"):
            if key not in entities:
                continue
            if getattr(self, key):
                raise ValueError(
                    f"`{key}` and `entities['{key}']` are the same "
                    f"filter: give only one of them"
                )
            setattr(self, key, entities.pop(key))
        self.entities = entities
        self.sub = {_strip(s, "sub-") for s in self.sub}
        self.exclude_sub = {_strip(s, "sub-") for s in self.exclude_sub}
        self.ses = {_strip(s, "ses-") for s in self.ses}
        self.task = {_strip(s, "task-") for s in self.task}
        self.datatype = set(self.datatype)
        self.suffix = set(self.suffix)
        self.folders = set(self.folders)
        unknown = self.folders - set(OPTIONAL_FOLDERS)
        if unknown:
            raise ValueError(f"Unknown optional folders: {unknown}")

    # ------------------------------------------------------------------
    #   Per-path predicates
    # ------------------------------------------------------------------
    def keeps_subject(self, label: str) -> bool:
        """
        Whether files of subject `label` (with or without `sub-`) may be kept.

        Use it to prune a listing early (e.g. not to list or extract
        the members of a subject that is dropped anyway).
        """
        label = _strip(label, "sub-")
        if self.sub and label not in self.sub:
            return False
        return label not in self.exclude_sub

    keep_subject = keeps_subject      # backward compatibility

    def keeps(self, relpath: str, partial: bool = False) -> bool | None:
        """
        Whether a path relative to the BIDS dataset root is kept.

        This needs no listing.

        With `partial=False`, `relpath` is the path of a file (or of a
        directory-format recording):

        * data files and recordings: True or False, as in `select`;
        * metadata (sidecars, companions): True if it *may* apply to a
          kept data file (its own entities, datatype and suffix do not
          contradict the filters). Whether such a data file exists can
          only be known from a listing (`select`);
        * dataset files: True; hidden files and skipped folders: False.

        With `partial=True`, `relpath` is a *claim*: an output directory
        (`sub-01`, `sub-01/ses-2/anat`, `derivatives/fs/sub-01`),
        optionally followed by the known part of the file name, as
        key-value pairs and possibly the suffix
        (`sub-01/ses-2/anat/sub-01_ses-2_acq-mprage`). Anything not in the
        claim is unknown. Returns False if no file of the claim can be
        kept, True if every file that it may describe is kept, and None
        if it depends on what is unknown.
        """
        relpath = str(PurePosixPath(str(relpath)))
        relpath = "" if relpath == "." else relpath
        if partial:
            return self._keeps_claim(relpath)
        kind, name = _classify(relpath)
        if kind == _SKIP:
            return False
        if kind == _FOLDER:
            ok = relpath.split("/")[0] in self.folders \
                and self._keeps_folder_file(relpath)
        elif kind == _DATA:
            ok = self._keeps_data(name, self._filters())
        elif kind == _META:
            ok = self._check(name, self._filters(), missing=True)
        else:
            ok = True
        target = (name.unit if name else None) or relpath
        return bool(ok) and not self._excluded(target)

    def _keeps_claim(self, relpath: str) -> bool | None:
        parts = PurePosixPath(relpath).parts
        if not parts:
            return self._unless_excluded(True)
        top = parts[0]
        if top in OPTIONAL_FOLDERS:
            if top not in self.folders:
                return False
            return self._unless_excluded(
                self._keeps_folder_file(relpath, missing=None)
            )
        if not top.startswith("sub-"):
            # dataset files and folders: decided by the path alone
            return self.keeps(relpath)
        if _hidden(parts):
            return False
        dirs, datatype, fragment = {}, None, None
        for part in parts:
            match = _DIR_ENTITY.fullmatch(part)
            if match and datatype is None:
                dirs.setdefault(match[1], match[2])
            elif datatype is None and not re.search(r"[-_.]", part):
                datatype = part
            else:
                fragment = part
                break
        entities, suffix, extension = _parse_filename(fragment or "")
        name = BidsName(relpath, {**dirs, **entities}, suffix, extension,
                        datatype)
        verdict = self._check(name, self._filters(), missing=None)
        return self._unless_excluded(verdict)

    def _unless_excluded(self, verdict: bool | None) -> bool | None:
        """Leave a claim undecided when exclude globs could match it."""
        if verdict is True and self.exclude:
            return None
        return verdict

    # ------------------------------------------------------------------
    #   Listing
    # ------------------------------------------------------------------
    def select(self, paths: Iterable[str]) -> list[str]:
        """Return the subset of `paths` to keep (sorted)."""
        filters = self._filters()
        data, meta, kept = [], [], set()
        units = {}      # path -> path of its recording
        for path in paths:
            kind, name = _classify(path)
            if kind == _DATA:
                data.append(name)
                if name.unit:
                    units[path] = name.unit
            elif kind == _META:
                meta.append(name)
            elif kind == _DATASET:
                kept.add(path)
            elif kind == _FOLDER:
                if path.split("/")[0] in self.folders and \
                        self._keeps_folder_file(path):
                    kept.add(path)

        # Index all data files by directory ancestors, with their verdict
        by_dir: dict[str, list[tuple[BidsName, bool]]] = {}
        promoted: dict[str, list[tuple[BidsName, bool]]] = {}

        def index(d: BidsName, keep: bool, by_dir=by_dir):
            parent = d._dir
            while True:
                by_dir.setdefault(parent, []).append((d, keep))
                if not parent:
                    break
                parent = parent.rpartition("/")[0]

        for d in data:
            keep = self._keeps_data(d, filters)
            if keep:
                kept.add(d.path)
            index(d, keep)

        # Inheritance closure. Metadata that describe no data file, or
        # that are requested explicitly, are judged as data files; those
        # kept then take part in the closure (e.g. `sessions.tsv`
        # of a subject with only `_blood.tsv` selected).
        pending = []
        for m in meta:
            verdict = _inherits(m, by_dir)
            if verdict:
                kept.add(m.path)
            elif (verdict is None or m.suffix in self.suffix) \
                    and self._keeps_data(m, filters):
                kept.add(m.path)
                index(m, True, promoted)
            else:
                pending.append(m)
        for m in pending if promoted else ():
            if _inherits(m, promoted):
                kept.add(m.path)

        if self.exclude:
            kept = {p for p in kept if not self._excluded(units.get(p, p))}
        return sorted(kept)

    # ------------------------------------------------------------------
    #   Internals
    # ------------------------------------------------------------------
    def _filters(self) -> dict[str, set[str]]:
        """Entity filters (shortcuts included), with normalised values."""
        filters = {"sub": self.sub, "ses": self.ses,
                   "task": self.task, **self.entities}
        return {k: {_norm(k, v) for v in values}
                for k, values in filters.items() if values}

    def _glob(self, path: str, patterns) -> bool:
        return any(fnmatch.fnmatchcase(path, pat) for pat in patterns)

    def _excluded(self, path: str) -> bool:
        return bool(self.exclude) and self._glob(path, self.exclude) \
            and path != "dataset_description.json"

    def _keeps_data(self, name: BidsName, filters) -> bool:
        return bool(self._check(name, filters, missing=False))

    def _check(self, name: BidsName, filters, missing: bool | None):
        """
        Evaluate all filters (but exclude globs) on a name.

        `missing` is
        the verdict of a filter on something the name does not have:
        False for data files, True for metadata (it applies to files with
        any value), None for claims (unknown).
        """
        return _fold(self._verdicts(name, filters, missing))

    def _verdicts(
        self, name: BidsName, filters, missing
    ) -> Iterator[bool | None]:
        # judged as a data file (missing=False), a companion is one too
        own = name.companion if missing is not False else None
        entities = name.entities
        for key, values in filters.items():
            if own and key in own:
                continue
            value = entities.get(key)
            yield missing if value is None else _norm(key, value) in values
        sub = entities.get("sub")
        if sub is not None:
            yield sub not in self.exclude_sub
        elif self.exclude_sub and missing is None:
            yield None
        if self.datatype:
            yield missing if name.datatype is None else \
                name.datatype in self.datatype
        if self.suffix and own is None:
            yield missing if name.suffix is None else \
                name.suffix in self.suffix
        if self.include:
            yield self._glob(name.unit or name.path, self.include) \
                if missing is False else missing

    def _keeps_folder_file(self, path: str, missing=True) -> bool | None:
        """
        Apply subject and session filters to files of optional folders.

        Files of optional folders: e.g.
        `derivatives/<pipeline>/sub-01/...`. Files that do not belong to
        a subject (or session) are kept (`missing`).
        """
        entities = {}
        for key, value in re.findall(
            r"(?:^|[/_])(sub|ses)-([A-Za-z0-9]+)", path
        ):
            entities.setdefault(key, value)
        verdicts = []
        if "sub" in entities:
            verdicts.append(self.keeps_subject(entities["sub"]))
        elif self.sub or self.exclude_sub:
            verdicts.append(missing)
        if self.ses:
            verdicts.append(entities["ses"] in self.ses
                            if "ses" in entities else missing)
        return _fold(verdicts)


def _inherits(meta: BidsName, by_dir) -> bool | None:
    """
    Whether a metadata file applies to kept data files.

    True if it applies to a kept data file, False if it applies only to
    dropped ones, None if it applies to none.
    """
    verdict = None
    for data, keep in by_dir.get(meta._dir, ()):
        if data is not meta and _applies(meta, data):
            if keep:
                return True
            verdict = False
    return verdict


def applies(meta: str, data: str) -> bool:
    """
    Whether a metadata file applies to a file (inheritance principle).

    Parameters
    ----------
    meta : str
        Path of a sidecar or companion, relative to the BIDS dataset
    data : str
        Path of a data file (or of another sidecar: its suffix and
        entities are those of the data files it describes), relative to
        the BIDS dataset

    Returns
    -------
    bool
        Whether `meta` is in a folder of `data` (or its own) and its
        suffix and entities match (see the module docstring, rule 2)
    """
    m, d = BidsName.parse(meta), BidsName.parse(data)
    folder = m._dir
    if folder and d._dir != folder and not d._dir.startswith(folder + "/"):
        return False
    return _applies(m, d)


def _applies(meta: BidsName, data: BidsName) -> bool:
    """Whether a metadata file applies to a data file (inheritance)."""
    own = meta.companion
    if own is None:
        # sidecar: same suffix
        if meta.suffix != data.suffix:
            return False
        own = ()
    if meta.datatype and meta.datatype != data.datatype:
        return False
    entities = data.entities
    for key, value in meta.entities.items():
        if key in own or entities.get(key) == value:
            continue
        if key not in INDEX_ENTITIES or key not in entities \
                or _norm(key, entities[key]) != _norm(key, value):
            return False
    return True
