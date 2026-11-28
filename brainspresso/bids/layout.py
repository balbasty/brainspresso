"""
On-disk layout of a local dataset (`<root>/<DS>/`).

Every module that reads or writes a dataset takes its paths from a
`Layout`; none builds `root / "rawbids"` (or any other fixed folder)
itself. Three layouts exist (see `docs/design/architecture.md`, §2):

**study** (BIDS >= 1.10.1 `DatasetType: "study"`, `rawbids/`)::

    <DS>/
      dataset_description.json     DatasetType "study"
      sourcedata/                  harvested files
      rawbids/                     raw BIDS dataset (DatasetType "raw")
        dataset_description.json  participants.tsv  README  phenotype/
        sub-*/  .bidsignore
      derivatives/<pipeline>/      SourceDatasets: [{"URL": "../../rawbids/"}]
      .brainspresso/               manifest, pristine tables, per-task state

**flat** (classic BIDS: the raw dataset is the root)::

    <DS>/
      dataset_description.json     DatasetType "raw"
      participants.tsv  README  phenotype/  sub-*/  .bidsignore
      sourcedata/
      derivatives/<pipeline>/      SourceDatasets: [{"URL": "../../"}]
      .brainspresso/

**sites**: one dataset per site (each a study or flat layout) under a
parent folder that is not a BIDS dataset. The parent holds the README,
a merged `participants.tsv` and its own `.brainspresso/` (which records
the layout of each site); each site has its own state folder.

The layout is chosen with `--layout`, recorded in
`<state>/manifest.json`, and detected by later commands
(`Layout.detect`). `resolve_layout` combines both and fails if they
conflict.
"""
from __future__ import annotations

import json
import os
import posixpath
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Iterable, Iterator, Literal

from brainspresso.actions.locks import (
    LOCKS_DIR,
    forget_datasets,
    is_state_folder,
)
from brainspresso.utils.path import sorted_paths

lg = getLogger(__name__)

LayoutChoice = Literal["study", "flat"]
SitesLayoutChoice = Literal["study", "flat", "sites"]
#: Values of `--layout` (`sites:<kind>`: the layout of each site)
LayoutValue = Literal["study", "flat", "sites", "sites:study", "sites:flat"]
#: `--layout` values of a dataset with one layout per site
ALL_LAYOUTS = ("study", "flat", "sites", "sites:study", "sites:flat")

#: Layout of new datasets (when `--layout` is not given)
DEFAULT_LAYOUT: LayoutChoice = "study"

STATE_DIR = ".brainspresso"      # state folder, in each dataset
MANIFEST = "manifest.json"       # in the state folder
TASKS_DIR = "tasks"              # per-task state files, in the state folder
ORIG_DIR = "orig"                # pristine tables, in the state folder
GENERATED_DIR = "generated"      # generated metadata (hosted datasets)
REPLACED_DIR = "replaced"        # backups of replaced files (`update`)
RAW_DIR = "rawbids"              # raw dataset of a study
SOURCE_DIR = "sourcedata"
DERIVATIVES_DIR = "derivatives"
DESCRIPTION = "dataset_description.json"
README = "README"                # README of a study (`study_readme`)

BIDS_VERSION = "1.11.2"          # version written in generated descriptions

# Fields of the raw description copied into the study description
_STUDY_FIELDS = (
    "License", "Authors", "Acknowledgements", "HowToAcknowledge",
    "Funding", "EthicsApprovals", "ReferencesAndLinks", "DatasetDOI",
)
# Folders of a sites parent that are never sites
_NOT_SITES = (SOURCE_DIR, DERIVATIVES_DIR, STATE_DIR, "phenotype", "code")


class LayoutError(ValueError):
    """Requested layout conflicts with the layout of an existing dataset."""


def _path(path: str | os.PathLike) -> Path:
    return Path(os.fspath(path))


def read_manifest(state: str | os.PathLike) -> dict | None:
    """
    Read `<state>/manifest.json`.

    Parameters
    ----------
    state : path
        State folder of a dataset

    Returns
    -------
    dict | None
        Its content (None if missing or invalid)
    """
    path = Path(state) / MANIFEST
    if not path.exists():
        return None
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):       # (also not UTF-8)
        lg.warning(f"Could not parse {path}")
        return None
    return manifest if isinstance(manifest, dict) else None


def parse_layout(value: str | None) -> tuple[str | None, str | None]:
    """
    Split a `--layout` value.

    Parameters
    ----------
    value : {"study", "flat", "sites", "sites:study", "sites:flat"} | None
        `sites` is one dataset per site, each with the layout of the
        existing sites (else `DEFAULT_LAYOUT`); `sites:<kind>` sets it.

    Returns
    -------
    kind : {"study", "flat", "sites"} | None
        Layout of the dataset
    child_kind : {"study", "flat"} | None
        Layout of each site (`sites:<kind>` only)

    Raises
    ------
    LayoutError
        If the value is not a layout
    """
    if value is None:
        return None, None
    value = str(getattr(value, "value", value))
    kind, _, child = value.partition(":")
    if kind not in ("study", "flat", "sites") or (
        child and (kind != "sites" or child not in ("study", "flat"))
    ):
        raise LayoutError(
            f"Unknown layout {value!r}. Choose from: "
            f"{', '.join(ALL_LAYOUTS)}"
        )
    return kind, child or None


def relative_url(target: Path, start: Path) -> str:
    """Relative URL (POSIX, trailing slash) from `start` to `target`."""
    rel = PurePosixPath(Path(os.path.relpath(target, start)).as_posix())
    return str(rel) + "/" if str(rel) != "." else "./"


def _read_json(path: Path) -> dict | None:
    try:
        out = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return out if isinstance(out, dict) else None


def _same(path: str | os.PathLike) -> str:
    """Key of a path, to compare files (absolute, case as the OS)."""
    return os.path.normcase(os.path.abspath(path))


def _write_json(path: Path, obj: dict) -> bool:
    """Write a JSON file if its content changes. Returns whether written."""
    if path.exists() and _read_json(path) == obj:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(description_bytes(obj))
    return True


class Layout:
    """
    Paths of a local dataset.

    Attributes
    ----------
    kind : {"study", "flat", "derivative", "sites"}
    root : Path
        Dataset folder (`<root>/<DS>`, or `<DS>/derivatives/<name>` for
        a derivative sub-layout)
    bids : Path
        Root of the BIDS dataset that this layout describes (`raw` for
        study and flat layouts, `root` for derivatives)
    raw : Path
        Raw BIDS dataset (for a derivative: the raw dataset it derives
        from; for sites: the parent folder, which holds the README and
        merged participants table)
    source : Path
        Source data (`sourcedata/`)
    state : Path
        State folder (`.brainspresso/`): manifest, pristine tables
        (`orig/`), per-task state (`tasks/`)
    derivatives : Path
        Folder that holds derivative datasets
    phenotype : Path
        `phenotype/` folder of the BIDS dataset
    bidsignore : Path
        `.bidsignore` of the BIDS dataset
    """

    kind: str = ""

    def __init__(self, root: str | os.PathLike):
        self.root = _path(root)

    # ------------------------------------------------------------------
    #   Paths
    # ------------------------------------------------------------------
    @property
    def raw(self) -> Path:
        """Raw BIDS dataset."""
        raise NotImplementedError

    @property
    def bids(self) -> Path:
        """Root of the BIDS dataset that this layout describes."""
        return self.raw

    @property
    def source(self) -> Path:
        """Source data (`sourcedata/`)."""
        return self.root / SOURCE_DIR

    @property
    def state(self) -> Path:
        """State folder (`.brainspresso/`)."""
        return self.root / STATE_DIR

    @property
    def derivatives(self) -> Path:
        """Folder that holds derivative datasets."""
        return self.root / DERIVATIVES_DIR

    @property
    def phenotype(self) -> Path:
        """`phenotype/` folder of the BIDS dataset."""
        return self.bids / "phenotype"

    @property
    def bidsignore(self) -> Path:
        """`.bidsignore` of the BIDS dataset."""
        return self.bids / ".bidsignore"

    @property
    def description(self) -> Path:
        """`dataset_description.json` of the BIDS dataset."""
        return self.bids / DESCRIPTION

    @property
    def manifest(self) -> Path:
        """Manifest of the dataset."""
        return self.state / MANIFEST

    @property
    def tasks(self) -> Path:
        """Folder of the per-task states."""
        return self.state / TASKS_DIR

    def original(self, path: str | os.PathLike) -> Path:
        """Pristine copy of a (trimmed) table, in `<state>/orig/`."""
        return self.state / ORIG_DIR / self._relpath(path)

    def generated(self, path: str | os.PathLike) -> Path:
        """
        Return the base of a generated file, in `<state>/generated/`.

        Hosted datasets (`roast.hosted`) keep there the files that their
        harvest generates (merged tables, curated descriptions, sidecars
        the host lacks), which their metadata roast reads as it reads
        the pristine copies of the hosted metadata in `sourcedata/`.

        Parameters
        ----------
        path : str | PathLike
            The file (absolute, or relative to `root`)

        Returns
        -------
        Path
        """
        return self.state / GENERATED_DIR / self._relpath(path)

    def replaced(self, path: str | os.PathLike) -> Path:
        """
        Return the backup of a replaced file, in `<state>/replaced/`.

        `roast --if-exists update` copies there a file that has no
        recorded hash and differs from what roast writes, before it
        replaces it (`bids.harvest.MIGRATION_RULE`).

        Parameters
        ----------
        path : str | PathLike
            The file (absolute, or relative to `root`)

        Returns
        -------
        Path
        """
        return self.state / REPLACED_DIR / self._relpath(path)

    def ensure_state(self) -> Path:
        """
        Create the state folder (`.brainspresso/`) if it does not exist.

        Every command that writes into the dataset calls it before it
        takes any lock: the state folder holds the lock files of the
        dataset, and a lock file moves when it appears
        (`actions.locks`).

        Returns
        -------
        Path
            The state folder
        """
        state = Path(self.state)
        if not is_state_folder(self.root):
            # new, or only a config file (`.brainspresso/config.toml`):
            # `locks/` makes it a dataset (`actions.locks.is_state_folder`)
            # before any lock is taken
            (state / LOCKS_DIR).mkdir(parents=True, exist_ok=True)
            # a nearer dataset for the folders already cached
            forget_datasets()
        return state

    # ------------------------------------------------------------------
    #   Sub-layouts
    # ------------------------------------------------------------------
    def derivative(self, name: str) -> "DerivativeLayout":
        """Layout of the derivative dataset `derivatives/<name>/`."""
        return DerivativeLayout(self, name)

    def list_derivatives(self) -> list["DerivativeLayout"]:
        """List the derivative datasets that exist on disk."""
        if not self.derivatives.is_dir():
            return []
        return [self.derivative(p.name)
                for p in sorted_paths(self.derivatives.iterdir())
                if p.is_dir()]

    def datasets(self) -> list["Layout"]:
        """Datasets to process one by one (itself, or each site)."""
        return [self]

    # ------------------------------------------------------------------
    #   Mapping
    # ------------------------------------------------------------------
    def _relpath(self, path: str | os.PathLike) -> PurePosixPath:
        """POSIX path relative to `root` (of a path under it, or not)."""
        path = _path(path)
        try:
            return PurePosixPath(path.relative_to(self.root).as_posix())
        except ValueError:
            pass
        # (`anchor`: also a rooted path without a drive on Windows)
        if path.is_absolute() or path.anchor or ".." in path.parts:
            raise ValueError(f"{path} is not inside {self.root}")
        return PurePosixPath(path.as_posix())

    def relative(self, path: str | os.PathLike) -> tuple[Path, str]:
        """
        Map a path to its BIDS dataset and its path inside it.

        Parameters
        ----------
        path : str | PathLike
            Absolute, or relative to `root`

        Returns
        -------
        dataset : Path
            The BIDS dataset that contains it (raw, a derivative, a
            site, or the study dataset)
        relpath : str
            Its POSIX path inside it ("." for the root)
        """
        rel = self._relpath(path)
        parts = rel.parts
        if len(parts) >= 2 and parts[0] == DERIVATIVES_DIR:
            return self.derivative(parts[1]).relative(self.root / rel)
        return self._relative_local(rel)

    def _relative_local(self, rel: PurePosixPath) -> tuple[Path, str]:
        return self.root, str(rel)

    def resolve(self, relpath: str) -> Path:
        """
        Local path of a file, given its path in a flat BIDS view.

        Paths of the flat view of the dataset are those listed by BIDS
        hosts (`sub-*/...`, `participants.tsv`, `sourcedata/...`,
        `derivatives/<name>/...`).
        """
        parts = PurePosixPath(relpath).parts
        if parts and parts[0] == SOURCE_DIR:
            return self.source.joinpath(*parts[1:])
        if len(parts) > 2 and parts[0] == DERIVATIVES_DIR:
            return self.derivative(parts[1]).resolve(
                "/".join(parts[2:])
            )
        if parts and parts[0] == DERIVATIVES_DIR:
            return self.derivatives.joinpath(*parts[1:])
        return self.raw.joinpath(*parts)

    # ------------------------------------------------------------------
    #   Descriptions
    # ------------------------------------------------------------------
    def dataset_description(self, kind: str | None = None) -> dict:
        """
        Build the minimal description of a dataset of this layout.

        Parameters
        ----------
        kind : {"study", "raw", "derivative"} | None
            Kind of dataset (default: this layout's BIDS dataset)

        Returns
        -------
        dict
            `Name`, `BIDSVersion`, `DatasetType` (and the credits of
            the raw description for a study)
        """
        kind = kind or ("derivative" if self.kind == "derivative" else
                        "study" if self.kind == "study" else "raw")
        raw = _read_json(self.raw / DESCRIPTION) or {}
        name = raw.get("Name") or self.root.name
        out = {"Name": name, "BIDSVersion": BIDS_VERSION, "DatasetType": kind}
        if kind == "study":
            out.update({k: raw[k] for k in _STUDY_FIELDS if k in raw})
        return out

    def write_descriptions(self, keep: Iterable[str | os.PathLike] = ()
                           ) -> list[Path]:
        """
        Write the descriptions that depend on the layout.

        * study: `<DS>/dataset_description.json` (DatasetType "study",
          name and credits taken from the raw description);
        * every derivative dataset that has a description: its
          `DatasetType` and a `SourceDatasets` entry pointing at the
          raw dataset with the URL of this layout.

        The raw description (copied from templates or downloaded) is
        not generated here: it lives at `raw/dataset_description.json`.

        Parameters
        ----------
        keep : list[path]
            Derivative descriptions left exactly as they are (files the
            user edited, `bids.harvest.finish_roast`)

        Returns the files written.
        """
        written = []
        for drv in self.list_derivatives():
            written += drv.write_descriptions(keep)
        return written

    # ------------------------------------------------------------------
    #   Manifest
    # ------------------------------------------------------------------
    def to_manifest(self) -> dict:
        """Layout fields of the manifest."""
        return {"layout": self.kind}

    # ------------------------------------------------------------------
    #   Detection
    # ------------------------------------------------------------------
    @staticmethod
    def detect(root: str | os.PathLike) -> "Layout | None":
        """
        Detect the layout of an existing dataset.

        From `.brainspresso/manifest.json` when present, else from the
        tree (`rawbids/` or a "study" description: study; `sub-*` folders
        or a raw description at the root: flat; site datasets under a
        folder that is not a dataset: sites). None if nothing is found.
        """
        root = _path(root)
        from_manifest = _from_manifest(root)
        if from_manifest is not None:
            return from_manifest
        return _from_tree(root)

    def __repr__(self):
        """Kind and root."""
        return f"{type(self).__name__}({str(self.root)!r})"

    def __eq__(self, other):
        """Compare kind, root and manifest fields."""
        return (
            type(self) is type(other) and self.root == other.root
            and self.to_manifest() == other.to_manifest()
        )

    def __hash__(self):
        """Hash of the kind and root."""
        return hash((type(self), self.root))


class StudyLayout(Layout):
    """`<DS>/` is a study dataset, the raw dataset is `<DS>/rawbids/`."""

    kind = "study"

    @property
    def raw(self) -> Path:
        """Raw dataset: `<DS>/rawbids/`."""
        return self.root / RAW_DIR

    def _relative_local(self, rel):
        parts = rel.parts
        if parts and parts[0] == RAW_DIR:
            return self.raw, "/".join(parts[1:]) or "."
        return self.root, str(rel)

    def write_descriptions(self, keep=()) -> list[Path]:
        """Write the study description, then those of derivatives."""
        written = []
        if _write_json(self.root / DESCRIPTION,
                       self.dataset_description("study")):
            written.append(self.root / DESCRIPTION)
        return written + super().write_descriptions(keep)

    def readme_of_user(self) -> Path | None:
        """
        Find a README of the user (not `study_readme`'s path).

        Any `README*` name, in any case (`readme.md`, `ReadMe.txt`), but
        `README` itself; None if there is none.
        """
        if not self.root.is_dir():
            return None
        return next((p for p in sorted(self.root.iterdir())
                     if p.name.lower().startswith(README.lower())
                     and p.name != README), None)


def study_readme(name: str) -> str:
    """
    Text of the README of a study dataset (BIDS: recommended).

    Written by the roasts (`roast.run.roast_dataset`) when the study has no
    README of the user (`StudyLayout.readme_of_user`).
    """
    return (f"# {name}\n\n"
            f"This folder is a BIDS study dataset (DatasetType "
            f"\"study\"), written by brainspresso.\n\n"
            f"* `{RAW_DIR}/`: the raw dataset (its own README, when it "
            f"has one, describes the data, its license and how to cite "
            f"it);\n"
            f"* `{DERIVATIVES_DIR}/`: its derivative datasets, if any;\n"
            f"* `{SOURCE_DIR}/`: the files it was converted from, if "
            f"they were kept.\n")


class FlatLayout(Layout):
    """`<DS>/` is the raw dataset."""

    kind = "flat"

    @property
    def raw(self) -> Path:
        """Raw dataset: `<DS>/` itself."""
        return self.root


class DerivativeLayout(Layout):
    """
    A derivative dataset `<parent>/derivatives/<name>/`.

    It has its own `sourcedata/` (e.g. FreeSurfer subjects unpacked from
    archives) and `.bidsignore`. It shares the state folder of its parent.
    """

    kind = "derivative"

    def __init__(self, parent: Layout, name: str):
        if not name or "/" in name or name in (".", ".."):
            raise ValueError(f"Invalid derivative name: {name!r}")
        super().__init__(parent.derivatives / name)
        self.parent = parent
        self.name = name

    @property
    def raw(self) -> Path:
        """Raw dataset of the parent layout."""
        return self.parent.raw

    @property
    def bids(self) -> Path:
        """The derivative dataset itself."""
        return self.root

    @property
    def state(self) -> Path:
        """State folder of the parent layout."""
        return self.parent.state

    def original(self, path):
        """Pristine copy of a table, in the state folder of the parent."""
        return self.parent.original(path)

    def generated(self, path):
        """Return the base of a generated file (parent's state folder)."""
        return self.parent.generated(path)

    def replaced(self, path):
        """Return the backup of a replaced file (parent's state folder)."""
        return self.parent.replaced(path)

    @property
    def source_url(self) -> str:
        """Relative URL of the raw dataset, for `SourceDatasets`."""
        return relative_url(self.raw, self.root)

    def resolve(self, relpath: str) -> Path:
        """Local path of a path relative to the derivative dataset."""
        return self.root.joinpath(*PurePosixPath(relpath).parts)

    def dataset_description(self, kind=None) -> dict:
        """Describe the derivative (`SourceDatasets`: the raw dataset)."""
        out = super().dataset_description(kind or "derivative")
        out["Name"] = self.name
        if out["DatasetType"] == "derivative":
            out["SourceDatasets"] = [{"URL": self.source_url}]
        return out

    def write_descriptions(self, keep=()) -> list[Path]:
        """Complete the description of the derivative, if it exists."""
        kept = {_same(p) for p in keep}
        desc = None if _same(self.description) in kept \
            else _read_json(self.description)
        written = []
        if desc is not None:
            desc = complete_description(desc, self.source_url)
            if _write_json(self.description, desc):
                written.append(self.description)
        return written + super().write_descriptions(keep)

    def to_manifest(self):
        """Layout fields of the manifest."""
        return {"layout": self.kind, "name": self.name}

    def __eq__(self, other):
        """Compare root and parent."""
        return isinstance(other, DerivativeLayout) and \
            self.root == other.root and self.parent == other.parent

    def __hash__(self):
        """Hash of the kind and root."""
        return hash((type(self), self.root))


def complete_description(desc: dict, url: str) -> dict:
    """
    Complete the description of a derivative dataset.

    `DatasetType` "derivative" if it has none, and a `SourceDatasets`
    entry with the URL of the raw dataset first (stale flat URLs
    dropped). Used by `DerivativeLayout.write_descriptions`, and by the
    hosted render (`roast.hosted`), so that a hosted derivative
    description equals its render and its edits are told by hash.

    Parameters
    ----------
    desc : dict
        The description
    url : str
        Relative URL of the raw dataset (`DerivativeLayout.source_url`)

    Returns
    -------
    dict
        A completed copy
    """
    desc = dict(desc)
    desc.setdefault("DatasetType", "derivative")
    sources = [s for s in desc.get("SourceDatasets", []) or []
               if not _is_flat_raw_url(s, url)]
    if not any(isinstance(s, dict) and s.get("URL") == url
               for s in sources):
        sources.insert(0, {"URL": url})
    desc["SourceDatasets"] = sources
    return desc


def description_bytes(desc: dict) -> bytes:
    """Format a description as `write_descriptions` writes it."""
    return (json.dumps(desc, indent=2) + "\n").encode("utf-8")


def _is_flat_raw_url(source, url: str) -> bool:
    """Whether a `SourceDatasets` URL is a stale flat one (`../../`)."""
    if not isinstance(source, dict):
        return False
    value = source.get("URL")
    if not isinstance(value, str) or "://" in value or ":" in value:
        return False
    return posixpath.normpath(value) == "../.." and \
        posixpath.normpath(url) != "../.."


class SitesLayout(Layout):
    """
    One dataset per site (each a study or flat layout) in `<DS>/<site>/`.

    The parent folder is not a BIDS dataset: it holds the README, a
    merged `participants.tsv` (`raw` is the parent folder, so that
    dataset-level files written to `layout.raw` land there),
    `sourcedata/` for files that belong to no site, and a state folder
    whose manifest records the layout of each site.
    """

    kind = "sites"

    def __init__(
        self,
        root: str | os.PathLike,
        children: dict[str, str | Layout] | None = None,
        child_kind: LayoutChoice | None = None,
    ):
        super().__init__(root)
        self.children: dict[str, Layout] = {}
        for name, child in (children or {}).items():
            if not isinstance(child, Layout):
                child = make_layout(child, self.root / name)
            self.children[name] = child
        kinds = {c.kind for c in self.children.values()}
        if child_kind is None and len(kinds) == 1:
            child_kind = next(iter(kinds))
        self.child_kind = child_kind or DEFAULT_LAYOUT

    @property
    def raw(self) -> Path:
        """The parent folder (README, merged participants table)."""
        return self.root

    def child(self, name: str, kind: LayoutChoice | None = None) -> Layout:
        """Layout of a site (created with `child_kind` if new)."""
        if name not in self.children:
            path = self.root / name
            found = Layout.detect(path)
            requested = kind or self.child_kind
            if found is not None and kind and found.kind != kind:
                raise LayoutError(
                    f"Site {path} has layout {found.kind!r}, not {kind!r}"
                )
            self.children[name] = found or make_layout(requested, path)
        return self.children[name]

    def datasets(self) -> list[Layout]:
        """List the site datasets."""
        return list(self.children.values())

    def ensure_state(self) -> Path:
        """
        Create the state folder of the parent and of each existing site.

        A new site gets its own when it is harvested (`Layout.
        ensure_state`, by `bids.harvest.harvest`).

        Returns
        -------
        Path
            The state folder of the parent
        """
        for child in self.children.values():
            if Path(child.root).is_dir():
                child.ensure_state()
        return super().ensure_state()

    def _relative_local(self, rel):
        parts = rel.parts
        if parts and parts[0] in self.children:
            return self.children[parts[0]].relative(self.root / rel)
        return self.root, str(rel)

    def relative(self, path):
        """Dataset (a site, or the parent) and relative path of a path."""
        return self._relative_local(self._relpath(path))

    def resolve(self, relpath: str) -> Path:
        """Local path of a path relative to the parent folder."""
        return self.root.joinpath(*PurePosixPath(relpath).parts)

    def write_descriptions(self, keep=()) -> list[Path]:
        """Write the descriptions of every site."""
        written = []
        for child in self.children.values():
            written += child.write_descriptions(keep)
        return written

    def to_manifest(self):
        """Layout fields of the manifest (with the layout of each site)."""
        return {
            "layout": self.kind,
            "children": {k: v.kind for k, v in sorted(self.children.items())},
        }


_KINDS = {"study": StudyLayout, "flat": FlatLayout}


def is_sites_parent(root: str | os.PathLike) -> bool:
    """
    Whether a folder is the parent of one dataset per site.

    From its manifest, else from its tree if it has a real state folder
    (`actions.locks.is_state_folder`): a `.brainspresso/` folder that
    only holds a config file (the root of all datasets) does not make
    the datasets under it sites.

    Parameters
    ----------
    root : path

    Returns
    -------
    bool
    """
    root = _path(root)
    found = _from_manifest(root)
    if found is not None:
        return isinstance(found, SitesLayout)
    return is_state_folder(root) and isinstance(_from_tree(root),
                                                SitesLayout)


def make_layout(kind: str | None, root: str | os.PathLike, **kwargs) -> Layout:
    """
    Layout factory.

    Parameters
    ----------
    kind : {"study", "flat", "sites"}, default=DEFAULT_LAYOUT
    root : path
        Dataset folder
    children : dict[str, str], optional (sites)
        Layout kind of each site
    child_kind : {"study", "flat"}, optional (sites)
        Layout of new sites (default: that of existing sites, else
        DEFAULT_LAYOUT)
    """
    kind = kind or DEFAULT_LAYOUT
    if kind == "sites":
        return SitesLayout(root, **kwargs)
    if kind not in _KINDS:
        raise ValueError(
            f"Unknown layout {kind!r}. Choose from: study, flat, sites"
        )
    if kwargs:
        raise TypeError(f"Unexpected arguments for layout {kind!r}: "
                        f"{sorted(kwargs)}")
    return _KINDS[kind](root)


def resolve_layout(
    root: str | os.PathLike | Layout,
    layout: str | None = None,
    **kwargs,
) -> Layout:
    """
    Choose the layout of a dataset.

    The detected layout of an existing dataset, else `layout` (default:
    DEFAULT_LAYOUT).

    Raises `LayoutError` if `layout` is given and differs from the
    detected one.
    """
    if isinstance(root, Layout):
        if layout and root.kind != layout:
            raise LayoutError(
                f"Layout {root.kind!r} given where {layout!r} is requested"
            )
        return root
    root = _path(root)
    found = Layout.detect(root)
    if found is None:
        return make_layout(layout, root, **kwargs)
    if layout and found.kind != layout:
        raise LayoutError(
            f"{root} has layout {found.kind!r} "
            f"(recorded in {STATE_DIR}/{MANIFEST} or detected from its "
            f"files), which conflicts with --layout {layout}. Use "
            f"--layout {found.kind}, or another folder."
        )
    if isinstance(found, SitesLayout) and kwargs.get("child_kind"):
        kind = kwargs["child_kind"]
        other = {c.kind for c in found.children.values()} - {kind}
        if other:
            raise LayoutError(
                f"Sites of {root} have layout {sorted(other)}, which "
                f"conflicts with --layout sites:{kind}"
            )
        found.child_kind = kind
    return found


# ----------------------------------------------------------------------
#   Detection helpers
# ----------------------------------------------------------------------

def _from_manifest(root: Path) -> Layout | None:
    manifest = read_manifest(root / STATE_DIR)
    kind = manifest.get("layout") if manifest else None
    if kind is None:
        return None
    if kind == "sites":
        children = {}
        for name, child in (manifest.get("children") or {}).items():
            children[name] = (
                _from_manifest(root / name) or make_layout(child, root / name)
            )
        return SitesLayout(root, children)
    if kind not in _KINDS:
        lg.warning(f"{root}: unknown layout {kind!r} in manifest, ignored")
        return None
    return _KINDS[kind](root)


def _iter_dirs(root: Path) -> Iterator[Path]:
    try:
        yield from sorted_paths(p for p in root.iterdir() if p.is_dir())
    except OSError:
        return


def _from_tree(root: Path, sites: bool = True) -> Layout | None:
    if not root.is_dir():
        return None
    desc = _read_json(root / DESCRIPTION)
    if (root / RAW_DIR).is_dir() or \
            (desc is not None and desc.get("DatasetType") == "study"):
        return StudyLayout(root)
    if desc is not None or any(
        p.name.startswith("sub-") for p in _iter_dirs(root)
    ):
        return FlatLayout(root)
    if not sites:
        return None
    children = {}
    for path in _iter_dirs(root):
        if path.name in _NOT_SITES or path.name.startswith("."):
            continue
        child = _from_manifest(path) or _from_tree(path, sites=False)
        if child is not None:
            children[path.name] = child
    if children:
        return SitesLayout(root, children)
    return None
