"""
Datasets hosted as BIDS: hybrid placement and fill-in sidecars.

Hosted BIDS datasets (OpenNeuro, the INDI projects of `s3://fcp-indi`)
are harvested in two kinds of files (`docs/design/architecture.md`, §4):

* files that are **never repaired or trimmed** (images, recordings,
  physiological recordings, files of `sourcedata/`, `code/`...) are
  downloaded directly to their final path (`placement(path) ==
  "direct"`);
* files that **may be repaired or trimmed** (JSON, TSV including events,
  `bval`/`bvec`, README, `dataset_description.json`, participants,
  sessions and scans tables: every file that `bids.kinds.default_kind`
  does not call "data") are downloaded to `sourcedata/<folder>/` under
  their remote path (the *pristine copy*), then **roasted** into the
  BIDS dataset (`placement(path) == "roasted"`, `roast.hosted`).

This module holds what is BIDS knowledge in that scheme, and no I/O
beyond reading the tree of a dataset: the classification, the pure
rendering of a roasted file (`render`: repair, fill-in, `IntendedFor`),
and the fill-in sidecar templates (`read_fill_templates`, `plan_fill`).

Fill-in templates
-----------------
A dataset may ship sidecar templates that **add** the keys its hosted
sidecars lack (e.g. scan parameters from protocol printouts), and
**never** change a key that the host (or a generated file) gives. For
the INDI projects they live in
`brainspresso/datasets/INDI/templates/<project>/[<site>/]<name>.json`
and are listed in `_fill.json`, next to them (written by
`scripts/make_sidecars.py` from the printouts, checked by its `--check`
and shipped with the package)::

    {"fill": ["T1w.json",               # dataset level: every site
              "ICBM/T1w.json",          # site level (folder: site name)
              "*/task-rest_bold.json"]} # globs, one segment at a time

* `<name>` is `[<key>-<value>_...]<suffix>.json`, a sidecar name
  without `sub-`/`ses-` (`T1w.json`, `task-rest_bold.json`); the
  content is a JSON object. `<site>` is the exact name of a site of the
  dataset. Every entry must match a file. Listed files (and
  `_fill.json`, as every `_*` file) are never copied into the dataset
  as plain templates.
* Placement (BIDS inheritance principle): a dataset-level template, and
  the template of the site a dataset *is* (a site dataset of `--layout
  sites`), are written as the top-level sidecar `<name>`; a site-level
  template of a dataset that merges sites is pushed down to each
  subject of that site, as `sub-<label>/sub-<label>_<name>`. The
  fill-in is **merged** into the file already at that path (hosted or
  generated: its keys win), else written as a new file. A hosted file
  at the same level with another name that may describe the same data
  (same suffix, compatible entities: `task-rest_bold.json` for a
  template `bold.json`) takes the fill-in instead, if it is at least as
  specific as the template; else the template is skipped there (two
  sidecars of one level must not describe one file). Keys that a
  hosted top-level sidecar gives are never pushed down (they would
  override it). Precedence: hosted or generated file, then site
  template, then dataset template.
* Why inherited sidecars rather than merging into every hosted sidecar:
  a data file without any hosted sidecar gets the fill-in too, the
  template is written once per level (not once per file), and lower
  levels win by the inheritance principle, which is exactly "add
  missing keys, never override".
* A new file is recorded as `generated` in the manifest, a merged one
  keeps its origin (`roasted`, or `generated`), both with the list of
  templates (`"fill": [...]`).
"""
from __future__ import annotations

import fnmatch
import json
import os
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Callable, Iterable, Literal, Mapping

from brainspresso.bids.kinds import default_kind
from brainspresso.bids.repair import (
    dumps,
    intended_for,
    loads,
    repair_sidecar,
    repair_table,
)
from brainspresso.bids.select import (
    INDEX_ENTITIES,
    OPTIONAL_FOLDERS,
    BidsName,
    applies,
)

lg = getLogger(__name__)

__all__ = [
    "FILL_FILE", "FillPlan", "FillTemplate", "Placement", "fill_files",
    "fill_in", "is_data", "placement", "plan_fill", "read_fill_templates",
    "relabel_intended", "relabel_scans", "render", "trim_rows",
]

Placement = Literal["direct", "roasted"]
#: File that lists the fill-in templates of a folder: `{"fill": [paths]}`
FILL_FILE = "_fill.json"


# ----------------------------------------------------------------------
#   Placement
# ----------------------------------------------------------------------

def _parts(path: str) -> list[str]:
    return [p for p in str(path).split("/") if p and p != "."]


def placement(path: str) -> Placement:
    """
    How a file of a hosted BIDS listing is placed.

    Parameters
    ----------
    path : str
        Path in the flat view of the dataset (as hosts list it:
        `sub-*/...`, `participants.tsv`, `derivatives/<name>/...`,
        `sourcedata/...`), after any remapping

    Returns
    -------
    {"direct", "roasted"}
        "roasted" for the metadata of a BIDS dataset (raw or derivative)
        that may be repaired or trimmed (every kind but "data",
        `bids.kinds.default_kind`); "direct" for data files, the files
        of directory-format recordings, and the files of `sourcedata/`,
        `code/`, `stimuli/`... (not BIDS metadata)
    """
    parts = _parts(path)
    if not parts:
        return "direct"
    if parts[0] == "derivatives":
        return placement("/".join(parts[2:])) if len(parts) > 2 \
            else "direct"
    if parts[0] in OPTIONAL_FOLDERS:
        return "direct"
    relpath = "/".join(parts)
    if BidsName.parse(relpath).unit is not None:
        return "direct"
    return "direct" if default_kind(relpath) == "data" else "roasted"


def is_data(path: str) -> bool:
    """
    Whether a file of a hosted listing is a data file of a subject.

    Images and recordings in `sub-*/` trees of the raw dataset or of a
    derivative dataset (what `--sidecars only` does not download).

    Parameters
    ----------
    path : str
        Path in the flat view of the dataset

    Returns
    -------
    bool
    """
    parts = _parts(path)
    if len(parts) > 2 and parts[0] == "derivatives":
        return is_data("/".join(parts[2:]))
    return len(parts) > 1 and parts[0].startswith("sub-") and \
        placement(path) == "direct"


# ----------------------------------------------------------------------
#   Fill-in templates
# ----------------------------------------------------------------------

@dataclass(frozen=True)
class FillTemplate:
    """
    A fill-in sidecar template.

    Parameters
    ----------
    name : str
        File name of the sidecar (`[<entities>_]<suffix>.json`)
    content : dict
        Keys to add
    site : str | None
        Site it applies to (None: the whole dataset)
    path : str | None
        File it was read from (locks and input signatures of roasts)
    previous : tuple[str, ...]
        SHA-256 of earlier versions of the file it generates (a local
        copy that is still one of them is not an edit)
    """

    name: str
    content: dict
    site: str | None = None
    path: str | None = None
    previous: tuple = ()

    @property
    def id(self) -> str:
        """Path relative to its folder (`[<site>/]<name>`)."""
        return f"{self.site}/{self.name}" if self.site else self.name


@dataclass(frozen=True)
class FillPlan:
    """
    What a fill-in adds to one sidecar.

    Parameters
    ----------
    layers : tuple[FillTemplate, ...]
        Templates, highest precedence first (site, then dataset)
    exclude : frozenset[str]
        Keys never added (given by a hosted sidecar of a higher level)
    """

    layers: tuple = ()
    exclude: frozenset = frozenset()

    @property
    def ids(self) -> list[str]:
        """Templates of the plan (`FillTemplate.id`)."""
        return [t.id for t in self.layers]

    @property
    def paths(self) -> list[str]:
        """Files of the templates."""
        return [t.path for t in self.layers if t.path]

    @property
    def previous(self) -> set[str]:
        """SHA-256 of the earlier versions of its templates."""
        return {h for t in self.layers for h in t.previous}


def _entities(name: str) -> tuple[dict, str | None, str]:
    parsed = BidsName.parse(name)
    return dict(parsed.entities), parsed.suffix, parsed.extension


def _match(relpath: str, pattern: str) -> bool:
    """Glob match, one path segment at a time (`*` never matches `/`)."""
    parts, pats = _parts(relpath), _parts(pattern)
    return len(parts) == len(pats) and all(
        fnmatch.fnmatchcase(p, q) for p, q in zip(parts, pats))


def _check_name(name: str, where: Path) -> None:
    entities, suffix, extension = _entities(name)
    if extension != ".json" or not suffix or \
            default_kind(name) != "sidecar":
        raise ValueError(f"{where}: {name} is not the name of a sidecar "
                         f"(`[<key>-<value>_...]<suffix>.json`)")
    if {"sub", "ses"} & set(entities):
        raise ValueError(f"{where}: {name}: fill-in templates are for a "
                         f"dataset or a site, not a subject or session")


def read_fill_templates(
    folder: str | os.PathLike,
    sites: Iterable[str] | None = None,
) -> list[FillTemplate]:
    """
    Read the fill-in templates listed in `<folder>/_fill.json`.

    See the module docstring for the convention.

    Parameters
    ----------
    folder : path
        Template folder of a dataset (`templates/<project>/`)
    sites : iterable of str, optional
        Names of the sites of the dataset (a site folder must be one of
        them; None: not checked)

    Returns
    -------
    list[FillTemplate]
        In the order of `_fill.json` (none if it does not exist)

    Raises
    ------
    ValueError
        If `_fill.json` is invalid, an entry matches no file, a file is
        not a sidecar template, or its folder is not a site
    """
    folder = Path(folder)
    spec = folder / FILL_FILE
    if not spec.is_file():
        return []
    try:
        data = json.loads(spec.read_text(encoding="utf-8"))
    except (ValueError, UnicodeDecodeError) as e:
        raise ValueError(f"{spec}: {e}") from e
    if not isinstance(data, dict):
        raise ValueError(f"{spec}: not a JSON object")
    unknown = set(data) - {"fill"}
    if unknown:
        raise ValueError(f"{spec}: unknown keys {sorted(unknown)}")
    patterns = data.get("fill", [])
    if not isinstance(patterns, list) or \
            not all(isinstance(p, str) for p in patterns):
        raise ValueError(f"{spec}: `fill` must be a list of paths")
    known = None if sites is None else {str(s) for s in sites if s}
    found = sorted(p.relative_to(folder).as_posix()
                   for p in folder.rglob("*.json") if p.is_file())
    out, seen = [], set()
    for pattern in patterns:
        matches = [f for f in found if _match(f, pattern)]
        if not matches:
            raise ValueError(f"{spec}: {pattern!r} matches no file")
        for relpath in matches:
            if relpath in seen:
                continue
            seen.add(relpath)
            parts = relpath.split("/")
            if len(parts) > 2:
                raise ValueError(f"{spec}: {relpath}: templates are "
                                 f"`<name>.json` or `<site>/<name>.json`")
            site = parts[0] if len(parts) == 2 else None
            if site is not None and known is not None and site not in known:
                raise ValueError(f"{spec}: {relpath}: {site!r} is not a "
                                 f"site of the dataset")
            _check_name(parts[-1], spec)
            path = folder / relpath
            try:
                content = json.loads(path.read_text(encoding="utf-8"))
            except (ValueError, UnicodeDecodeError) as e:
                raise ValueError(f"{path}: invalid JSON ({e})") from e
            if not isinstance(content, dict):
                raise ValueError(f"{path}: not a JSON object")
            out.append(FillTemplate(parts[-1], content, site, str(path)))
    return out


def fill_files(folder: str | os.PathLike) -> set[str]:
    """
    Files of a template folder that belong to its fill-in templates.

    `_fill.json` and the files it lists, relative to `folder` (POSIX), so
    that they are not copied into datasets as plain templates.

    Parameters
    ----------
    folder : path
        Template folder

    Returns
    -------
    set[str]
    """
    folder = Path(folder)
    if not (folder / FILL_FILE).is_file():
        return set()
    return {FILL_FILE} | {t.id for t in read_fill_templates(folder)}


def _compatible(a: dict, b: dict) -> bool:
    """Entities that may describe one file (equal on common keys)."""
    for key in set(a) & set(b):
        x, y = str(a[key]), str(b[key])
        if key in INDEX_ENTITIES and x.isdigit() and y.isdigit():
            x, y = str(int(x)), str(int(y))
        if x != y:
            return False
    return True


def _within(a: dict, b: dict) -> bool:
    """Every entity of `a` is in `b` (`b` at least as specific)."""
    return set(a) <= set(b) and _compatible(a, b)


def _level(path: str) -> str:
    return path.rpartition("/")[0]


def _resolve(target: str, placements: set[str]) -> list[str]:
    """
    Files that take the fill-in of `target` (see the module docstring).

    Parameters
    ----------
    target : str
        Where the template would be written
    placements : set[str]
        Hosted or generated metadata of the dataset

    Returns
    -------
    list[str]
        `[target]`, the hosted files of its level that take it instead,
        or nothing (skipped)
    """
    if target in placements:
        return [target]
    level = _level(target)
    entities, suffix, _ = _entities(target.rpartition("/")[2])
    overlapping = []
    for path in placements:
        if _level(path) != level or not path.endswith(".json"):
            continue
        other, other_suffix, _ = _entities(path.rpartition("/")[2])
        if other_suffix == suffix and _compatible(entities, other):
            overlapping.append((path, other))
    if not overlapping:
        return [target]
    out = sorted(p for p, other in overlapping if _within(entities, other))
    if len(out) < len(overlapping):
        lg.warning(f"Fill-in {target} skipped where "
                   f"{', '.join(sorted(p for p, _ in overlapping))} "
                   f"(a more general sidecar) already describes the data")
        return []
    return out


def plan_fill(
    templates: Iterable[FillTemplate],
    *,
    present: Iterable[str],
    placements: Iterable[str] = (),
    read: Callable[[str], dict | None] = lambda path: None,
    site: str | None = None,
    subject_sites: Mapping[str, str] | None = None,
) -> dict[str, FillPlan]:
    """
    Decide where fill-in templates go in a raw dataset.

    See the module docstring for the rules.

    Parameters
    ----------
    templates : iterable of FillTemplate
        Templates of the dataset (dataset and site level)
    present : iterable of str
        Files that the dataset has or selects (data and metadata),
        relative to the raw dataset: a new fill-in file is only written
        where it describes one of them
    placements : iterable of str
        Hosted or generated metadata of the dataset, relative to the
        raw dataset (the fill-in is merged into them)
    read : callable(path) -> dict | None
        Content of a placement (keys of the top-level sidecars; None:
        unreadable, so no fill-in of its suffix is pushed down)
    site : str | None
        Site that the dataset is (one dataset per site): its templates
        are written at the top level, over the dataset ones
    subject_sites : dict[str, str] | None
        Site of each subject of a dataset that merges sites: site
        templates are pushed down to the subjects of their site

    Returns
    -------
    dict[str, FillPlan]
        Plans, by path relative to the raw dataset
    """
    templates = list(templates)
    placements = set(placements)
    present = list(present)
    wanted: dict[str, list[FillTemplate]] = {}

    def add(target, template):
        wanted.setdefault(target, []).append(template)

    # site templates first: they take precedence (`FillPlan.layers`)
    for template in sorted(templates, key=lambda t: t.site is None):
        if template.site is None or template.site == site:
            add(template.name, template)
        elif subject_sites and site is None:
            for label in sorted(subject_sites):
                if subject_sites[label] == template.site:
                    add(f"sub-{label}/sub-{label}_{template.name}",
                        template)

    # keys of the top-level sidecars (None: unreadable, so unknown)
    top = {p: read(p) for p in placements
           if "/" not in p and p.endswith(".json")}
    by_subject: dict[str, list[str]] = {}
    for path in present:
        by_subject.setdefault(path.split("/")[0], []).append(path)
    plans: dict[str, list] = {}
    for target, layers in wanted.items():
        resolved = _resolve(target, placements)
        for path in resolved:
            if path not in placements:
                scope = present if "/" not in path else \
                    by_subject.get(path.split("/")[0], [])
                if not any(applies(path, p) for p in scope if p != path):
                    continue
            exclude, unknown = set(), False
            if "/" in path:
                entities, suffix, _ = _entities(path.rpartition("/")[2])
                for name, content in top.items():
                    other, other_suffix, _ = _entities(name)
                    if other_suffix == suffix and \
                            _compatible(other, entities):
                        if content is None:
                            unknown = True
                        else:
                            exclude |= set(content)
            if unknown:
                # the keys a pushed-down fill-in must not give are not
                # known: none is pushed down (never "no keys")
                lg.warning(f"Fill-in {path} skipped: a top-level sidecar "
                           f"of its suffix cannot be read")
                continue
            plan = plans.setdefault(path, [[], set()])
            plan[0] += [t for t in layers if t not in plan[0]]
            plan[1] |= exclude
    return {path: FillPlan(tuple(layers), frozenset(exclude))
            for path, (layers, exclude) in sorted(plans.items())}


def fill_in(meta: dict, plan: FillPlan | None) -> dict:
    """
    Add the keys of a fill-in plan that a sidecar lacks.

    Parameters
    ----------
    meta : dict
        Content of the sidecar (its keys always win)
    plan : FillPlan | None
        The fill-in

    Returns
    -------
    dict
        A new dictionary: the keys of `meta` first, then those added
    """
    out = dict(meta)
    for template in (plan.layers if plan else ()):
        for key, value in template.content.items():
            if key not in out and key not in plan.exclude:
                out[key] = value
    return out


# ----------------------------------------------------------------------
#   Relabelling: paths that a hosted file gives, in the local layout
# ----------------------------------------------------------------------

def relabel_intended(meta: dict, mapping: Mapping[str, str],
                     remote: str, local: str) -> dict:
    """
    Rewrite the `IntendedFor` paths of a field map in the local layout.

    The images are remapped (site codes, label maps, runs from folders,
    typos...): the paths that point at them are remapped the same way
    (before they are trimmed to what is present). Free-text tags and
    paths of no remapped file are left as they are.

    Parameters
    ----------
    meta : dict
        Content of the sidecar
    mapping : dict[str, str]
        Remote path -> local path, both relative to their dataset
    remote, local : str
        Remote and local subject folders of the sidecar (`sub-01`,
        `sub-A01`): `IntendedFor` paths are relative to them

    Returns
    -------
    dict
        The new content (`meta` itself if nothing changed)
    """
    targets = meta.get("IntendedFor") if isinstance(meta, dict) else None
    if not mapping or targets is None:
        return meta
    single = isinstance(targets, str)
    out = []
    for target in ([targets] if single else list(targets)):
        if not isinstance(target, str):
            out.append(target)
        elif target.startswith("bids::"):
            path = target[len("bids::"):]
            out.append("bids::" + mapping.get(path, path))
        else:
            path = f"{remote}/{target}"
            mapped = mapping.get(path, path)
            out.append(mapped[len(local) + 1:]
                       if mapped.startswith(local + "/") else target)
    out = out[0] if single else out
    if out == targets:
        return meta
    return {**meta, "IntendedFor": out}


#: Trimmed tables: the column of each that names what a row describes
TRIMMED_COLUMNS = {"participants.tsv": ("participant_id", True),
                   "_sessions.tsv": ("session_id", True),
                   "_scans.tsv": ("filename", False)}


def trim_rows(text: str, name: str,
              here: Callable[[str, bool], bool]) -> str:
    """
    Keep the rows of a participants, sessions or scans table that are here.

    A pure filter (the content of the table is not otherwise changed):
    the text is returned as it is if no row is dropped.

    Parameters
    ----------
    text : str
        Content of the table
    name : str
        Its file name
    here : callable(path, folder) -> bool
        Whether a path relative to the folder of the table (a subject or
        session folder, `folder=True`; a file, `folder=False`) is
        present

    Returns
    -------
    str
    """
    for end, (column, folder) in TRIMMED_COLUMNS.items():
        if name == end or (end.startswith("_") and name.endswith(end)):
            break
    else:
        return text
    newline = "\r\n" if "\r\n" in text else "\n"
    lines = text.split(newline)
    header = lines[0].split("\t")
    if column not in header:
        return text
    col = header.index(column)
    out, dropped = [lines[0]], False
    for line in lines[1:]:
        cells = line.split("\t")
        if line and (len(cells) <= col or not here(cells[col], folder)):
            dropped = True
            continue
        out.append(line)
    return newline.join(out) if dropped else text


def relabel_scans(text: str, mapping: Mapping[str, str],
                  remote: str, local: str) -> str:
    """
    Rewrite the `filename` column of a scans table in the local layout.

    Parameters
    ----------
    text : str
        Content of the table
    mapping : dict[str, str]
        Remote path -> local path, relative to their dataset
    remote, local : str
        Remote and local folders of the table (filenames are relative
        to them)

    Returns
    -------
    str
    """
    if not mapping:
        return text
    newline = "\r\n" if "\r\n" in text else "\n"
    lines = text.split(newline)
    header = lines[0].split("\t")
    if "filename" not in header:
        return text
    col = header.index("filename")
    out, changed = [lines[0]], False
    for line in lines[1:]:
        cells = line.split("\t")
        if len(cells) > col and cells[col]:
            path = f"{remote}/{cells[col]}"
            mapped = mapping.get(path, path)
            if mapped.startswith(local + "/") and \
                    mapped[len(local) + 1:] != cells[col]:
                cells[col] = mapped[len(local) + 1:]
                changed = True
        out.append("\t".join(cells))
    # (line endings kept; unchanged text as it was)
    return newline.join(out) if changed else text


# ----------------------------------------------------------------------
#   Rendering of a roasted file
# ----------------------------------------------------------------------

def render(
    content: bytes | None,
    name: str,
    *,
    repair: bool = False,
    fill: FillPlan | None = None,
    intended: tuple | None = None,
    sibling: Callable[[str], dict | None] | None = None,
    relabel: tuple | None = None,
    rows: Callable[[str, bool], bool] | None = None,
) -> bytes:
    """
    Content of a roasted metadata file, from its pristine copy.

    The pristine bytes are returned as they are when nothing changes
    (a roast of a valid file is a byte-identical copy).

    Parameters
    ----------
    content : bytes | None
        Pristine (or generated) content; None for a new fill-in sidecar
    name : str
        File name of the output
    repair : bool
        Apply `bids.repair` (JSON and TSV)
    fill : FillPlan | None
        Fill-in of a sidecar
    intended : tuple(root, path, trim[, present]) | None
        Field map sidecar: repair free-text `IntendedFor` tags and, with
        `trim`, drop the targets that are not in the BIDS dataset `root`
        (`path`: the sidecar, relative to `root`; `present`: files that
        count as there, relative to `root`)
    sibling : callable(name) -> dict | None
        Content of a sibling sidecar (phase difference repairs)
    relabel : tuple(mapping, remote, local) | None
        Remote -> local paths (relative to their dataset) and the remote
        and local folders of the file: `IntendedFor` of a field map and
        the `filename` column of a scans table point at local files
        (`relabel_intended`, `relabel_scans`)
    rows : callable(path, folder) -> bool | None
        Trim a participants, sessions or scans table to the rows that
        are here (`trim_rows`)

    Returns
    -------
    bytes
    """
    if name.endswith(".tsv"):
        scans = relabel is not None and name.endswith("_scans.tsv")
        if not (repair or scans or rows) or content is None:
            return content or b""
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError as e:
            lg.warning(f"{name}: not valid UTF-8, not repaired ({e})")
            return content
        fixed = repair_table(text, name) if repair else text
        if fixed is not None and scans:
            fixed = relabel_scans(fixed, *relabel)
        if fixed is not None and rows is not None:
            fixed = trim_rows(fixed, name, rows)
        return content if fixed is None or fixed == text \
            else fixed.encode("utf-8")
    if not name.endswith(".json"):
        return content or b""
    original = None
    if content is None:
        meta = {}
    else:
        if not (repair or fill or intended or relabel):
            return content
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError as e:
            lg.warning(f"{name}: not valid UTF-8, copied as is ({e})")
            return content
        try:
            meta = loads(text) if repair else json.loads(text)
        except json.JSONDecodeError as e:
            lg.warning(f"{name}: invalid JSON, copied as is ({e})")
            return content
        if not isinstance(meta, dict):
            return content
        try:
            original = json.loads(text)
        except json.JSONDecodeError:
            original = None
    fixed = repair_sidecar(meta, name, sibling) if repair else dict(meta)
    fixed = fill_in(fixed, fill)
    if relabel is not None:
        fixed = relabel_intended(fixed, *relabel)
    if intended is not None:
        root, path, trim, *more = intended
        fixed = intended_for(fixed, PurePosixPath(path), Path(root), trim,
                             *more)
    if original is not None and fixed == original:
        return content
    return dumps(fixed).encode("utf-8")
