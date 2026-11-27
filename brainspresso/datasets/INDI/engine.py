"""
Curated INDI projects on `s3://fcp-indi`, harvested as BIDS datasets.

Many INDI / FCP projects are mirrored on the public `fcp-indi` bucket in
a layout that is BIDS, or close to it. A `Project` describes, for one of
them:

* where its data live in the bucket (one or several `Site`s);
* how bucket paths map to BIDS paths (`remap`), e.g. to fix typos, make
  subject labels unique across sites, or move non-BIDS files out of the
  way;
* which files must be generated locally (`InlineEntry`): a merged
  `participants.tsv`, sidecars the bucket lacks, or curated
  `dataset_description.json`/`README` (from `templates/<name>/`).

The harvest then reuses the generic BIDS machinery (`bids.harvest`):
selection by subject/session/datatype/suffix/task/globs, participant
queries (`--where`), trimmed tables and a manifest. Listing is pruned:
only the subjects that are selected are listed.

Local layouts (`bids.layout`):

* "study" (default) or "flat": `<root>/<Project.folder>/` holds one
  BIDS dataset (in `rawbids/`, or at the root), with derivatives in
  `derivatives/<pipeline>/`. Sites are merged: labels are prefixed with
  a site code when they are not unique across sites, and
  `participants.tsv` has a `site` column.
* "sites": `<root>/<Project.folder>/<site>/` is one dataset per site
  (each a study or flat layout, `--site-layout`), with the labels of
  the bucket (BIDS "Option 1" for multi-site studies). The parent
  folder is not a BIDS dataset: it holds the README and the merged
  `participants.tsv`, for pooled lookups.

Paths of the listing are those of a flat BIDS dataset (`sub-*/...`,
`sourcedata/...`, `derivatives/<name>/...`); `Layout.resolve` places
them.
"""
from __future__ import annotations

import asyncio
import copy
import csv
import dataclasses
import io
import json
import re
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Iterable, Iterator

from brainspresso.bids.harvest import InlineEntry
from brainspresso.bids.select import TOP_LEVEL_FILES, BidsName
from brainspresso.download.session import new_session
from brainspresso.sources import fcp
from brainspresso.sources.base import LabelMap, prefix_labels
from brainspresso.sources.s3 import S3Object

lg = getLogger(__name__)

CONCURRENCY = 32                    # concurrent listing requests
# Order of entities in BIDS file names
ENTITIES = (
    "sub", "ses", "sample", "task", "tracksys", "acq", "nuc", "voi", "ce",
    "trc", "stain", "rec", "dir", "run", "mod", "echo", "flip", "inv", "mt",
    "part", "proc", "hemi", "space", "split", "recording", "chunk", "seg",
    "res", "den", "label", "desc",
)
TEMPLATES = Path(__file__).parent / "templates"
# Fill-in templates of `templates/<name>/` (sidecars converted from the
# scanner printouts, `scripts/make_sidecars.py`): `{"fill": [paths]}`.
# They only fill in what the bucket lacks: a bucket file at the same path
# is kept (see `Project.entries`).
FILL_INDEX = "_fill.json"
# SHA-256 of earlier versions of templates (`<name>/<path>`): a harvest
# refreshes a local copy that is still such a version (see
# `bids.harvest.write_inline`; later versions are recognised from the
# manifest)
PREVIOUS_TEMPLATES = {
    # hand-written {TaskName, RepetitionTime}, before printouts/SLIM
    "slim/task-rest_bold.json": (
        "518c8fbc5ae23d9cb5a9fccf4420a3c2119823d3bf360fd211c7dc8fccc00269",
    ),
}
NA = "n/a"


# ----------------------------------------------------------------------
#   Description of a project
# ----------------------------------------------------------------------

@dataclass(frozen=True)
class Site:
    """
    A folder of the bucket that holds (part of) a project.

    Parameters
    ----------
    name : str
        Site name, as shown to users and written in `participants.tsv`
        ("" for single-site projects).
    prefix : str
        Location in the bucket, relative to `data/Projects`.
    code : str
        Prepended to subject labels, to make them unique across sites
        (e.g. "AnnArborA": `sub-04111` -> `sub-AnnArborA04111`).
    target : str
        Local folder (relative to the dataset root) that the site maps to,
        e.g. "derivatives/niak".
    derivative : bool
        Only list this site if derivatives are requested.
    """
    name: str
    prefix: str
    code: str = ""
    target: str = ""
    derivative: bool = False

    def label(self, original: str) -> str:
        return self.code + original

    @property
    def labels(self) -> LabelMap:
        """Bucket labels <-> labels of the merged dataset (site code)"""
        return prefix_labels(self.code)

    @property
    def key(self) -> str:
        return f"{fcp.PROJECTS}/{self.prefix.strip('/')}/"


@dataclass(frozen=True)
class Item:
    """An object of the bucket, relative to its site"""
    site: Site
    rel: str
    obj: S3Object


class Project:
    """
    A curated INDI project.

    The default implementation handles projects whose sites are BIDS
    datasets: subject labels are prefixed with the site code, site-level
    sidecars are moved to the subject level when several sites are
    merged, and top-level files that are not BIDS go to `sourcedata/`.
    """

    name: str = ""                  # command name
    folder: str = ""                # local folder
    title: str = ""
    help: str = ""                  # CLI help (markdown)
    sites: tuple[Site, ...] = ()
    # site sub-folders that are never listed
    ignore: tuple[str, ...] = ()
    # site sub-folders that are only listed if derivatives are requested
    derivative_folders: tuple[str, ...] = ("derivatives",)
    # top-level tables moved to `phenotype/` (by stem)
    phenotype: tuple[str, ...] = ()
    # remote tables fetched to build participants.tsv {name: key}
    tables: dict[str, str] = {}
    # files outside of the sites {local path: key (relative to
    # data/Projects)}, always selected
    extra_files: dict[str, str] = {}

    # ------------------------------------------------------------------
    @property
    def multisite(self) -> bool:
        return len([s for s in self.sites if not s.derivative]) > 1

    def select_sites(self, names: Iterable[str] = ()) -> list[Site]:
        """Sites matching `names` (case-insensitive name or code)"""
        names = {str(n).lower() for n in names or ()}
        if not names:
            return list(self.sites)
        out = [s for s in self.sites
               if s.derivative or s.name.lower() in names
               or (s.code and s.code.lower() in names)]
        known = {s.name.lower() for s in self.sites} | {
            s.code.lower() for s in self.sites if s.code
        }
        unknown = names - known
        if unknown:
            raise ValueError(
                f"Unknown sites {sorted(unknown)}. Available: "
                f"{', '.join(s.name for s in self.sites if s.name)}"
            )
        return out

    # ------------------------------------------------------------------
    #   Hooks
    # ------------------------------------------------------------------

    def participants(self, tables: dict[str, str]) -> str | None:
        """
        Build `participants.tsv` from the fetched `tables`. Return None
        to use the bucket's own `participants.tsv` (single site).
        """
        return None

    def remap(self, items: list[Item]) -> Iterator[tuple[str, Item]]:
        """Map bucket objects to local paths (several per item allowed)"""
        subjects: dict[Site, set[str]] = {}
        for it in items:
            parts = it.rel.split("/")
            if parts[0].startswith("sub-") and len(parts) > 1:
                subjects.setdefault(it.site, set()).add(parts[0][4:])

        for it in items:
            site = it.site
            parts = it.rel.split("/")
            if any(p.startswith(".") for p in parts):
                continue
            if site.derivative or len(parts) > 1:
                path = relabel(it.rel, site)
                yield join(site.target, path), it
                continue
            # top-level file of a site
            name = parts[0]
            stem = name.split(".")[0]
            if stem in self.phenotype:
                yield f"phenotype/{name}", it
            elif self.multisite and _is_sidecar(name):
                # site-level sidecar: push down to each of its subjects
                for sub in sorted(subjects.get(site, ())):
                    label = site.label(sub)
                    yield f"sub-{label}/sub-{label}_{name}", it
            elif self.multisite:
                # dataset-level files are merged or generated
                if name not in TOP_LEVEL_FILES:
                    yield f"sourcedata/{site.name}/{name}", it
            elif name in TOP_LEVEL_FILES or _is_sidecar(name):
                if name == "participants.tsv" and self.tables:
                    continue
                yield name, it
            else:
                yield f"sourcedata/{name}", it

    def generated(self, items: list[Item], tables: dict[str, str]):
        """Files generated from the listing and tables (beyond templates)"""
        return []

    # ------------------------------------------------------------------
    #   Machinery
    # ------------------------------------------------------------------

    def templates(self) -> list[InlineEntry]:
        """Curated files in `templates/<name>/` (mirrored at the root)"""
        entries = self._templates()
        site = getattr(self, "site", None)
        if site is None:
            return entries
        # Site dataset: own README and name; site columns are gone
        out = []
        for e in entries:
            if e.path == "README":
                continue
            if e.path == "dataset_description.json":
                desc = json.loads(e.content)
                desc["Name"] = f"{desc.get('Name', self.title)}: {site.name}"
                e = InlineEntry(e.path, json_bytes(desc))
            if e.path == "participants.json":
                # describe the columns of the site table only
                keep = set(getattr(self, "columns", None) or ()) - {
                    "participant_id"
                }
                columns = {k: v for k, v in json.loads(e.content).items()
                           if k in keep}
                e = InlineEntry(e.path, json_bytes(columns))
            out.append(e)
        paths = {e.path for e in out}
        if "dataset_description.json" not in paths:
            out.append(InlineEntry("dataset_description.json", json_bytes({
                "Name": f"{self.title}: {site.name}",
                "BIDSVersion": "1.9.0",
                "DatasetType": "raw",
            })))
        parent = getattr(self, "parent_dir", "..").rstrip("/")
        out.append(InlineEntry("README", (
            f"{self.title}: site {site.name}\n\n"
            f"One site of a multi-site dataset, downloaded by brainspresso "
            f"with `--layout sites`.\nSee {parent}/README for the "
            f"description of the dataset, how to cite it, and how the files "
            f"of the bucket were curated. {parent}/participants.tsv lists "
            f"the participants of all sites.\n"
        )))
        return out

    def fill_templates(self) -> frozenset[str]:
        """Paths of the fill-in templates (`templates/<name>/_fill.json`)"""
        path = TEMPLATES / self.name / FILL_INDEX
        if not path.is_file():
            return frozenset()
        return frozenset(
            json.loads(path.read_text(encoding="utf-8")).get("fill", ())
        )

    def _templates(self) -> list[InlineEntry]:
        root = TEMPLATES / self.name
        if not root.is_dir():
            return []
        def keep(p):
            if not p.is_file():
                return False
            return p.name == ".bidsignore" or \
                not p.name.startswith((".", "_"))

        out = []
        for p in sorted(root.rglob("*")):
            if not keep(p):
                continue
            rel = p.relative_to(root).as_posix()
            out.append(InlineEntry(
                rel, p.read_bytes(),
                previous=PREVIOUS_TEMPLATES.get(f"{self.name}/{rel}", ()),
            ))
        return out

    # ------------------------------------------------------------------
    #   Layout "sites": one BIDS dataset per site
    # ------------------------------------------------------------------

    def site_view(
        self, site: Site, tables: dict[str, str], parent: str = ".."
    ) -> "Project":
        """
        The project restricted to one site, as a single-site project with
        the labels of the bucket (no site code). `parent` is the
        relative path from the raw dataset of the site to the parent
        folder (which holds the README and merged participants).
        """
        view = copy.copy(self)
        view.parent_dir = parent
        local = dataclasses.replace(site, code="")
        view.sites = (local,)
        view.parent = self
        view.site = site
        merged = self.participants(tables)
        local = None if merged is None else split_participants(merged, site)
        view.participants = lambda tables: local
        view.columns = None if local is None else \
            local.split("\n", 1)[0].split("\t")
        return view

    def site_items(self, items: list[Item], site: Site) -> list[Item]:
        """Items of a site, as seen from its site view"""
        local = dataclasses.replace(site, code="")
        return [dataclasses.replace(it, site=local)
                for it in items if it.site == site]

    def site_entries(
        self, items: list[Item], tables: dict[str, str], site: Site,
        parent: str = "..",
    ) -> list:
        """Local listing of the BIDS dataset of one site"""
        view = self.site_view(site, tables, parent)
        return view.entries(self.site_items(items, site), tables)

    def parent_files(self) -> list[InlineEntry]:
        """Files of the parent folder (layout "sites")"""
        return [e for e in self._templates()
                if e.path in ("README", "participants.json")]

    def entries(self, items: list[Item], tables: dict[str, str]) -> list:
        """Local listing: remote files, generated files and templates"""
        out: dict[str, object] = {}
        site_of: dict[str, str] = {}
        for path, it in self.remap(items):
            if path.startswith("sub-"):
                site_of.setdefault(path.split("/")[0], it.site.name)
            if path in out:
                lg.warning(
                    f"{self.name}: {it.site.prefix}/{it.rel} skipped "
                    f"({path} already comes from another file)"
                )
                continue
            path = reorder_entities(path)
            out[path] = it.obj.entry(fcp.get_bucket().url(it.obj.key), path)
        inline = list(self.generated(items, tables))
        participants = self.participants(tables)
        if participants is not None:
            subjects = {p.split("/")[0] for p in out if p.startswith("sub-")
                        and "/" in p}
            participants = complete_participants(
                participants, subjects,
                site_of if self.multisite else None,
            )
            inline.append(InlineEntry("participants.tsv", participants))
        fill = self.fill_templates()
        templates = self.templates()
        for entry in inline:        # generated files override the bucket's
            out[entry.path] = entry
        site = getattr(self, "site", None)
        where = f" (site {site.name})" if site is not None and site.name \
            else ""
        for entry in templates:
            if entry.path in fill and entry.path in out:
                # fill-in templates never replace a file of the bucket
                # (nor a generated one). Until the placement hook merges
                # keys, the whole template is skipped.
                lg.warning(
                    f"{self.name}{where}: fill-in template {entry.path} "
                    f"not used (the dataset has its own)"
                )
                continue
            out[entry.path] = entry
        return [out[k] for k in sorted(out)]


def split_participants(text: str, site: Site) -> str:
    """
    Rows of a merged participants table that belong to a site, with the
    labels of the site (`site_participant_id`, or the label without the
    site code), without the site columns and without the columns that
    are empty for this site.
    """
    rows, header = read_table(text, "\t"), text.split("\n", 1)[0].split("\t")
    out = []
    for row in rows:
        if row.get("site", site.name) != site.name:
            continue
        row = dict(row)
        row.pop("site", None)
        pid = row.pop("site_participant_id", None)
        if not pid:
            pid = row["participant_id"]
            if site.code and pid.startswith(f"sub-{site.code}"):
                pid = "sub-" + pid[len(f"sub-{site.code}"):]
        row["participant_id"] = pid
        out.append(row)
    header = [
        h for h in header
        if h not in ("site", "site_participant_id")
        and (h == "participant_id"
             or any(r.get(h, "n/a") not in ("", "n/a") for r in out))
    ]
    out = [{h: r.get(h) for h in header} for r in out]
    return format_tsv(out, header)


def site_label(label: str, site: Site) -> str | None:
    """Label of a site dataset, from a merged label (None if not this site)"""
    if not site.code:
        return label
    if label.startswith(site.code):
        return label[len(site.code):]
    return None


def complete_participants(
    text: str, subjects: Iterable[str], sites: dict[str, str] | None = None
) -> str:
    """
    Add a row (with `n/a` values) for subjects missing from the table,
    with their site if `sites` (`{participant_id: site}`) is given.
    """
    rows = read_table(text, "\t")
    header = text.split("\n", 1)[0].split("\t")
    known = {r.get("participant_id") for r in rows}
    missing = sorted(set(subjects) - known)
    if not missing:
        return text
    for s in missing:
        row = {"participant_id": s}
        if sites and "site" in header:
            row["site"] = sites.get(s)
        rows.append(row)
    return format_tsv(rows, header)


def reorder_entities(path: str) -> str:
    """Put the entities of a BIDS file name in the standard order"""
    folder, _, name = path.rpartition("/")
    stem, dot, ext = name.partition(".")
    parts = stem.split("_")
    if len(parts) < 2 or "-" in parts[-1]:
        return path
    pairs = [p.split("-", 1) for p in parts[:-1]]
    if any(len(p) != 2 or p[0] not in ENTITIES for p in pairs):
        return path
    pairs.sort(key=lambda p: ENTITIES.index(p[0]))
    name = "_".join(["-".join(p) for p in pairs] + [parts[-1]]) + dot + ext
    return f"{folder}/{name}" if folder else name


def join(*parts: str) -> str:
    return "/".join(p.strip("/") for p in parts if p)


def relabel(path: str, site: Site) -> str:
    """Prefix subject labels (in folders and file names) with the site code"""
    if not site.code:
        return path
    return re.sub(
        r"(?<![A-Za-z0-9])sub-([A-Za-z0-9]+)",
        lambda m: f"sub-{site.label(m.group(1))}", path,
    )


def _is_sidecar(name: str) -> bool:
    """Top-level sidecar (`T1w.json`, `task-rest_bold.json`...)"""
    parsed = BidsName.parse(name)
    return name.endswith(".json") and bool(parsed.suffix) and \
        name not in TOP_LEVEL_FILES


# ----------------------------------------------------------------------
#   Listing
# ----------------------------------------------------------------------

async def list_sites(
    project: Project,
    sites: Iterable[Site],
    keep_subject=lambda label: True,
    derivatives: bool = False,
    selection=None,
) -> list[Item]:
    """
    List the objects of some sites, pruning subjects that are not kept
    (`keep_subject(label)`, with site-prefixed labels, or the
    `selection`, through the label mapping of each site) and derivative
    folders (unless `derivatives`).
    """
    if selection is not None:
        def keeps(site, label):
            return site.labels.keeps(selection, label)
    else:
        def keeps(site, label):
            return keep_subject(site.label(label))
    sem = asyncio.Semaphore(CONCURRENCY)
    async with new_session() as session:
        bucket = fcp.get_bucket()
        bucket.session = session

        async def ls(key):
            async with sem:
                return await bucket.ls(key)

        async def full(key):
            async with sem:
                return [o async for o in bucket.list(key)]

        async def visit(site: Site, rel: str) -> list[Item]:
            objects, dirs = await ls(site.key + rel)
            out = [Item(site, o.key[len(site.key):], o) for o in objects
                   if not o.key.endswith("/")]
            tasks = []
            for d in dirs:
                name = d[len(site.key) + len(rel):].rstrip("/")
                sub = rel + name + "/"
                if name.startswith("."):
                    continue
                if not rel and name in project.ignore:
                    continue
                if not rel and name in project.derivative_folders \
                        and not derivatives and not site.derivative:
                    continue
                if name.startswith("sub-") and not keeps(site, name[4:]):
                    continue
                if name.startswith("sub-"):
                    tasks.append(full(d))
                else:
                    tasks.append(visit(site, sub))
            for res in await asyncio.gather(*tasks):
                for o in res:
                    if isinstance(o, Item):
                        out.append(o)
                    elif not o.key.endswith("/"):
                        out.append(Item(site, o.key[len(site.key):], o))
            return out

        sites = [s for s in sites if derivatives or not s.derivative]
        results = await asyncio.gather(*(visit(s, "") for s in sites))
    return [it for res in results for it in res]


async def list_extra(project: Project) -> list[tuple[str, S3Object]]:
    """Objects of `project.extra_files`"""
    if not project.extra_files:
        return []
    async with new_session() as session:
        bucket = fcp.get_bucket()
        bucket.session = session
        folders = {}
        for key in project.extra_files.values():
            folders.setdefault(
                f"{fcp.PROJECTS}/{PurePosixPath(key).parent}/", []
            ).append(key)
        listed = await asyncio.gather(*(bucket.ls(k) for k in folders))
    found = {o.key: o for objects, _ in listed for o in objects}
    out = []
    for path, key in project.extra_files.items():
        obj = found.get(f"{fcp.PROJECTS}/{key}")
        if obj is None:
            lg.warning(f"{project.name}: {key} not found in the bucket")
            continue
        out.append((path, obj))
    return out


async def fetch_tables(keys: dict[str, str]) -> dict[str, str]:
    """Download small text files `{name: key}` (missing ones are skipped)"""
    if not keys:
        return {}
    bucket = fcp.get_bucket()
    async with new_session() as session:
        async def get(name, key):
            async with session.get(
                bucket.url(f"{fcp.PROJECTS}/{key}")
            ) as r:
                if r.status == 404:
                    lg.warning(f"{key} not found in the bucket")
                    return name, None
                r.raise_for_status()
                raw = await r.read()
            for encoding in ("utf-8-sig", "latin1"):
                try:
                    return name, raw.decode(encoding)
                except UnicodeDecodeError:
                    continue
        results = await asyncio.gather(*(get(*kv) for kv in keys.items()))
    return {k: v for k, v in results if v is not None}


# ----------------------------------------------------------------------
#   Tables
# ----------------------------------------------------------------------

def read_table(text: str, delimiter: str | None = None) -> list[dict]:
    """Read a CSV/TSV table (delimiter guessed from the header)"""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if delimiter is None:
        first = text.split("\n", 1)[0]
        delimiter = "\t" if first.count("\t") >= first.count(",") else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    rows = []
    for row in reader:
        rows.append({
            (k or "").strip(): (v or "").strip() for k, v in row.items()
            if k is not None
        })
    return rows


def format_tsv(rows: list[dict], first: Iterable[str] = ()) -> str:
    """Write rows as a BIDS table (missing values are `n/a`)"""
    header = list(first)
    for row in rows:
        header += [k for k in row if k not in header]
    out = io.StringIO()
    writer = csv.writer(out, delimiter="\t", lineterminator="\n")
    writer.writerow(header)
    for row in rows:
        writer.writerow([
            _cell(row.get(k)) for k in header
        ])
    return out.getvalue()


def _cell(value) -> str:
    if value is None:
        return NA
    value = str(value).strip()
    return value if value and value.lower() not in ("nan", "na") else NA


def subject_id(value: str) -> str:
    """Normalise a participant id to `sub-<label>`"""
    value = value.strip()
    label = value[4:] if value.startswith("sub-") else value
    label = re.sub(r"[^A-Za-z0-9]", "", label)
    return f"sub-{label}"


def json_bytes(obj) -> bytes:
    return (json.dumps(obj, indent=2) + "\n").encode()
