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

Local layout: `<root>/<Project.folder>/` is a BIDS dataset, with
derivatives in `derivatives/<pipeline>/`.
"""
from __future__ import annotations

import asyncio
import csv
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
from brainspresso.sources.openneuro import RemoteEntry
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
        root = TEMPLATES / self.name
        if not root.is_dir():
            return []
        def keep(p):
            if not p.is_file():
                return False
            return p.name == ".bidsignore" or \
                not p.name.startswith((".", "_"))

        return [
            InlineEntry(p.relative_to(root).as_posix(), p.read_bytes())
            for p in sorted(root.rglob("*")) if keep(p)
        ]

    def entries(self, items: list[Item], tables: dict[str, str]) -> list:
        """Local listing: remote files, generated files and templates"""
        out: dict[str, object] = {}
        for path, it in self.remap(items):
            if path in out:
                lg.warning(
                    f"{self.name}: {it.site.prefix}/{it.rel} skipped "
                    f"({path} already comes from another file)"
                )
                continue
            path = reorder_entities(path)
            out[path] = RemoteEntry(
                path, fcp.get_bucket().url(it.obj.key),
                size=it.obj.size, mtime=it.obj.mtime,
                digests={"md5": it.obj.md5} if it.obj.md5 else {},
            )
        inline = list(self.generated(items, tables))
        participants = self.participants(tables)
        if participants is not None:
            subjects = {p.split("/")[0] for p in out if p.startswith("sub-")
                        and "/" in p}
            participants = complete_participants(participants, subjects)
            inline.append(InlineEntry("participants.tsv", participants))
        inline += self.templates()
        for entry in inline:        # generated files override the bucket's
            out[entry.path] = entry
        return [out[k] for k in sorted(out)]


def complete_participants(text: str, subjects: Iterable[str]) -> str:
    """Add a row (with `n/a` values) for subjects missing from the table"""
    rows = read_table(text, "\t")
    header = text.split("\n", 1)[0].split("\t")
    known = {r.get("participant_id") for r in rows}
    missing = sorted(set(subjects) - known)
    if not missing:
        return text
    rows += [{"participant_id": s} for s in missing]
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
) -> list[Item]:
    """
    List the objects of some sites, pruning subjects that are not kept
    (`keep_subject(label)`, with site-prefixed labels) and derivative
    folders (unless `derivatives`).
    """
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
                if name.startswith("sub-") and \
                        not keep_subject(site.label(name[4:])):
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
