"""
Download a filtered subset of a remote BIDS dataset.

This is independent of where the dataset is hosted: it takes a listing
of `sources.base.RemoteEntry` (relative path, URL, size, digests) and a
`Selection`; hosted-BIDS sources (`sources.openneuro.OpenNeuro`, INDI
projects) fetch through it.
Entries may also be `InlineEntry` (relative path and content): files
generated locally (e.g. a merged `participants.tsv`, or metadata that the
host lacks) that are selected and written like downloaded files.
"""
from __future__ import annotations

import asyncio
import json
import shutil
from collections import Counter
from dataclasses import asdict, dataclass
from logging import getLogger
from pathlib import Path
from typing import Iterable

from humanize import naturalsize

from brainspresso.errors import UsageError
from brainspresso.actions.state import now
from brainspresso.bids.repair import fix_intended_for
from brainspresso.bids.layout import (
    MANIFEST, ORIG_DIR, STATE_DIR, Layout, resolve_layout,
)
from brainspresso.bids.repair import repair as repair_metadata
from brainspresso.bids.select import BidsName, Selection
from brainspresso.bids.tables import is_trimmable, query_participants
from brainspresso.bids.tables import trim_tables
from brainspresso.download import Downloader, DownloadManager, IfExists
from brainspresso.download.session import new_session

lg = getLogger(__name__)

__all__ = [
    "InlineEntry", "STATE_DIR", "finish_roast", "harvest", "read_manifest",
    "record_run", "trim_originals",
    "write_manifest",
]


@dataclass
class InlineEntry:
    """A file of the dataset generated locally (not downloaded)."""

    path: str                   # relative to dataset root
    content: bytes

    url = None
    mtime = None
    digests = None

    def __post_init__(self):
        """Encode text content."""
        if isinstance(self.content, str):
            self.content = self.content.encode()

    @property
    def size(self) -> int:
        """Size of the content, in bytes."""
        return len(self.content)


def write_inline(entry: InlineEntry, dst: Path, ifexists: str) -> bool:
    """Write a generated file. Returns whether it was (re)written."""
    dst = Path(dst)
    if dst.exists():
        if dst.read_bytes() == entry.content:
            return False
        if ifexists == "error":
            raise FileExistsError(dst)
        if ifexists == "skip":
            return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(entry.content)
    return True


def resolve_participants(entries, selection: Selection, where: Iterable[str]):
    """
    Apply participant queries (`age>=60`, `sex==F`...).

    The remote `participants.tsv` is read, and `selection.subs`
    restricted accordingly.
    """
    where = list(where or [])
    if not where:
        return selection
    table = next((e for e in entries if e.path == "participants.tsv"), None)
    if table is None:
        raise UsageError("--where requires a participants.tsv file")

    async def fetch():
        if getattr(table, "content", None) is not None:
            return table.content.decode()
        async with new_session() as session:
            async with session.get(table.url) as r:
                r.raise_for_status()
                return await r.text()

    matching = query_participants(asyncio.run(fetch()), where)
    if selection.subs:
        matching &= set(selection.subs)
    lg.info(f"{len(matching)} participants match {where}")
    selection.subs = matching or {"__none__"}
    return selection


def summarize(entries) -> dict:
    """Summary of a list of entries (for display)."""
    subs, datatypes = set(), Counter()
    for e in entries:
        name = BidsName.parse(e.path)
        if "sub" in name.entities:
            subs.add(name.entities["sub"])
        if name.datatype and not name.is_metadata:
            datatypes[name.datatype] += 1
    return {
        "files": len(entries),
        "size": sum(e.size or 0 for e in entries),
        "subjects": len(subs),
        "datatypes": dict(sorted(datatypes.items())),
    }


def format_summary(summary: dict) -> str:
    dt = ", ".join(f"{k}: {v}" for k, v in summary["datatypes"].items())
    return (
        f"{summary['files']} files, {naturalsize(summary['size'])}, "
        f"subjects: {summary['subjects']}"
        + (f" (data files per datatype: {dt})" if dt else "")
    )


def subjects_of(paths: Iterable[str]) -> set[str]:
    """
    Subject labels of the paths of a listing (`sub-<label>/...`).

    Parameters
    ----------
    paths : iterable[str]

    Returns
    -------
    set[str]
    """
    out = set()
    for path in paths:
        head = str(path).split("/", 1)[0]
        if head.startswith("sub-"):
            out.add(head[4:])
    return out


def check_selected_subjects(selection, entries, found=None) -> None:
    """
    Warn about `--subs` labels that match nothing (error if none does).

    Parameters
    ----------
    selection : Selection
        The selection (its `subs`)
    entries : list[RemoteEntry]
        The remote listing
    found : set[str], optional
        Subjects found (default: those of `entries`)

    Raises
    ------
    UsageError
        If no selected subject is in the listing
    """
    wanted = {str(s) for s in getattr(selection, "subs", None) or ()}
    if not wanted:
        return
    if found is None:
        found = subjects_of(e.path for e in entries)
    missing = sorted(wanted - set(found))
    if missing and len(missing) == len(wanted):
        raise UsageError(f"No selected subject is in the dataset: "
                         f"{', '.join(missing)}")
    if missing:
        lg.warning(f"Selected subjects not in the dataset: "
                   f"{', '.join(missing)}")


def harvest(
    entries: list,
    root: Path | Layout,
    selection: Selection,
    *,
    where: Iterable[str] = (),
    trim: bool = True,
    if_exists: IfExists.Choice = "skip",
    jobs: int = 8,
    dry_run: bool = False,
    listing: bool = False,
    provenance: dict | None = None,
    repair: bool = False,
    layout: str | None = None,
    insecure: bool = False,
    chunk_size: int | None = None,
    check_subjects: bool = True,
) -> list:
    """
    Select and download a subset of a remote BIDS dataset.

    Parameters
    ----------
    entries : list[RemoteEntry]
        Full listing of the remote dataset (paths relative to its root)
    root : Path | Layout
        Local dataset folder, or its layout. Entry paths are placed with
        `layout.resolve` (raw files in `layout.raw`, `sourcedata/` in
        `layout.source`, `derivatives/<name>/` in derivative datasets).
    selection : Selection
        Filters
    where : list[str]
        Queries on participants.tsv columns (e.g. "age>=60")
    trim : bool
        Rewrite participants/sessions/scans tables so that they only
        list what is present locally, so that they describe the
        subset. Untouched copies are kept in `.brainspresso/orig/`.
        Field map `IntendedFor` targets that are not present locally
        are removed too.
    if_exists : {"skip", "overwrite", "different", "refresh", "error"}
        Behaviour if a file already exists
    jobs : int
        Number of concurrent downloads
    dry_run : bool
        Only print what would be downloaded
    listing : bool
        Print the list of selected files
    provenance : dict
        Extra information saved in the manifest (source, dataset, tag...)
    repair : bool
        Repair common defects of the downloaded JSON and TSV files
        (see `bids.repair`), before tables are trimmed.
    layout : {"study", "flat"}, optional
        Layout of a new dataset (if `root` is a path). Must match the
        layout of an existing dataset.
    insecure : bool
        Do not verify TLS certificates
    chunk_size : int, optional
        Download chunk size, in bytes
    check_subjects : bool
        Warn about selected subjects (`selection.subs`) that match
        nothing, and raise `UsageError` if none matches

    Returns
    -------
    selected : list[RemoteEntry]
    """
    if check_subjects:
        check_selected_subjects(selection, entries)
    selection = resolve_participants(entries, selection, where)
    keep = set(selection.select(e.path for e in entries))
    selected = [e for e in entries if e.path in keep]

    if listing or dry_run:
        for e in selected:
            size = naturalsize(e.size) if e.size is not None else "?"
            print(f"{size:>10}  {e.path}")
    print(f"Selected: {format_summary(summarize(selected))}")
    print(f"     out of {format_summary(summarize(entries))}")
    if dry_run or not selected:
        return selected

    layout = resolve_layout(root, layout)
    # before any download takes a lock (`actions.locks`)
    layout.ensure_state()
    originals = {}
    destinations = {}
    downloaders = []
    nb_inline = 0
    for e in selected:
        dst = destinations[e.path] = layout.resolve(e.path)
        relpath = dst.relative_to(layout.root).as_posix()
        if trim and is_trimmable(e.path):
            originals[relpath] = dst = layout.original(dst)
        if isinstance(e, InlineEntry):
            # generated tables are always refreshed (they are trimmed)
            nb_inline += write_inline(
                e, dst, "overwrite" if relpath in originals else if_exists
            )
            continue
        downloaders.append(Downloader(
            e.url, dst,
            size=e.size,
            mtime=e.mtime,
            digests=e.digests or None,
            ifexists=if_exists,
            insecure=insecure,
            **({"chunk_size": chunk_size} if chunk_size else {}),
        ))

    if nb_inline:
        lg.info(f"Wrote {nb_inline} generated files")
    if downloaders:
        DownloadManager(
            downloaders, ifexists=if_exists, jobs=jobs, path="short",
        ).run()

    if repair:
        targets = [Path(d.dst) for d in downloaders
                   if str(d.dst).endswith((".json", ".tsv"))]
        repaired = repair_metadata(targets)
        if repaired:
            lg.info(f"Repaired {len(repaired)} metadata files")

    if repair or trim:
        # field maps: repair free-text `IntendedFor`, drop missing targets
        fmaps = [Path(d.dst) for d in downloaders
                 if Path(d.dst).parent.name == "fmap"
                 and str(d.dst).endswith(".json")]
        for path in fmaps:
            dataset, _ = layout.relative(path)
            if repair:
                fix_intended_for(path, dataset, trim=False)
            if trim:
                fix_intended_for(path, dataset, trim=True)

    if trim:
        written = trim_tables(layout.root, originals)
        lg.info(f"Trimmed tables: {written}")

    layout.write_descriptions()
    write_manifest(layout, selected, selection, where, provenance or {},
                   destinations)
    return selected


# ----------------------------------------------------------------------
#   State: manifest and per-task state
# ----------------------------------------------------------------------
#
# `<state>/manifest.json` (`<state>` is `layout.state`, i.e.
# `<DS>/.brainspresso/`; each site of a "sites" layout has its own):
#
#     {
#       "schema": 1,
#       "layout": "study" | "flat" | "sites",
#       "children": {"<site>": "study" | "flat"},    # "sites" only
#       "runs": [                                    # stages run, in order
#         {"stage": "harvest" | "roast" | "compost" | "grind",
#          "date": "<ISO 8601, UTC>",
#          ...                                       # stage-specific, e.g.
#          "source": "openneuro", "dataset": "ds000001", "tag": "1.0.0",
#          "keys": [...], "filters": {...}, "where": [...], "nb_files": 12}
#       ],
#       "files": {                                   # harvested files
#         "<path relative to layout.root>": {
#           "url": "...", "size": 123, "digests": {"md5": "..."},
#           "origin": "direct" | "roasted" | "generated"
#         }
#       }
#     }
#
# `origin` says how a file got to its place: downloaded to its final
# path (`direct`), downloaded to `sourcedata/` then roasted (`roasted`,
# hybrid placement of hosted-BIDS sources, step 2b-iii), or generated
# locally (`generated`). Manifests without `schema` (written before the
# layouts) are read as is; their `files` keys are relative to the raw
# dataset, which is `layout.root` for a flat layout.
#
# `<state>/tasks/<task id>.json`: one file per task (roast shard, grind
# unit...), so that concurrent jobs never write the same file. The
# manifest is an index that can be rebuilt from them. Written by roast
# shards (`actions.engine`, which adds `run`, `counts`, `progress` and
# `children`; helpers in `actions.state`). Task ids are free-form (e.g.
# "grind/fs/sub-01/ses-1") and URL-quoted into file names.
#
#     {
#       "schema": 1,
#       "task": "<task id>",
#       "stage": "roast" | "grind",
#       "pipeline": "<name>" | null,              # grind
#       "unit": {"sub": "01", "ses": "1"},        # entities of the unit
#       "status": "pending" | "running" | "done" | "failed" | "skipped",
#       "config_hash": "<hex>" | null,            # skip/redo decisions
#       "started": "<ISO 8601>" | null,
#       "finished": "<ISO 8601>" | null,
#       "error": "<message>" | null,
#       "outputs": ["<path relative to layout.root>", ...]
#     }

MANIFEST_SCHEMA = 1


def read_manifest(state: Path) -> dict | None:
    """Content of `<state>/manifest.json` (None if missing or invalid)."""
    path = Path(state) / MANIFEST
    if not path.exists():
        return None
    try:
        manifest = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        lg.warning(f"Could not parse {path}")
        return None
    return manifest if isinstance(manifest, dict) else None


def record_run(
    layout: Layout,
    stage: str,
    info: dict | None = None,
    *,
    files: dict[str, dict] | None = None,
    deleted: Iterable[str] = (),
    create: bool = True,
) -> Path | None:
    """
    Record a stage run in `<state>/manifest.json`.

    It is merged with previous runs, with the layout of the dataset.

    Parameters
    ----------
    layout : Layout
    stage : {"harvest", "roast", "compost", "grind"}
    info : dict
        Stage-specific fields of the run
    files : dict[str, dict]
        Files to add, keyed by path relative to `layout.root`
    deleted : list[str]
        Files to remove, relative to `layout.root`
    create : bool
        Create the manifest if it does not exist

    Returns
    -------
    path : Path | None
        Path of the manifest (None if it does not exist and `create` is
        False)
    """
    state = Path(layout.state)
    path = state / MANIFEST
    if not path.exists() and not create:
        return None
    manifest = {}
    if path.exists():
        try:
            manifest = json.loads(path.read_text())
        except json.JSONDecodeError:
            shutil.copy(path, path.with_suffix(".json.bak"))
            manifest = {}
    manifest["schema"] = MANIFEST_SCHEMA
    manifest.update(layout.to_manifest())
    manifest.setdefault("runs", []).append({
        "stage": stage, "date": now(), **(info or {}),
    })
    entries = manifest.setdefault("files", {})
    for f in deleted:
        entries.pop(f, None)
    entries.update(files or {})
    layout.ensure_state()
    path.write_text(json.dumps(manifest, indent=2, default=str))
    return path


def _filters(selection) -> dict:
    if selection is None:
        return {}
    return {
        k: sorted(v) if isinstance(v, (set, list, tuple)) else v
        for k, v in asdict(selection).items() if v
    }


def write_manifest(
    layout: Layout, selected, selection, where, provenance,
    destinations: dict[str, Path] | None = None,
):
    """
    Save what `harvest` requested and downloaded in the manifest.

    See `record_run`. `destinations` maps entry paths to their local
    path (default: `layout.resolve(entry.path)`).
    """
    destinations = destinations or {}
    files = {}
    for e in selected:
        dst = destinations.get(e.path) or layout.resolve(e.path)
        key = Path(dst).relative_to(layout.root).as_posix()
        if isinstance(e, InlineEntry):
            files[key] = {"origin": "generated", "size": e.size}
            continue
        files[key] = {
            "url": e.url,
            "size": e.size,
            **({"digests": e.digests} if e.digests else {}),
            "origin": "direct",
        }
    return record_run(layout, "harvest", {
        **provenance,
        "filters": _filters(selection),
        "where": list(where or []),
        "nb_files": len(selected),
    }, files=files)


def trim_originals(layout: Layout) -> list[str]:
    """
    Derive the visible tables of a roasted dataset from pristine copies.

    The participants/sessions/scans tables are derived from their
    pristine copies (`<state>/orig/<path>`, written complete by
    roasters), keeping the rows that refer to something on
    disk. A sessions or scans table whose folder does not exist (subject
    or session not roasted) is not written.

    Returns the tables written, relative to `layout.root`.
    """
    orig = Path(layout.state) / ORIG_DIR
    if not orig.is_dir():
        return []
    originals = {}
    for path in orig.rglob("*.tsv"):
        relpath = path.relative_to(orig).as_posix()
        if not is_trimmable(relpath) and \
                not relpath.endswith("/participants.tsv"):
            continue
        if Path(relpath).name != "participants.tsv" and \
                not (Path(layout.root) / relpath).parent.is_dir():
            continue
        originals[relpath] = path
    written = trim_tables(layout.root, originals)
    if written:
        lg.info(f"Trimmed tables: {written}")
    return written


def finish_roast(layout: Layout, **info) -> None:
    """
    Run the steps shared by all roasts, once their actions have run.

    Write the descriptions that depend on the layout (study description,
    `SourceDatasets` of derivatives) and record the run in the manifest.
    """
    layout.write_descriptions()
    info = {k: sorted(v) if isinstance(v, (set, frozenset)) else v
            for k, v in info.items()}
    record_run(layout, "roast", info)
