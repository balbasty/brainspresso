"""
Download a filtered subset of a remote BIDS dataset.

This is independent of where the dataset is hosted: it takes a listing
of `RemoteEntry` (relative path, URL, size, digests) and a `Selection`.
Entries may also be `InlineEntry` (relative path and content): files
generated locally (e.g. a merged `participants.tsv`, or metadata that the
host lacks) that are selected and written like downloaded files.
"""
from __future__ import annotations

import asyncio
import datetime
import json
import shutil
from collections import Counter
from dataclasses import asdict, dataclass
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Iterable

from humanize import naturalsize

from brainspresso.bids.repair import fix_intended_for
from brainspresso.bids.repair import repair as repair_metadata
from brainspresso.bids.select import BidsName, Selection
from brainspresso.bids.tables import is_trimmable, query_participants
from brainspresso.bids.tables import trim_tables
from brainspresso.download import Downloader, DownloadManager, IfExists
from brainspresso.download.session import new_session

lg = getLogger(__name__)

STATE_DIR = ".brainspresso"


@dataclass
class InlineEntry:
    """A file of the dataset generated locally (not downloaded)"""
    path: str                   # relative to dataset root
    content: bytes

    url = None
    mtime = None
    digests = None

    def __post_init__(self):
        if isinstance(self.content, str):
            self.content = self.content.encode()

    @property
    def size(self) -> int:
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
    Apply participant queries (`age>=60`, `sex==F`...) by reading the
    remote `participants.tsv`, and restrict `selection.subs` accordingly.
    """
    where = list(where or [])
    if not where:
        return selection
    table = next((e for e in entries if e.path == "participants.tsv"), None)
    if table is None:
        raise ValueError("--where requires a participants.tsv file")

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
    """Summary of a list of entries (for display)"""
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


def harvest(
    entries: list,
    root: Path,
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
) -> list:
    """
    Select and download a subset of a remote BIDS dataset.

    Parameters
    ----------
    entries : list[RemoteEntry]
        Full listing of the remote dataset (paths relative to its root)
    root : Path
        Local BIDS root
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

    Returns
    -------
    selected : list[RemoteEntry]
    """
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

    root = Path(root)
    state = root / STATE_DIR
    originals = {}
    downloaders = []
    nb_inline = 0
    for e in selected:
        dst = root / PurePosixPath(e.path)
        if trim and is_trimmable(e.path):
            dst = state / "orig" / PurePosixPath(e.path)
            originals[e.path] = dst
        if isinstance(e, InlineEntry):
            # generated tables are always refreshed (they are trimmed)
            nb_inline += write_inline(
                e, dst, "overwrite" if e.path in originals else if_exists
            )
            continue
        downloaders.append(Downloader(
            e.url, dst,
            size=e.size,
            mtime=e.mtime,
            digests=e.digests or None,
            ifexists=if_exists,
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
            if repair:
                fix_intended_for(path, root, trim=False)
            if trim:
                fix_intended_for(path, root, trim=True)

    if trim:
        written = trim_tables(root, originals)
        lg.info(f"Trimmed tables: {written}")

    write_manifest(state, selected, selection, where, provenance or {})
    return selected


def write_manifest(state: Path, selected, selection, where, provenance):
    """
    Save what was requested and downloaded in `.brainspresso/manifest.json`
    (merged with previous runs).
    """
    state.mkdir(parents=True, exist_ok=True)
    path = state / "manifest.json"
    manifest = {"runs": [], "files": {}}
    if path.exists():
        try:
            manifest = json.loads(path.read_text())
        except json.JSONDecodeError:
            shutil.copy(path, path.with_suffix(".json.bak"))
    filters = {
        k: sorted(v) if isinstance(v, (set, list, tuple)) else v
        for k, v in asdict(selection).items() if v
    }
    manifest.setdefault("runs", []).append({
        "date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        **provenance,
        "filters": filters,
        "where": list(where or []),
        "nb_files": len(selected),
    })
    files = manifest.setdefault("files", {})
    for e in selected:
        if isinstance(e, InlineEntry):
            files[e.path] = {"generated": True, "size": e.size}
            continue
        files[e.path] = {
            "url": e.url,
            "size": e.size,
            **({"digests": e.digests} if e.digests else {}),
        }
    path.write_text(json.dumps(manifest, indent=2, default=str))
