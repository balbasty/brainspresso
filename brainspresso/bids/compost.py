"""
Compost: delete what is no longer needed in a local dataset.

Two things can be composted:

1. **Source data** (`<dataset>/sourcedata/`): archives and raw files that
   were downloaded by `harvest` and converted by `roast`. Once the
   dataset is roasted, they are usually only taking up space.
   Raw files unpacked under `derivatives/<pipeline>/sourcedata/` while
   roasting (e.g. FreeSurfer outputs) can be composted too.

2. **BIDS data** that falls outside a selection. The filters are the
   same as for `harvest`, and describe what is *kept*: composting with
   `subs=["01", "02"]` leaves exactly what harvesting with the same
   filters would have downloaded. Metadata that still applies to a kept
   data file (BIDS inheritance principle) is kept, dataset-level files
   are never touched, and participants/sessions/scans tables are
   rewritten so that they only list what is left.

The BIDS tree is `<dataset>/rawdata/` if it exists (datasets roasted by
brainspresso), and `<dataset>/` otherwise (BIDS datasets harvested as is,
e.g. OpenNeuro).
"""
from __future__ import annotations

import datetime
import fnmatch
import json
import os
import sys
from dataclasses import asdict, dataclass, field
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Callable, Iterable

from humanize import naturalsize

from brainspresso.bids.select import BidsName, Selection, TOP_LEVEL_FILES
from brainspresso.bids.tables import is_trimmable, query_participants
from brainspresso.bids.tables import trim_tables

lg = getLogger(__name__)

STATE_DIR = ".brainspresso"


@dataclass
class CompostPlan:
    """Files that will be deleted, relative to the dataset root"""
    root: Path
    bids: Path
    sourcedata: list[str] = field(default_factory=list)
    data: list[str] = field(default_factory=list)
    derivatives: list[str] = field(default_factory=list)
    kept_subjects: set[str] | None = None

    @property
    def files(self) -> list[str]:
        return [*self.sourcedata, *self.data, *self.derivatives]

    def size(self, files: Iterable[str] | None = None) -> int:
        files = self.files if files is None else files
        total = 0
        for f in files:
            try:
                total += (self.root / f).lstat().st_size
            except OSError:
                pass
        return total

    def summary(self) -> str:
        lines = []
        for label, files in (
            ("sourcedata", self.sourcedata),
            ("BIDS data", self.data),
            ("derivatives", self.derivatives),
        ):
            if files:
                lines.append(
                    f"{label:>12}: {len(files)} files, "
                    f"{naturalsize(self.size(files))}"
                )
        if not lines:
            return "Nothing to compost."
        lines.append(
            f"{'total':>12}: {len(self.files)} files, "
            f"{naturalsize(self.size())}"
        )
        return "\n".join(lines)


# ----------------------------------------------------------------------
#   Helpers
# ----------------------------------------------------------------------


def bids_tree(root: Path) -> Path:
    """BIDS tree of a local dataset (`rawdata/` if it exists)"""
    root = Path(root)
    raw = root / "rawdata"
    return raw if raw.is_dir() else root


def has_subjects(path: Path) -> bool:
    """Whether a folder contains at least one `sub-*` folder"""
    return Path(path).is_dir() and any(
        p.is_dir() for p in Path(path).glob("sub-*")
    )


def list_files(folder: Path, relative_to: Path) -> list[str]:
    """
    List all files (and symlinks) under a folder, without following
    symlinks, as POSIX paths relative to `relative_to`.
    """
    folder, relative_to = Path(folder), Path(relative_to)
    if not folder.is_dir():
        return []
    out = []
    for dirpath, dirnames, filenames in os.walk(folder):
        dirpath = Path(dirpath)
        # symlinks to folders are listed in dirnames but not walked
        links = [d for d in dirnames if (dirpath / d).is_symlink()]
        for name in [*filenames, *links]:
            out.append((dirpath / name).relative_to(relative_to).as_posix())
    return sorted(out)


def _matches(path: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatchcase(path, p) for p in patterns)


def _relative(path: Path, root: Path) -> str:
    return Path(path).relative_to(root).as_posix()


def find_participants(root: Path, bids: Path) -> Path | None:
    """Find the participants table (original copy first, if trimmed)"""
    for folder in (bids, root):
        for candidate in (
            folder / STATE_DIR / "orig" / "participants.tsv",
            folder / "participants.tsv",
        ):
            if candidate.exists():
                return candidate
    return None


def restrict_participants(
    root: Path, bids: Path, selection: Selection, where: Iterable[str]
) -> Selection:
    """Apply participant queries (`age>=60`...) on the local table"""
    where = list(where or [])
    if not where:
        return selection
    table = find_participants(root, bids)
    if table is None:
        raise ValueError("--where requires a participants.tsv file")
    matching = query_participants(table.read_text(), where)
    if selection.subs:
        matching &= set(selection.subs)
    lg.info(f"{len(matching)} participants match {where}")
    selection.subs = matching or {"__none__"}
    return selection


def is_filtering(selection: Selection | None, where=()) -> bool:
    """Whether a selection (or queries) actually filters something"""
    if where:
        return True
    if selection is None:
        return False
    return any((
        selection.subs, selection.exclude_subs, selection.sessions,
        selection.datatypes, selection.suffixes, selection.tasks,
        selection.include, selection.exclude,
    ))


def _bids_candidates(paths: Iterable[str]) -> list[str]:
    """
    Files that BIDS filtering may delete: everything under `sub-*/`, and
    top-level sidecars (`T1w.json`, `task-rest_bold.json`), which are
    deleted once they do not apply to any data file anymore.
    """
    out = []
    for path in paths:
        parts = PurePosixPath(path).parts
        if any(p.startswith(".") for p in parts):
            continue
        if parts[0].startswith("sub-") and len(parts) > 1:
            out.append(path)
        elif len(parts) == 1 and path not in TOP_LEVEL_FILES:
            name = BidsName.parse(path)
            if name.is_metadata and name.suffix:
                out.append(path)
    return out


def _filter_tree(tree: Path, selection: Selection) -> list[str]:
    """Files of a BIDS tree that fall outside a selection"""
    paths = list_files(tree, tree)
    keep = set(selection.select(paths))
    return [p for p in _bids_candidates(paths) if p not in keep]


# ----------------------------------------------------------------------
#   Plan
# ----------------------------------------------------------------------


def plan_compost(
    root: Path,
    *,
    sourcedata: bool = True,
    nested: bool = False,
    keep: Iterable[str] = (),
    selection: Selection | None = None,
    where: Iterable[str] = (),
    derivatives: bool = False,
    force: bool = False,
) -> CompostPlan:
    """
    Compute the list of files to delete (nothing is deleted).

    Parameters
    ----------
    root : Path
        Dataset root (e.g. `<path>/IXI`)
    sourcedata : bool
        Compost `sourcedata/`
    nested : bool
        Also compost `derivatives/*/sourcedata/`
    keep : list[str]
        Glob patterns (relative to the dataset root, `*` matches `/`) of
        source files to keep, e.g. `"*.xls"`
    selection : Selection
        BIDS filters describing what to keep
    where : list[str]
        Queries on participants.tsv columns describing who to keep
    derivatives : bool
        Apply subject/session filters to `derivatives/*/` too
    force : bool
        Compost source data even if nothing seems to have been roasted
    """
    root = Path(root)
    if not root.is_dir():
        raise FileNotFoundError(f"Dataset folder not found: {root}")
    bids = bids_tree(root)
    plan = CompostPlan(root, bids)
    keep = list(keep or [])

    # --- source data --------------------------------------------------
    if sourcedata:
        folders = [root / "sourcedata"]
        if nested:
            folders += sorted((root / "derivatives").glob("*/sourcedata"))
        files = []
        for folder in folders:
            files += list_files(folder, root)
        files = [f for f in files if not _matches(f, keep)]
        if files and not force and not has_subjects(bids):
            raise ValueError(
                f"No BIDS subject found in {bids}: the dataset does not "
                f"seem to have been roasted, and deleting its source "
                f"data would delete the only copy. Use --force to "
                f"compost anyway."
            )
        plan.sourcedata = files

    # --- BIDS data ----------------------------------------------------
    if is_filtering(selection, where):
        selection = selection or Selection()
        selection = restrict_participants(root, bids, selection, where)
        prefix = _relative(bids, root)
        prefix = "" if prefix == "." else prefix + "/"
        plan.data = [prefix + p for p in _filter_tree(bids, selection)]

        if derivatives:
            # Only subject/session filters make sense for derivatives
            subsel = Selection(
                subs=selection.subs,
                exclude_subs=selection.exclude_subs,
                sessions=selection.sessions,
            )
            pipelines = {
                *(root / "derivatives").glob("*/"),
                *(bids / "derivatives").glob("*/"),
            }
            for pipeline in sorted(p for p in pipelines if p.is_dir()):
                prefix = _relative(pipeline, root) + "/"
                plan.derivatives += [
                    prefix + p for p in _filter_tree(pipeline, subsel)
                ]

        # Subjects that will still have a folder after composting
        removed = set(plan.data)
        prefix = _relative(bids, root)
        prefix = "" if prefix == "." else prefix + "/"
        plan.kept_subjects = set()
        for sub in bids.glob("sub-*"):
            if not sub.is_dir():
                continue
            files = list_files(sub, root)
            if any(f not in removed for f in files):
                plan.kept_subjects.add(sub.name)

    return plan


# ----------------------------------------------------------------------
#   Execute
# ----------------------------------------------------------------------


def _prune_empty(folder: Path, stop: Path) -> None:
    """Remove empty folders from `folder` up to (excluding) `stop`"""
    folder, stop = Path(folder), Path(stop)
    while folder != stop and stop in folder.parents:
        try:
            folder.rmdir()
        except OSError:
            return
        folder = folder.parent


def delete_files(root: Path, files: Iterable[str]) -> int:
    """Delete files (relative to root) and the folders they leave empty"""
    root = Path(root)
    count = 0
    folders = set()
    for f in files:
        path = root / f
        try:
            path.unlink()
            count += 1
        except FileNotFoundError:
            pass
        folders.add(path.parent)
    # deepest first
    for folder in sorted(folders, key=lambda p: len(p.parts), reverse=True):
        _prune_empty(folder, root)
    return count


def retrim_tables(root: Path, bids: Path) -> list[str]:
    """
    Rewrite participants/sessions/scans tables so that they only list
    what is present on disk. Untouched copies saved by `harvest` in
    `.brainspresso/orig/` are used as reference when they exist.
    """
    written = []

    def original(base: Path, relpath: str) -> Path:
        orig = base / STATE_DIR / "orig" / relpath
        return orig if orig.exists() else base / relpath

    # participants.tsv, in the BIDS tree or at the dataset root
    for base in {root, bids}:
        if (base / "participants.tsv").exists():
            written += [
                _relative(base / p, root) for p in trim_tables(
                    base,
                    {"participants.tsv": original(base, "participants.tsv")},
                    subjects=bids,
                )
            ]

    # sessions/scans tables, inside subject folders
    tables = {}
    for path in list_files(bids, bids):
        if path.startswith("sub-") and is_trimmable(path):
            tables[path] = original(bids, path)
    written += [_relative(bids / p, root) for p in trim_tables(bids, tables)]
    return written


def update_manifest(root: Path, plan: CompostPlan, record: dict) -> None:
    """Record the compost run in `.brainspresso/manifest.json`, if any"""
    path = Path(root) / STATE_DIR / "manifest.json"
    if not path.exists():
        return
    try:
        manifest = json.loads(path.read_text())
    except json.JSONDecodeError:
        lg.warning(f"Could not parse {path}: not updated")
        return
    prefix = _relative(plan.bids, root)
    prefix = "" if prefix == "." else prefix + "/"
    deleted = set(plan.files)
    files = manifest.get("files", {})
    for f in list(files):
        if (prefix + f) in deleted:
            del files[f]
    manifest.setdefault("runs", []).append({
        "date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "action": "compost",
        **record,
        "nb_files": len(plan.files),
    })
    path.write_text(json.dumps(manifest, indent=2, default=str))


def confirm(message: str, ask: Callable[[str], str] = input) -> bool:
    """Ask the user for confirmation (refuse if not interactive)"""
    if ask is input and not sys.stdin.isatty():
        raise ValueError(
            "Refusing to delete files without confirmation in a "
            "non-interactive session. Use --yes to confirm."
        )
    try:
        answer = ask(f"{message} [y/N] ")
    except EOFError:
        return False
    return answer.strip().lower() in ("y", "yes")


def compost(
    root: Path,
    *,
    sourcedata: bool | None = None,
    nested: bool = False,
    keep: Iterable[str] = (),
    selection: Selection | None = None,
    where: Iterable[str] = (),
    derivatives: bool = False,
    trim: bool = True,
    force: bool = False,
    yes: bool = False,
    dry_run: bool = False,
    listing: bool = False,
    ask: Callable[[str], str] = input,
) -> CompostPlan:
    """
    Delete source data and/or BIDS data outside of a selection.

    Parameters
    ----------
    root : Path
        Dataset root (e.g. `<path>/IXI`)
    sourcedata : bool | None
        Compost `sourcedata/`. By default (None), source data is
        composted only if no BIDS filter is given.
    nested : bool
        Also compost `derivatives/*/sourcedata/`
    keep : list[str]
        Glob patterns of source files to keep
    selection : Selection
        BIDS filters describing what to keep
    where : list[str]
        Queries on participants.tsv columns describing who to keep
    derivatives : bool
        Apply subject/session filters to `derivatives/*/` too
    trim : bool
        Rewrite participants/sessions/scans tables after composting
        BIDS data
    force : bool
        Compost source data even if nothing seems to have been roasted
    yes : bool
        Do not ask for confirmation
    dry_run : bool
        Only print what would be deleted
    listing : bool
        Print the files to delete
    ask : callable
        Function used to ask for confirmation

    Returns
    -------
    plan : CompostPlan
    """
    root = Path(root)
    filtering = is_filtering(selection, where)
    if sourcedata is None:
        sourcedata = not filtering
    if not sourcedata and not filtering:
        raise ValueError(
            "Nothing to compost: source data is disabled and no BIDS "
            "filter was given."
        )

    plan = plan_compost(
        root,
        sourcedata=sourcedata,
        nested=nested,
        keep=keep,
        selection=selection,
        where=where,
        derivatives=derivatives,
        force=force,
    )

    if listing or dry_run:
        for f in plan.files:
            try:
                size = naturalsize((root / f).lstat().st_size)
            except OSError:
                size = "?"
            print(f"{size:>10}  {f}")
    print(f"Compost {root}:")
    print(plan.summary())
    if filtering and plan.kept_subjects is not None:
        print(f"{'subjects':>12}: {len(plan.kept_subjects)} left")
        if not plan.kept_subjects and plan.data:
            lg.warning("All subjects will be deleted")

    if dry_run or not plan.files:
        return plan
    if not yes and not confirm(
        f"Permanently delete {len(plan.files)} files?", ask
    ):
        print("Aborted.")
        plan.sourcedata, plan.data, plan.derivatives = [], [], []
        return plan

    freed = plan.size()
    count = delete_files(root, plan.files)
    lg.info(f"Deleted {count} files under {root}")

    if trim and (plan.data or plan.derivatives):
        written = retrim_tables(root, plan.bids)
        lg.info(f"Trimmed tables: {written}")

    filters = {}
    if filtering and selection is not None:
        filters = {
            k: sorted(v) if isinstance(v, (set, list, tuple)) else v
            for k, v in asdict(selection).items() if v
        }
    update_manifest(root, plan, {
        "sourcedata": bool(plan.sourcedata),
        "filters": filters,
        "where": list(where or []),
    })
    print(f"Composted {count} files ({naturalsize(freed)}).")
    return plan
