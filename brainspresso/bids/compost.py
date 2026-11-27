"""
Compost: delete what is no longer needed in a local dataset.

Two things can be composted:

1. **Source data** (`<dataset>/sourcedata/`): archives and raw files that
   were downloaded by `harvest` and converted by `roast`. Once the
   dataset is roasted, they are usually only taking up space.
   Raw files unpacked under `derivatives/<pipeline>/sourcedata/` while
   roasting (e.g. FreeSurfer outputs) can be composted too. The
   pristine metadata of a dataset hosted as BIDS (the copies in
   `sourcedata/<folder>/` that its metadata roast reads, `roast.hosted`)
   is kept unless asked for explicitly (`--sourcedata`): the next
   harvest then fetches it again.

2. **BIDS data** that falls outside a selection. The filters are the
   same as for `harvest`, and describe what is *kept*: composting with
   `subs=["01", "02"]` leaves exactly what harvesting with the same
   filters would have downloaded. Metadata that still applies to a kept
   data file (BIDS inheritance principle) is kept, dataset-level files
   are never touched, and participants/sessions/scans tables are
   rewritten so that they only list what is left.

Paths come from the dataset `Layout` (`bids.layout`): the BIDS tree is
`layout.raw` (`<dataset>/rawbids/` for a study, `<dataset>/` for a flat
dataset), source data is `layout.source`, derivatives are
`layout.list_derivatives()`. A "sites" layout is composted site by site
(see `datasets.compost`).
"""
from __future__ import annotations

import fnmatch
import os
import sys
from dataclasses import dataclass, field
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Callable, Iterable

from humanize import naturalsize

from brainspresso.bids.harvest import _filters, record_run, update_files
from brainspresso.bids.layout import (
    Layout,
    SitesLayout,
    read_manifest,
    resolve_layout,
)
from brainspresso.bids.select import TOP_LEVEL_FILES, BidsName, Selection
from brainspresso.bids.tables import (
    is_trimmable,
    query_participants,
    trim_tables,
)
from brainspresso.errors import DatasetNotFound, UsageError

lg = getLogger(__name__)


@dataclass
class CompostPlan:
    """Files that will be deleted, relative to the dataset root."""

    root: Path
    bids: Path
    layout: Layout | None = None
    sourcedata: list[str] = field(default_factory=list)
    data: list[str] = field(default_factory=list)
    derivatives: list[str] = field(default_factory=list)
    kept_subjects: set[str] | None = None
    pristine: list[str] = field(default_factory=list)
    #: manifest entries of selected files never fetched (`--sidecars
    #: only`) that the filters drop: forgotten, nothing to delete
    forgotten: list[str] = field(default_factory=list)

    @property
    def files(self) -> list[str]:
        """Every file to delete."""
        return [*self.sourcedata, *self.data, *self.derivatives]

    def size(self, files: Iterable[str] | None = None) -> int:
        """Return the size of files (default: all), in bytes."""
        files = self.files if files is None else files
        total = 0
        for f in files:
            try:
                total += (self.root / f).lstat().st_size
            except OSError:
                pass
        return total

    def summary(self) -> str:
        """Describe what would be deleted."""
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
        if self.pristine:
            lines.append(
                f"{'kept':>12}: {len(self.pristine)} pristine metadata "
                f"files of the hosted dataset (--sourcedata to delete "
                f"them; the next harvest fetches them again)"
            )
        if not self.files:
            return "\n".join(["Nothing to compost.", *lines])
        lines.append(
            f"{'total':>12}: {len(self.files)} files, "
            f"{naturalsize(self.size())}"
        )
        return "\n".join(lines)


# ----------------------------------------------------------------------
#   Helpers
# ----------------------------------------------------------------------


def has_subjects(path: Path) -> bool:
    """Whether a folder contains at least one `sub-*` folder."""
    return Path(path).is_dir() and any(
        p.is_dir() for p in Path(path).glob("sub-*")
    )


def list_files(folder: Path, relative_to: Path) -> list[str]:
    """
    List all files (and symlinks) under a folder.

    Symlinks are not followed; paths are POSIX, relative to
    `relative_to`.
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


def find_participants(layout: Layout) -> Path | None:
    """Find the participants table (original copy first, if trimmed)."""
    table = layout.raw / "participants.tsv"
    for candidate in (layout.original(table), table):
        if candidate.exists():
            return candidate
    return None


def restrict_participants(
    layout: Layout, selection: Selection, where: Iterable[str]
) -> Selection:
    """Apply participant queries (`age>=60`...) on the local table."""
    where = list(where or [])
    if not where:
        return selection
    table = find_participants(layout)
    if table is None:
        raise UsageError("--where requires a participants.tsv file")
    matching = query_participants(table.read_text(encoding="utf-8"), where)
    if selection.subs:
        matching &= set(selection.subs)
    lg.info(f"{len(matching)} participants match {where}")
    selection.subs = matching or {"__none__"}
    return selection


def is_filtering(selection: Selection | None, where=()) -> bool:
    """Whether a selection (or queries) actually filters something."""
    if where:
        return True
    if selection is None:
        return False
    return any((
        selection.subs, selection.exclude_subs, selection.sessions,
        selection.datatypes, selection.suffixes, selection.tasks,
        selection.entities, selection.include, selection.exclude,
    ))


def _bids_candidates(paths: Iterable[str]) -> list[str]:
    """
    Files that BIDS filtering may delete.

    Everything under `sub-*/`, and top-level sidecars (`T1w.json`,
    `task-rest_bold.json`), which are deleted once they do not apply to
    any data file anymore.
    """
    out = []
    for path in paths:
        parts = PurePosixPath(path).parts
        name = BidsName.parse(path)
        # hidden files, but those of recordings (`*.ome.zarr/.zattrs`)
        unit = PurePosixPath(name.unit or path)
        if any(p.startswith(".") for p in unit.parts):
            continue
        if parts[0].startswith("sub-") and len(parts) > 1:
            out.append(path)
        elif len(parts) == 1 and path not in TOP_LEVEL_FILES:
            if name.is_metadata and name.suffix:
                out.append(path)
    return out


def pristine_files(layout: Layout) -> set[str]:
    """
    Pristine copies of the metadata of a hosted dataset.

    The `source` of its roasted files (`roast.hosted`), from the
    manifest, relative to `layout.root`.

    Parameters
    ----------
    layout : Layout

    Returns
    -------
    set[str]
    """
    manifest = read_manifest(layout.state) or {}
    return {
        entry["source"] for entry in (manifest.get("files") or {}).values()
        if isinstance(entry, dict) and entry.get("origin") == "roasted"
        and isinstance(entry.get("source"), str)
    }


def unfetched_files(layout: Layout) -> list[str]:
    """
    Files of a hosted dataset selected but never fetched.

    Data files of a harvest with `--sidecars only`, recorded with
    `"fetched": false`: they count as present when tables and
    `IntendedFor` are trimmed.

    Parameters
    ----------
    layout : Layout

    Returns
    -------
    list[str]
        Paths relative to `layout.root`
    """
    manifest = read_manifest(layout.state) or {}
    files = manifest.get("files")
    if not isinstance(files, dict):
        return []
    return sorted(k for k, v in files.items()
                  if isinstance(v, dict) and v.get("fetched") is False)


def _filter_tree(tree: Path, selection: Selection) -> list[str]:
    """Files of a BIDS tree that fall outside a selection."""
    paths = list_files(tree, tree)
    keep = set(selection.select(paths))
    return [p for p in _bids_candidates(paths) if p not in keep]


# ----------------------------------------------------------------------
#   Plan
# ----------------------------------------------------------------------


def plan_compost(
    root: Path | Layout,
    *,
    sourcedata: bool = True,
    nested: bool = False,
    keep: Iterable[str] = (),
    selection: Selection | None = None,
    where: Iterable[str] = (),
    derivatives: bool = False,
    force: bool = False,
    layout: str | None = None,
    pristine: bool = False,
) -> CompostPlan:
    """
    Compute the list of files to delete (nothing is deleted).

    Parameters
    ----------
    root : Path | Layout
        Dataset root (e.g. `<path>/IXI`), or its layout
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
    layout : {"study", "flat"}, optional
        Expected layout (default: detected)
    pristine : bool
        Also compost the pristine metadata of a hosted dataset (kept by
        default, `CompostPlan.pristine`)
    """
    if not isinstance(root, Layout) and not Path(root).is_dir():
        raise DatasetNotFound(f"Dataset folder not found: {root}")
    layout = resolve_layout(root, layout)
    if isinstance(layout, SitesLayout):
        raise UsageError(
            f"{layout.root} holds one dataset per site: compost each "
            f"site (`layout.datasets()`)"
        )
    root, bids = layout.root, layout.raw
    plan = CompostPlan(root, bids, layout)
    keep = list(keep or [])

    # --- source data --------------------------------------------------
    if sourcedata:
        folders = [layout.source]
        if nested:
            folders += [d.source for d in layout.list_derivatives()]
        files = []
        for folder in folders:
            files += list_files(folder, root)
        files = [f for f in files if not _matches(f, keep)]
        if not pristine:
            kept = pristine_files(layout)
            plan.pristine = [f for f in files if f in kept]
            files = [f for f in files if f not in kept]
        if files and not force and not has_subjects(bids):
            raise UsageError(
                f"No BIDS subject found in {bids}: the dataset does not "
                f"seem to have been roasted, and deleting its source "
                f"data would delete the only copy. Use --force to "
                f"compost anyway."
            )
        plan.sourcedata = files

    # --- BIDS data ----------------------------------------------------
    if is_filtering(selection, where):
        selection = selection or Selection()
        selection = restrict_participants(layout, selection, where)
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
            pipelines = [d.root for d in layout.list_derivatives()]
            for pipeline in pipelines:
                prefix = _relative(pipeline, root) + "/"
                plan.derivatives += [
                    prefix + p for p in _filter_tree(pipeline, subsel)
                ]

        # Files selected but never fetched (`--sidecars only`): those
        # that the filters drop are forgotten
        unfetched = unfetched_files(layout)
        if unfetched:
            listed = {
                _relative(root / k, bids): k for k in unfetched
                if (root / k).is_relative_to(bids)
            }
            kept = set(selection.select(
                list_files(bids, bids) + list(listed)))
            plan.forgotten = sorted(k for rel, k in listed.items()
                                    if rel not in kept)

        # Subjects that will still have a folder after composting
        removed = set(plan.data)
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
    """Remove empty folders from `folder` up to (excluding) `stop`."""
    folder, stop = Path(folder), Path(stop)
    while folder != stop and stop in folder.parents:
        try:
            folder.rmdir()
        except OSError:
            return
        folder = folder.parent


def delete_files(root: Path, files: Iterable[str]) -> int:
    """Delete files (relative to root) and the folders they leave empty."""
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


def retrim_tables(layout: Layout, present: Iterable[str] = ()) -> list[str]:
    """
    Rewrite participants/sessions/scans tables to what is on disk.

    Untouched copies saved by `harvest` in
    `.brainspresso/orig/` are used as reference when they exist; files of
    `present` (relative to `layout.root`) count as there.
    Returns the tables written, relative to `layout.root`.
    """
    root, bids = layout.root, layout.raw

    def original(path: Path) -> Path:
        orig = layout.original(path)
        return orig if orig.exists() else path

    tables = {}
    if (bids / "participants.tsv").exists():
        path = bids / "participants.tsv"
        tables[_relative(path, root)] = original(path)
    # sessions/scans tables, inside subject folders
    for path in list_files(bids, bids):
        if path.startswith("sub-") and is_trimmable(path):
            tables[_relative(bids / path, root)] = original(bids / path)
    return trim_tables(root, tables, present=present)


def update_manifest(layout: Layout, plan: CompostPlan, record: dict) -> None:
    """Record the compost run in `.brainspresso/manifest.json`, if any."""
    record_run(
        layout, "compost", {**record, "nb_files": len(plan.files)},
        deleted=[*plan.files, *plan.forgotten], create=False,
    )


def confirm(message: str, ask: Callable[[str], str] = input) -> bool:
    """Ask the user for confirmation (refuse if not interactive)."""
    if ask is input and not sys.stdin.isatty():
        raise UsageError(
            "Refusing to delete files without confirmation in a "
            "non-interactive session. Use --yes to confirm."
        )
    try:
        answer = ask(f"{message} [y/N] ")
    except EOFError:
        return False
    return answer.strip().lower() in ("y", "yes")


def compost(
    root: Path | Layout,
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
    layout: str | None = None,
    after_data: Callable[[Layout], object] | None = None,
) -> CompostPlan:
    """
    Delete source data and/or BIDS data outside of a selection.

    Parameters
    ----------
    root : Path | Layout
        Dataset root (e.g. `<path>/IXI`), or its layout
    sourcedata : bool | None
        Compost `sourcedata/`. By default (None), source data is
        composted only if no BIDS filter is given, but for the pristine
        metadata of a hosted dataset, which only True composts.
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
    layout : {"study", "flat"}, optional
        Expected layout (default: detected)
    after_data : callable(layout), optional
        Run after BIDS data is deleted and before source data is (e.g.
        the metadata roast of a hosted dataset, which reads its pristine
        copies in `sourcedata/`)

    Returns
    -------
    plan : CompostPlan
    """
    filtering = is_filtering(selection, where)
    # pristine metadata of hosted datasets: only when asked explicitly
    pristine = sourcedata is True
    if sourcedata is None:
        sourcedata = not filtering
    if not sourcedata and not filtering:
        raise UsageError(
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
        layout=layout,
        pristine=pristine,
    )
    root = plan.root

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
    # (the state folder holds the lock files of the dataset)
    plan.layout.ensure_state()
    # BIDS data first, then what reads source data (`after_data`: the
    # metadata roast of a hosted dataset reads the pristine copies),
    # then source data
    bids_files = [*plan.data, *plan.derivatives]
    count = delete_files(root, bids_files)
    if bids_files or plan.forgotten:
        update_files(plan.layout, deleted=[*bids_files, *plan.forgotten])
    if trim and bids_files:
        written = retrim_tables(plan.layout, unfetched_files(plan.layout))
        lg.info(f"Trimmed tables: {written}")
    if after_data is not None and bids_files:
        after_data(plan.layout)
    count += delete_files(root, plan.sourcedata)
    lg.info(f"Deleted {count} files under {root}")

    filters = _filters(selection) if filtering else {}
    update_manifest(plan.layout, plan, {
        "sourcedata": bool(plan.sourcedata),
        "filters": filters,
        "where": list(where or []),
    })
    print(f"Composted {count} files ({naturalsize(freed)}).")
    return plan
