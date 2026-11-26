"""
`bx <project> harvest` and `bx <project> info` for curated INDI projects.
"""
from __future__ import annotations

import asyncio
import copy
import functools
import sys
from pathlib import Path
from typing import Annotated, Iterable, Literal

from cyclopts import App, Parameter
from humanize import naturalsize

from brainspresso.bids.harvest import (
    InlineEntry, format_summary, harvest, resolve_participants, summarize,
    write_inline,
)
from brainspresso.bids.tables import read_tsv, write_tsv
from brainspresso.bids.select import Selection
from brainspresso.cli import app
from brainspresso.datasets.compost import add_compost
from brainspresso.download import IfExists
from brainspresso.utils.log import setup_filelog
from brainspresso.utils.path import get_tree_path
from brainspresso.datasets.INDI.engine import (
    Project, Site, complete_participants, fetch_tables, list_extra,
    list_sites, site_label,
)
from brainspresso.sources import fcp
from brainspresso.sources.openneuro import RemoteEntry

FolderChoice = Literal["derivatives", "sourcedata"]
LayoutChoice = Literal["merged", "sites"]
Many = Parameter(consume_multiple=True)


def user_errors(func):
    """Print user-facing errors without a traceback"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (KeyError, ValueError) as e:
            message = e.args[0] if isinstance(e, KeyError) else e
            print(f"Error: {message}", file=sys.stderr)
            sys.exit(1)
    return wrapper


def prepare(
    project: Project,
    *,
    sites: Iterable[str] = (),
    selection: Selection | None = None,
    where: Iterable[str] = (),
    site_labels: bool = False,
):
    """
    List a project, pruned to the selection.

    Subject labels are those of the merged dataset. With `site_labels`,
    labels of the site datasets (without site code) are accepted too.

    Returns
    -------
    items : list[Item]
    tables : dict[str, str]
    selection : Selection   (participant queries applied)
    sites : list[Site]      (selected sites)
    """
    selection = selection or Selection()
    chosen = project.select_sites(sites)
    if site_labels:
        for attr in ("subs", "exclude_subs"):
            labels = set(getattr(selection, attr))
            setattr(selection, attr, labels | {
                s.label(x) for s in chosen for x in labels
            })
    tables = asyncio.run(fetch_tables(project.tables))
    participants = project.participants(tables)
    where = list(where or [])
    if where:
        if participants is None:
            raise ValueError(
                f"--where is not available for {project.name} "
                f"(no participants table)"
            )
        selection = resolve_participants(
            [InlineEntry("participants.tsv", participants)], selection, where
        )
    items = asyncio.run(list_sites(
        project, chosen,
        keep_subject=selection.keep_subject,
        derivatives="derivatives" in selection.folders,
    ))
    return items, tables, selection, chosen


def extra_entries(project: Project) -> list:
    return [
        RemoteEntry(
            path, fcp.get_bucket().url(obj.key), size=obj.size,
            mtime=obj.mtime, digests={"md5": obj.md5} if obj.md5 else {},
        )
        for path, obj in asyncio.run(list_extra(project))
    ]


def collect(
    project: Project,
    *,
    sites: Iterable[str] = (),
    selection: Selection | None = None,
    where: Iterable[str] = (),
) -> tuple[list, Selection]:
    """
    List a project (pruned to the selection) and return its local
    listing as one merged dataset (remote and generated entries) and the
    selection, with participant queries applied.
    """
    items, tables, selection, _ = prepare(
        project, sites=sites, selection=selection, where=where,
    )
    entries = project.entries(items, tables) + extra_entries(project)
    return entries, selection


def site_selection(selection: Selection, site: Site) -> Selection:
    """Selection with the labels of a site dataset"""
    def local(labels):
        out = {site_label(x, site) for x in labels}
        return {x for x in out if x is not None}
    out = copy.copy(selection)
    if selection.subs:
        out.subs = local(selection.subs) or {"__none__"}
    out.exclude_subs = local(selection.exclude_subs)
    return out


def harvest_sites(
    project: Project, root: Path, selection: Selection, *,
    sites=(), where=(), provenance=None, **options,
) -> None:
    """Layout "sites": one BIDS dataset per site in `root/<site>/`"""
    items, tables, selection, chosen = prepare(
        project, sites=sites, selection=selection, where=where,
        site_labels=True,
    )
    dry_run = options.get("dry_run", False)
    for site in chosen:
        if site.derivative:
            continue
        entries = project.site_entries(items, tables, site)
        local = site_selection(selection, site)
        if not local.select(e.path for e in entries if "/" in e.path):
            continue
        print(f"--- {site.name}")
        harvest(entries, root / site.name, local, repair=True,
                provenance={**(provenance or {}), "site": site.name},
                **options)
    extra = extra_entries(project)
    parent = Selection(folders=list(selection.folders))
    if parent.select(e.path for e in extra):
        print("--- (dataset)")
        harvest(extra, root, parent, provenance=provenance, **options)
    if dry_run:
        return
    # Parent folder: README and merged participants (trimmed)
    for entry in project.parent_files():
        write_inline(entry, root / entry.path, "overwrite")
    merged = project.participants(tables)
    if merged is not None:
        present = {
            f"sub-{s.label(p.name[4:])}": s.name
            for s in chosen if (root / s.name).is_dir()
            for p in (root / s.name).glob("sub-*") if p.is_dir()
        }
        merged = complete_participants(merged, present, present)
        header, rows = read_tsv(merged)
        write_tsv(root / "participants.tsv", header,
                  [r for r in rows if r["participant_id"] in present])


def compost_roots(project: Project, folder: Path) -> list[Path]:
    """
    Datasets to compost: the dataset folder, or the site datasets if it
    was harvested with `--layout sites` (the parent is not BIDS).
    """
    if (folder / "dataset_description.json").exists() or \
            not project.multisite:
        return [folder]
    sites = [folder / s.name for s in project.sites
             if (folder / s.name / "dataset_description.json").exists()]
    return sites or [folder]


def register(project: Project) -> App:
    """Register `bx <project.name> {harvest,info}`"""
    cmd = App(name=project.name, help=project.help)
    app.command(cmd)
    add_compost(cmd, project.folder, roots=functools.partial(
        compost_roots, project
    ))

    @cmd.command(name="harvest")
    @user_errors
    def download(
        path: str | None = None,
        *,
        sites: Annotated[Iterable[str], Many] = (),
        subs: Annotated[Iterable[str], Many] = (),
        exclude_subs: Annotated[Iterable[str], Many] = (),
        sessions: Annotated[Iterable[str], Many] = (),
        datatypes: Annotated[Iterable[str], Many] = (),
        suffixes: Annotated[Iterable[str], Many] = (),
        tasks: Annotated[Iterable[str], Many] = (),
        include: Annotated[Iterable[str], Many] = (),
        exclude: Annotated[Iterable[str], Many] = (),
        where: Annotated[Iterable[str], Many] = (),
        folders: Annotated[Iterable[FolderChoice], Many] = (),
        layout: LayoutChoice = "merged",
        trim: bool = True,
        out: str | None = None,
        if_exists: IfExists.Choice = "skip",
        jobs: int = 8,
        dry_run: bool = False,
        list: bool = False,
        log: str | None = None,
        level: str = "info",
    ):
        """
        Download (a subset of) the dataset, as BIDS.

        Metadata (sidecars, tables, top-level files) that applies to the
        selected data files is always included. Files that the bucket
        lacks (merged participants table, curated description, missing
        sidecars) are generated.

        Parameters
        ----------
        path
            Path to root of all datasets. The dataset folder is created
            inside. Ignored if `--out` is used.
        sites
            Only download these sites (multi-site datasets)
        subs
            Only download these subjects (labels, with or without `sub-`).
            With `--layout sites`, labels of the site datasets (e.g.
            `--sites ICBM --subs 00448`) are accepted too.
        exclude_subs
            Do not download these subjects
        sessions
            Only download these sessions
        datatypes
            Only download these datatypes (anat, func, dwi, fmap...)
        suffixes
            Only download these suffixes (T1w, T2w, bold, dwi...)
        tasks
            Only download these tasks
        include
            Only download data files matching these glob patterns
            (relative to the dataset root, e.g. "sub-*/anat/*")
        exclude
            Do not download files matching these glob patterns
        where
            Keep participants matching these queries on participants.tsv
            columns, e.g. "age>=60" or "sex==F" (all queries must match)
        folders
            Also download these optional folders
        layout
            "merged": one BIDS dataset, sites merged (labels made unique
            with a site code when needed, `site` column in
            participants.tsv). "sites": one BIDS dataset per site in
            `<DATASET>/<site>/`, with the labels of the bucket; the
            parent folder holds the README and the merged
            participants.tsv. Single-site datasets ignore it.
        trim
            Rewrite participants/sessions/scans tables so that they only
            list what is present locally. Originals are kept in
            `.brainspresso/orig/`.
        out
            Output BIDS root (overrides `path`)
        if_exists
            Behaviour if a file already exists
        jobs
            Number of concurrent downloads
        dry_run
            Print the selected files and their total size, then exit
        list
            Print the selected files before downloading
        log
            Path to log file
        level
            Logging level
        """
        setup_filelog(log, level=level)
        selection = Selection(
            subs=subs, exclude_subs=exclude_subs, sessions=sessions,
            datatypes=datatypes, suffixes=suffixes, tasks=tasks,
            include=include, exclude=exclude, folders=folders,
        )
        root = Path(out) if out else get_tree_path(path) / project.folder
        provenance = {
            "source": "s3://fcp-indi",
            "dataset": project.name,
            "sites": sorted(sites or []),
            "layout": layout,
        }
        if layout == "sites" and project.multisite:
            harvest_sites(
                project, root, selection, sites=sites, where=where,
                provenance=provenance, trim=trim, if_exists=if_exists,
                jobs=jobs, dry_run=dry_run, listing=list,
            )
            if not dry_run:
                print(f"Site datasets in: {root}")
            return
        entries, selection = collect(
            project, sites=sites, selection=selection, where=where,
        )
        harvest(
            entries, root, selection,
            trim=trim, if_exists=if_exists, jobs=jobs,
            dry_run=dry_run, listing=list, repair=True,
            provenance=provenance,
        )
        if not dry_run:
            print(f"BIDS root: {root}")

    @cmd.command(name="info")
    @user_errors
    def info(
        *,
        sites: Annotated[Iterable[str], Many] = (),
        derivatives: bool = False,
    ):
        """
        Show the content of the dataset (lists the whole bucket folder).

        Parameters
        ----------
        sites
            Only show these sites
        derivatives
            Also list derivatives
        """
        selection = Selection(folders=["derivatives"] if derivatives else [])
        entries, _ = collect(project, sites=sites, selection=selection)
        print(f"{project.title}")
        print(f"  {format_summary(summarize(entries))}")
        print(f"  total: {naturalsize(sum(e.size or 0 for e in entries))}")
        if project.multisite:
            names = [s.name for s in project.select_sites(sites)
                     if s.name and not s.derivative]
            print(f"  sites ({len(names)}): {', '.join(names)}")

    return cmd
