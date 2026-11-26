"""
`bx <project> harvest` and `bx <project> info` for curated INDI projects.
"""
from __future__ import annotations

import asyncio
import functools
import sys
from pathlib import Path
from typing import Annotated, Iterable, Literal

from cyclopts import App, Parameter
from humanize import naturalsize

from brainspresso.bids.harvest import (
    InlineEntry, format_summary, harvest, resolve_participants, summarize,
)
from brainspresso.bids.select import Selection
from brainspresso.cli import app
from brainspresso.download import IfExists
from brainspresso.utils.log import setup_filelog
from brainspresso.utils.path import get_tree_path
from brainspresso.datasets.INDI.engine import (
    Project, fetch_tables, list_extra, list_sites,
)
from brainspresso.sources import fcp
from brainspresso.sources.openneuro import RemoteEntry

FolderChoice = Literal["derivatives", "sourcedata"]
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


def collect(
    project: Project,
    *,
    sites: Iterable[str] = (),
    selection: Selection | None = None,
    where: Iterable[str] = (),
) -> tuple[list, Selection]:
    """
    List a project (pruned to the selection) and return its local
    listing (remote and generated entries) and the selection, with
    participant queries applied.
    """
    selection = selection or Selection()
    chosen = project.select_sites(sites)
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
    entries = project.entries(items, tables)
    for path, obj in asyncio.run(list_extra(project)):
        entries.append(RemoteEntry(
            path, fcp.get_bucket().url(obj.key), size=obj.size,
            mtime=obj.mtime,
            digests={"md5": obj.md5} if obj.md5 else {},
        ))
    return entries, selection


def register(project: Project) -> App:
    """Register `bx <project.name> {harvest,info}`"""
    cmd = App(name=project.name, help=project.help)
    app.command(cmd)

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
            Only download these subjects (labels, with or without `sub-`)
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
        entries, selection = collect(
            project, sites=sites, selection=selection, where=where,
        )
        root = Path(out) if out else get_tree_path(path) / project.folder
        harvest(
            entries, root, selection,
            trim=trim, if_exists=if_exists, jobs=jobs,
            dry_run=dry_run, listing=list, repair=True,
            provenance={
                "source": "s3://fcp-indi",
                "dataset": project.name,
                "sites": sorted(sites or []),
            },
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
