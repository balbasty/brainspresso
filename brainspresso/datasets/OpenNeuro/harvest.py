import asyncio
import functools
import sys
from pathlib import Path
from typing import Annotated, Iterable, Literal

from cyclopts import Parameter

from humanize import naturalsize

from brainspresso.bids.harvest import format_summary, summarize
from brainspresso.bids.layout import LayoutChoice, resolve_layout
from brainspresso.bids.select import Selection
from brainspresso.download import IfExists
from brainspresso.sources.base import DownloadOptions
from brainspresso.sources.credentials import CredentialsError
from brainspresso.sources.openneuro import (
    OpenNeuro, OpenNeuroAPI, OpenNeuroError, APIUnavailable, list_snapshot,
)
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.log import setup_filelog
from brainspresso.datasets.OpenNeuro.command import openneuro

SourceChoice = Literal["auto", "graphql", "s3"]
FolderChoice = Literal["derivatives", "sourcedata", "code", "stimuli"]
# Options that accept several values after a single flag (`--subs 01 02`)
Many = Parameter(consume_multiple=True)


def user_errors(func):
    """Print user-facing errors without a traceback"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (OpenNeuroError, KeyError, ValueError, PermissionError,
                CredentialsError) as e:
            message = e.args[0] if isinstance(e, KeyError) else e
            print(f"Error: {message}", file=sys.stderr)
            sys.exit(1)
    return wrapper


@openneuro.command(name="harvest")
@user_errors
def download(
    dataset: str,
    path: str | None = None,
    *,
    tag: str | None = None,
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
    layout: LayoutChoice | None = None,
    out: str | None = None,
    source: SourceChoice = "auto",
    if_exists: IfExists.Choice = "skip",
    jobs: int = 8,
    dry_run: bool = False,
    list: bool = False,
    token: str | None = None,
    insecure: bool = False,
    log: str | None = None,
    level: str = "info",
):
    """
    Download (a subset of) an OpenNeuro dataset.

    Metadata (sidecars, events, scans/sessions tables, top-level
    files) that applies to the selected data files is always included,
    following the BIDS inheritance principle.

    Examples
    --------
    ```
    bx openneuro harvest ds000224 --datatypes anat --dry-run
    bx openneuro harvest ds000224 /data --subs MSC01 MSC02 --suffixes T1w
    bx openneuro harvest ds000224 --out /data/msc --include "sub-*/ses-s*/*"
    bx openneuro harvest ds000030 --where "diagnosis==CONTROL" "age>=30"
    ```

    Parameters
    ----------
    dataset
        Accession number (e.g. ds000224)
    path
        Path to root of all datasets. An `OpenNeuro/<dataset>` folder
        will be created. Ignored if `--out` is used.
    tag
        Snapshot tag (default: latest)
    subs
        Only download these subjects (labels, with or without `sub-`)
    exclude_subs
        Do not download these subjects
    sessions
        Only download these sessions
    datatypes
        Only download these datatypes (anat, func, dwi, fmap, perf...)
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
        Also download these optional top-level folders
    trim
        Rewrite participants/sessions/scans tables so that they only
        list what is present locally, so that they describe the
        subset. Originals are kept in `.brainspresso/orig/`.
    layout
        Local layout: "study" (the OpenNeuro dataset goes to
        `<dataset>/rawbids/`, its `sourcedata/` and `derivatives/` next
        to it) or "flat" (the OpenNeuro dataset is `<dataset>/`).
        Default: the layout of an existing dataset, else "study".
    out
        Output dataset folder (overrides `path`)
    source
        Listing route. "auto" uses the OpenNeuro API and falls back
        to the public S3 bucket (latest snapshot only).
    if_exists
        Behaviour if a file already exists
    jobs
        Number of concurrent downloads
    dry_run
        Print the selected files and their total size, then exit
    list
        Print the selected files before downloading
    token
        OpenNeuro API key (only needed for private datasets)
    insecure
        Do not verify TLS certificates
    log
        Path to log file
    level
        Logging level
    """
    setup_filelog(log, level=level)
    remote = OpenNeuro(dataset, tag, source, token=token)
    snapshot = remote.snapshot()
    print(
        f"{dataset} snapshot {snapshot.tag or '(unknown tag)'} "
        f"[listed from {snapshot.source}]"
    )
    root = Path(out) if out else (
        get_tree_path(path) / "OpenNeuro" / dataset
    )
    local = resolve_layout(root, layout)
    selection = Selection(
        subs=subs,
        exclude_subs=exclude_subs,
        sessions=sessions,
        datatypes=datatypes,
        suffixes=suffixes,
        tasks=tasks,
        include=include,
        exclude=exclude,
        folders=folders,
    )
    remote.harvest(
        local,
        selection,
        options=DownloadOptions(ifexists=if_exists, jobs=jobs,
                                dry_run=dry_run, insecure=insecure),
        where=where,
        trim=trim,
        listing=list,
    )
    if not dry_run:
        print(f"BIDS root: {local.raw}")


@openneuro.command(name="info")
@user_errors
def info(
    dataset: str,
    *,
    source: SourceChoice = "auto",
    token: str | None = None,
):
    """
    Show the snapshots and content of an OpenNeuro dataset.

    Parameters
    ----------
    dataset
        Accession number (e.g. ds000224)
    source
        Listing route ("auto", "graphql" or "s3")
    token
        OpenNeuro API key (only needed for private datasets)
    """
    api = OpenNeuroAPI(token=token)
    if source != "s3":
        try:
            snapshots = asyncio.run(api.snapshots(dataset))
            print("Snapshots:")
            for snap in snapshots:
                print(f"  {snap['tag']:>10}  {snap.get('created', '')}")
        except APIUnavailable as e:
            if source == "graphql":
                raise
            print(f"Snapshots: unavailable ({e})")
    snapshot = asyncio.run(list_snapshot(dataset, None, source, api=api))
    summary = summarize(snapshot.files)
    print(
        f"Latest snapshot {snapshot.tag or '(unknown tag)'} "
        f"[listed from {snapshot.source}]:"
    )
    print(f"  {format_summary(summary)}")
    tops = sorted({f.path.split('/')[0] + ('/' if '/' in f.path else '')
                   for f in snapshot.files
                   if not f.path.startswith('sub-')})
    print(f"  top level: {', '.join(tops)}")
    print(f"  total: {naturalsize(snapshot.size)}")
