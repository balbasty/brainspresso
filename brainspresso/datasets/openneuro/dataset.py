"""`bx openneuro <accession>`: any OpenNeuro dataset (BIDS)."""
import asyncio
from pathlib import Path
from typing import Annotated, Literal

from cyclopts import Parameter
from humanize import naturalsize

from brainspresso.bids.harvest import format_summary, summarize
from brainspresso.bids.select import Selection
from brainspresso.datasets.base import Argument, Dataset
from brainspresso.datasets.options import OptionGroup, Panel, option_group
from brainspresso.sources.openneuro import (
    APIUnavailable,
    OpenNeuro,
    OpenNeuroAPI,
    list_snapshot,
)

HELP = """
Any OpenNeuro dataset (BIDS)

* **Host**          https://openneuro.org (GraphQL API) and s3://openneuro.org
* **Format**        Already BIDS: harvest downloads a filtered subset
                    (images in place, metadata in `sourcedata/openneuro/`,
                    then roasted; `roast` re-roasts the metadata)
* **License**       Per dataset (mostly CC0)
"""  # noqa: E501


@option_group("OpenNeuro", Panel.OWN)
class OpenNeuroOptions(OptionGroup):
    """`--tag`, `--source` (listing route)."""

    tag: Annotated[str | None, Parameter(
        help="Snapshot tag (default: the latest snapshot)"
    )] = None
    route: Annotated[Literal["auto", "graphql", "s3"], Parameter(
        name="--source",
        help="Listing route: the GraphQL API, the S3 bucket, or the API "
        "with the bucket as a fallback (auto)",
    )] = "auto"


@option_group("OpenNeuro", Panel.OWN)
class RouteOptions(OptionGroup):
    """`--source` (listing route) of `info`."""

    route: Annotated[Literal["auto", "graphql", "s3"], Parameter(
        name="--source", help="Listing route (auto, graphql or s3)",
    )] = "auto"


class OpenNeuroDataset(Dataset):
    """Any OpenNeuro dataset, named by its accession number."""

    name = "openneuro"
    folder = "OpenNeuro/{dataset}"
    title = "an OpenNeuro dataset"
    help = HELP
    arguments = (Argument("dataset", str,
                          "Accession number (e.g. ds000224)"),)
    sessions = True
    hosted = True
    credentials = "token"
    harvest_jobs = 8
    option_defaults = {"harvest": {"insecure": False}}
    # (no `sources()`: `--token` is read by the OpenNeuro API)
    panels = {"token": ("OpenNeuro", Panel.OWN)}
    folder_choices = ("derivatives", "sourcedata", "code", "stimuli")
    commands = ("harvest", "roast", "compost", "info")

    def options(self, command):
        """`--tag`, `--source` (harvest); `--source` (info)."""
        if command == "harvest":
            return {"openneuro": OpenNeuroOptions}
        if command == "info":
            return {"openneuro": RouteOptions}
        return super().options(command)

    def groups(self, command):
        """`info` takes `--token`."""
        if command == "info":
            return ("token", "log")
        return super().groups(command)

    def describe(self, command):
        """Help of the commands."""
        if command == "harvest":
            return (
                "Download (a subset of) an OpenNeuro dataset, as BIDS.\n\n"
                "Metadata that applies to the selected data files is "
                "always included. The dataset is saved in "
                "`<path>/OpenNeuro/<dataset>/` (or `--out`). Images are "
                "placed directly; metadata (sidecars, tables, README...) "
                "is downloaded to `sourcedata/openneuro/` and roasted into "
                "the dataset (tables and field maps trimmed to the subset)."
            )
        if command == "roast":
            return (
                "Roast the metadata again, from `sourcedata/openneuro/` "
                "(after deleting or editing roasted files, or to change "
                "`--sidecars`)."
            )
        if command == "info":
            return "Show the snapshots and content of an OpenNeuro dataset."
        return super().describe(command)

    def harvest(self, layout, args):
        """List the snapshot and download the selection."""
        on, f = args.extra("openneuro"), args.filters
        remote = OpenNeuro(self.bound["dataset"], on.tag, on.route,
                           token=args.token.token)
        snapshot = remote.snapshot()
        print(f"{self.bound['dataset']} snapshot "
              f"{snapshot.tag or '(unknown tag)'} "
              f"[listed from {snapshot.source}]")
        sessions = {s[4:] if s.startswith("ses-") else s
                    for s in args.sessions.sessions}
        selection = Selection(
            subs=self.labels(args.select.subs),
            exclude_subs=self.labels(args.select.exclude_subs),
            sessions=sessions, folders=list(args.hosted.folders),
            **f.filters(),
        )
        options = args.harvest.runtime()
        remote.harvest(layout, selection, options=options, where=f.where,
                       trim=args.hosted.trim, listing=args.hosted.list,
                       sidecars=args.hosted.sidecars,
                       repair=args.hosted.repair,
                       fill=self.fill_templates())
        if not options.dry_run:
            print(f"BIDS root: {Path(layout.raw)}")

    def info(self, layout=None, args=None):
        """Snapshots and content of the latest snapshot."""
        dataset = self.bound["dataset"]
        route = args.extra("openneuro", "route", "auto") if args else "auto"
        api = OpenNeuroAPI(token=args.token.token if args else None)
        if route != "s3":
            try:
                snapshots = asyncio.run(api.snapshots(dataset))
                print("Snapshots:")
                for snap in snapshots:
                    print(f"  {snap['tag']:>10}  {snap.get('created', '')}")
            except APIUnavailable as e:
                if route == "graphql":
                    raise
                print(f"Snapshots: unavailable ({e})")
        snapshot = asyncio.run(list_snapshot(dataset, None, route, api=api))
        summary = summarize(snapshot.files)
        print(f"Latest snapshot {snapshot.tag or '(unknown tag)'} "
              f"[listed from {snapshot.source}]:")
        print(f"  {format_summary(summary)}")
        tops = sorted({f.path.split('/')[0] + ('/' if '/' in f.path else '')
                       for f in snapshot.files
                       if not f.path.startswith('sub-')})
        print(f"  top level: {', '.join(tops)}")
        print(f"  total: {naturalsize(snapshot.size)}")
