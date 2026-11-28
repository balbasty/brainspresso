"""
`bx <project>` for the curated INDI projects (`engine.Project`).

Each project is a subpackage (`indi/<project>/dataset.py`), whose
catalog entry is `project_dataset(<Project class>)`.

`INDIDataset` wraps a `Project` (composition: a project keeps its own
`sites` and `templates()`): harvest as BIDS with hybrid placement
(merged sites, or one dataset per site with `--layout sites`), roast
(the metadata, from `sourcedata/fcp-indi/`), compost, info.
"""
import copy
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter
from humanize import naturalsize

from brainspresso.bids.harvest import (
    check_selected_subjects,
    format_summary,
    summarize,
)
from brainspresso.bids.layout import ALL_LAYOUTS, SitesLayout
from brainspresso.bids.select import Selection
from brainspresso.datasets.base import Dataset
from brainspresso.datasets.indi.harvest import (
    FOLDER,
    collect,
    harvest_sites,
    site_selection,
)
from brainspresso.datasets.options import OptionGroup, Panel, option_group
from brainspresso.errors import UsageError
from brainspresso.roast.hosted import harvest_hosted


@option_group("INDI", Panel.COLLECTION)
class InfoOptions(OptionGroup):
    """`bx <project> info --derivatives`."""

    derivatives: Annotated[bool, Parameter(
        help="Also list derivatives"
    )] = False


class INDIDataset(Dataset):
    """
    A curated INDI project.

    Parameters
    ----------
    project : engine.Project
        The project
    """

    keys = None
    sessions = True
    has_sites = True
    hosted = True
    layouts = ALL_LAYOUTS
    harvest_jobs = 8
    option_defaults = {"harvest": {"insecure": False}}
    folder_choices = ("derivatives", "sourcedata")
    commands = ("harvest", "roast", "compost", "info")

    def __init__(self, project, **arguments):
        super().__init__(**arguments)
        self.project = project

    @property
    def name(self):
        """Command name of the project."""
        return self.project.name

    @property
    def folder(self):
        """Folder of the project."""
        return self.project.folder

    @property
    def title(self):
        """Title of the project."""
        return self.project.title

    @property
    def help(self):
        """Help of the project."""
        return self.project.help

    @property
    def indi_category(self):
        """INDI sharing category of the project (`Project.category`)."""
        return self.project.category

    @property
    def site_names(self):
        """Names of the sites (multi-site projects)."""
        return tuple(s.name for s in self.project.sites
                     if s.name and not s.derivative)

    def select_sites(self, values):
        """Sites by name or code (case-insensitive), as names."""
        values = [str(v) for v in values or ()]
        if not values:
            return ()
        try:
            chosen = self.project.select_sites(values)
        except ValueError as e:
            raise UsageError(str(e)) from e
        return tuple(s.name for s in chosen if not s.derivative)

    @property
    def template_dir(self):
        """Templates of the project (`Project.template_dir`)."""
        return self.project.template_dir

    def fill_templates(self):
        """Fill-in sidecar templates of the project (`_fill.json`)."""
        return self.project.fill_templates()

    def hosted_datasets(self, layout, args=None):
        """
        Datasets of a hosted roast: the merged one, or each site.

        Site datasets take the labels of the bucket; `--subs` also
        accepts merged labels (with the site code), as harvest does.
        """
        if not isinstance(layout, SitesLayout):
            return super().hosted_datasets(layout, args)
        selection = self.hosted_selection(args)
        names = set(self.chosen_sites(args)) if args is not None else set()
        out = []
        for name, child in sorted(layout.children.items()):
            if names and name not in names:
                continue
            if not Path(child.root).is_dir():
                continue
            site = next((s for s in self.project.sites if s.name == name),
                        None)
            local = selection
            if site is not None:
                expanded = copy.copy(selection)
                for attr in ("subs", "exclude_subs"):
                    labels = set(getattr(selection, attr))
                    setattr(expanded, attr, labels | {
                        site.label(x) for x in labels})
                local = site_selection(expanded, site)
            out.append((child, name, local))
        return out

    def options(self, command):
        """`info --derivatives`; `roast --repair` (hosted)."""
        if command == "info":
            return {"info": InfoOptions}
        return super().options(command)

    def groups(self, command):
        """`info` takes `--sites`."""
        if command == "info":
            return ("sites", "log")
        return super().groups(command)

    def describe(self, command):
        """Help of harvest."""
        if command == "harvest":
            return (
                "Download (a subset of) the dataset, as BIDS.\n\n"
                "Metadata (sidecars, tables, top-level files) that applies "
                "to the selected data files is always included. Files that "
                "the bucket lacks (merged participants table, curated "
                "description, missing sidecars) are generated. Images are "
                "placed directly; metadata is downloaded to "
                "`sourcedata/fcp-indi/` and roasted (repaired, filled in, "
                "trimmed). With "
                "`--layout study` or `flat`, sites are merged (labels made "
                "unique with a site code when needed, `site` column in "
                "participants.tsv); with `--layout sites`, one dataset per "
                "site in `<DATASET>/<site>/`, with the labels of the bucket "
                "(`--subs` also accepts them, e.g. `--sites ICBM --subs "
                "00448`), and the parent folder holds the README and the "
                "merged participants.tsv. Single-site projects treat "
                "`sites:<layout>` as `<layout>`."
            )
        if command == "roast":
            return (
                "Roast the metadata again, from `sourcedata/fcp-indi/` "
                "(repaired, filled in from the templates of the project, "
                "trimmed): after deleting or editing roasted files, or to "
                "change `--sidecars`."
            )
        if command == "info":
            return "Show the content of the dataset (lists the bucket)."
        return super().describe(command)

    def layout(self, root, layout=None):
        """Single-site projects: `sites[:<kind>]` is `<kind>`."""
        if layout and layout.startswith("sites") and \
                not self.project.multisite:
            layout = layout.partition(":")[2] or None
        return super().layout(root, layout)

    def harvest(self, layout, args):
        """Harvest from s3://fcp-indi (merged, or one dataset per site)."""
        f, h = args.filters, args.harvest
        sessions = {s[4:] if s.startswith("ses-") else s
                    for s in args.sessions.sessions}
        selection = Selection(
            subs=self.labels(args.select.subs),
            exclude_subs=self.labels(args.select.exclude_subs),
            sessions=sessions, folders=list(args.hosted.folders),
            **f.filters(),
        )
        sites = self.chosen_sites(args)
        provenance = {"source": "s3://fcp-indi", "dataset": self.name,
                      "sites": sorted(sites)}
        options = dict(
            trim=args.hosted.trim, if_exists=h.if_exists, jobs=h.jobs,
            dry_run=h.dry_run, listing=args.hosted.list,
            insecure=bool(h.insecure),
            chunk_size=h.runtime().chunk_size,
            sidecars=args.hosted.sidecars, repair=args.hosted.repair,
        )
        fill = self.fill_templates()
        if isinstance(layout, SitesLayout):
            harvest_sites(self.project, layout, selection, sites=sites,
                          where=f.where, provenance=provenance,
                          wait=args.lock_wait, fill=fill,
                          **options)
            if not h.dry_run:
                print(f"Site datasets in: {layout.root}")
            return
        asked = copy.copy(selection)
        entries, selection = collect(self.project, sites=sites,
                                     selection=selection, where=f.where)
        check_selected_subjects(asked, entries)
        harvest_hosted(entries, layout, selection, folder=FOLDER,
                       fill=fill, provenance=provenance,
                       check_subjects=False, **options)
        if not h.dry_run:
            print(f"BIDS root: {Path(layout.raw)}")

    def info(self, layout=None, args=None):
        """List the bucket folder of the project."""
        derivatives = bool(args and args.extra("info", "derivatives"))
        sites = self.chosen_sites(args) if args is not None else ()
        selection = Selection(folders=["derivatives"] if derivatives else [])
        entries, _ = collect(self.project, sites=sites, selection=selection)
        print(f"{self.title}")
        if self.indi_category:
            print(f"  INDI category: {self.indi_category}")
        print(f"  {format_summary(summarize(entries))}")
        print(f"  total: {naturalsize(sum(e.size or 0 for e in entries))}")
        if self.project.multisite:
            names = [s.name for s in self.project.select_sites(sites)
                     if s.name and not s.derivative]
            print(f"  sites ({len(names)}): {', '.join(names)}")


def project_dataset(project: type):
    """
    The catalog entry of a project: `f(name) -> INDIDataset`.

    Parameters
    ----------
    project : type[engine.Project]
        The class of the project

    Returns
    -------
    callable
        `f(name)`, the dataset of the project (`project` attribute: the
        class, read by `scripts/make_catalog.py`)
    """
    def load(name: str) -> INDIDataset:
        return INDIDataset(project())

    load.project = project
    return load
