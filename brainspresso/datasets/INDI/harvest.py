"""
Harvest of the curated INDI projects (`bx <project> harvest`), as BIDS.

`INDIDataset` (`dataset.py`) calls `collect` (one merged dataset) or
`harvest_sites` (one dataset per site).
"""
from __future__ import annotations

import asyncio
import contextlib
import copy
from typing import Iterable


from brainspresso.bids.harvest import (
    InlineEntry, check_selected_subjects, harvest, record_run,
    resolve_participants, subjects_of, write_inline,
)
from brainspresso.actions.runlock import run_lock
from brainspresso.bids.layout import SitesLayout, relative_url
from brainspresso.bids.tables import read_tsv, write_tsv
from brainspresso.bids.select import Selection
from brainspresso.datasets.INDI.engine import (
    Project, Site, complete_participants, fetch_tables, list_extra,
    list_sites, site_label,
)
from brainspresso.errors import UsageError
from brainspresso.sources import fcp



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
            raise UsageError(
                f"--where is not available for {project.name} "
                f"(no participants table)"
            )
        selection = resolve_participants(
            [InlineEntry("participants.tsv", participants)], selection, where
        )
    items = asyncio.run(list_sites(
        project, chosen, selection=selection,
        derivatives="derivatives" in selection.folders,
    ))
    return items, tables, selection, chosen


def extra_entries(project: Project) -> list:
    return [
        obj.entry(fcp.get_bucket().url(obj.key), path)
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
    project: Project, layout: SitesLayout, selection: Selection, *,
    sites=(), where=(), provenance=None, wait: float = 0, **options,
) -> None:
    """Layout "sites": one dataset per site in `<DS>/<site>/`"""
    asked = copy.copy(selection or Selection())
    found = set()
    items, tables, selection, chosen = prepare(
        project, sites=sites, selection=selection, where=where,
        site_labels=True,
    )
    dry_run = options.get("dry_run", False)
    # Plan every site first: a selection that matches nothing is refused
    # before anything is written
    plan = []
    for site in chosen:
        if site.derivative:
            continue
        known = site.name in layout.children
        child = layout.child(site.name)
        entries = project.site_entries(
            items, tables, site, parent=relative_url(layout.root, child.raw),
        )
        local = site_selection(selection, site)
        for label in subjects_of(e.path for e in entries):
            found |= {label, site.label(label)}
        if not local.select(e.path for e in entries if "/" in e.path):
            if not known:               # nothing harvested: not a site
                del layout.children[site.name]
            continue
        plan.append((site, child, entries, local))
    check_selected_subjects(asked, (), found)
    if not dry_run:
        # before any download takes a lock (`actions.locks`)
        layout.ensure_state()
    harvested = []
    with contextlib.ExitStack() as stack:
        for site, child, entries, local in plan:
            print(f"--- {site.name}")
            if not dry_run:
                # a new site holds its own run lock too (a compost of the
                # site alone, `--out <DS>/<site>`, must wait for it)
                child.ensure_state()
                stack.enter_context(run_lock(child.state, "harvest",
                                             wait=wait))
            harvest(entries, child, local, repair=True,
                    provenance={**(provenance or {}), "site": site.name},
                    check_subjects=False, **options)
            harvested.append(site.name)
    extra = extra_entries(project)
    parent = Selection(folders=list(selection.folders))
    recorded = False
    if parent.select(e.path for e in extra):
        print("--- (dataset)")
        recorded = bool(harvest(extra, layout, parent, provenance=provenance,
                                check_subjects=False, **options))
    if dry_run:
        return
    # Parent folder: README and merged participants (trimmed)
    for entry in project.parent_files():
        write_inline(entry, layout.raw / entry.path, "overwrite")
    merged = project.participants(tables)
    if merged is not None:
        present = {
            f"sub-{s.label(p.name[4:])}": s.name
            for s in chosen if s.name in layout.children
            for p in layout.child(s.name).raw.glob("sub-*") if p.is_dir()
        }
        merged = complete_participants(merged, present, present)
        header, rows = read_tsv(merged)
        write_tsv(layout.raw / "participants.tsv", header,
                  [r for r in rows if r["participant_id"] in present])
    if not recorded:
        record_run(layout, "harvest", {**(provenance or {}),
                                       "harvested_sites": harvested})
